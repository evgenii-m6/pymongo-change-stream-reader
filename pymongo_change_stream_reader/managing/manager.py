import logging
import os
import signal
import time
from multiprocessing import Queue
from queue import Empty

import psutil

from pymongo_change_stream_reader.change_stream_reading import (
    build_change_stream_reader_process
)
from pymongo_change_stream_reader.committing import build_commit_process
from pymongo_change_stream_reader.exceptions import (
    WaitTimeoutError,
    UnexpectedMessageType,
    UnexpectedStatusReceived,
    SubprocessStoppedUnexpectedly
)
from pymongo_change_stream_reader.messages import (
    Status,
    ChangeStatus, decode_message, encode_message,
)
from pymongo_change_stream_reader.models import ProcessData, Statuses, ChangeStatuses
from pymongo_change_stream_reader.producing import build_producer_process
from pymongo_change_stream_reader.settings import (
    Settings,
    NewTopicConfiguration,
    Pipeline,
)
from pymongo_change_stream_reader.utils import TaskIdGenerator


default_logger = logging.Logger(__name__, logging.INFO)


class Manager:
    def __init__(
        self,
        settings: Settings,
        pipeline: Pipeline,
        new_topic_configuration: NewTopicConfiguration,
        logger: logging.Logger = default_logger,
    ):
        self._pid = os.getpid()
        self._current_process = psutil.Process(self._pid)
        self._manager_create_time = self._current_process.create_time()
        self._settings = settings
        self._pipeline = pipeline
        self._new_topic_configuration = new_topic_configuration
        self._logger = logger

        self._task_id_generator = TaskIdGenerator()

        self._producer_queues: dict[int, Queue] = {
            i: Queue(maxsize=int(settings.max_queue_size/settings.producers_count))
            for i in range(settings.producers_count)
        }
        self._commit_queue = Queue(maxsize=settings.max_queue_size)
        self._response_queue = Queue(maxsize=(settings.producers_count + 2)*100)
        self._request_queues: dict[int, Queue] = {}  # dict[task_id: Queue]

        self._processes: dict[int, ProcessData] = {}  # dict[task_id: ProcessData]

        self._producers_task_id: set[int] = self._build_producers()
        self._change_log_reader_task_id: int = self._build_change_log_reader()
        self._committer_task_id: int = self._build_committer()

        self._program_start_timeout = self._settings.program_start_timeout

        self._should_run = False

        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        self._should_run = False

    def _all_queues(self) -> list[Queue]:
        queue = list()
        queue.extend(list(self._producer_queues.values()))
        queue.extend(list(self._request_queues.values()))
        queue.extend([self._commit_queue, self._response_queue])
        return queue

    def _build_producers(self) -> set[int]:
        result = set()
        for i in range(self._settings.producers_count):
            request_queue = Queue(maxsize=1000)
            process_data = build_producer_process(
                manager_pid=self._pid,
                manager_create_time=self._manager_create_time,
                task_id_generator=self._task_id_generator,
                producer_queue=self._producer_queues[i],
                request_queue=request_queue,
                response_queue=self._response_queue,
                committer_queue=self._commit_queue,
                new_topic_configuration=self._new_topic_configuration,
                settings=self._settings,
                kafka_producer_config=self._settings.kafka_producer_config_dict,
            )
            self._request_queues[process_data.task_id] = request_queue
            self._processes[process_data.task_id] = process_data
            result.add(process_data.task_id)
        return result

    def _build_change_log_reader(self) -> int:
        request_queue = Queue(maxsize=1000)
        process_data = build_change_stream_reader_process(
            manager_pid=self._pid,
            manager_create_time=self._manager_create_time,
            task_id_generator=self._task_id_generator,
            producer_queues=self._producer_queues,
            request_queue=request_queue,
            response_queue=self._response_queue,
            committer_queue=self._commit_queue,
            pipeline=self._pipeline.pipeline,
            settings=self._settings,
        )
        self._request_queues[process_data.task_id] = request_queue
        self._processes[process_data.task_id] = process_data
        return process_data.task_id

    def _build_committer(self) -> int:
        request_queue = Queue(maxsize=1000)
        process_data = build_commit_process(
            manager_pid=self._pid,
            manager_create_time=self._manager_create_time,
            task_id_generator=self._task_id_generator,
            request_queue=request_queue,
            response_queue=self._response_queue,
            committer_queue=self._commit_queue,
            settings=self._settings,
        )
        self._request_queues[process_data.task_id] = request_queue
        self._processes[process_data.task_id] = process_data
        return process_data.task_id

    def _run(self):
        self._logger.info(f"Manager started")
        self._should_run = True

        for process_data in self._processes.values():
            process_data.process.start()

        self._logger.info(f"Subprocesses initialized")

        self._wait_for_subprocess_status(
            status=Statuses.starting,
            timeout=self._settings.program_start_timeout
        )

        self._send_command_change_statuses(ChangeStatuses.started)

        self._wait_for_subprocess_status(
            status=Statuses.started,
            timeout=self._settings.program_start_timeout
        )

        self._send_command_change_statuses(ChangeStatuses.running)

        self._wait_for_subprocess_status(
            status=Statuses.running,
            timeout=self._settings.program_start_timeout
        )

        self._run_monitoring()

    def run(self):
        try:
            self._run()
        except BaseException as ex:
            self._logger.error(
                f"Error in {self.worker_name}: {ex}",
                stack_info=True
            )
        finally:
            try:
                self.stop()
            except BaseException as err:
                self._logger.error(
                    f"Error when EXIT in {self.worker_name}: {err}",
                    stack_info=True
                )
                raise err

    def _wait_for_subprocess_status(self, status: Statuses, timeout: float):
        expected_responses_from_tasks = set()
        expected_responses_from_tasks.update(self._producers_task_id)
        expected_responses_from_tasks.add(self._change_log_reader_task_id)
        expected_responses_from_tasks.add(self._committer_task_id)

        start_time = time.monotonic()
        while expected_responses_from_tasks and self._should_run:
            if time.monotonic() - start_time > timeout:
                raise WaitTimeoutError()
            try:
                message_bytes = self._response_queue.get(
                    timeout=self._settings.queue_get_timeout
                )
            except Empty:
                continue
            if message_bytes:
                received_status = self._get_status(message_bytes)
                if received_status != status:
                    raise UnexpectedStatusReceived(
                        received=received_status, expected=status
                    )

    def _get_status(self, data: bytes) -> Statuses:
        message = decode_message(data)

        if not isinstance(message, Status):
            raise UnexpectedMessageType(message_type=ChangeStatus, received=message)

        return message.status

    def _send_command_change_statuses(self, status: ChangeStatuses):
        for task_id, queue in self._request_queues.items():
            message = ChangeStatus(task_id=task_id, status=status)
            message_bytes = encode_message(message)
            queue.put(message_bytes)

    def _run_monitoring(self):
        while self._should_run:
            for task_id, process_data in self._processes.items():
                process = process_data.process
                if not process.is_alive():
                    raise SubprocessStoppedUnexpectedly(task_id)

    def stop(self):
        self._should_run = False

        for task_id, process_data in self._processes.items():
            process_data.process.terminate()

        stop_started = time.monotonic()
        for task_id, process_data in self._processes.items():
            process_data.process.join(
                timeout=self._settings.program_graceful_stop_timeout
            )
        left_timeout = self._settings.program_graceful_stop_timeout - (
                time.monotonic() - stop_started
        )
        if left_timeout <= 0:
            for task_id, process_data in self._processes.items():
                process_data.process.kill()

        for task_id, process_data in self._processes.items():
            process_data.process.close()

        for queue in self._all_queues():
            while True:
                try:
                    queue.get_nowait()
                except Empty:
                    break

    @property
    def worker_name(self):
        return f"{type(self).__name__}"
