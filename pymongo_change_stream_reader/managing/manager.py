import logging
import time
from json import loads, dumps
from multiprocessing import Queue
from queue import Empty

from pydantic import BaseModel

from pymongo_change_stream_reader.change_stream_reading import (
    build_change_stream_reader_process
)
from pymongo_change_stream_reader.committing import build_commit_process
from pymongo_change_stream_reader.messages import (
    deserialize_message,
    serialize_message,
    Status,
    ChangeStatus,
)
from pymongo_change_stream_reader.producing import build_producer_process
from pymongo_change_stream_reader.settings import (
    Settings,
    NewTopicConfiguration,
    Pipeline,
)
from pymongo_change_stream_reader.utils import TaskIdGenerator, ProcessData, Statuses, \
    ChangeStatuses

default_logger = logging.Logger(__name__, logging.INFO)


class BaseError(Exception):
    ...


class StartTimeoutError(BaseError):
    ...


class StopError(BaseError):
    def __init__(self, task_id):
        self.task_id = task_id


class Manager:
    def __init__(
        self,
        settings: Settings,
        pipeline: Pipeline,
        new_topic_configuration: NewTopicConfiguration,
        logger: logging.Logger = default_logger,
    ):
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

        self._program_started_at = time.monotonic()
        self._program_start_timeout = self._settings.program_start_timeout

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
                task_id_generator=self._task_id_generator,
                producer_queue=self._producer_queues[i],
                request_queue=request_queue,
                response_queue=self._response_queue,
                committer_queue=self._commit_queue,
                new_topic_configuration=self._new_topic_configuration,
                settings=self._settings,
            )
            self._request_queues[process_data.task_id] = request_queue
            self._processes[process_data.task_id] = process_data
            result.add(process_data.task_id)
        return result

    def _build_change_log_reader(self) -> int:
        request_queue = Queue(maxsize=1000)
        process_data = build_change_stream_reader_process(
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
            task_id_generator=self._task_id_generator,
            request_queue=request_queue,
            response_queue=self._response_queue,
            committer_queue=self._commit_queue,
            settings=self._settings,
        )
        self._request_queues[process_data.task_id] = request_queue
        self._processes[process_data.task_id] = process_data
        return process_data.task_id

    def start(self):
        self._program_started_at = time.monotonic()
        self._should_run = True

        for process_data in self._processes.values():
            process_data.process.start()

        expected_responses_from_tasks = set()
        expected_responses_from_tasks.update(self._producers_task_id)
        expected_responses_from_tasks.add(self._change_log_reader_task_id)
        expected_responses_from_tasks.add(self._committer_task_id)

        try:
            while expected_responses_from_tasks and self._should_run:
                if self._is_start_timeout():
                    raise StartTimeoutError()
                try:
                    result = self._response_queue.get(
                        timeout=self._settings.queue_get_timeout
                    )
                except Empty:
                    continue
                if result:
                    message = self._decode_message(result)
                    if isinstance(message, Status):
                        if message.status in (Statuses.started, Statuses.running):
                            expected_responses_from_tasks.discard(message.task_id)
                        elif message.status == Statuses.stopped:
                            raise StopError(message.task_id)
        except BaseException as ex:
            self._logger.error(
                f"Error when start in {self.worker_name}: {ex}",
                stack_info=True
            )
            self.stop()
        else:
            self.run()

    def run(self):
        for task_id, request_queue in self._request_queues.items():
            new_status = ChangeStatus(task_id=task_id, status=ChangeStatuses.running)
            request_queue.put(self._encode_message(new_status))

        self.run_monitoring()

    def run_monitoring(self):
        try:
            while self._should_run:
                try:
                    result = self._response_queue.get(
                        timeout=self._settings.queue_get_timeout
                    )
                except Empty:
                    continue
                if result:
                    message = self._decode_message(result)
                    if isinstance(message, Status) and message.status == Statuses.stopped:
                        raise StopError(message.task_id)

                for task_id, process_data in self._processes.items():
                    process = process_data.process
                    if not process.is_alive():
                        raise StopError(task_id)
        except BaseException as ex:
            self._logger.error(
                f"Error when run in {self.worker_name}: {ex}",
                stack_info=True
            )
        self.stop()

    def stop(self):
        self._should_run = False

        for task_id, process_data in self._processes.items():
            process_data.process.terminate()

        for task_id, process_data in self._processes.items():
            process_data.process.join()

        for queue in self._all_queues():
            while True:
                try:
                    queue.get_nowait()
                except Empty:
                    break

    @property
    def worker_name(self):
        return f"{type(self).__name__}"

    @staticmethod
    def _encode_message(message: BaseModel):
        message_bytes = dumps(serialize_message(message)).encode()
        return message_bytes

    @staticmethod
    def _decode_message(message: bytes):
        message = deserialize_message(loads(message.decode()))
        return message

    def _is_start_timeout(self) -> bool:
        should_not_run = (
            time.monotonic() - self._program_started_at >
            self._program_start_timeout
        )
        return should_not_run