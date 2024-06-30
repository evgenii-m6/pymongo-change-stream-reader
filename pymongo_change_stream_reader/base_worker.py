import logging
import signal
from abc import ABC, abstractmethod
from json import dumps, loads
from multiprocessing import Queue
from queue import Empty
import time

from pydantic import BaseModel

from .messages import Status, serialize_message, deserialize_message, ChangeStatus
from .utils import Statuses, ChangeStatuses


class BaseWorker(ABC):
    def __init__(
        self,
        task_id: int,
        stream_reader_name: str,
        request_queue: Queue,
        response_queue: Queue,
        logger: logging.Logger,
        queue_get_timeout: int,
        queue_put_timeout: int,
        program_start_timeout: int,
    ):
        self._task_id = task_id
        self._stream_reader_name = stream_reader_name
        self._request_queue = request_queue
        self._response_queue = response_queue
        self._queue_put_timeout: int = queue_put_timeout
        self._queue_get_timeout: int = queue_get_timeout
        self._program_start_timeout: int = program_start_timeout
        self._logger = logger
        self._program_started_at = time.monotonic()

        self._should_run = False

        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        self._should_run = False

    @abstractmethod
    def _start(self):
        raise NotImplementedError

    @abstractmethod
    def _stop(self):
        raise NotImplementedError

    @abstractmethod
    def _task(self):
        raise NotImplementedError

    @property
    def worker_name(self):
        return f"{type(self).__name__}(task_id={self._task_id})"

    def start(self):
        self._program_started_at = time.monotonic()
        self._logger.info(f"Start {self.worker_name}")
        try:
            self._start()
        except BaseException as ex:
            self._logger.error(
                f"Error when start {self.worker_name}: {ex}",
                stack_info=True
            )
            self.stop()
        else:
            self._should_run = True
            self._send_status(Statuses.started)
            self._logger.info(f"{self.worker_name} started")

    def stop(self):
        self._logger.info(f"Stop {self.worker_name}")
        self._should_run = False
        self._stop()
        self._send_status(Statuses.stopped)
        self._logger.info(f"{self.worker_name} stopped")

    def _send_status(self, status: Statuses):
        message = Status(task_id=self._task_id, status=status)
        message_bytes = self._encode_message(message)
        self._response_queue.put(message_bytes)

    @staticmethod
    def _encode_message(message: BaseModel):
        message_bytes = dumps(serialize_message(message)).encode()
        return message_bytes

    @staticmethod
    def _decode_message(message: bytes):
        message = deserialize_message(loads(message.decode()))
        return message

    def task(self):
        self._logger.info(f"{self.worker_name} task started")
        try:
            self._task()
            self._logger.warning(f"Normal exit from {self.worker_name}")
        except BaseException as ex:
            self._logger.error(
                f"Error in {self.worker_name}: {ex}",
                stack_info=True
            )
        finally:
            self._logger.warning(f"Call stop on {self.worker_name}")
            self.stop()

    def run_task(self):
        is_ready_to_run = False
        while not is_ready_to_run and self._is_start_timeout() and self._should_run:
            try:
                message_bytes = self._request_queue.get(timeout=self._queue_get_timeout)
                if self._is_start_timeout():
                    self.stop()
                if message_bytes:
                    message = self._encode_message(message_bytes)
                    if isinstance(message, ChangeStatus):
                        if message.status == ChangeStatuses.running:
                            self._should_run = True
                            is_ready_to_run = True
                        elif message.status == ChangeStatuses.stopped:
                            self.stop()
            except Empty:
                continue
            except BaseException as ex:
                self._logger.error(
                    f"Error in {self.worker_name}: {ex}",
                    stack_info=True
                )
                self.stop()

        if self._should_run:
            self.task()

    def _is_start_timeout(self) -> bool:
        should_not_run = (
            time.monotonic() - self._program_started_at >
            self._program_start_timeout
        )
        return should_not_run
