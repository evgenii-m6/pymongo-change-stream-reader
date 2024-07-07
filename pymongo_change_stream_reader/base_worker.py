import logging
import signal
from abc import ABC
from multiprocessing import Queue

import psutil

from .base_application import BaseApplication
from .exceptions import IncorrectManagerProcess, IncorrectReceiver, \
    UnexpectedMessageType, UnexpectedStatusChangeReceived, StopSubprocess
from .messages import (
    Status,
    ChangeStatus,
    encode_message,
    decode_message,
)
from .models import Statuses, ChangeStatuses

default_logger = logging.Logger(__name__, logging.INFO)


class BaseWorker(ABC):
    def __init__(
        self,
        manager_pid: int,
        manager_create_time: float,
        task_id: int,
        request_queue: Queue,
        response_queue: Queue,
        application: BaseApplication,
        logger: logging.Logger = default_logger,
        queue_get_timeout: int = 1,
        queue_put_timeout: int = 10,
    ):
        self._manager_pid = manager_pid
        self._manager_create_time = manager_create_time
        self._manager_process = psutil.Process(self._manager_pid)
        self._task_id = task_id
        self._request_queue = request_queue
        self._response_queue = response_queue
        self._queue_put_timeout: int = queue_put_timeout
        self._queue_get_timeout: int = queue_get_timeout
        self._logger = logger
        self._application = application

        self._should_run = False

        signal.signal(signal.SIGINT, self._handle_interruption_signal)
        signal.signal(signal.SIGTERM, self._handle_interruption_signal)

    def _validate_manager(self) -> None:
        ident1 = (self._manager_pid, self._manager_create_time)
        ident2 = (self._manager_process.pid, self._manager_process.create_time())
        if not ident1 == ident2:
            raise IncorrectManagerProcess(
                pid=self._manager_pid,
                create_time=self._manager_create_time,
                process=self._manager_process
            )

    def _handle_interruption_signal(self, signum, frame):
        self._should_run = False
        self._application.exit_gracefully(signum, frame)

    @property
    def worker_name(self):
        return f"{type(self).__name__}(task_id={self._task_id})"

    def _start(self):
        self._logger.info(f"Start {self.worker_name}")
        self._application.start()
        self._logger.info(f"{self.worker_name} started")

    def _stop(self):
        self._logger.info(f"Stop {self.worker_name}")
        self._should_run = False
        self._application.stop()
        self._logger.info(f"{self.worker_name} stopped")

    def _task(self):
        self._application.task()

    def _send_status(self, status: Statuses):
        message = Status(task_id=self._task_id, status=status)
        message_bytes = encode_message(message)
        self._response_queue.put(message_bytes)

    def _get_change_status(self, data: bytes) -> ChangeStatuses:
        message = decode_message(data)

        if not isinstance(message, ChangeStatus):
            raise UnexpectedMessageType(message_type=ChangeStatus, received=message)

        if message.task_id != self._task_id:
            raise IncorrectReceiver(task_id=self._task_id, received=message)

        return message.status

    def _run(self):
        self._logger.info(f"{self.worker_name} task starting")
        self._should_run = True
        self._validate_manager()
        self._send_status(status=Statuses.starting)
        self._wait_for_change_status_command(ChangeStatuses.started)
        self._start()
        self._send_status(status=Statuses.started)
        self._wait_for_change_status_command(ChangeStatuses.running)
        self._send_status(status=Statuses.running)
        self._task()

    def run(self):
        try:
            self._run()
            self._logger.warning(f"Normal exit from {self.worker_name}")
        except BaseException as ex:
            self._logger.error(
                f"Error in {self.worker_name}: {ex}",
                stack_info=True
            )
        finally:
            try:
                self._stop()
            except BaseException as err:
                self._logger.error(
                    f"Error when EXIT in {self.worker_name}: {err}",
                    stack_info=True
                )
                raise err

    def _wait_for_change_status_command(self, status: ChangeStatuses):
        while self._should_run and self._manager_process.is_running():
            message_bytes = self._request_queue.get(timeout=self._queue_get_timeout)

            if message_bytes:
                received_status = self._get_change_status(message_bytes)

                if received_status == ChangeStatuses.stopped:
                    raise StopSubprocess(self._task_id)

                if received_status != status:
                    raise UnexpectedStatusChangeReceived(
                        expected=status,
                        received=received_status
                    )
                else:
                    break
