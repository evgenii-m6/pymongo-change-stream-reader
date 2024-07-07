import logging
from abc import abstractmethod
from asyncio import Protocol
from contextlib import contextmanager
from multiprocessing import Queue
from typing import Callable

from .base_worker import BaseWorker
from .messages import (
    Status,
    encode_message,
)
from .models import Statuses


class ServiceApplicationProtocol(Protocol):
    @abstractmethod
    def run(self):
        raise NotImplementedError


default_logger = logging.Logger(__name__, logging.INFO)


class ApplicationContext:
    build_worker: Callable[..., BaseWorker]

    def __init__(
        self,
        task_id: int,
        response_queue: Queue,
        logger: logging.Logger = default_logger,
    ):
        self._task_id = task_id
        self._response_queue = response_queue
        self._logger = logger

    @classmethod
    def run_application(cls, task_id: int, response_queue: Queue, **kwargs):
        appcontext = cls(task_id=task_id, response_queue=response_queue)
        with appcontext._context():
            worker = appcontext.build_worker(
                task_id=task_id,
                response_queue=response_queue,
                **kwargs
            )
            worker.run()

    @contextmanager
    def _context(self):
        try:
            yield
        finally:
            self._send_stopped()

    def _send_stopped(self):
        self._logger.warning(
            f"Send stopped status to response queue from task: {self._task_id}"
        )
        message = Status(task_id=self._task_id, status=Statuses.stopped)
        message_bytes = encode_message(message)
        try:
            self._response_queue.put(message_bytes)
        except BaseException as ex:
            self._logger.error(
                f"Error when send stopped status "
                f"to response queue from task {self._task_id}: {ex}"
            )
        else:
            self._logger.warning(
                f"Successfully sent stopped status "
                f"to response queue from task {self._task_id}"
            )
