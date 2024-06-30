import functools
import threading
from enum import Enum
from multiprocessing import Process
from typing import NamedTuple

from pydantic import BaseModel


def check_command_format(func):
    @functools.wraps(func)
    def wrapper(command: dict) -> dict:
        message = Message.parse_obj(command)
        return func(message.dict())
    return wrapper


class Message(BaseModel):
    type: str
    payload: dict


class Statuses(str, Enum):
    running = "running"
    started = "started"
    stopped = "stopped"


class ChangeStatuses(str, Enum):
    running = "running"
    stopped = "stopped"


class TaskIdGenerator:
    counter: int = 1
    _lock = threading.Lock()

    @classmethod
    def get(cls) -> int:
        cls._lock.acquire()
        try:
            cls.counter += 1
            counter = cls.counter
        finally:
            cls._lock.release()
        return counter


class ProcessData(NamedTuple):
    task_id: int
    process: Process
    kwargs: dict
