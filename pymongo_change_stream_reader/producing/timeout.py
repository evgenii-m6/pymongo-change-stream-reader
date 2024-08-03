from collections import deque
from threading import Thread
from typing import Callable, TypeVar, Generic

T = TypeVar('T')


class WithTimeout(Generic[T]):
    def __init__(self, target: Callable[..., T], timeout: float | None = None):
        self._func = target
        self._timeout = timeout
        self.thread = Thread(target=self._task, daemon=True)
        self._queue: deque[T] = deque()

    def run(self) -> T:
        self.thread.start()
        self.thread.join(self._timeout)
        if not self._queue:
            raise TimeoutError()
        return self._queue.popleft()

    def _task(self):
        r = self._func()
        self._queue.append(r)
