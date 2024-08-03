from collections import deque
from threading import Thread


class WithTimeout:
    def __init__(self, target, timeout=None):
        self._func = target
        self._timeout = timeout
        self.thread = Thread(target=self._task, daemon=True)
        self._queue = deque()

    def run(self):
        self.thread.start()
        if self._timeout:
            self.thread.join(self._timeout)
        if not self._queue:
            raise TimeoutError()
        return self._queue.popleft()

    def _task(self):
        r = self._func()
        self._queue.append(r)
