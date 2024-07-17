import logging
from multiprocessing import Queue
from queue import Empty
from typing import Iterator

from pymongo import MongoClient

from pymongo_change_stream_reader.base_application import BaseApplication
from pymongo_change_stream_reader.models import (
    CommitEvent, RecheckCommitEvent,
)
from .commit_event_handler import CommitEventHandler

default_logger = logging.Logger(__name__, logging.INFO)


class CommitFlow(BaseApplication):
    _token_mongo_client_cls = MongoClient

    def __init__(
        self,
        committer_queue: Queue,
        commit_event_handler: CommitEventHandler,
        logger: logging.Logger = default_logger,
        queue_get_timeout: float = 10,
    ):
        super().__init__()
        self._commit_event_handler = commit_event_handler
        self._committer_queue = committer_queue
        self._queue_get_timeout = queue_get_timeout
        self._logger = logger

    def exit_gracefully(self, signum, frame):
        self._should_run = False

    def _start_dependencies(self):
        self._commit_event_handler.start()

    def _stop_dependencies(self):
        self._commit_event_handler.stop()

    def task(self):
        while self._should_run:
            self._get_change_event_and_process()

    def _get_change_event_and_process(self) -> None:
        for event in self.iter_event():
            if isinstance(event, CommitEvent):
                self._commit_event_handler.handle_commit_event(event)
            elif isinstance(event, RecheckCommitEvent):
                self._commit_event_handler.handle_recheck_event(event)

    def iter_event(self) -> Iterator[CommitEvent | RecheckCommitEvent]:
        try:
            result = self._committer_queue.get(
                timeout=self._queue_get_timeout
            )
        except Empty as ex:
            yield RecheckCommitEvent()
        else:
            if result:
                event = self._decode_commit_event(result)
                yield event

    @staticmethod
    def _decode_commit_event(data: bytes) -> CommitEvent:
        count = int.from_bytes(data[0:8], byteorder='big')
        need_confirm = bool.from_bytes(data[8:9], byteorder='big')
        resume_token: bytes | None

        if len(data) > 9:
            resume_token = data[9:]
        else:
            resume_token = None

        return CommitEvent(
            count=count,
            need_confirm=need_confirm,
            resume_token=resume_token
        )
