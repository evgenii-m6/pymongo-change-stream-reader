import logging
import signal
from datetime import datetime
from multiprocessing import Queue
from queue import Empty

from bson import Binary
from pymongo import MongoClient

from pymongo_change_stream_reader.base_worker import BaseWorker
from pymongo_change_stream_reader.committing.commit_processing import (
    ProcessCommitEvent,
)
from pymongo_change_stream_reader.models import (
    SavedToken,
    CommitEvent,
    CommittableEvents,
)

default_logger = logging.Logger(__name__, logging.INFO)


class CommitFlow(BaseWorker):
    def __init__(
        self,
        manager_pid: int,
        manager_create_time: float,
        task_id: int,
        request_queue: Queue,
        response_queue: Queue,
        committer_queue: Queue,
        stream_reader_name: str,
        token_mongo_uri: str,
        token_database: str,
        token_collection: str,
        logger: logging.Logger = default_logger,
        commit_interval: int = 30,
        max_uncommitted_events: int = 10000,
        queue_get_timeout: int = 10,
        queue_put_timeout: int = 10,
    ):
        super().__init__(
            manager_pid=manager_pid,
            manager_create_time=manager_create_time,
            task_id=task_id,
            request_queue=request_queue,
            response_queue=response_queue,
            logger=logger,
            queue_get_timeout=queue_get_timeout,
            queue_put_timeout=queue_put_timeout,
            stream_reader_name=stream_reader_name
        )
        self._committer_queue = committer_queue
        self._token_mongo_client = MongoClient(host=token_mongo_uri)
        self._token_database = token_database
        self._token_collection = token_collection

        self._collection = self._token_mongo_client.get_database(
            self._token_database
        ).get_collection(self._token_collection)

        self._should_run = False

        self._commit_event_processor = ProcessCommitEvent(
            max_uncommitted_events=max_uncommitted_events,
            commit_interval=commit_interval
        )

        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        self._should_run = False

    def _start(self):
        self._logger.info("Connecting to mongo")
        server_info = self._token_mongo_client.server_info()
        self._logger.info(f"Connected to mongo server: {server_info}")

        self._logger.info(f"Create index on token collection")
        self._collection.create_index(
            keys="stream_reader_name",
            name="stream_reader_name",
            unique=True,
        )

    def _stop(self):
        self._token_mongo_client.close()

    def _task(self):
        while self._should_run:
            try:
                result = self._committer_queue.get(
                    timeout=self._queue_get_timeout
                )
            except Empty as ex:
                continue

            if result:
                event = self._decode_commit_event(result)
                self._process_event(event)

    @staticmethod
    def _decode_commit_event(data: bytes) -> CommitEvent:
        count = int.from_bytes(data[0:8], byteorder='big')
        need_confirm = bool.from_bytes(data[8], byteorder='big')
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

    def _process_event(self, event: CommitEvent):
        for committable_event in self._commit_event_processor.process_event(event):
            self._commit_events(committable_event)

    def _commit_events(self, commit_events: CommittableEvents):
        token_data = self._build_token_model(commit_events)
        self._save_resume_token_to_db(token_data)

    def _build_token_model(self, events: CommittableEvents) -> SavedToken:
        return SavedToken(
            stream_reader_name=self._stream_reader_name,
            token=events.resume_token,
            date=datetime.utcnow()
        )

    def _save_resume_token_to_db(self, token_data: SavedToken):
        replacement = {
            'stream_reader_name': self._stream_reader_name,
            'token': Binary(token_data.token, subtype=0),
            'date': token_data.date
        }

        self._collection.replace_one(
            filter={"stream_reader_name": self._stream_reader_name},
            replacement=replacement,
            upsert=True
        )
