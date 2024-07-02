import logging
import signal
import time
from json import dumps
from multiprocessing import Queue
from typing import Iterator, Mapping, Any

from bson import decode
from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient
from pymongo.change_stream import ChangeStream
from pymongo.collection import Collection
from pymongo.database import Database

from pymongo_change_stream_reader.base_worker import BaseWorker
from pymongo_change_stream_reader.messages import Status, serialize_message
from pymongo_change_stream_reader.models import SavedToken
from pymongo_change_stream_reader.settings import (
    FullDocumentBeforeChange,
    FullDocument,
)
from pymongo_change_stream_reader.models import Statuses
from pymongo_change_stream_reader.models import ChangeEvent


default_logger = logging.Logger(__name__, logging.INFO)


class ChangeStreamReader(BaseWorker):
    _allowed_operation_types = {"insert", "replace", "update", "delete"}

    def __init__(
        self,
        task_id: int,
        producer_queues: dict[int, Queue],
        request_queue: Queue,
        response_queue: Queue,
        committer_queue: Queue,
        mongo_uri: str,
        token_mongo_uri: str,
        token_database: str,
        token_collection: str,
        stream_reader_name: str,
        logger: logging.Logger = default_logger,
        database: str | None = None,
        collection: str | None = None,
        pipeline: list[dict] | None = None,
        full_document_before_change: FullDocumentBeforeChange = (
            FullDocumentBeforeChange.when_available
        ),
        full_document: FullDocument = FullDocument.when_available,
        reader_batch_size: int | None = None,
        queue_get_timeout: int = 1,
        queue_put_timeout: int = 10,
        program_start_timeout: int = 60,
    ):
        super().__init__(
            task_id=task_id,
            request_queue=request_queue,
            response_queue=response_queue,
            logger=logger,
            queue_put_timeout=queue_put_timeout,
            queue_get_timeout=queue_get_timeout,
            program_start_timeout=program_start_timeout,
            stream_reader_name=stream_reader_name
        )
        self._producer_queues = producer_queues
        self._number_of_producers = len(self._producer_queues)

        self._committer_queue = committer_queue
        self._mongo_client = MongoClient(
            host=mongo_uri,
            document_class=RawBSONDocument
        )
        self._token_mongo_uri = token_mongo_uri
        self._token_database = token_database
        self._token_collection = token_collection
        self._token_mongo_client = MongoClient(
            host=self._token_mongo_uri,
            document_class=RawBSONDocument
        )
        self._watcher = self._get_watcher(database, collection)

        self._pipeline = pipeline
        self._full_document_before_change = full_document_before_change
        self._full_document = full_document
        self._reader_batch_size = reader_batch_size

        self._last_resume_token: bytes | None = None
        self._should_run = False
        self._counter = 0

        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        self._should_run = False

    def _get_watcher(
        self, collection: str, database: str
    ) -> MongoClient | Database | Collection:
        if collection is not None and database is None:
            raise ValueError("Can't use collection without database")

        if database is not None:
            db = self._mongo_client.get_database(database)
            if collection is not None:
                watcher = db.get_collection(collection)
            else:
                watcher = db
        else:
            watcher = self._mongo_client
        return watcher

    def _start(self):
        self._logger.info("Connecting to mongo")
        server_info = self._mongo_client.server_info()
        self._logger.info(f"Connected to mongo server: {server_info}")

        self._logger.info("Connecting to token mongo")
        token_server_info = self._token_mongo_client.server_info()
        self._logger.info(f"Connected to mongo token server: {token_server_info}")

        token_collection = self._token_mongo_client.get_database(
            self._token_database
        ).get_collection(self._token_collection)

        self._logger.info(
            f"Request last token for stream_reader_name={self._stream_reader_name}"
        )
        received_saved_token: SavedToken | None = token_collection.find_one(filter={
            "stream_reader_name": self._stream_reader_name
        })
        self._logger.debug(
            f"Got token document: {received_saved_token} "
            f"for stream_reader_name={self._stream_reader_name}"
        )

        self._logger.info(f"Close connection to mongo token server")
        self._token_mongo_client.close()

        saved_token = SavedToken.parse_obj(received_saved_token)

        if received_saved_token:
            self._last_resume_token = saved_token.token
            self._logger.info(
                f"Got last token {self._last_resume_token} "
                f"for stream_reader_name={self._stream_reader_name}"
            )
        else:
            self._logger.info(
                f"Last token for stream_reader_name={self._stream_reader_name} "
                f"wasn't found"
            )

    def _stop(self):
        self._mongo_client.close()
        self._token_mongo_client.close()

    def _task(self):
        for event in self._iterate_stream():
            if (
                event.bson_document is not None and
                self._is_need_to_send_to_producer(event.bson_document)
            ):
                self._send_data(
                    bson_document=event.bson_document,
                    count=event.count
                )
                need_confirm = 1
            else:
                need_confirm = 0

            self._send_resume_token(
                token=event.token,
                count=event.count,
                need_confirm=need_confirm
            )

    @staticmethod
    def _get_resume_token_from_stream(stream: ChangeStream) -> RawBSONDocument | None:
        return stream.resume_token  # type: ignore

    @staticmethod
    def _decode_resume_token(resume_token: bytes | None) -> Mapping[str, Any] | None:
        if resume_token:
            return decode(resume_token)
        else:
            return None

    def _iterate_stream(self) -> Iterator[ChangeEvent]:
        decoded_token = self._decode_resume_token(self._last_resume_token)
        with self._watcher.watch(
            pipeline=self._pipeline,
            resume_after=decoded_token,
            full_document_before_change=self._full_document_before_change.value,
            full_document=self._full_document.value,
            batch_size=self._reader_batch_size
        ) as stream:
            while stream.alive and self._should_run:
                change = self._try_next(stream)
                resume_token = self._get_resume_token_from_stream(stream)
                event = self._build_change_event(
                    change=change,
                    resume_token=resume_token
                )
                if not self._is_valid_event(event):
                    break

                if event is not None:
                    yield event
                else:
                    # We end up here when there are no recent changes.
                    # Sleep for a while before trying again to avoid flooding
                    # the server with getMore requests when no changes are
                    # available.
                    time.sleep(0.5)

    def _is_need_to_send_to_producer(self, bson_document: RawBSONDocument) -> int:
        operation_type = decode(bson_document["operationType"].raw)
        if operation_type in self._allowed_operation_types:
            return 1
        else:
            return 0

    @staticmethod
    def _try_next(stream: ChangeStream) -> RawBSONDocument | None:
        change = stream.try_next()  # type: ignore
        return change

    def _build_change_event(
        self,
        change: RawBSONDocument | None,
        resume_token: RawBSONDocument | None,
    ) -> ChangeEvent | None:
        # Note that the ChangeStream's resume token may be updated
        # even when no changes are returned.
        token: bytes | None = resume_token.raw if resume_token is not None else None

        if change is not None:
            self._last_resume_token = token
            self._counter += 1
            event = ChangeEvent(
                bson_document=change,
                token=self._last_resume_token,
                count=self._counter
            )
        else:
            if self._last_resume_token != token and token is not None:
                self._last_resume_token = token
                self._counter += 1
                event = ChangeEvent(
                    bson_document=change,
                    token=self._last_resume_token,
                    count=self._counter
                )
            else:
                event = None
        return event

    def _is_valid_event(self, event: ChangeEvent | None):
        if event is not None:
            if self._is_greater_then_max_value(event.count):
                self._logger.warning("Counter bigger then max value. Stop process")
                return False
        return True

    @staticmethod
    def _is_greater_then_max_value(counter: int) -> bool:
        int64 = 18446744073709551615
        return counter > int64

    def _send_status(self, status: Statuses):
        message = Status(task_id=self._task_id, status=status)
        message_bytes = dumps(serialize_message(message)).encode()
        self._response_queue.put(message_bytes)

    def _send_resume_token(
        self,
        token: bytes | None,
        count: int,
        need_confirm: int
    ):
        """
        Bytes: 0-7 - number of message
        Bytes: 8 - is need to confirm from producers
        Bytes: 9-end - resume_token
        """
        data: bytes = (
            count.to_bytes(length=8, byteorder='big') +
            need_confirm.to_bytes(length=1, byteorder='big')
        )
        if token:
            data = data + token

        self._put_to_queue(
            queue=self._committer_queue,
            data=data,
            timeout=self._queue_put_timeout
        )

    def _send_data(self, bson_document: RawBSONDocument, count: int):
        """
        Bytes: 0-7 - number of message
        Bytes: 8-end - bson_document
        """
        queue_number = self._get_queue_number(bson_document)
        queue = self._producer_queues[queue_number]
        data: bytes = count.to_bytes(length=8, byteorder='big') + bson_document.raw
        self._put_to_queue(queue=queue, data=data, timeout=self._queue_put_timeout)

    def _get_queue_number(self, bson_document: RawBSONDocument) -> int:
        document_key = decode(bson_document["documentKey"].raw)["_id"]
        number = sum([i for i in (document_key.binary)])
        queue_number = number % self._number_of_producers
        return queue_number

    @staticmethod
    def _put_to_queue(queue: Queue, data: bytes, timeout: int):
        queue.put(data, timeout=timeout)
