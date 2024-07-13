import logging
import time
from typing import Iterator, Mapping, Any

from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient
from pymongo.change_stream import ChangeStream
from pymongo.collection import Collection
from pymongo.database import Database

from pymongo_change_stream_reader.exceptions import CounterMaxValueExceedError
from pymongo_change_stream_reader.models import ChangeEvent
from pymongo_change_stream_reader.settings import (
    FullDocumentBeforeChange,
    FullDocument,
)

default_logger = logging.Logger(__name__, logging.INFO)


class ChangeStreamWatch:
    def __init__(
        self,
        mongo_client: MongoClient,
        collection: str | None = None,
        database: str | None = None,
        pipeline: list[dict] | None = None,
        full_document_before_change: FullDocumentBeforeChange = (
                FullDocumentBeforeChange.when_available
        ),
        full_document: FullDocument = FullDocument.when_available,
        reader_batch_size: int | None = None,
        logger: logging.Logger = default_logger,
    ):
        self._collection = collection
        self._database = database
        self._mongo_client = mongo_client
        self._pipeline = pipeline
        self._full_document_before_change = full_document_before_change
        self._full_document = full_document
        self._reader_batch_size = reader_batch_size
        self._counter = 0
        self._should_run = False
        self._logger = logger

    def _get_watcher(self) -> MongoClient | Database | Collection:
        if self._collection is not None and self._database is None:
            raise ValueError("Can't use collection without database")

        if self._database is not None:
            db = self._mongo_client.get_database(self._database)
            if self._collection is not None:
                watcher = db.get_collection(self._collection)
            else:
                watcher = db
        else:
            watcher = self._mongo_client
        return watcher

    def exit_gracefully(self):
        self._should_run = False

    def start(self):
        self._logger.info("Connecting to mongo")
        server_info = self._mongo_client.server_info()
        self._logger.info(f"Connected to mongo server: {server_info}")
        self._should_run = True

    def stop(self):
        self._should_run = False
        self._mongo_client.close()

    def _get_stream_context_manager(
        self,
        resume_after: Mapping[str, Any] | None = None
    ) -> ChangeStream:
        watcher = self._get_watcher()
        return watcher.watch(
            pipeline=self._pipeline,
            resume_after=resume_after,
            full_document_before_change=self._full_document_before_change.value,
            full_document=self._full_document.value,
            batch_size=self._reader_batch_size
        )

    def iter(self, resume_token:  Mapping[str, Any] | None) -> Iterator[ChangeEvent]:
        with self._get_stream_context_manager(resume_token) as stream_context:
            while stream_context.alive and self._should_run:
                change = stream_context.try_next()
                resume_token = stream_context.resume_token
                yield from self._iter_change_event(change, resume_token)

    def _iter_change_event(
        self,
        change: RawBSONDocument | None,
        resume_token: RawBSONDocument | None
    ) -> Iterator[ChangeEvent]:
        event = self._build_change_event(
            change=change,
            resume_token=resume_token
        )
        self._is_valid_event(event)

        if event is not None:
            yield event
        else:
            # We end up here when there are no recent changes.
            # Sleep for a while before trying again to avoid flooding
            # the server with getMore requests when no changes are
            # available.
            time.sleep(0.5)

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
                self._logger.error("Counter bigger then max value. Stop process")
                raise CounterMaxValueExceedError()
        return True

    @staticmethod
    def _is_greater_then_max_value(counter: int) -> bool:
        int64 = 18446744073709551615
        return counter > int64
