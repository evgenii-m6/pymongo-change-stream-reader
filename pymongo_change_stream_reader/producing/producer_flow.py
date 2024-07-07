import logging
from multiprocessing import Queue
from queue import Empty
from typing import Iterator

import bson

from pymongo_change_stream_reader.base_application import BaseApplication
from pymongo_change_stream_reader.models import DecodedChangeEvent
from .change_event_handler import ChangeEventHandler

default_logger = logging.Logger(__name__, logging.INFO)


class ProducerFlow(BaseApplication):
    def __init__(
        self,
        producer_queue: Queue,
        event_handler: ChangeEventHandler,
        logger: logging.Logger = default_logger,
        queue_get_timeout: int = 1,
    ):
        super().__init__()
        self._producer_queue = producer_queue
        self._queue_get_timeout = queue_get_timeout
        self._event_handler = event_handler
        self._logger = logger

    def exit_gracefully(self, signum, frame):
        self._should_run = False

    def _start_dependencies(self):
        self._event_handler.start()

    def _stop_dependencies(self):
        self._event_handler.stop()

    def task(self):
        for event in self.iter_change_events():
            self._event_handler.handle(event)

    def iter_change_events(self) -> Iterator[DecodedChangeEvent]:
        while self._should_run:
            try:
                result = self._producer_queue.get(timeout=self._queue_get_timeout)
            except Empty as ex:
                continue

            if result:
                event = self._decode_data(result)
                yield event

    @staticmethod
    def _decode_data(data: bytes) -> DecodedChangeEvent:
        """
        Bytes: 0-7 - number of message
        Bytes: 8-end - bson_document
        """
        count = int.from_bytes(data[0:8], byteorder='big')
        bson_document = bson.decode(data[8:])
        return DecodedChangeEvent(
            bson_document=bson_document,
            count=count,
        )
