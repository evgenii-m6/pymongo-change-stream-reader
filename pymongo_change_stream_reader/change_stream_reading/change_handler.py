from multiprocessing import Queue

from bson.raw_bson import RawBSONDocument
from bson import decode

from pymongo_change_stream_reader.commit_event_decoder import encode_commit_event
from pymongo_change_stream_reader.models import ChangeEvent


class ChangeHandler:
    _allowed_operation_types = {"insert", "replace", "update", "delete"}

    def __init__(
        self,
        committer_queue: Queue,
        producer_queues: dict[int, Queue],
        queue_put_timeout: float = 10,
    ):
        self._committer_queue = committer_queue
        self._queue_put_timeout = queue_put_timeout
        self._producer_queues = producer_queues
        self._number_of_producers = len(self._producer_queues)

    def handle(self, event: ChangeEvent):
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

    def _is_need_to_send_to_producer(self, bson_document: RawBSONDocument) -> int:
        operation_type = bson_document["operationType"]
        if operation_type in self._allowed_operation_types:
            return 1
        else:
            return 0

    def _send_resume_token(
        self,
        token: bytes | None,
        count: int,
        need_confirm: int
    ):
        data = encode_commit_event(
            count=count,
            need_confirm=need_confirm,
            token=token
        )
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
    def _put_to_queue(queue: Queue, data: bytes, timeout: float):
        queue.put(data, timeout=timeout)