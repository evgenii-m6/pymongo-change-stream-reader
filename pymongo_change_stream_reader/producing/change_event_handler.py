import logging
from multiprocessing import Queue

from bson import json_util

from pymongo_change_stream_reader.models import DecodedChangeEvent
from .producer import Producer

default_logger = logging.Logger(__name__, logging.INFO)


class ChangeEventHandler:
    _operation_map = {
        'replace': 'u',
        'update': "u",
        'insert': 'c',
        'delete': 'd'
    }

    def __init__(
        self,
        kafka_client: Producer,
        committer_queue: Queue,
        kafka_prefix: str = "",
        logger: logging.Logger = default_logger,
    ):
        self._kafka_prefix = kafka_prefix
        self._created_topics: set[str] = set()
        self._kafka_client = kafka_client
        self._committer_queue = committer_queue
        self._logger = logger

    def start(self):
        self._kafka_client.start()
        topics = self._kafka_client.get_topics()
        for topic_name in topics:
            if topic_name.startswith(self._kafka_prefix):
                self._created_topics.add(topic_name)

    def stop(self):
        self._kafka_client.stop()

    def _maybe_create_topic(self, topic: str):
        if topic not in self._created_topics:
            self._kafka_client.create_topic(topic)
            self._created_topics.add(topic)

    def handle(self, event: DecodedChangeEvent):
        topic = self._get_topic_from_event(event)
        self._maybe_create_topic(topic)
        self._kafka_client.produce(
            topic=topic,
            key=self._get_key_from_event(event),
            value=self._get_value_from_event(event),
            on_delivery=self._on_message_delivered(event.count)
        )

    def _get_topic_from_event(self, event: DecodedChangeEvent) -> str:
        namespace = event.bson_document['ns']
        collection = namespace['coll']
        database = namespace['db']
        if self._kafka_prefix:
            return f"{self._kafka_prefix}.{database}.{collection}"
        else:
            return f"{database}.{collection}"

    @staticmethod
    def _get_key_from_event(event: DecodedChangeEvent) -> bytes:
        return json_util.dumps(
            event.bson_document["documentKey"],
            json_options=json_util.LEGACY_JSON_OPTIONS
        ).encode()

    def _get_value_from_event(self, event: DecodedChangeEvent) -> bytes:
        new_doc = {}
        if 'fullDocumentBeforeChange' in event.bson_document:
            new_doc['before'] = event.bson_document['fullDocumentBeforeChange']
        if 'updateDescription' in event.bson_document:
            new_doc['updateDescription'] = event.bson_document['updateDescription']
        if 'fullDocument' in event.bson_document:
            new_doc['after'] = event.bson_document['fullDocument']
        if 'operationType' in event.bson_document:
            new_doc['op'] = self._operation_map[event.bson_document['operationType']]
        return json_util.dumps(
            new_doc,
            json_options=json_util.LEGACY_JSON_OPTIONS
        ).encode()

    def _on_message_delivered(self, count: int):
        """
        Bytes: 0-7 - number of message
        Bytes: 8 - is need to confirm from producers
        """

        def wrapped(err, msg):
            if err:
                self._should_run = False
                self._logger.error(
                    f"Error when send message {count} to kafka: {err}",
                    stack_info=True
                )
            else:
                need_confirm = 0
                message = (
                    count.to_bytes(length=8, byteorder='big') +
                    need_confirm.to_bytes(length=1, byteorder='big')
                )
                self._committer_queue.put(message)

        return wrapped
