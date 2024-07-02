import logging
import signal
from concurrent.futures import Future
from json import dumps
from multiprocessing import Queue
from queue import Empty

import bson
from bson import json_util
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, TopicMetadata

from pymongo_change_stream_reader.base_worker import BaseWorker
from pymongo_change_stream_reader.messages import Status, serialize_message
from pymongo_change_stream_reader.models import DecodedChangeEvent, Statuses
from pymongo_change_stream_reader.settings import NewTopicConfiguration

default_logger = logging.Logger(__name__, logging.INFO)


class ProducerFlow(BaseWorker):
    _operation_map = {
        'replace': 'u',
        'update': "u",
        'insert': 'c',
        'delete': 'd'
    }

    def __init__(
        self,
        task_id: int,
        producer_queue: Queue,
        request_queue: Queue,
        response_queue: Queue,
        committer_queue: Queue,
        stream_reader_name: str,
        kafka_bootstrap_servers: str,
        new_topic_configuration: NewTopicConfiguration,
        kafka_prefix: str = "",
        logger: logging.Logger = default_logger,
        queue_get_timeout: int = 1,
        queue_put_timeout: int = 10,
        program_start_timeout: int = 60,
    ):
        super().__init__(
            task_id=task_id,
            request_queue=request_queue,
            response_queue=response_queue,
            logger=logger,
            queue_get_timeout=queue_get_timeout,
            queue_put_timeout=queue_put_timeout,
            program_start_timeout=program_start_timeout,
            stream_reader_name=stream_reader_name,
        )

        self._producer_queue = producer_queue
        self._committer_queue = committer_queue
        self._kafka_bootstrap_servers = kafka_bootstrap_servers
        self._kafka_prefix = kafka_prefix

        self._created_topics: set[str] = set()
        self._config = {
            'bootstrap.servers': kafka_bootstrap_servers,  # Kafka broker address
            'client.id': f"producer_{self._stream_reader_name}_{task_id}"
        }
        self._kafka_client = Producer(self._config)
        self._admin_kafka = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})
        self._new_topic_configuration = new_topic_configuration

        self._should_run = False

        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        self._should_run = False

    def _start(self):
        self._logger.info("Connecting to kafka")
        topics: dict[str, TopicMetadata] = self._kafka_client.list_topics().topics
        self._logger.info(f"Connected to kafka. Got list of topics: {topics}")
        for topic_name in topics.keys():
            if topic_name.startswith(self._kafka_prefix):
                self._created_topics.add(topic_name)

    def _stop(self):
        ...

    def _send_status(self, status: Statuses):
        message = Status(task_id=self._task_id, status=status)
        message_bytes = dumps(serialize_message(message)).encode()
        self._response_queue.put(message_bytes)

    def _task(self):
        while self._should_run:
            try:
                result = self._producer_queue.get(timeout=self._queue_get_timeout)
            except Empty as ex:
                continue

            if result:
                event = self._decode_data(result)
                self._produce_event(event)

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

    def _get_topic_from_event(self, event: DecodedChangeEvent) -> str:
        namespace = event.bson_document['ns']
        collection = namespace['coll']
        database = namespace['db']
        if self._kafka_prefix:
            return f"{self._kafka_prefix}.{database}.{collection}"
        else:
            return f"{database}.{collection}"

    def _get_key_from_event(self, event: DecodedChangeEvent) -> bytes:
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

    def _maybe_create_topic(self, topic: str):
        replication_factor = self._new_topic_configuration.new_topic_replication_factor
        num_partitions = self._new_topic_configuration.new_topic_num_partitions
        config = self._new_topic_configuration.new_topic_config
        new_topic = NewTopic(
            topic=topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config,
        )
        result: dict[str, Future] = self._admin_kafka.create_topics([new_topic])
        result[topic].result()

    def _produce_event(self, event: DecodedChangeEvent):
        topic = self._get_topic_from_event(event)
        if topic not in self._created_topics:
            self._maybe_create_topic(topic)
        self._kafka_client.produce(
            topic=topic,
            key=self._get_key_from_event(event),
            value=self._get_value_from_event(event),
            on_delivery=self._on_message_delivered
        )

    def _on_message_delivered(self, count: int):
        """
        Bytes: 0-7 - number of message
        Bytes: 8 - is need to confirm from producers
        """

        def wrapped(err, msg):
            need_confirm = 0
            message = (
                count.to_bytes(length=8, byteorder='big') +
                need_confirm.to_bytes(length=1, byteorder='big')
            )
            self._committer_queue.put(message)
            if err:
                self._should_run = False
                self._logger.error(
                    f"Error when send message {count} to kafka: {err}",
                    stack_info=True
                )
        return wrapped
