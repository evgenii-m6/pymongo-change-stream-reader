import logging
from concurrent.futures import Future
from typing import Callable

from confluent_kafka.admin import NewTopic, TopicMetadata

from pymongo_change_stream_reader.settings import NewTopicConfiguration
from .kafka_wrapper import KafkaClient

default_logger = logging.Logger(__name__, logging.INFO)


class Producer:
    def __init__(
        self,
        kafka_client: KafkaClient,
        new_topic_configuration: NewTopicConfiguration,
        logger: logging.Logger = default_logger,
    ):
        self._new_topic_configuration = new_topic_configuration
        self._kafka_client = kafka_client
        self._logger = logger

    def start(self):
        self._kafka_client.start()

    def stop(self):
        self._kafka_client.stop()

    def create_topic(self, topic: str):
        replication_factor = self._new_topic_configuration.new_topic_replication_factor
        num_partitions = self._new_topic_configuration.new_topic_num_partitions
        config = self._new_topic_configuration.new_topic_config
        new_topic = NewTopic(
            topic=topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config,
        )
        result: dict[str, Future] = self._kafka_client.create_topics([new_topic])
        result[topic].result()

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Callable):
        self._kafka_client.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=on_delivery
        )

    def get_topics(self) -> list[str]:
        self._logger.info("Connecting to kafka")
        topics: dict[str, TopicMetadata] = self._kafka_client.list_topics().topics
        self._logger.info(f"Connected to kafka. Got list of topics: {topics}")
        return list(topics.keys())
