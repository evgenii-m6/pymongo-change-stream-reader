import logging
from concurrent.futures import Future
from typing import Callable

from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import NewTopic, TopicMetadata

from pymongo_change_stream_reader.settings import NewTopicConfiguration
from .kafka_wrapper import KafkaClient

default_logger = logging.Logger(__name__, logging.INFO)


class Producer:
    def __init__(
        self,
        task_id: int,
        kafka_client: KafkaClient,
        new_topic_configuration: NewTopicConfiguration,
        logger: logging.Logger = default_logger,
    ):
        self._task_id = task_id
        self._new_topic_configuration = new_topic_configuration
        self._kafka_client = kafka_client
        self._logger = logger
        self._wait_for_flush = 5.0
        self._should_run = False

    def start(self):
        self._logger.info("Connecting to kafka")
        if not self._should_run:
            self._kafka_client.start()
            self._should_run = True
        self._logger.info(f"Connected to kafka")

    def stop(self):
        self._logger.info("Disconnecting from kafka")
        if self._should_run:
            self._should_run = False
            self._kafka_client.stop()
        self._logger.info(f"Disconnected from kafka")

    def create_topic(self, topic: str) -> None:
        self._logger.info(f"Creating topic {topic}")
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
        try:
            result[topic].result()
            self._logger.info(f"Created topic {topic}")
        except KafkaException as ex:
            kafka_error: KafkaError = ex.args[0]
            if kafka_error.code() == 36:  # TOPIC_ALREADY_EXISTS
                self._logger.info(f"Topic {topic} already exists")
            else:
                self._logger.error(f"Error when create topic {topic}: {ex!r}")
                raise

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable
    ) -> None:
        count = 0
        while self._should_run:
            try:
                if count > 0:
                    self._logger.info(
                        f"Producer retry number {count} to send message."
                    )

                self._kafka_client.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=on_delivery
                )
                break
            except BufferError as ex:
                before_flush = len(self._kafka_client)
                count += 1

                wait_count = 0
                while self._should_run:
                    self._logger.warning(
                        f"Producer's queue task_id={self._task_id} "
                        f"is overcrowded ({before_flush} messages in queue). "
                        f"Wait number {wait_count} for flush  {self._wait_for_flush}s."
                    )
                    after_flush = self._kafka_client.flush(
                        timeout=self._wait_for_flush
                    )
                    wait_count += 1
                    if after_flush < before_flush:
                        break

    def get_topics(self) -> list[str]:
        self._logger.info("Request topics from kafka")
        topics: dict[str, TopicMetadata] = self._kafka_client.list_topics().topics
        result = list(topics.keys())
        self._logger.info(f"Got {len(result)} topics from kafka")
        return result
