from typing import Callable

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, ClusterMetadata


class KafkaClient:
    def __init__(self, producer_config: dict[str, str], admin_config: dict[str, str]):
        self._producer_config = producer_config
        self._admin_config = admin_config
        self._kafka_producer: Producer | None = None
        self._kafka_admin: AdminClient | None = None

    @property
    def _producer(self) -> Producer:
        if not self._kafka_producer:
            raise ValueError("Should start kafka client firstly")
        return self._kafka_producer

    @property
    def _admin(self) -> AdminClient:
        if not self._kafka_admin:
            raise ValueError("Should start kafka client firstly")
        return self._kafka_admin

    def start(self):
        self._kafka_producer = Producer(self._producer_config)
        self._kafka_admin = AdminClient(self._admin_config)

    def stop(self):
        ...

    def create_topics(self, new_topics: list[NewTopic]):
        return self._admin.create_topics(new_topics)

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Callable):
        return self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=on_delivery
        )

    def list_topics(self) -> ClusterMetadata:
        return self._producer.list_topics()
