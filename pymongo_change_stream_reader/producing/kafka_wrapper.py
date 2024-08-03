from concurrent.futures import Future
from threading import Thread
from typing import Callable

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, ClusterMetadata

from .timeout import WithTimeout


class KafkaClient:
    def __init__(
        self,
        producer_config: dict[str, str],
        admin_config: dict[str, str],
        kafka_connect_timeout: float = 10.0,
        poll_timeout: float = 5.0
    ):
        self._producer_config = producer_config
        self._admin_config = admin_config
        self._kafka_connect_timeout = kafka_connect_timeout
        self._kafka_producer: Producer | None = None
        self._kafka_admin: AdminClient | None = None
        self._poll_thread = Thread(target=self._poll_loop, daemon=True)
        self._poll_timeout = poll_timeout
        self._should_run = False

    @property
    def _producer(self) -> Producer:
        if not self._should_run:
            raise ValueError("Should start kafka client firstly")
        return self._kafka_producer

    @property
    def _admin(self) -> AdminClient:
        if not self._should_run:
            raise ValueError("Should start kafka client firstly")
        return self._kafka_admin

    def start(self):
        self._kafka_producer = Producer(self._producer_config)
        self._kafka_admin = AdminClient(self._admin_config)
        WithTimeout(
            target=self._kafka_producer.list_topics,
            timeout=self._kafka_connect_timeout
        ).run()
        WithTimeout(
            target=self._kafka_admin.list_topics,
            timeout=self._kafka_connect_timeout
        ).run()
        self._should_run = True

    def stop(self):
        self._should_run = False
        self._kafka_producer.flush()

    def create_topics(self, new_topics: list[NewTopic]) -> dict[str, Future]:
        return WithTimeout(
            target=self._admin.create_topics(new_topics),
            timeout=self._kafka_connect_timeout
        ).run()

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable
    ) -> None:
        return self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=on_delivery
        )

    def __len__(self):
        return len(self._producer)

    def flush(self, timeout: float | None = None):
        self._producer.flush(timeout=timeout)

    def _poll_loop(self):
        while self._should_run:
            self._producer.poll(timeout=self._poll_timeout)

    def list_topics(self) -> ClusterMetadata:
        return WithTimeout(
            target=self._producer.list_topics,
            timeout=self._kafka_connect_timeout
        ).run()
