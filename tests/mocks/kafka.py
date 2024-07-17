import json
from collections import defaultdict
from concurrent.futures import Future
from copy import deepcopy
from typing import Callable, NamedTuple

from confluent_kafka.admin import NewTopic, ClusterMetadata, TopicMetadata


class KafkaMessageMock(NamedTuple):
    key: bytes
    value: bytes


class KafkaClientMock:
    def __init__(self, producer_config: dict[str, str], admin_config: dict[str, str]):
        self._cluster_metadata = ClusterMetadata()
        default_topic_1 = TopicMetadata()
        default_topic_2 = TopicMetadata()
        default_topic_1.topic = "test.topic"
        default_topic_2.topic = "other.topic"
        self._cluster_metadata.topics[default_topic_1.topic] = default_topic_1
        self._cluster_metadata.topics[default_topic_2.topic] = default_topic_2

        self._producer_config = producer_config
        self._admin_config = admin_config
        self._kafka_producer = False
        self._kafka_admin = False
        self.produce_error = None
        self.produced: dict[str, list[KafkaMessageMock]] = defaultdict(list)

    @property
    def _producer(self):
        if not self._kafka_producer:
            raise ValueError("Should start kafka client firstly")

    @property
    def _admin(self):
        if not self._kafka_admin:
            raise ValueError("Should start kafka client firstly")

    def start(self):
        self._kafka_producer = True
        self._kafka_admin = True

    def stop(self):
        ...

    def create_topics(self, new_topics: list[NewTopic]):
        self._admin
        futures = {}
        for topic in new_topics:
            tm = TopicMetadata()
            tm.topic = topic.topic
            self._cluster_metadata.topics[tm.topic] = tm
            future = Future()
            future.set_result(None)
            futures[tm.topic] = future
        return futures

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable
    ) -> None:
        self._producer
        message = KafkaMessageMock(key=key, value=value)
        self.produced[topic].append(message)
        on_delivery(self.produce_error, str(self.produce_error))

    def list_topics(self) -> ClusterMetadata:
        self._producer
        return deepcopy(self._cluster_metadata)


class KafkaClientFileWriter(KafkaClientMock):
    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable
    ) -> None:
        super().produce(topic=topic, key=key, value=value, on_delivery=on_delivery)
        data_dict = {
            "key": json.loads(key.decode()),
            "value": json.loads(value.decode()),
        }
        data = "\n" + json.dumps(data_dict, indent=4)
        with open(file=topic, mode='a+') as f:
            f.write(data)
