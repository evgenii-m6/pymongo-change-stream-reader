from pymongo_change_stream_reader.producing import ProducerFlow
from .kafka import MockProducer, MockAdminClient


class MockProducerFlow(ProducerFlow):
    _producer_cls = MockProducer
    _admin_kafka_cls = MockAdminClient
