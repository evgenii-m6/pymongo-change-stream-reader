import pytest

from tests.mocks.kafka import KafkaClientFileWriter


@pytest.fixture
def kafka_client_0():
    return KafkaClientFileWriter(producer_config={}, admin_config={})


@pytest.fixture
def kafka_client_1():
    return KafkaClientFileWriter(producer_config={}, admin_config={})
