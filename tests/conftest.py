from multiprocessing import Queue
from queue import Empty

import pytest
from bson.raw_bson import RawBSONDocument

from pymongo_change_stream_reader.change_stream_reading import (
    RetrieveResumeToken,
    ChangeStreamWatch,
    ChangeHandler, ChangeStreamReader,
)
from pymongo_change_stream_reader.producing import Producer, ChangeEventHandler, \
    ProducerFlow
from pymongo_change_stream_reader.settings import NewTopicConfiguration
from tests.mocks.events import events, events_raw_bson
from tests.mocks.kafka import KafkaClientMock
from tests.mocks.mongo_client import ChangeStreamMock, MongoClientMock


@pytest.fixture
def change_stream() -> ChangeStreamMock:
    return ChangeStreamMock()


@pytest.fixture
def mongo_client(change_stream) -> MongoClientMock:
    return MongoClientMock(
        "test_url",
        change_stream=change_stream,
        document_class=RawBSONDocument
    )


@pytest.fixture
def fill_oplog_with_events_raw_bson(change_stream):
    change_stream.values = events_raw_bson()


@pytest.fixture
def fill_oplog_with_events(change_stream):
    change_stream.values = events_raw_bson()


@pytest.fixture
def token_mongo_client(change_stream) -> MongoClientMock:
    return MongoClientMock("test_url", change_stream=change_stream)


def new_queue():
    queue = Queue()
    try:
        yield queue
    finally:
        while True:
            try:
                queue.get_nowait()
            except Empty:
                break
        queue.close()


def create_producer_flow_application(
    producer_queue: Queue,
    committer_queue: Queue,
    kafka_client: KafkaClientMock,
) -> ProducerFlow:
    new_topic_config = NewTopicConfiguration()
    producer = Producer(
        kafka_client=kafka_client,  # type: ignore
        new_topic_configuration=new_topic_config,
    )
    change_event_handler = ChangeEventHandler(
        kafka_client=producer,
        committer_queue=committer_queue,
        kafka_prefix="test",
    )
    application = ProducerFlow(
        producer_queue=producer_queue,
        event_handler=change_event_handler,
        queue_get_timeout=0.001,
    )
    return application


@pytest.fixture
def committer_queue():
    yield from new_queue()


@pytest.fixture
def producer_queue_0():
    yield from new_queue()


@pytest.fixture
def producer_queue_1():
    yield from new_queue()


@pytest.fixture
def kafka_client_0():
    return KafkaClientMock(producer_config={}, admin_config={})


@pytest.fixture
def kafka_client_1():
    return KafkaClientMock(producer_config={}, admin_config={})


@pytest.fixture
def producer_flow_application_0(
    producer_queue_0, committer_queue, kafka_client_0
) -> ProducerFlow:
    return create_producer_flow_application(
        producer_queue_0, committer_queue, kafka_client_0
    )


@pytest.fixture
def producer_flow_application_1(
    producer_queue_1, committer_queue, kafka_client_1
) -> ProducerFlow:
    return create_producer_flow_application(
        producer_queue_1, committer_queue, kafka_client_1
    )


@pytest.fixture
def producer_queues(producer_queue_0, producer_queue_1):
    return {0: producer_queue_0, 1: producer_queue_1}


@pytest.fixture
def token_retriever(token_mongo_client) -> RetrieveResumeToken:
    return RetrieveResumeToken(
        stream_reader_name="test-stream-reader-name",
        token_mongo_client=token_mongo_client,  # type: ignore
        token_database="test-database",
        token_collection="SavedToken",
    )


@pytest.fixture
def change_stream_watcher(mongo_client) -> ChangeStreamWatch:
    watcher = ChangeStreamWatch(
        mongo_client=mongo_client,  # type: ignore
    )
    return watcher


@pytest.fixture
def change_handler(committer_queue, producer_queues):
    return ChangeHandler(
        committer_queue=committer_queue,
        producer_queues=producer_queues,
        queue_put_timeout=0.01,
    )


@pytest.fixture
def change_stream_reading_application(
    token_retriever,
    change_handler,
    change_stream_watcher
):
    return ChangeStreamReader(
        token_retriever=token_retriever,
        watcher=change_stream_watcher,
        change_handler=change_handler,
    )
