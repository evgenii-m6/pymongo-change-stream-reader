import bson
import pytest
from bson.raw_bson import RawBSONDocument

from tests.mocks.events import events_raw_bson, events, update, to_raw_bson, \
    get_resume_token_from_raw_event, replace, get_resume_token_from_event
from tests.mocks.mongo_client import ChangeStreamMock


@pytest.fixture
def raw_client(mongo_client, change_stream) -> ChangeStreamMock:
    raw_clients = [
        mongo_client.watch(),
        mongo_client.get_database().watch(),
        mongo_client.get_database().get_collection('TestCollection').watch()
    ]
    assert raw_clients[0] is raw_clients[1] is raw_clients[2] is change_stream
    raw_clients[0].values = events_raw_bson()
    return raw_clients[0]


@pytest.fixture
def client(token_mongo_client, change_stream) -> ChangeStreamMock:
    clients = [
        token_mongo_client.watch(),
        token_mongo_client.get_database().watch(),
        token_mongo_client.get_database().get_collection('TestCollection').watch()
    ]
    assert clients[0] is clients[1] is clients[2] is change_stream
    clients[0].values = events()
    return clients[0]


def test_get_database(mongo_client):
    db_test = mongo_client.get_database('test')
    db_default = mongo_client.get_database()

    assert mongo_client.get_database('test') is db_test
    assert mongo_client.get_database() is db_default
    assert mongo_client.get_database('test') is mongo_client._databases['test']
    assert mongo_client.get_database() is mongo_client._databases['test-database']


def test_get_collection(mongo_client):
    db = mongo_client.get_database()
    collection_test = db.get_collection('test')
    collection_default = db.get_collection('TestCollection')

    assert db.get_collection('test') is collection_test
    assert db.get_collection('TestCollection') is collection_default
    assert db.get_collection('test') is db._collections['test']
    assert db.get_collection('TestCollection') is db._collections['TestCollection']


@pytest.mark.parametrize("events_list", [events(), events_raw_bson()])
def test_try_next_raw_client(raw_client, events_list):
    raw_client.values = events_list
    with raw_client as stream:
        while stream.alive:
            next_value: RawBSONDocument = stream.try_next()
            if next_value is None:
                continue
            resume_token: RawBSONDocument = stream.resume_token
            assert bson.decode(next_value.raw)['_id'] == bson.decode(resume_token.raw)


@pytest.mark.parametrize("events_list", [events(), events_raw_bson()])
def test_try_next_client(client, events_list):
    client.values = events_list
    with client as stream:
        while stream.alive:
            next_value: dict = stream.try_next()
            if next_value is None:
                continue
            resume_token: dict = stream.resume_token
            assert next_value['_id'] == resume_token


def test_find_last_token(token_mongo_client):
    collection = token_mongo_client.get_database().get_collection("SavedToken")
    result = collection.find_one(
        filter={'stream_reader_name': 'test-stream-reader-name'}
    )
    assert result is not None
    assert result['token'] == get_resume_token_from_raw_event(to_raw_bson(update())).raw

    result_2 = collection.find_one(
        filter={}
    )
    assert result == result_2

    assert collection.find_one(
        filter={'stream_reader_name': 'test-stream-reader-name-2'}
    ) is None

    with pytest.raises(NotImplementedError):
        col = token_mongo_client.get_database().get_collection("Other")
        col.find_one(filter={})


def test_replace_token_upsert_false_do_nothing(token_mongo_client):
    collection = token_mongo_client.get_database().get_collection("SavedToken")
    collection._saved_tokens.clear()

    collection.replace_one(
        filter={'stream_reader_name': 'test-stream-reader-name-2'},
        replacement={"test_field": "test_value"},
        upsert=False
    )
    assert collection.find_one(
        filter={'stream_reader_name': 'test-stream-reader-name-2'}
    ) is None


def test_replace_token_upsert_false_successfully_replace(token_mongo_client):
    collection = token_mongo_client.get_database().get_collection("SavedToken")
    collection._saved_tokens.clear()

    collection.replace_one(
        filter={'stream_reader_name': 'test-stream-reader-name-2'},
        replacement={"test_field": "test_value"},
        upsert=True
    )

    collection.replace_one(
        filter={'stream_reader_name': 'test-stream-reader-name-2'},
        replacement={"test_field": "test_value2"},
        upsert=False
    )
    assert collection.find_one(
        filter={'stream_reader_name': 'test-stream-reader-name-2'}
    ) == {"test_field": "test_value2"}


def test_replace_token_upsert_true(token_mongo_client):
    collection = token_mongo_client.get_database().get_collection("SavedToken")
    collection._saved_tokens.clear()

    collection.replace_one(
        filter={'stream_reader_name': 'test-stream-reader-name-2'},
        replacement={"test_field": "test_value"},
        upsert=True
    )
    assert collection.find_one(
        filter={'stream_reader_name': 'test-stream-reader-name-2'}
    ) == {"test_field": "test_value"}


def test_replace_token_with_unknown_filter(token_mongo_client):
    collection = token_mongo_client.get_database().get_collection("SavedToken")
    collection._saved_tokens.clear()
    with pytest.raises(NotImplementedError):
        collection.replace_one(
            filter={'some_field': 'test-stream-reader-name-2'},
            replacement={"test_field": "test_value"},
            upsert=True
        )


def test_replace_token_with_unknown_collection(token_mongo_client):
    collection = token_mongo_client.get_database().get_collection("Test")
    collection._saved_tokens.clear()
    with pytest.raises(NotImplementedError):
        collection.replace_one(
            filter={'stream_reader_name': 'test-stream-reader-name-2'},
            replacement={"test_field": "test_value"},
            upsert=True
        )


def test_server_info(token_mongo_client, mongo_client):
    assert (
        token_mongo_client.server_info()
        == mongo_client.server_info()
        == {'version': '7.0.9'}
    )


def test_resume_after_raw_client(token_mongo_client, change_stream):
    change_stream.values = events_raw_bson()
    stream = token_mongo_client.watch()
    assert stream._index == -1
    token = get_resume_token_from_event(replace())
    stream = token_mongo_client.watch(resume_after=token)
    assert stream._index == 2
    with pytest.raises(Exception):
        stream = token_mongo_client.watch(resume_after={})
        assert stream is change_stream
    assert change_stream._index == 2


def test_resume_after_client(mongo_client, change_stream):
    change_stream.values = events()
    token = get_resume_token_from_event(replace())
    stream = mongo_client.watch()
    assert stream._index == -1
    stream = mongo_client.watch(resume_after=token)
    assert stream._index == 2
    with pytest.raises(Exception):
        stream = mongo_client.watch(resume_after={})
        assert stream is change_stream
    assert change_stream._index == 2