from datetime import datetime
from unittest.mock import Mock, call

import pytest
from bson import Binary
from pymongo.errors import PyMongoError, NetworkTimeout

from pymongo_change_stream_reader.committing import TokenSaving
from pymongo_change_stream_reader.models import SavedToken


@pytest.fixture
def token_saver(token_mongo_client):
    class TokenSavingMock(TokenSaving):
        _save = Mock()

    saver = TokenSavingMock(
        token_mongo_client=token_mongo_client,
        token_database='test-database',
        token_collection='TestCollection'
    )
    return saver


def test_start(token_saver):
    token_saver.start()


def test_stop(token_saver):
    token_saver.stop()


def test_transform_to_dict(token_saver):
    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()
    assert token_saver._transform_to_dict(
        SavedToken(
            stream_reader_name=stream_reader_name,
            token=token,
            date=dt,
        )
    ) == {
       'stream_reader_name': stream_reader_name,
       'token': Binary(token, subtype=0),
       'date': dt
    }


def test_normal_save(token_saver):
    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()

    saved_token = SavedToken(
        stream_reader_name=stream_reader_name,
        token=token,
        date=dt,
    )
    replacement = token_saver._transform_to_dict(saved_token)

    token_saver.save(saved_token)
    token_saver._save.assert_called_once_with(replacement=replacement, count=0)


def test_cannot_save(token_saver):
    token_saver._save.side_effect = PyMongoError()

    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()

    saved_token = SavedToken(
        stream_reader_name=stream_reader_name,
        token=token,
        date=dt,
    )
    replacement = token_saver._transform_to_dict(saved_token)
    with pytest.raises(PyMongoError) as ex:
        token_saver.save(saved_token)

    token_saver._save.assert_called_once_with(replacement=replacement, count=0)


def test_timeout_when_save(token_saver):
    token_saver._save.side_effect = NetworkTimeout()

    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()

    saved_token = SavedToken(
        stream_reader_name=stream_reader_name,
        token=token,
        date=dt,
    )
    replacement = token_saver._transform_to_dict(saved_token)
    with pytest.raises(NetworkTimeout) as ex:
        token_saver.save(saved_token)

    assert token_saver._save.call_count == 3
    token_saver._save.assert_has_calls(
        [
            call(replacement=replacement, count=0),
            call(replacement=replacement, count=1),
            call(replacement=replacement, count=2),
        ],
    )


def test_save_from_second_try(token_saver):
    def _save(replacement, count):
        if count >= 1:
            return
        else:
            raise NetworkTimeout()

    token_saver._save.side_effect = _save

    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()

    saved_token = SavedToken(
        stream_reader_name=stream_reader_name,
        token=token,
        date=dt,
    )
    replacement = token_saver._transform_to_dict(saved_token)
    token_saver.save(saved_token)

    assert token_saver._save.call_count == 2
    token_saver._save.assert_has_calls(
        [
            call(replacement=replacement, count=0),
            call(replacement=replacement, count=1),
        ],
    )
