from unittest.mock import Mock

import pytest

from pymongo_change_stream_reader.committing import CommitEventHandler
from pymongo_change_stream_reader.models import CommitEvent, SavedToken


class SaverMock(Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.save = Mock()

    def get_saved_token_by_call_index(self, index: int):
        return self.save.call_args_list[index].args[0]


@pytest.fixture
def token_saver():
    return SaverMock()


@pytest.fixture
def stream_reader_name():
    return 'test'


@pytest.fixture
def commit_event_handler(process_commit_event, token_saver, stream_reader_name):
    return CommitEventHandler(
        stream_reader_name=stream_reader_name,
        commit_event_processor=process_commit_event,
        token_saver=token_saver,
    )


def test_confirmed_confirmed(commit_event_handler, token_saver, stream_reader_name):
    count = 1
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=False, resume_token=b"test2")

    commit_event_handler.handle_event(event_1)
    commit_event_handler.handle_event(event_2)

    token_saver.save.assert_called_once()
    saved_token = token_saver.get_saved_token_by_call_index(0)

    assert saved_token.stream_reader_name == stream_reader_name
    assert saved_token.token == event_1.resume_token
