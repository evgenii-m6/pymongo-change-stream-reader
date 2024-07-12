import pytest

from pymongo_change_stream_reader.committing.commit_processing import ProcessCommitEvent


@pytest.fixture
def process_commit_event():
    test_commit = ProcessCommitEvent(1, 0)
    return test_commit


