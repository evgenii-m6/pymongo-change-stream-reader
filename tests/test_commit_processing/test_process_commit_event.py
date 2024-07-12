from copy import deepcopy

import pytest

from pymongo_change_stream_reader.committing.commit_processing import ProcessCommitEvent
from pymongo_change_stream_reader.models import CommitEvent
from pymongo_change_stream_reader.models import CommittableEvents


def test_is_need_commit_events_none(process_commit_event: ProcessCommitEvent):
    assert not process_commit_event._is_need_commit_events(None)
    commit = CommittableEvents(numbers=[1, 2, 3], resume_token=b"test")
    assert process_commit_event._is_need_commit_events(commit)
    commit = CommittableEvents(numbers=[1], resume_token=b"test")
    process_commit_event._commit_interval = 1300000
    assert not process_commit_event._is_need_commit_events(commit)
    commit = CommittableEvents(numbers=[1, 2], resume_token=b"test")
    assert process_commit_event._is_need_commit_events(commit)


def test_process_unconfirmed_event(process_commit_event: ProcessCommitEvent):
    event = CommitEvent(count=1, need_confirm=True, resume_token=b"test")
    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    process_commit_event._process_unconfirmed_event(event)
    assert event.count in process_commit_event._unconfirmed_events
    assert process_commit_event._unconfirmed_events[event.count] is event
    assert not process_commit_event._confirmed_events


@pytest.mark.parametrize(
    "old_token, new_token, actual_token",
    [
        (None, None, None),
        (b"test", None, b"test"),
        (None, b"test", b"test"),
        (b"test_1", b"test_2", b"test_2"),
    ],
)
def test_get_actual_token(
    process_commit_event: ProcessCommitEvent, old_token, new_token, actual_token
):
    assert actual_token == process_commit_event._get_actual_token(old_token, new_token)


def test_update_confirmed_events(process_commit_event: ProcessCommitEvent):
    event_unconfirmed = CommitEvent(count=1, need_confirm=True, resume_token=b"test")
    event_confirmed = CommitEvent(count=1, need_confirm=False, resume_token=b"test")
    process_commit_event._update_confirmed_events(event_unconfirmed)
    assert event_confirmed.count in process_commit_event._confirmed_events
    process_commit_event._process_unconfirmed_event(event_unconfirmed)
    assert process_commit_event._unconfirmed_events == {1: event_unconfirmed}
    process_commit_event._update_confirmed_events(deepcopy(event_confirmed))
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._confirmed_events == {1: event_confirmed}


def test_clear_previous_commit_events(process_commit_event: ProcessCommitEvent):
    events = CommittableEvents(numbers=[1, 2], resume_token=b"test")
    process_commit_event._confirmed_events = {
        1: CommitEvent(count=1, need_confirm=False, resume_token=b"test"),
        4: CommitEvent(count=4, need_confirm=False, resume_token=b"test"),
    }
    process_commit_event._unconfirmed_events = {
        2: CommitEvent(count=2, need_confirm=True, resume_token=b"test"),
        5: CommitEvent(count=5, need_confirm=True, resume_token=b"test"),
    }
    process_commit_event._clear_previous_commit_events(events)
    assert process_commit_event._confirmed_events == {
        4: CommitEvent(count=4, need_confirm=False, resume_token=b"test")
    }
    assert process_commit_event._unconfirmed_events == {
        5: CommitEvent(count=5, need_confirm=True, resume_token=b"test")
    }
    assert process_commit_event._last_sent_commit_event == 2


def test_get_committable_events(process_commit_event: ProcessCommitEvent):
    process_commit_event._confirmed_events = {
        1: CommitEvent(count=1, need_confirm=True, resume_token=b"test"),
        2: CommitEvent(count=2, need_confirm=True, resume_token=b"test_2"),
    }
    assert process_commit_event._get_committable_events() is None
    process_commit_event._confirmed_events = {
        1: CommitEvent(count=1, need_confirm=True, resume_token=b"test"),
        2: CommitEvent(count=2, need_confirm=False, resume_token=b"test_2"),
    }
    assert process_commit_event._get_committable_events() is None
    process_commit_event._confirmed_events = {
        1: CommitEvent(count=1, need_confirm=False, resume_token=b"test"),
        2: CommitEvent(count=2, need_confirm=False, resume_token=b"test_2"),
    }
    result = CommittableEvents([1, 2], resume_token=b"test_2")
    assert process_commit_event._get_committable_events() == result
    process_commit_event._confirmed_events = {
        2: CommitEvent(count=2, need_confirm=True, resume_token=b"test"),
    }
    assert process_commit_event._get_committable_events() is None
    process_commit_event._confirmed_events = {
        2: CommitEvent(count=2, need_confirm=False, resume_token=b"test"),
    }
    assert process_commit_event._get_committable_events() is None
