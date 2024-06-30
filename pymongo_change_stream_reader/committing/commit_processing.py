import time
from typing import Iterator

from pymongo_change_stream_reader.models import CommitEvent, CommittableEvents


class ProcessCommitEvent:
    def __init__(self, max_uncommitted_events: int, commit_interval: int) -> None:
        self._confirmed_events: dict[int, CommitEvent] = dict()
        self._unconfirmed_events: dict[int, CommitEvent] = dict()
        self._last_sent_commit_event = 0
        self._last_commit_time: float = time.monotonic()
        self._max_uncommitted_events = max_uncommitted_events
        self._commit_interval = commit_interval

    def process_event(self, event: CommitEvent) -> Iterator[CommittableEvents]:
        if event.need_confirm:
            self._process_unconfirmed_event(event)
        else:
            yield from self._process_confirmed_event(event)

    def _process_unconfirmed_event(self, event: CommitEvent) -> None:
        self._unconfirmed_events[event.count] = event

    def _process_confirmed_event(
        self,
        event: CommitEvent
    ) -> Iterator[CommittableEvents]:
        self._update_confirmed_events(event)
        commit_events = self._get_committable_events(event.count)
        if self._is_need_commit_events(commit_events):
            yield commit_events  # type: ignore
            self._clear_previous_commit_events(commit_events)

    def _update_confirmed_events(self, event: CommitEvent) -> None:
        if event.count in self._unconfirmed_events:
            token = self._get_actual_token(
                unconfirmed_event=self._unconfirmed_events[event.count],
                confirmed_event=event,
            )
            self._confirmed_events[event.count] = CommitEvent(
                count=event.count,
                need_confirm=False,
                resume_token=token
            )
            del self._unconfirmed_events[event.count]
        else:
            self._confirmed_events[event.count] = event

    @staticmethod
    def _get_actual_token(
        unconfirmed_event: CommitEvent,
        confirmed_event: CommitEvent
    ) -> bytes | None:
        unconfirmed_token = unconfirmed_event.resume_token
        confirmed_token = confirmed_event.resume_token
        if unconfirmed_token and not confirmed_token:
            return unconfirmed_token
        elif not unconfirmed_token and not confirmed_token:
            return None
        elif not unconfirmed_token and confirmed_token:
            return confirmed_token
        elif unconfirmed_token and confirmed_token:
            return confirmed_token

    def _get_committable_events(self, count: int) -> CommittableEvents | None:
        confirmed_numbers_with_tokens: list[int] = []
        for i in range(self._last_sent_commit_event+1, count):
            event: CommitEvent
            if event := self._confirmed_events.get(i):
                if event.resume_token:
                    confirmed_numbers_with_tokens.append(event.count)

        if confirmed_numbers_with_tokens:
            last_number = confirmed_numbers_with_tokens[-1]
            event = self._confirmed_events[last_number]
            return CommittableEvents(
                numbers=list(range(self._last_sent_commit_event+1, last_number+1)),
                resume_token=event.resume_token
            )
        else:
            return None

    def _is_need_commit_events(self, events: CommittableEvents | None) -> bool:
        if time.monotonic() - self._last_commit_time > self._commit_interval:
            return True
        if len(events.numbers) > self._max_uncommitted_events:
            return True
        if events is None:
            return False
        return False

    def _clear_previous_commit_events(self, events: CommittableEvents) -> None:
        for number in events.numbers:
            if number in self._confirmed_events:
                del self._confirmed_events[number]
            if number in self._unconfirmed_events:
                del self._unconfirmed_events[number]
            self._last_sent_commit_event = number
