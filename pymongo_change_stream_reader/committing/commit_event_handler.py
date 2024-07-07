import logging
from datetime import datetime

from pymongo_change_stream_reader.models import (
    SavedToken,
    CommitEvent,
    CommittableEvents, RecheckCommitEvent,
)
from .commit_processing import ProcessCommitEvent
from .token_saver import TokenSaving


default_logger = logging.Logger(__name__, logging.INFO)


class CommitEventHandler:
    def __init__(
        self,
        stream_reader_name: str,
        commit_event_processor: ProcessCommitEvent,
        token_saver: TokenSaving,
        logger: logging.Logger = default_logger,
    ):
        self._stream_reader_name = stream_reader_name
        self._commit_event_processor = commit_event_processor
        self._token_saver = token_saver
        self._logger = logger

    def start(self):
        self._token_saver.start()

    def stop(self):
        self._token_saver.stop()

    def handle_recheck_event(self, event: RecheckCommitEvent):
        for committable_event in self._commit_event_processor.process_recheck_event(
            event
        ):
            self._commit_events(committable_event)

    def handle_commit_event(self, event: CommitEvent):
        for committable_event in self._commit_event_processor.process_event(event):
            self._commit_events(committable_event)

    def _commit_events(self, commit_events: CommittableEvents):
        self._logger.info(f"Process commit events: {commit_events}")
        token_data = self._build_token_model(commit_events)
        self._logger.info(f"Save token data to db: {token_data}")
        self._save_resume_token_to_db(token_data)

    def _build_token_model(self, events: CommittableEvents) -> SavedToken:
        return SavedToken(
            stream_reader_name=self._stream_reader_name,
            token=events.resume_token,
            date=datetime.utcnow()
        )

    def _save_resume_token_to_db(self, token_data: SavedToken):
        self._token_saver.save(token_data)
