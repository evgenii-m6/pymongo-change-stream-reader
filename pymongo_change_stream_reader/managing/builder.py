from pymongo_change_stream_reader.settings import (
    Settings,
)
from pymongo_change_stream_reader.change_stream_reading import ChangeStreamReaderContext
from pymongo_change_stream_reader.committing import CommitFlowContext
from pymongo_change_stream_reader.producing import ProducerFlowContext
from .manager import Manager


def build_manager(settings: Settings) -> Manager:
    return Manager(
        change_stream_reader=ChangeStreamReaderContext,
        producing=ProducerFlowContext,
        committing=CommitFlowContext,
        settings=settings,
    )
