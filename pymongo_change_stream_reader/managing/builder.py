from pymongo_change_stream_reader.settings import (
    Settings,
)
from .manager import Manager


def build_manager(settings: Settings) -> Manager:
    new_topic_configuration = settings.new_topic_configuration
    return Manager(
        settings=settings,
        pipeline=settings.cursor_pipeline,
        new_topic_configuration=new_topic_configuration
    )
