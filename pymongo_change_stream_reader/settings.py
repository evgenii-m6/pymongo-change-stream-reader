import json
from enum import Enum

from pydantic import BaseModel

from bson import json_util


def convert_bson(value: str) -> dict:
    return json.loads(
        value,
        object_hook=json_util.object_hook,
    )


class Pipeline(BaseModel):
    pipeline: list[dict]


class FullDocumentBeforeChange(str, Enum):
    when_available = 'whenAvailable'
    required = 'required'


class FullDocument(str, Enum):
    when_available = 'whenAvailable'
    update_lookup = 'updateLookup'
    required = 'required'


class NewTopicConfiguration(BaseModel):
    new_topic_num_partitions: int
    new_topic_replication_factor: int
    new_topic_config: dict[str, str]


class Settings(BaseModel):
    stream_reader_name: str
    mongo_uri: str
    kafka_bootstrap_servers: str  # example: 'host1:9092,host2:9092'
    producers_count: int
    max_queue_size: int  # TODO: check that value 2 times more then self.producers_count
    token_mongo_uri: str
    token_database: str
    token_collection: str
    pipeline: str = None
    database: str | None = None
    collection: str | None = None
    full_document_before_change: FullDocumentBeforeChange = (
        FullDocumentBeforeChange.when_available
    )
    full_document: FullDocument = FullDocument.when_available
    reader_batch_size: int | None = None
    queue_put_timeout: int = 10
    queue_get_timeout: int = 1
    program_start_timeout: int = 60
    program_graceful_stop_timeout: int = 20
    commit_interval: int = 30
    max_uncommitted_events: int = 10000
    new_topic_num_partitions: int = 1
    new_topic_replication_factor: int = 1
    new_topic_config: str | None = None
    kafka_prefix: str = ""
    kafka_producer_config: str | None = None

    @property
    def cursor_pipeline(self) -> Pipeline:
        if self.pipeline is not None:
            return Pipeline(pipeline=convert_bson(self.pipeline))
        else:
            return Pipeline(pipeline=[])

    @property
    def new_topic_config_dict(self) -> dict[str, str]:
        if self.new_topic_config is not None:
            return json.loads(self.new_topic_config)
        else:
            return {}

    @property
    def kafka_producer_config_dict(self) -> dict[str, str]:
        if self.kafka_producer_config is not None:
            return json.loads(self.kafka_producer_config)
        else:
            return {}

    @property
    def new_topic_configuration(self) -> NewTopicConfiguration:
        return NewTopicConfiguration(
            new_topic_num_partitions=self.new_topic_num_partitions,
            new_topic_replication_factor=self.new_topic_replication_factor,
            new_topic_config=self.new_topic_config_dict
        )
