from pymongo_change_stream_reader.committing import CommitFlow
from .mongo import MongoClientMock


class MockCommitFlow(CommitFlow):
    _token_mongo_client_cls = MongoClientMock
