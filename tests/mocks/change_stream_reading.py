from pymongo_change_stream_reader.change_stream_reading import ChangeStreamReader
from .mongo import MongoClientMock


class MockChangeStreamReader(ChangeStreamReader):
    _mongo_client_cls = MongoClientMock
    _token_mongo_client_cls = MongoClientMock
