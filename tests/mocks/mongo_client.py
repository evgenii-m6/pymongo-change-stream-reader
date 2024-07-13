from datetime import datetime
from typing import Type

from bson.raw_bson import RawBSONDocument

from pymongo_change_stream_reader.models import SavedToken
from .events import (
    get_resume_token_from_raw_event,
    get_resume_token_from_event,
    to_raw_bson, from_raw_bson, update
)


class ChangeStreamMock:
    def __init__(self):
        self.values: list[dict | RawBSONDocument] = []
        self.token = None
        self._index = -1
        self.document_class: Type[RawBSONDocument] | None = None
        self._is_alive = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...

    def _set_resume_after(self, resume_after: dict):
        is_found = False
        for idx, value in enumerate(self.values):
            if isinstance(value, RawBSONDocument):
                decoded = from_raw_bson(value)
            else:
                decoded = value
            token = get_resume_token_from_event(decoded)
            if resume_after == token:
                self._index = idx
                is_found = True
                break

        if not is_found:
            raise Exception("Not found place on oplog")

    @property
    def alive(self) -> bool:
        return self._is_alive

    def try_next(self) -> dict | RawBSONDocument | None:
        new_index = self._index + 1
        min_length = new_index + 1
        if len(self.values) < min_length:
            self._is_alive = False
            return None

        self._index = new_index
        next_value = self.values[self._index]
        if self.document_class is RawBSONDocument:
            if not isinstance(next_value, RawBSONDocument):
                return to_raw_bson(next_value)
            else:
                return next_value
        else:
            if isinstance(next_value, RawBSONDocument):
                return from_raw_bson(next_value)
            else:
                return next_value

    @property
    def resume_token(self) -> dict | RawBSONDocument:
        next_value = self.values[self._index]
        if self.document_class is RawBSONDocument:
            if not isinstance(next_value, RawBSONDocument):
                return to_raw_bson(get_resume_token_from_event(next_value))
            else:
                return get_resume_token_from_raw_event(next_value)
        else:
            if isinstance(next_value, RawBSONDocument):
                return get_resume_token_from_event(from_raw_bson(next_value))
            else:
                return get_resume_token_from_event(next_value)


class WatchMixin:
    _change_stream: ChangeStreamMock

    def watch(self, *args, resume_after=None, **kwargs, ) -> ChangeStreamMock:
        if resume_after is not None:
            self._change_stream._set_resume_after(resume_after)
        return self._change_stream


class CollectionMock(WatchMixin):
    def __init__(
        self,
        name: str,
        change_stream: ChangeStreamMock,
    ):
        self.name = name
        self._change_stream = change_stream
        self._saved_tokens = {
            'test-stream-reader-name': SavedToken(
                stream_reader_name='test-stream-reader-name',
                token=to_raw_bson(get_resume_token_from_event(update())).raw,
                date=datetime(
                    year=2024, month=7, day=13,
                    hour=17, minute=11, second=58,
                    microsecond=473000
                )
            ).dict()
        }

    def find_one(self, filter: dict) -> dict | None:
        if self.name == "SavedToken":
            if 'stream_reader_name' in filter:
                return self._saved_tokens.get(filter['stream_reader_name'])
            else:
                if self._saved_tokens:
                    return list(self._saved_tokens.values()).pop()
                else:
                    return None
        raise NotImplementedError

    def replace_one(
        self,
        filter: dict,
        replacement: dict,
        upsert: bool = False
    ):
        if self.name == "SavedToken":
            if 'stream_reader_name' in filter:
                stream_reader_name = filter['stream_reader_name']
                if upsert:
                    self._saved_tokens[stream_reader_name] = replacement
                    return
                else:
                    if stream_reader_name in self._saved_tokens:
                        self._saved_tokens[stream_reader_name] = replacement
                        return
                    else:
                        return
        raise NotImplementedError


class DatabaseMock(WatchMixin):
    def __init__(
        self,
        name: str,
        change_stream: ChangeStreamMock,
    ):
        self.name = name
        self._change_stream = change_stream
        self._collections: dict[str, CollectionMock] = {}

    def get_collection(self, name: str):
        if name not in self._collections:
            self._collections[name] = CollectionMock(
                name,
                change_stream=self._change_stream
            )
        return self._collections[name]


class MongoClientMock(WatchMixin):
    def __init__(
        self,
        url: str,
        change_stream: ChangeStreamMock,
        document_class: Type[RawBSONDocument] | None = None,
    ):
        self._url = url
        self._change_stream = change_stream
        self._document_class = document_class
        self._change_stream.document_class = self._document_class
        self._databases: dict[str, DatabaseMock] = {}

    def server_info(self):
        return {'version': '7.0.9'}

    def get_database(self, name: str = 'test-database'):
        if name not in self._databases:
            self._databases[name] = DatabaseMock(
                name,
                change_stream=self._change_stream
            )
        return self._databases[name]

    def close(self):
        ...
