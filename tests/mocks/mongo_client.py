class ChangeStreamMock:
    def __init__(self):
        self.values = []
        self.token = None
        self._index = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...

    @property
    def try_next(self):
        new_index = self._index + 1
        min_length = new_index + 1
        if len(self.values) < min_length:
            raise StopIteration

        self._index = new_index
        return self.values[self._index]

    @property
    def resume_token(self):
        return self.token


class WatchMixin:
    _change_stream: ChangeStreamMock

    def watch(self, *args, **kwargs) -> ChangeStreamMock:
        return self._change_stream


class CollectionMock(WatchMixin):
    def __init__(self, name: str, change_stream: ChangeStreamMock):
        self.name = name
        self._change_stream = change_stream

    def find_one(self, *args, **kwargs):
        raise NotImplementedError

    def replace_one(self, *args, **kwargs):
        raise NotImplementedError


class DatabaseMock(WatchMixin):
    def __init__(self, name: str, change_stream: ChangeStreamMock):
        self.name = name
        self._change_stream = change_stream

    def get_collection(self, name: str):
        return CollectionMock(name, change_stream=self._change_stream)


class MongoClientMock(WatchMixin):
    def __init__(self, url: str, change_stream: ChangeStreamMock):
        self._url = url
        self._change_stream = change_stream

    def server_info(self):
        return {"test_server": "test_value"}

    def get_database(self, name: str):
        return DatabaseMock(name, change_stream=self._change_stream)
