import bson

from tests.mocks.events import events_raw_bson, events


def test_encode_decode():
    decoded = [bson.decode(i.raw) for i in events_raw_bson()]
    assert events() == decoded
