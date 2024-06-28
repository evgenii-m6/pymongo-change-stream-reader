from datetime import datetime
from typing import NamedTuple, Any

from bson import RawBSONDocument
from pydantic import BaseModel


class SavedToken(BaseModel):
    stream_reader_name: str
    token: bytes
    date: datetime


class CommitEvent(NamedTuple):
    count: int
    need_confirm: bool
    resume_token: bytes | None


class CommittableEvents(NamedTuple):
    numbers: list[int]
    resume_token: bytes


class ChangeEvent(NamedTuple):
    bson_document: RawBSONDocument | None
    token: bytes | None
    count: int


class DecodedChangeEvent(NamedTuple):
    bson_document: dict[str, Any]
    count: int
