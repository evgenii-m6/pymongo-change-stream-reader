from pydantic import BaseModel

from .utils import Statuses, check_command_format, ChangeStatuses


def serialize_message(command: BaseModel) -> dict:
    return dict(
        type=command.__class__.__name__,
        payload=command.dict()
    )


@check_command_format
def deserialize_message(command: dict) -> BaseModel:
    command_type = command["type"]
    payload = command["payload"]
    command_class = globals()[command_type]
    return command_class.parse_obj(payload)


class BaseMessage(BaseModel):
    task_id: int


class Status(BaseMessage):
    task_id: int
    status: Statuses


class ChangeStatus(BaseMessage):
    task_id: int
    status: ChangeStatuses
