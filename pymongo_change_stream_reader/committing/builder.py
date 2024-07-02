from asyncio import Queue
from multiprocessing import Process

from .committer import CommitFlow
from pymongo_change_stream_reader.settings import Settings
from pymongo_change_stream_reader.utils import TaskIdGenerator
from pymongo_change_stream_reader.models import ProcessData


def build_commit_process(
    manager_pid: int,
    manager_create_time: float,
    task_id_generator: TaskIdGenerator,
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    settings: Settings,
) -> ProcessData:
    task_id = task_id_generator.get()
    kwargs = {
        'manager_pid': manager_pid,
        'manager_create_time': manager_create_time,
        'task_id': task_id,
        'request_queue': request_queue,
        'response_queue': response_queue,
        'committer_queue': committer_queue,
        'stream_reader_name': settings.stream_reader_name,
        'token_mongo_uri': settings.token_mongo_uri,
        'token_database': settings.token_database,
        'token_collection': settings.token_collection,
        'commit_interval': settings.commit_interval,
        'max_uncommitted_events': settings.max_uncommitted_events,
        'queue_get_timeout': settings.queue_get_timeout,
        'queue_put_timeout': settings.queue_put_timeout,
    }
    process = Process(target=run_commit_flow, kwargs=kwargs)
    return ProcessData(task_id=task_id, process=process, kwargs=kwargs)


def run_commit_flow(**kwargs):
    worker = CommitFlow(**kwargs)
    worker.run()
