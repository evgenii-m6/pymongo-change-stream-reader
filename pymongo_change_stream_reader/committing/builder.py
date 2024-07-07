from asyncio import Queue
from multiprocessing import Process

from pymongo import MongoClient

from pymongo_change_stream_reader.settings import Settings
from pymongo_change_stream_reader.utils import TaskIdGenerator
from pymongo_change_stream_reader.models import ProcessData
from pymongo_change_stream_reader.app_context import ApplicationContext
from pymongo_change_stream_reader.base_worker import BaseWorker
from .commit_processing import ProcessCommitEvent
from .commit_event_handler import CommitEventHandler
from .commit_flow import CommitFlow
from .token_saver import TokenSaving


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
    process = Process(target=CommitFlowContext.run_application, kwargs=kwargs)
    return ProcessData(task_id=task_id, process=process, kwargs=kwargs)


def build_commit_worker(
    manager_pid: int,
    manager_create_time: float,
    task_id: int,
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    stream_reader_name: str,
    token_mongo_uri: str,
    token_database: str,
    token_collection: str,
    commit_interval: int,
    max_uncommitted_events: int,
    queue_get_timeout: int,
    queue_put_timeout: int,
) -> BaseWorker:
    token_mongo_client = MongoClient(host=token_mongo_uri)
    commit_event_processor = ProcessCommitEvent(
        max_uncommitted_events=max_uncommitted_events,
        commit_interval=commit_interval,
    )
    token_saver = TokenSaving(
        token_mongo_client=token_mongo_client,
        token_database=token_database,
        token_collection=token_collection,
    )
    commit_event_handler = CommitEventHandler(
        stream_reader_name=stream_reader_name,
        commit_event_processor=commit_event_processor,
        token_saver=token_saver,
    )
    application = CommitFlow(
        committer_queue=committer_queue,
        commit_event_handler=commit_event_handler,
        queue_get_timeout=queue_get_timeout,
    )
    worker = BaseWorker(
        manager_pid=manager_pid,
        manager_create_time=manager_create_time,
        task_id=task_id,
        request_queue=request_queue,
        response_queue=response_queue,
        queue_get_timeout=queue_get_timeout,
        queue_put_timeout=queue_put_timeout,
        application=application,
    )
    return worker


class CommitFlowContext(ApplicationContext):
    build_worker = staticmethod(build_commit_worker)
