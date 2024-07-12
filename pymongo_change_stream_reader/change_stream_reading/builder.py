from multiprocessing import Process, Queue

from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

from pymongo_change_stream_reader.app_context import ApplicationContext
from pymongo_change_stream_reader.base_worker import BaseWorker
from pymongo_change_stream_reader.models import ProcessData
from pymongo_change_stream_reader.settings import (
    FullDocumentBeforeChange,
    FullDocument,
)
from pymongo_change_stream_reader.settings import Settings
from pymongo_change_stream_reader.utils import TaskIdGenerator
from .change_handler import ChangeHandler
from .change_stream_reader import ChangeStreamReader
from .resume_token import RetrieveResumeToken
from .watch import ChangeStreamWatch


def build_change_stream_reader_process(
    manager_pid: int,
    manager_create_time: float,
    task_id_generator: TaskIdGenerator,
    producer_queues: dict[int, Queue],
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    pipeline: list[dict],
    settings: Settings,
) -> ProcessData:
    task_id = task_id_generator.get()
    kwargs = {
        'manager_pid': manager_pid,
        'manager_create_time': manager_create_time,
        'task_id': task_id,
        'producer_queues': producer_queues,
        'request_queue': request_queue,
        'response_queue': response_queue,
        'committer_queue': committer_queue,
        'mongo_uri': settings.mongo_uri,
        'token_mongo_uri': settings.token_mongo_uri,
        'token_database': settings.token_database,
        'token_collection': settings.token_collection,
        'stream_reader_name': settings.stream_reader_name,
        'database': settings.database,
        'collection': settings.collection,
        'pipeline': pipeline,
        'full_document_before_change': settings.full_document_before_change.value,
        'full_document': settings.full_document.value,
        'reader_batch_size': settings.reader_batch_size,
        'queue_get_timeout': settings.queue_get_timeout,
        'queue_put_timeout': settings.queue_put_timeout,
    }
    process = Process(target=ChangeStreamReaderContext.run_application, kwargs=kwargs)
    return ProcessData(task_id=task_id, process=process, kwargs=kwargs)


def build_change_stream_reader_worker(
    manager_pid: int,
    manager_create_time: float,
    task_id: int,
    producer_queues: dict[int, Queue],
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    mongo_uri: str,
    token_mongo_uri: str,
    token_database: str,
    token_collection: str,
    stream_reader_name: str,
    database: str | None = None,
    collection: str | None = None,
    pipeline: list[dict] | None = None,
    full_document_before_change: FullDocumentBeforeChange = (
            FullDocumentBeforeChange.when_available
    ),
    full_document: FullDocument = FullDocument.when_available,
    reader_batch_size: int | None = None,
    queue_get_timeout: int = 1,
    queue_put_timeout: int = 10,
) -> BaseWorker:
    token_mongo_client = MongoClient(
        host=token_mongo_uri,
    )
    mongo_client = MongoClient(
        host=mongo_uri,
        document_class=RawBSONDocument
    )
    token_retriever = RetrieveResumeToken(
        stream_reader_name=stream_reader_name,
        token_mongo_client=token_mongo_client,
        token_database=token_database,
        token_collection=token_collection,
    )
    watcher = ChangeStreamWatch(
        mongo_client=mongo_client,
        collection=collection,
        database=database,
        pipeline=pipeline,
        full_document_before_change=full_document_before_change,
        full_document=full_document,
        reader_batch_size=reader_batch_size,
    )
    change_handler = ChangeHandler(
        committer_queue=committer_queue,
        producer_queues=producer_queues,
        queue_put_timeout=queue_put_timeout,
    )
    application = ChangeStreamReader(
        token_retriever=token_retriever,
        watcher=watcher,
        change_handler=change_handler,
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


class ChangeStreamReaderContext(ApplicationContext):
    build_worker = staticmethod(build_change_stream_reader_worker)
