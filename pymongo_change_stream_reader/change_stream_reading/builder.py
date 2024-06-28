from asyncio import Queue
from multiprocessing import Process

from .change_stream_reader import ChangeStreamReader
from pymongo_change_stream_reader.settings import Settings
from pymongo_change_stream_reader.utils import TaskIdGenerator, ProcessData


def build_change_stream_reader_process(
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
        'program_start_timeout': settings.program_start_timeout,
    }
    process = Process(target=run_change_stream_reader, kwargs=kwargs)
    return ProcessData(task_id=task_id, process=process, kwargs=kwargs)


def run_change_stream_reader(**kwargs):
    worker = ChangeStreamReader(**kwargs)
    worker.start()
    worker.run_task()
