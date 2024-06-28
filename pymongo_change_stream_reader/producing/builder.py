from asyncio import Queue
from multiprocessing import Process

from .producer import ProducerFlow
from pymongo_change_stream_reader.settings import Settings, NewTopicConfiguration
from pymongo_change_stream_reader.utils import TaskIdGenerator, ProcessData


def build_producer_process(
    task_id_generator: TaskIdGenerator,
    producer_queue: Queue,
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    new_topic_configuration: NewTopicConfiguration,
    settings: Settings,
) -> ProcessData:
    task_id = task_id_generator.get()
    kwargs = {
        'task_id': task_id,
        'producer_queue': producer_queue,
        'request_queue': request_queue,
        'response_queue': response_queue,
        'committer_queue': committer_queue,
        'stream_reader_name': settings.stream_reader_name,
        'kafka_bootstrap_servers': settings.kafka_bootstrap_servers,
        'new_topic_configuration': new_topic_configuration.dict(),
        'kafka_prefix': settings.kafka_prefix,
        'queue_get_timeout': settings.queue_get_timeout,
        'queue_put_timeout': settings.queue_put_timeout,
        'program_start_timeout': settings.program_start_timeout,
    }
    process = Process(target=run_producer, kwargs=kwargs)
    return ProcessData(
        task_id=task_id,
        process=process,
        kwargs=kwargs
    )


def run_producer(**kwargs):
    worker = ProducerFlow(**kwargs)
    worker.start()
    worker.run_task()
