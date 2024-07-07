from asyncio import Queue
from multiprocessing import Process
from typing import Any

from confluent_kafka import Producer as KafkaProducer
from confluent_kafka.admin import AdminClient

from pymongo_change_stream_reader.app_context import ApplicationContext
from pymongo_change_stream_reader.base_worker import BaseWorker
from pymongo_change_stream_reader.models import ProcessData
from pymongo_change_stream_reader.settings import Settings, NewTopicConfiguration
from pymongo_change_stream_reader.utils import TaskIdGenerator
from .change_event_handler import ChangeEventHandler
from .producer import Producer
from .producer_flow import ProducerFlow


def build_producer_process(
    manager_pid: int,
    manager_create_time: float,
    task_id_generator: TaskIdGenerator,
    producer_queue: Queue,
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    new_topic_configuration: NewTopicConfiguration,
    settings: Settings,
    kafka_producer_config: dict,
) -> ProcessData:
    task_id = task_id_generator.get()
    kwargs = {
        'manager_pid': manager_pid,
        'manager_create_time': manager_create_time,
        'task_id': task_id,
        'producer_queue': producer_queue,
        'request_queue': request_queue,
        'response_queue': response_queue,
        'committer_queue': committer_queue,
        'stream_reader_name': settings.stream_reader_name,
        'kafka_bootstrap_servers': settings.kafka_bootstrap_servers,
        'new_topic_configuration': new_topic_configuration.dict(),
        'kafka_prefix': settings.kafka_prefix,
        'kafka_producer_config': kafka_producer_config,
        'queue_get_timeout': settings.queue_get_timeout,
        'queue_put_timeout': settings.queue_put_timeout,
    }
    process = Process(target=ProducerFlowContext.run_application, kwargs=kwargs)
    return ProcessData(
        task_id=task_id,
        process=process,
        kwargs=kwargs
    )


def build_producer_worker(
    manager_pid: int,
    manager_create_time: float,
    task_id: int,
    producer_queue: Queue,
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    stream_reader_name: str,
    kafka_bootstrap_servers: str,
    new_topic_configuration: dict[str, Any],
    kafka_prefix: str,
    kafka_producer_config: dict[str, str],
    queue_get_timeout: int,
    queue_put_timeout: int,
) -> BaseWorker:
    new_topic_configu = NewTopicConfiguration.parse_obj(new_topic_configuration)

    kafka_config = {}
    kafka_config.update(kafka_producer_config)
    kafka_config.update(
        {
            'bootstrap.servers': kafka_bootstrap_servers,  # Kafka broker address
            'client.id': f"producer_{stream_reader_name}_{task_id}"
        }
    )
    kafka_producer = KafkaProducer(kafka_producer_config)
    kafka_admin = AdminClient(
        {'bootstrap.servers': kafka_bootstrap_servers}
    )
    producer = Producer(
        kafka_producer=kafka_producer,
        kafka_admin=kafka_admin,
        new_topic_configuration=new_topic_configu,
    )
    change_event_handler = ChangeEventHandler(
        kafka_client=producer,
        committer_queue=committer_queue,
        kafka_prefix=kafka_prefix,
    )
    application = ProducerFlow(
        producer_queue=producer_queue,
        event_handler=change_event_handler,
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


class ProducerFlowContext(ApplicationContext):
    build_worker = staticmethod(build_producer_worker)
