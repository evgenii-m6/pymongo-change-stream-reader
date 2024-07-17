import pytest

from tests.mocks.events import replace, invalidate, get_resume_token_from_event


@pytest.mark.skip("Only for manual use")
def test_smoke_change_stream_reading_application(
    change_stream_reading_application,
    producer_queue_1,
    producer_queue_0,
    committer_queue,
    fill_oplog_with_events_raw_bson,
    producer_flow_application_0,
    producer_flow_application_1,
    kafka_client_0,
    kafka_client_1,
    commit_flow,
    process_commit_event,
    token_retriever,
):
    change_stream_reading_application.start()
    change_stream_reading_application.task()
    producer_flow_application_0.start()
    producer_flow_application_1.start()
    commit_flow.start()

    assert producer_queue_0.qsize() == 0
    assert producer_queue_1.qsize() == 2
    assert committer_queue.qsize() == 5

    for i in range(5):
        commit_flow._get_change_event_and_process()

    assert len(process_commit_event._confirmed_events) == 3
    assert len(process_commit_event._unconfirmed_events) == 2

    producer_flow_application_1._get_change_event_and_process()
    assert 1 == len(kafka_client_1.produced["test.test-database.TestCollection"])

    commit_flow._get_change_event_and_process()
    assert len(process_commit_event._confirmed_events) == 3
    assert len(process_commit_event._unconfirmed_events) == 1

    token = token_retriever.get_token()
    assert token == get_resume_token_from_event(replace())

    producer_flow_application_1._get_change_event_and_process()
    assert 2 == len(kafka_client_1.produced["test.test-database.TestCollection"])
    producer_flow_application_1._get_change_event_and_process()
    assert 2 == len(kafka_client_1.produced["test.test-database.TestCollection"])
    producer_flow_application_0._get_change_event_and_process()
    assert not kafka_client_0.produced

    commit_flow._get_change_event_and_process()
    assert len(process_commit_event._confirmed_events) == 0
    assert len(process_commit_event._unconfirmed_events) == 0

    token = token_retriever.get_token()
    assert token == get_resume_token_from_event(invalidate())
