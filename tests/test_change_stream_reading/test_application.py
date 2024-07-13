def test_smoke_change_stream_reading_application(
    change_stream_reading_application,
    producer_queue_1,
    producer_queue_0,
    committer_queue,
    fill_oplog_with_events_raw_bson,
):
    change_stream_reading_application.start()
    change_stream_reading_application.task()
