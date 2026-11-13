# pylint: skip-file
"""Tests for the explicit processing-completion mode
(enable_processing_completion)."""
import threading
from types import SimpleNamespace

import pytest

import kafka.errors as Errors
from kafka import KafkaConsumer, TopicPartition
from kafka.consumer.completion import ProcessingCompletionTracker
from kafka.consumer.fetcher import ConsumerRecord
from kafka.consumer.subscription_state import SubscriptionState
from kafka.structs import OffsetAndMetadata


@pytest.fixture
def subscription():
    return SubscriptionState()


@pytest.fixture
def tracker():
    return ProcessingCompletionTracker()


def assign(subscription, tracker, *tps):
    if tracker is not None and tracker not in subscription.assignment_listeners:
        subscription.add_assignment_listener(tracker)
    subscription.assign_from_user(list(tps))
    for tp in tps:
        # establish an initial valid position (no pending state yet)
        subscription.seek(tp, 0)


def set_position(subscription, tp, offset):
    """Advance the fetch position like the fetcher would (no seek/reset)."""
    subscription.assignment[tp].position = OffsetAndMetadata(offset, '', None)


def test_out_of_order_completion(subscription, tracker):
    tp = TopicPartition('topic', 0)
    assign(subscription, tracker, tp)
    for offset in (0, 1, 2):
        tracker.note_delivered(tp, offset)
    set_position(subscription, tp, 3)

    assert tracker.complete(tp, 1) is True
    assert tracker.committable_offsets(subscription)[tp].offset == 0
    assert tracker.complete(tp, 0) is True
    assert tracker.committable_offsets(subscription)[tp].offset == 2
    assert tracker.complete(tp, 2) is True
    # nothing pending: committable is the fetch position
    assert tracker.committable_offsets(subscription)[tp].offset == 3


def test_compaction_gap_is_not_awaited(subscription, tracker):
    """10 and 12 delivered, 11 never exists (compacted / control record):
    completing 10 must advance the committable position to 12 without
    waiting for the nonexistent 11."""
    tp = TopicPartition('topic', 0)
    assign(subscription, tracker, tp)
    tracker.note_delivered(tp, 10)
    tracker.note_delivered(tp, 12)
    set_position(subscription, tp, 13)

    assert tracker.complete(tp, 10) is True
    assert tracker.committable_offsets(subscription)[tp].offset == 12
    assert tracker.complete(tp, 12) is True
    assert tracker.committable_offsets(subscription)[tp].offset == 13


def test_uncompleted_head_blocks_commit(subscription, tracker):
    """Same deliveries as above, but 10 is not yet completed: the
    committable position must stay at 10 even though 12 completed."""
    tp = TopicPartition('topic', 0)
    assign(subscription, tracker, tp)
    tracker.note_delivered(tp, 10)
    tracker.note_delivered(tp, 12)
    set_position(subscription, tp, 13)

    assert tracker.complete(tp, 12) is True
    assert tracker.committable_offsets(subscription)[tp].offset == 10


def test_duplicate_completion_ignored(subscription, tracker):
    tp = TopicPartition('topic', 0)
    assign(subscription, tracker, tp)
    tracker.note_delivered(tp, 5)
    set_position(subscription, tp, 6)

    assert tracker.complete(tp, 5) is True
    assert tracker.complete(tp, 5) is False
    # never-delivered offsets are no-ops too
    assert tracker.complete(tp, 42) is False
    assert tracker.committable_offsets(subscription)[tp].offset == 6


def test_seek_discards_pending(subscription, tracker):
    tp = TopicPartition('topic', 0)
    assign(subscription, tracker, tp)
    tracker.note_delivered(tp, 0)
    tracker.note_delivered(tp, 1)

    subscription.seek(tp, 10)
    assert tracker.pending_offsets(tp) == set()
    # completions for pre-seek deliveries no longer count
    assert tracker.complete(tp, 0) is False
    assert tracker.committable_offsets(subscription)[tp].offset == 10


def test_revoke_and_reassign_starts_new_epoch(subscription, tracker):
    tp = TopicPartition('topic', 0)
    assign(subscription, tracker, tp)
    tracker.note_delivered(tp, 5)
    old_epoch = tracker.current_epoch(tp)

    # revoke, then reassign
    subscription.assign_from_user([])
    assert tracker.pending_offsets(tp) == set()
    subscription.assign_from_user([tp])
    new_epoch = tracker.current_epoch(tp)
    assert new_epoch > old_epoch

    # the same offset is redelivered under the new assignment
    subscription.seek(tp, 5)
    tracker.note_delivered(tp, 5)
    set_position(subscription, tp, 6)
    redelivery_epoch = tracker.current_epoch(tp)
    assert redelivery_epoch > old_epoch

    # a completion notification from the old assignment must not
    # complete the redelivery
    assert tracker.complete(tp, 5, epoch=old_epoch) is False
    assert tracker.pending_offsets(tp) == {5}
    assert tracker.committable_offsets(subscription)[tp].offset == 5
    # the current-epoch completion works
    assert tracker.complete(tp, 5, epoch=redelivery_epoch) is True
    assert tracker.committable_offsets(subscription)[tp].offset == 6


def test_stale_completion_after_revoke_is_noop(subscription, tracker):
    tp = TopicPartition('topic', 0)
    assign(subscription, tracker, tp)
    tracker.note_delivered(tp, 7)
    subscription.assign_from_user([])
    subscription.assign_from_user([tp])
    subscription.seek(tp, 3)

    # offset 7 was delivered only under the old assignment
    assert tracker.complete(tp, 7) is False
    assert tracker.committable_offsets(subscription)[tp].offset == 3


def test_unsubscribe_revokes_all(subscription, tracker):
    tp = TopicPartition('topic', 0)
    assign(subscription, tracker, tp)
    tracker.note_delivered(tp, 0)
    subscription.unsubscribe()
    assert tracker.pending_offsets(tp) == set()
    assert tracker.complete(tp, 0) is False


def test_delivery_without_assignment_ignored(tracker):
    tp = TopicPartition('topic', 0)
    assert tracker.note_delivered(tp, 0) is False
    assert tracker.complete(tp, 0) is False


# ---------------------------------------------------------------------------
# KafkaConsumer integration: fake fetch + fake coordinator, verifying the
# positions carried by the actual OffsetCommitRequest.
# ---------------------------------------------------------------------------

TOPIC = 'topic'
TP0 = TopicPartition(TOPIC, 0)
TP1 = TopicPartition(TOPIC, 1)


def make_record(tp, offset):
    return ConsumerRecord(tp.topic, tp.partition, None, offset, None, None,
                          None, b'value', [], None, -1, 5, -1)


def install_fake_poll(mocker, consumer, batches):
    """Replace _poll_once with a fake fetch: each batch is a dict
    {TopicPartition: [offset, ...]} returned by one poll() call, advancing
    the partition position like the real fetcher would."""
    remaining = list(batches)

    def fake_poll_once(timer, max_records, update_offsets=True):
        if not remaining:
            return {}
        batch = remaining.pop(0)
        records = {}
        for tp, offsets in batch.items():
            records[tp] = [make_record(tp, offset) for offset in offsets]
            if update_offsets:
                state = consumer._subscription.assignment[tp]
                state.position = OffsetAndMetadata(offsets[-1] + 1, '', None)
        return records

    mocker.patch.object(consumer, '_poll_once',
                        side_effect=fake_poll_once)


@pytest.fixture
def commit_requests(mocker):
    """Capture the actual OffsetCommitRequest structs sent by the
    consumer's coordinator, faking a successful broker response."""
    captured = []
    sent = threading.Event()

    def install(consumer):
        coordinator = consumer._coordinator
        mocker.patch.object(coordinator, 'coordinator', return_value=0)

        async def fake_send(request, node_id=None, request_timeout_ms=None):
            captured.append(request)
            sent.set()
            return SimpleNamespace(topics=[
                (topic.name, [(p.partition_index, 0) for p in topic.partitions])
                for topic in request.topics
            ])

        mocker.patch.object(coordinator._manager, 'send', new=fake_send)
        return captured

    install.sent = sent
    return install


def committed_positions(request):
    return {
        TopicPartition(topic.name, partition.partition_index):
            partition.committed_offset
        for topic in request.topics for partition in topic.partitions
    }


@pytest.fixture
def consumer():
    consumer = KafkaConsumer(
        api_version=(2, 5, 0),
        group_id='completion-test',
        enable_auto_commit=False,
        enable_processing_completion=True,
    )
    try:
        yield consumer
    finally:
        consumer.close(autocommit=False)


def test_interleaved_partitions_commit_request_positions(mocker, consumer,
                                                         commit_requests):
    captured = commit_requests(consumer)
    consumer.assign([TP0, TP1])
    consumer._subscription.seek(TP0, 10)
    consumer._subscription.seek(TP1, 100)

    # Interleaved delivery across two partitions. Offset 11 of partition 0
    # does not exist (compaction gap); it is never delivered.
    install_fake_poll(mocker, consumer, [
        {TP0: [10, 12], TP1: [100]},
        {TP0: [13], TP1: [101, 102]},
    ])
    records = consumer.poll(timeout_ms=0)
    assert [r.offset for r in records[TP0]] == [10, 12]
    records2 = consumer.poll(timeout_ms=0)
    assert [r.offset for r in records2[TP1]] == [101, 102]

    # Everything except TP0@10 completes (out of order across partitions)
    for record in records[TP1] + records2[TP1]:
        assert consumer.complete(record) is True
    assert consumer.complete(TP0, 13) is True

    # TP0@10 is still being processed: the commit must not pass it, while
    # TP1 is fully drained up to its fetch position.
    assert consumer.committable_offsets()[TP0].offset == 10
    assert consumer.committable_offsets()[TP1].offset == 103
    consumer.commit()
    assert committed_positions(captured[-1]) == {TP0: 10, TP1: 103}

    # TP0@10 completes: position advances to 12 (still pending), skipping
    # the nonexistent offset 11 -- it must not wait for it.
    assert consumer.complete(TP0, 10) is True
    assert consumer.committable_offsets()[TP0].offset == 12
    consumer.commit()
    assert committed_positions(captured[-1]) == {TP0: 12, TP1: 103}

    # TP0 fully drained: committable catches up to the fetch position.
    assert consumer.complete(TP0, 12) is True
    assert consumer.committable_offsets()[TP0].offset == 14
    consumer.commit()
    assert committed_positions(captured[-1]) == {TP0: 14, TP1: 103}


def test_commit_waits_for_uncompleted_not_for_gap(mocker, consumer,
                                                  commit_requests):
    """Contrast: with 10 and 12 delivered (11 nonexistent), an uncompleted
    10 pins the commit at 10; once 10 completes the commit jumps straight
    to the fetch position 13 -- the nonexistent 11 is never awaited."""
    captured = commit_requests(consumer)
    consumer.assign([TP0])
    consumer._subscription.seek(TP0, 10)
    install_fake_poll(mocker, consumer, [{TP0: [10, 12]}])
    consumer.poll(timeout_ms=0)

    # 12 done, 10 not: commit request carries 10, not 13
    consumer.complete(TP0, 12)
    consumer.commit()
    assert committed_positions(captured[-1]) == {TP0: 10}

    # 10 done: commit request carries 13 -- everything delivered below the
    # fetch position is complete, and the nonexistent 11 is never awaited
    consumer.complete(TP0, 10)
    consumer.commit()
    assert committed_positions(captured[-1]) == {TP0: 13}


def test_duplicate_and_unknown_completion(mocker, consumer):
    consumer.assign([TP0])
    consumer._subscription.seek(TP0, 0)
    install_fake_poll(mocker, consumer, [{TP0: [0]}])
    records = consumer.poll(timeout_ms=0)

    assert consumer.complete(records[TP0][0]) is True
    assert consumer.complete(records[TP0][0]) is False
    assert consumer.complete(TP0, 99) is False
    assert consumer.committable_offsets()[TP0].offset == 1


def test_seek_clears_pending(mocker, consumer, commit_requests):
    captured = commit_requests(consumer)
    consumer.assign([TP0])
    consumer._subscription.seek(TP0, 0)
    install_fake_poll(mocker, consumer, [{TP0: [0, 1]}])
    consumer.poll(timeout_ms=0)

    consumer.seek(TP0, 10)
    # pre-seek completions must not count anymore
    assert consumer.complete(TP0, 0) is False
    consumer.commit()
    assert committed_positions(captured[-1]) == {TP0: 10}


def test_revoke_reassign_isolates_completions(mocker, consumer,
                                              commit_requests):
    captured = commit_requests(consumer)
    consumer.assign([TP0])
    consumer._subscription.seek(TP0, 0)
    install_fake_poll(mocker, consumer, [{TP0: [0, 1]}])
    consumer.poll(timeout_ms=0)
    assert consumer.complete(TP0, 0) is True

    # revoke TP0, then reassign: fresh state, fresh position
    consumer.assign([TP1])
    consumer.assign([TP0])
    consumer._subscription.seek(TP0, 50)
    install_fake_poll(mocker, consumer, [{TP0: [50]}])
    consumer.poll(timeout_ms=0)

    # completion for offset 1 (delivered under the old assignment only)
    # must not advance the new assignment
    assert consumer.complete(TP0, 1) is False
    assert consumer.committable_offsets()[TP0].offset == 50
    consumer.commit()
    assert committed_positions(captured[-1]) == {TP0: 50}

    # and the redelivered record still needs its own completion
    assert consumer.complete(TP0, 50) is True
    consumer.commit()
    assert committed_positions(captured[-1]) == {TP0: 51}


def test_iterator_registers_delivery(mocker, consumer):
    consumer.assign([TP0])
    consumer._subscription.seek(TP0, 0)
    install_fake_poll(mocker, consumer, [{TP0: [0, 1]}])

    first = next(consumer)
    second = next(consumer)
    assert (first.offset, second.offset) == (0, 1)

    assert consumer.complete(first) is True
    # second record still pending
    assert consumer.committable_offsets()[TP0].offset == 1
    assert consumer.complete(second) is True
    assert consumer.committable_offsets()[TP0].offset == 2


def test_auto_commit_uses_committable_offsets(mocker, commit_requests):
    consumer = KafkaConsumer(
        api_version=(2, 5, 0),
        group_id='completion-test',
        enable_auto_commit=True,
        auto_commit_interval_ms=0,
        enable_processing_completion=True,
    )
    try:
        captured = commit_requests(consumer)
        consumer.assign([TP0])
        consumer._subscription.seek(TP0, 10)
        install_fake_poll(mocker, consumer, [{TP0: [10, 12]}])
        consumer.poll(timeout_ms=0)
        consumer.complete(TP0, 12)

        consumer._coordinator.maybe_auto_commit_offsets_now()
        assert commit_requests.sent.wait(timeout=10)
        # TP0@10 not completed: auto-commit carries 10, not 13
        assert committed_positions(captured[-1]) == {TP0: 10}
    finally:
        consumer.close(autocommit=False)


def test_complete_requires_processing_completion_mode():
    consumer = KafkaConsumer(
        api_version=(2, 5, 0),
        group_id='completion-test',
        enable_auto_commit=False,
    )
    try:
        with pytest.raises(Errors.IllegalStateError):
            consumer.complete(TP0, 0)
        with pytest.raises(Errors.IllegalStateError):
            consumer.committable_offsets()
    finally:
        consumer.close(autocommit=False)
