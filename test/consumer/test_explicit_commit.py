# pylint: skip-file
"""Tests for explicit processing-completion commit mode.

Three layers are covered:

* ``PartitionCompletionTracker`` on its own: gap-tolerant watermarking,
  out-of-order and duplicate completion.
* ``SubscriptionState`` integration: seek resets tracking, revocation
  drops it, a re-assigned partition starts fresh, and stale notices from
  the previous assignment cannot move the new one.
* Wire level: a fake fetch feeds interleaved partitions through the real
  ``Fetcher`` and the real ``ConsumerCoordinator`` against a MockBroker,
  and the assertions read the offsets out of the actual decoded
  ``OffsetCommitRequest`` s.
"""

from unittest.mock import MagicMock

import pytest

from kafka import KafkaConsumer
from kafka.consumer.fetcher import CompletedFetch, ConsumerRecord, Fetcher
from kafka.consumer.subscription_state import (
    PartitionCompletionTracker,
    SubscriptionState,
)
from kafka.coordinator.base import Generation, MemberState
from kafka.coordinator.consumer import ConsumerCoordinator
from kafka.protocol.consumer import (
    FetchResponse,
    OffsetCommitRequest,
    OffsetCommitResponse,
)
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import OffsetAndMetadata, TopicPartition


TOPIC = 'explicit'
_ResponsePartition = FetchResponse.FetchableTopicResponse.PartitionData
_CommitTopic = OffsetCommitResponse.OffsetCommitResponseTopic
_CommitPartition = _CommitTopic.OffsetCommitResponsePartition


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #

def _batch(offsets):
    """Build a record batch whose records carry the given offsets.

    MemoryRecordsBuilder writes records consecutively from ``offset``, so a
    batch with numeric gaps is emulated by concatenating several single-record
    batches. This is what a compacted log segment looks like to a consumer:
    the batch headers spell out base offsets and the missing offsets are
    simply absent.
    """
    blob = b''
    for offset in offsets:
        builder = MemoryRecordsBuilder(
            magic=2, compression_type=0, batch_size=9999999, offset=offset)
        builder.append(key=None, value=b'v%d' % offset,
                       timestamp=0, headers=[])
        builder.close()
        blob += builder.buffer()
    return blob


def _completed_fetch(tp, records, fetched_offset, high_watermark):
    partition_data = _ResponsePartition(
        error_code=0,
        high_watermark=high_watermark,
        records=records,
    )
    return CompletedFetch(
        tp, fetched_offset, FetchResponse.max_version, partition_data,
        MagicMock())


def _deliver(fetcher, tp, offsets, fetched_offset=None, high_watermark=None):
    """Push one fake fetch response for ``tp`` and drain it through
    ``fetched_records`` so the delivery path (and therefore the tracker)
    runs exactly as it would on the wire."""
    fetched_offset = fetched_offset if fetched_offset is not None else offsets[0]
    high_watermark = high_watermark if high_watermark is not None else offsets[-1] + 1
    fetcher._completed_fetches.append(
        _completed_fetch(tp, _batch(offsets), fetched_offset, high_watermark))
    records, _ = fetcher.fetched_records()
    delivered = [record.offset for record in records.get(tp, [])]
    assert delivered == offsets, (tp, delivered, offsets)
    return records[tp]


# --------------------------------------------------------------------------- #
# PartitionCompletionTracker                                                   #
# --------------------------------------------------------------------------- #

def test_gap_in_delivered_offsets_does_not_block_commit():
    """10 and 12 delivered, 11 never existed: both complete -> commit 13.

    Waiting for 11 would stall forever on a compacted log or a skipped
    control record.
    """
    tracker = PartitionCompletionTracker()
    tracker.register_delivery(10)
    tracker.register_delivery(12)
    assert tracker.mark_processed(10)
    assert tracker.mark_processed(12)
    assert tracker.committable_position() == 13


def test_gap_advances_over_missing_offset():
    """10 and 12 delivered, 11 never existed; only 10 completed -> 11.

    The watermark does not wait for the missing offset: once 10 is done
    the next position (11) is committable even though record 11 was never
    delivered. Contrast with test_uncompleted_head_stops_commit, where a
    *delivered* 10 that is still processing pins the commit to 10.
    """
    tracker = PartitionCompletionTracker()
    tracker.register_delivery(10)
    tracker.register_delivery(12)
    tracker.mark_processed(10)
    assert tracker.committable_position() == 11


def test_uncompleted_head_stops_commit():
    """10, 11, 12 delivered; 11 and 12 completed but 10 is not -> commit 10."""
    tracker = PartitionCompletionTracker()
    for offset in (10, 11, 12):
        tracker.register_delivery(offset)
    assert tracker.mark_processed(12)
    assert tracker.mark_processed(11)
    assert tracker.committable_position() == 10


def test_out_of_order_completion_advances_watermark():
    tracker = PartitionCompletionTracker()
    for offset in (10, 11, 12, 13):
        tracker.register_delivery(offset)
    tracker.mark_processed(13)
    assert tracker.committable_position() == 10
    tracker.mark_processed(10)
    assert tracker.committable_position() == 11
    tracker.mark_processed(12)
    # 11 is still outstanding, so the prefix ends at 11.
    assert tracker.committable_position() == 11
    tracker.mark_processed(11)
    assert tracker.committable_position() == 14


def test_duplicate_completion_is_idempotent():
    tracker = PartitionCompletionTracker()
    tracker.register_delivery(10)
    tracker.register_delivery(11)
    assert tracker.mark_processed(10) is True
    assert tracker.mark_processed(10) is False
    assert tracker.committable_position() == 11
    # A completion for an offset that was never delivered is ignored.
    assert tracker.mark_processed(12) is False


def test_nothing_delivered_nothing_committable():
    tracker = PartitionCompletionTracker()
    assert tracker.committable_position() is None
    tracker.mark_processed(7)
    assert tracker.committable_position() is None


def test_reset_forgets_deliveries_and_completions():
    tracker = PartitionCompletionTracker()
    tracker.register_delivery(10)
    tracker.register_delivery(11)
    tracker.mark_processed(10)
    tracker.reset()
    assert tracker.committable_position() is None
    assert tracker.mark_processed(10) is False


def test_duplicate_delivery_is_harmless():
    tracker = PartitionCompletionTracker()
    tracker.register_delivery(10)
    tracker.register_delivery(10)
    tracker.mark_processed(10)
    assert tracker.committable_position() == 11


# --------------------------------------------------------------------------- #
# SubscriptionState integration                                                #
# --------------------------------------------------------------------------- #

@pytest.fixture
def explicit_state():
    state = SubscriptionState(explicit_commit_mode=True)
    state.subscribe(topics=[TOPIC])
    return state


def test_legacy_mode_commits_fetch_position(explicit_state):
    """Explicit mode off: the committable offsets are just the positions."""
    tp = TopicPartition(TOPIC, 0)
    explicit_state.assign_from_subscribed([tp])
    explicit_state.seek(tp, 10)
    explicit_state.register_delivery(tp, 10)  # ignored: mode is off
    explicit_state.explicit_commit_mode = False
    assert explicit_state.all_committable_offsets() == {
        tp: OffsetAndMetadata(10, '', -1)}


def test_tracking_only_when_enabled(explicit_state):
    tp = TopicPartition(TOPIC, 0)
    explicit_state.assign_from_subscribed([tp])
    explicit_state.seek(tp, 10)
    assert explicit_state.explicit_commit_mode is True
    explicit_state.register_delivery(tp, 10)
    assert explicit_state.mark_processed(tp, 10) is True
    assert explicit_state.all_committable_offsets() == {
        tp: explicit_state.all_consumed_offsets()[tp]._replace(offset=11)}


def test_seek_resets_tracking(explicit_state):
    """Deliveries accepted before a seek must not advance after it."""
    tp = TopicPartition(TOPIC, 0)
    explicit_state.assign_from_subscribed([tp])
    explicit_state.seek(tp, 10)
    explicit_state.register_delivery(tp, 10)
    explicit_state.register_delivery(tp, 11)
    explicit_state.mark_processed(tp, 10)
    explicit_state.seek(tp, 0)  # user seeks back: everything re-read
    # A completion notice for the pre-seek offset is now stale.
    assert explicit_state.mark_processed(tp, 10) is False
    assert explicit_state.all_committable_offsets() == {}
    explicit_state.register_delivery(tp, 0)
    explicit_state.mark_processed(tp, 0)
    assert explicit_state.all_committable_offsets()[tp].offset == 1


def test_revoked_partition_tracker_is_dropped(explicit_state):
    tp = TopicPartition(TOPIC, 0)
    explicit_state.assign_from_subscribed([tp])
    explicit_state.seek(tp, 10)
    explicit_state.register_delivery(tp, 10)
    assert explicit_state.mark_processed(tp, 10) is True
    explicit_state.assign_from_subscribed([])
    assert tp not in explicit_state._completion_trackers
    assert explicit_state.mark_processed(tp, 10) is False
    assert explicit_state.register_delivery(tp, 20) is None
    assert explicit_state.all_committable_offsets() == {}


def test_reassignment_starts_from_clean_state(explicit_state):
    """A partition dropped and re-assigned must not inherit old completions."""
    tp = TopicPartition(TOPIC, 0)
    explicit_state.assign_from_subscribed([tp])
    explicit_state.seek(tp, 10)
    explicit_state.register_delivery(tp, 10)
    explicit_state.mark_processed(tp, 10)
    assert explicit_state.all_committable_offsets() == {
        tp: OffsetAndMetadata(11, '', -1)}

    explicit_state.assign_from_subscribed([])
    explicit_state.assign_from_subscribed([tp])
    explicit_state.seek(tp, 10)
    # The old generation's completion is gone: nothing committable until the
    # new delivery is processed.
    assert explicit_state.mark_processed(tp, 10) is False
    assert explicit_state.all_committable_offsets() == {}
    explicit_state.register_delivery(tp, 10)
    explicit_state.mark_processed(tp, 10)
    assert explicit_state.all_committable_offsets()[tp].offset == 11


def test_stale_completion_after_reassignment_other_offset(explicit_state):
    """Even an offset overlapping the new delivery can't be borrowed: the
    notice arrives before the new delivery is registered, so it is dropped."""
    tp = TopicPartition(TOPIC, 1)
    explicit_state.assign_from_subscribed([tp])
    explicit_state.seek(tp, 5)
    explicit_state.register_delivery(tp, 5)
    explicit_state.mark_processed(tp, 5)

    explicit_state.assign_from_subscribed([])
    explicit_state.assign_from_subscribed([tp])
    explicit_state.seek(tp, 99)
    # Completion notice racing in from the old assignment.
    assert explicit_state.mark_processed(tp, 5) is False
    assert explicit_state.all_committable_offsets() == {}
    explicit_state.register_delivery(tp, 99)
    explicit_state.mark_processed(tp, 99)
    assert explicit_state.all_committable_offsets()[tp].offset == 100


def test_committable_offsets_per_partition_are_independent(explicit_state):
    tp0, tp1 = TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)
    explicit_state.assign_from_subscribed([tp0, tp1])
    explicit_state.seek(tp0, 10)
    explicit_state.seek(tp1, 10)
    for tp in (tp0, tp1):
        explicit_state.register_delivery(tp, 10)
        explicit_state.register_delivery(tp, 11)
    explicit_state.mark_processed(tp0, 10)
    explicit_state.mark_processed(tp0, 11)
    explicit_state.mark_processed(tp1, 11)
    offsets = explicit_state.all_committable_offsets()
    assert offsets[tp0].offset == 12
    assert offsets[tp1].offset == 10


# --------------------------------------------------------------------------- #
# Wire level: fake fetch -> real Fetcher -> real ConsumerCoordinator           #
# --------------------------------------------------------------------------- #

TP0 = TopicPartition(TOPIC, 0)
TP1 = TopicPartition(TOPIC, 1)


@pytest.fixture
def wire(broker, client, net, manager, metrics):
    """A Fetcher and ConsumerCoordinator sharing one explicit-mode
    SubscriptionState, wired to a MockBroker. The fetcher is fed
    ``CompletedFetch`` objects directly (the fake fetch); commits go over
    the wire to the broker and are captured from the decoded requests."""
    manager.bootstrap(timeout_ms=5000)
    state = SubscriptionState(explicit_commit_mode=True)
    state.subscribe(topics=[TOPIC])
    state.assign_from_subscribed([TP0, TP1])
    state.seek(TP0, 10)
    state.seek(TP1, 10)
    fetcher = Fetcher(client, state, metrics=metrics, check_crcs=False)
    coordinator = ConsumerCoordinator(
        client, state, metrics=metrics,
        api_version=broker.broker_version,
        explicit_commit_mode=True)
    coordinator.coordinator_id = 0
    coordinator._generation = Generation(1, 'member-1', b'')
    coordinator.state = MemberState.STABLE
    coordinator.rejoin_needed = False
    try:
        yield fetcher, coordinator
    finally:
        coordinator.reset_generation()
        coordinator.close(timeout_ms=0)


def _capture_commits(broker):
    """Capture every OffsetCommitRequest's offsets as
    {TopicPartition: committed_offset}, answering each with success."""
    captured = []

    def handler(api_key, api_version, correlation_id, request_bytes):
        request = OffsetCommitRequest.decode(
            request_bytes, version=api_version, header=True)
        offsets = {}
        for topic in request.topics:
            for part in topic.partitions:
                offsets[TopicPartition(topic.name, part.partition_index)] = \
                    part.committed_offset
        captured.append(offsets)
        return OffsetCommitResponse(
            throttle_time_ms=0,
            topics=[_CommitTopic(name=topic.name, partitions=[
                _CommitPartition(partition_index=part.partition_index,
                                 error_code=0)
                for part in topic.partitions])
                for topic in request.topics])

    broker.respond_always(OffsetCommitRequest, handler)
    return captured


def test_gap_vs_unprocessed_head_over_the_wire(broker, wire):
    """Two partitions interleaved through one fetcher:

    * TP0 gets records 10 and 12 (offset 11 does not exist - a compacted
      gap). Once both are processed the commit must advance past the gap
      to 13, not wait for 11.
    * TP1 gets records 10, 11, 12; 11 and 12 complete but 10 is still in
      flight. The commit must stop at 10, even though later offsets are
      done.
    """
    fetcher, coordinator = wire
    captured = _capture_commits(broker)

    # Interleave deliveries across the two partitions.
    _deliver(fetcher, TP0, [10, 12], high_watermark=13)
    _deliver(fetcher, TP1, [10, 11, 12], high_watermark=13)

    # Out-of-order, cross-partition completion notifications.
    assert coordinator._subscription.mark_processed(TP1, 12) is True
    assert coordinator._subscription.mark_processed(TP0, 10) is True
    assert coordinator._subscription.mark_processed(TP1, 11) is True
    assert coordinator._subscription.mark_processed(TP0, 12) is True
    # A duplicate notice is ignored.
    assert coordinator._subscription.mark_processed(TP0, 12) is False

    coordinator.commit_offsets_sync(coordinator._offsets_to_commit())

    assert captured == [{TP0: 13, TP1: 10}], captured


def test_sync_commit_waits_for_in_flight_record(broker, wire):
    """Same deliveries, but TP0's offset 10 is still being processed when
    the commit runs: the request carries 10, and only after it completes
    does the next commit advance to 13."""
    fetcher, coordinator = wire
    captured = _capture_commits(broker)

    _deliver(fetcher, TP0, [10, 12], high_watermark=13)
    coordinator._subscription.mark_processed(TP0, 12)

    coordinator.commit_offsets_sync(coordinator._offsets_to_commit())
    assert captured == [{TP0: 10}], captured

    coordinator._subscription.mark_processed(TP0, 10)
    coordinator.commit_offsets_sync(coordinator._offsets_to_commit())
    assert captured == [{TP0: 10}, {TP0: 13}], captured


def test_nothing_processed_means_nothing_committed(broker, wire):
    """Delivered-but-unprocessed records must not produce a commit."""
    fetcher, coordinator = wire
    captured = _capture_commits(broker)

    _deliver(fetcher, TP0, [10, 12], high_watermark=13)
    _deliver(fetcher, TP1, [10, 11], high_watermark=12)

    assert coordinator._offsets_to_commit() == {}
    coordinator.commit_offsets_sync(coordinator._offsets_to_commit())
    assert captured == []


def test_auto_commit_path_uses_processed_watermark(broker, wire):
    """The periodic auto-commit path commits the processed watermark, not
    the fetch position."""
    fetcher, coordinator = wire
    captured = _capture_commits(broker)

    _deliver(fetcher, TP0, [10, 12], high_watermark=13)
    _deliver(fetcher, TP1, [10, 11, 12], high_watermark=13)
    coordinator._subscription.mark_processed(TP0, 10)
    coordinator._subscription.mark_processed(TP0, 12)
    coordinator._subscription.mark_processed(TP1, 11)
    coordinator._subscription.mark_processed(TP1, 12)

    coordinator.maybe_auto_commit_offsets_now()
    # Drive the loop until the async commit lands.
    deadline = 0
    while not captured and deadline < 100:
        coordinator._net.poll(timeout_ms=10)
        deadline += 1
    assert captured == [{TP0: 13, TP1: 10}], captured


def test_stale_completion_does_not_advance_new_assignment(broker, wire):
    """TP1 is revoked and re-assigned; a completion notice for the old
    generation must not move the new assignment's commit."""
    fetcher, coordinator = wire
    captured = _capture_commits(broker)

    _deliver(fetcher, TP1, [10, 11], high_watermark=12)
    coordinator._subscription.mark_processed(TP1, 10)

    # Rebalance: TP1 revoked, then re-assigned. The tracker is rebuilt.
    coordinator._subscription.assign_from_subscribed([TP0])
    coordinator._subscription.assign_from_subscribed([TP0, TP1])
    coordinator._subscription.seek(TP1, 10)

    # Late completion notice from the old generation.
    assert coordinator._subscription.mark_processed(TP1, 11) is False
    assert coordinator._offsets_to_commit() == {}

    coordinator.commit_offsets_sync(coordinator._offsets_to_commit())
    assert captured == []

    # New deliveries on the re-assigned partition work normally.
    _deliver(fetcher, TP1, [10], high_watermark=11)
    coordinator._subscription.mark_processed(TP1, 10)
    coordinator.commit_offsets_sync(coordinator._offsets_to_commit())
    assert captured == [{TP1: 11}], captured


# --------------------------------------------------------------------------- #
# KafkaConsumer public API                                                     #
# --------------------------------------------------------------------------- #

def _consumer_record(tp, offset):
    return ConsumerRecord(
        topic=tp.topic, partition=tp.partition, leader_epoch=-1,
        offset=offset, timestamp=0, timestamp_type=0, key=None,
        value=b'v', headers=[], checksum=None,
        serialized_key_size=0, serialized_value_size=1,
        serialized_header_size=0)


def test_kafka_consumer_mark_processed_and_default_commit():
    """The public mark_processed API feeds the default commit offsets."""
    consumer = KafkaConsumer(api_version=(0, 10, 0), group_id='g',
                             enable_auto_commit=False,
                             explicit_commit_mode=True)
    try:
        tp = TopicPartition(TOPIC, 0)
        consumer.assign([tp])
        consumer.seek(tp, 10)
        consumer._subscription.register_delivery(tp, 10)
        consumer._subscription.register_delivery(tp, 11)
        consumer._subscription.register_delivery(tp, 12)

        # Out-of-order: 11 done first. The safe commit stays at 10, so a
        # restart re-reads the still-unprocessed record 10.
        assert consumer.mark_processed(_consumer_record(tp, 11)) is True
        assert consumer._offsets_to_commit()[tp].offset == 10
        assert consumer.mark_processed(_consumer_record(tp, 10)) is True
        # Duplicate notice is ignored.
        assert consumer.mark_processed(_consumer_record(tp, 10)) is False
        offsets = consumer._offsets_to_commit()
        assert offsets[tp].offset == 12
    finally:
        consumer.close()


def test_kafka_consumer_default_mode_commits_fetch_position():
    """Without explicit_commit_mode, the default commit uses the position."""
    consumer = KafkaConsumer(api_version=(0, 10, 0), group_id='g',
                             enable_auto_commit=False)
    try:
        tp = TopicPartition(TOPIC, 0)
        consumer.assign([tp])
        consumer.seek(tp, 42)
        assert consumer._offsets_to_commit()[tp].offset == 42
    finally:
        consumer.close()
