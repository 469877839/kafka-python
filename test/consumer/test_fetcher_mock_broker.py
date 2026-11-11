# pylint: skip-file
"""Wire-level integration tests for KIP-320 offset validation.

These tests drive the Fetcher through a real KafkaConnectionManager wired
to a scriptable MockBroker, so every ``OffsetForLeaderEpochRequest`` and
``FetchRequest`` actually goes through the protocol layer. Use these to
verify end-to-end behavior that the pure-mock tests in
``test_fetcher.py`` can't cover - request encoding, response decoding,
and the interactions between Fetcher, ClusterMetadata, and SubscriptionState
as metadata updates arrive over the wire.
"""

import time

import pytest

import kafka.errors as Errors
from kafka.consumer.fetcher import Fetcher
from kafka.consumer.subscription_state import SubscriptionState
from kafka.protocol.consumer import (
    FetchRequest, FetchResponse,
    ListOffsetsRequest, ListOffsetsResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse,
)
from kafka.protocol.metadata import MetadataResponse
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import OffsetAndMetadata, TopicPartition


TOPIC = 'foobar'
PARTITION = 0


# --------------------------------------------------------------------------- #
# Builders                                                                    #
# --------------------------------------------------------------------------- #

_MetaTopic = MetadataResponse.MetadataResponseTopic
_MetaPartition = _MetaTopic.MetadataResponsePartition
_MetaBroker = MetadataResponse.MetadataResponseBroker
_OFLE_Topic = OffsetForLeaderEpochResponse.OffsetForLeaderTopicResult
_OFLE_Partition = _OFLE_Topic.EpochEndOffset
_FetchTopic = FetchResponse.FetchableTopicResponse
_FetchPartition = _FetchTopic.PartitionData


def _broker_metadata(broker, leader_epoch):
    """Configure the MockBroker to advertise TOPIC/PARTITION with the
    given leader_epoch, leader=node 0 (self)."""
    broker.set_metadata(
        brokers=[_MetaBroker(node_id=broker.node_id, host=broker.host,
                             port=broker.port, rack=None)],
        topics=[_MetaTopic(
            error_code=0, name=TOPIC, is_internal=False,
            partitions=[_MetaPartition(
                error_code=0, partition_index=PARTITION,
                leader_id=broker.node_id, leader_epoch=leader_epoch,
                replica_nodes=[broker.node_id], isr_nodes=[broker.node_id],
                offline_replicas=[])],
        )],
    )


def _ofle_response(error_code, leader_epoch, end_offset):
    return OffsetForLeaderEpochResponse(
        throttle_time_ms=0,
        topics=[_OFLE_Topic(topic=TOPIC, partitions=[_OFLE_Partition(
            error_code=error_code, partition=PARTITION,
            leader_epoch=leader_epoch, end_offset=end_offset)])])


def _fetch_response_with_error(errno):
    """FetchResponse for TOPIC/PARTITION carrying a single error code."""
    return FetchResponse(
        throttle_time_ms=0, error_code=0, session_id=0,
        responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
            partition_index=PARTITION, error_code=errno, high_watermark=-1,
            last_stable_offset=-1, log_start_offset=-1,
            aborted_transactions=[], preferred_read_replica=-1,
            records=b'')])])


# --------------------------------------------------------------------------- #
# Fixtures                                                                    #
# --------------------------------------------------------------------------- #


@pytest.fixture
def subscription_state():
    return SubscriptionState()


@pytest.fixture
def fetcher(broker, client, manager, subscription_state, metrics):
    """Fetcher wired to a bootstrapped client+manager+MockBroker."""
    # Bootstrap so the manager has a connected node and cluster metadata.
    _broker_metadata(broker, leader_epoch=3)
    manager.bootstrap(timeout_ms=5000)

    subscription_state.subscribe(topics=[TOPIC])
    subscription_state.assign_from_subscribed([TopicPartition(TOPIC, PARTITION)])
    fetcher = Fetcher(client, subscription_state, metrics=metrics,
                      retry_backoff_ms=50)
    return fetcher


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #


class TestKIP320OffsetValidation:
    """End-to-end OffsetsForLeaderEpoch flow through the wire."""

    def test_advanced_cluster_epoch_triggers_validation_request(
            self, broker, net, manager, fetcher):
        """When the metadata-cached leader_epoch advances past the
        consumer's position epoch, the next ``maybe_validate_positions``
        marks the partition and ``_validate_offsets_async`` issues an
        OffsetForLeaderEpochRequest over the wire."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))

        # Bump the broker's leader_epoch and force a metadata refresh so
        # the consumer's cluster cache sees the new epoch.
        _broker_metadata(broker, leader_epoch=5)
        manager.cluster.request_update()
        net.wait_for(manager.cluster.request_update(), 1000)

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['version'] = api_version
            return _ofle_response(error_code=0, leader_epoch=5, end_offset=100)
        broker.respond_fn(OffsetForLeaderEpochRequest, handler)

        fetcher.maybe_validate_positions()
        assert fetcher._subscriptions.assignment[tp].awaiting_validation

        net.run(fetcher._validate_offsets_async, 1000)

        assert 'version' in captured, 'OffsetForLeaderEpochRequest never reached broker'
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation
        # No truncation: the offset is confirmed but the position keeps the
        # record epoch of the record it actually consumed (3); the response
        # epoch is the requested/log-end point, not the record's epoch.
        assert fetcher._subscriptions.assignment[tp].position.leader_epoch == 3
        assert fetcher._subscriptions.assignment[tp].position.offset == 50

    def test_validated_position_not_revalidated_forever(
            self, broker, net, manager, fetcher):
        """Regression for #3106: consumer stalls after one fetched batch.

        After a leader election the cluster epoch advances (3 -> 5), but
        OffsetForLeaderEpoch reports the epoch of the *requested* (older)
        point - here 3, below the cluster epoch - because that's the epoch
        the position's record actually belongs to. The position must be
        reconciled against the current leader epoch (5) so the *next* poll
        does not re-mark it for validation. Otherwise the partition is gated
        out of fetching on every poll and the consumer stalls forever with a
        frozen position and growing lag.
        """
        tp = TopicPartition(TOPIC, PARTITION)
        # Position recorded under leader epoch 3 (e.g. last fetched batch).
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))

        # Cluster leader epoch advances to 5; refresh the consumer's cache.
        _broker_metadata(broker, leader_epoch=5)
        net.wait_for(manager.cluster.request_update(), 1000)

        # No truncation (end_offset 100 >= position 50), and the broker
        # reports the requested epoch (3), NOT the current cluster epoch (5).
        ofle_requests = [0]

        def handler(api_key, api_version, correlation_id, request_bytes):
            ofle_requests[0] += 1
            return _ofle_response(error_code=0, leader_epoch=3, end_offset=100)
        broker.respond_fn(OffsetForLeaderEpochRequest, handler)
        broker.respond_fn(OffsetForLeaderEpochRequest, handler)  # guard 2nd call

        # Round 1: poll marks + drives validation to completion.
        fetcher.maybe_validate_positions()
        assert fetcher._subscriptions.assignment[tp].awaiting_validation
        net.run(fetcher._validate_offsets_async, 1000)

        assert fetcher._cached_log_truncation is None
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation
        assert ofle_requests[0] == 1

        # Round 2: the next poll must NOT re-mark the partition - it has
        # already been validated against cluster leader epoch 5 - and the
        # partition must be fetchable again.
        fetcher.maybe_validate_positions()
        state = fetcher._subscriptions.assignment[tp]
        assert not state.awaiting_validation, \
            'position re-marked for validation -> consumer stalls (#3106)'
        assert state.is_fetchable()

        # And no further OffsetForLeaderEpoch request is issued.
        net.run(fetcher._validate_offsets_async, 1000)
        assert ofle_requests[0] == 1

    def test_seek_forces_revalidation_of_new_position(
            self, broker, net, manager, fetcher):
        """A seek must re-arm validation even after a prior validation.

        Companion to #3106: the per-partition reconciled-leader-epoch must be
        reset on seek. A position validated against leader epoch 5 says nothing
        about a *different* offset the user then seeks to - that offset may sit
        in a truncated region of an older epoch. If the stale reconciliation
        leaked across the seek, the new position would be (wrongly) treated as
        already validated and fetched without an OffsetForLeaderEpoch check.
        """
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))

        _broker_metadata(broker, leader_epoch=5)
        net.wait_for(manager.cluster.request_update(), 1000)

        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=3, end_offset=100))

        # Validate the first position against the current leader (epoch 5).
        fetcher.maybe_validate_positions()
        net.run(fetcher._validate_offsets_async, 1000)
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation

        # User seeks to a different offset whose record epoch (4) is below the
        # current cluster leader epoch (5). This new position has never been
        # reconciled and must be re-marked for validation.
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(200, '', 4))
        fetcher.maybe_validate_positions()
        assert fetcher._subscriptions.assignment[tp].awaiting_validation, \
            'seeked position not re-validated -> could consume past truncation'

    def test_diverged_seeks_to_endpoint_with_policy(
            self, broker, net, manager, fetcher):
        """end_offset < position.offset (valid epoch) on the wire triggers
        a seek to the broker-reported divergence point - preserves progress
        and tags position with the confirmed epoch. Mirrors Java's
        ``state.seekValidated(newPosition)``."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=3, end_offset=80))

        net.run(fetcher._validate_offsets_async, 1000)

        assert fetcher._cached_log_truncation is None
        assert not fetcher._subscriptions.assignment[tp].awaiting_reset
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation
        pos = fetcher._subscriptions.assignment[tp].position
        assert pos.offset == 80
        assert pos.leader_epoch == 3

    def test_diverged_raises_when_no_reset_policy(
            self, broker, net, client, manager, metrics):
        """With offset_reset_strategy=NONE, the same wire response
        produces LogTruncationError carrying the divergent offset and
        leaves the position untouched."""
        _broker_metadata(broker, leader_epoch=3)
        manager.bootstrap(timeout_ms=5000)
        subs = SubscriptionState(offset_reset_strategy='none')
        subs.subscribe(topics=[TOPIC])
        tp = TopicPartition(TOPIC, PARTITION)
        subs.assign_from_subscribed([tp])
        subs.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher = Fetcher(client, subs, metrics=metrics, retry_backoff_ms=50)
        subs.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=3, end_offset=80))

        net.run(fetcher._validate_offsets_async, 1000)

        with pytest.raises(Errors.LogTruncationError) as exc_info:
            fetcher.validate_offsets_if_needed()
        divergent = exc_info.value.divergent_offsets
        assert tp in divergent
        assert divergent[tp] is not None
        assert divergent[tp].offset == 80
        assert divergent[tp].leader_epoch == 3
        # Position untouched.
        assert subs.assignment[tp].position.offset == 100

    def test_undefined_response_resets_with_policy(
            self, broker, net, fetcher):
        """UNDEFINED end_offset/leader_epoch (broker has no record of our
        epoch) with a reset policy: no known seek point, so fall back to
        auto_offset_reset rather than silently dropping the epoch."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=-1, end_offset=-1))

        net.run(fetcher._validate_offsets_async, 1000)

        assert fetcher._cached_log_truncation is None
        assert fetcher._subscriptions.assignment[tp].awaiting_reset
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation

    def test_undefined_response_raises_when_no_reset_policy(
            self, broker, net, client, manager, metrics):
        """UNDEFINED response with reset policy NONE: LogTruncationError
        with divergent_offsets[tp] == None (no known recovery point)."""
        _broker_metadata(broker, leader_epoch=3)
        manager.bootstrap(timeout_ms=5000)
        subs = SubscriptionState(offset_reset_strategy='none')
        subs.subscribe(topics=[TOPIC])
        tp = TopicPartition(TOPIC, PARTITION)
        subs.assign_from_subscribed([tp])
        subs.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher = Fetcher(client, subs, metrics=metrics, retry_backoff_ms=50)
        subs.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=-1, end_offset=-1))

        net.run(fetcher._validate_offsets_async, 1000)

        with pytest.raises(Errors.LogTruncationError) as exc_info:
            fetcher.validate_offsets_if_needed()
        assert tp in exc_info.value.divergent_offsets
        assert exc_info.value.divergent_offsets[tp] is None
        # Position untouched; caller decides.
        assert subs.assignment[tp].position.offset == 100

    def test_fenced_epoch_on_fetch_marks_validation_then_succeeds(
            self, broker, net, fetcher):
        """A FENCED_LEADER_EPOCH on a real Fetch response routes through
        ``request_position_validation``; a subsequent
        ``_validate_offsets_async`` issues the OffsetForLeaderEpochRequest
        and clears the flag."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))

        # Script: one Fetch -> FENCED_LEADER_EPOCH, one OffsetForLeaderEpoch -> success
        broker.respond(FetchRequest, _fetch_response_with_error(
            Errors.FencedLeaderEpochError.errno))
        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=3, end_offset=100))

        # Drive a fetch+poll cycle. send_fetches schedules the request;
        # we then await it and let the response handler run.
        fetcher.send_fetches()
        # Drain the fetch response into _completed_fetches and parse it.
        # fetch_records pulls everything pending and runs _parse_fetched_data.
        fetcher.fetch_records(timeout_ms=500)
        assert fetcher._subscriptions.assignment[tp].awaiting_validation

        # Now run the validation driver; it should send OffsetForLeaderEpoch
        # and clear the flag.
        net.run(fetcher._validate_offsets_async, 1000)
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation

    def test_validation_retries_on_fenced_epoch_response(
            self, broker, net, fetcher):
        """FENCED_LEADER_EPOCH in the OffsetForLeaderEpoch response itself
        is retried after ``retry_backoff_ms`` within a single
        ``_validate_offsets_async`` invocation."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        # First response: FENCED_LEADER_EPOCH. Second: success.
        broker.respond(OffsetForLeaderEpochRequest, _ofle_response(
            error_code=Errors.FencedLeaderEpochError.errno,
            leader_epoch=-1, end_offset=-1))
        broker.respond(OffsetForLeaderEpochRequest, _ofle_response(
            error_code=0, leader_epoch=5, end_offset=100))

        start = time.monotonic()
        net.run(fetcher._validate_offsets_async, 2000)
        elapsed = time.monotonic() - start

        assert not fetcher._subscriptions.assignment[tp].awaiting_validation
        # Validation succeeded without truncation; the position's record
        # epoch is preserved (the response epoch is not the record's epoch).
        assert fetcher._subscriptions.assignment[tp].position.leader_epoch == 3
        assert elapsed >= 0.05, (
            '_validate_offsets_async did not sleep for retry_backoff_ms; %.3fs'
            % elapsed)


# --------------------------------------------------------------------------- #
# KIP-392: rack-aware fetching                                                #
# --------------------------------------------------------------------------- #


def _fetch_response_with_preferred_replica(preferred_read_replica):
    """Empty-records FetchResponse advertising a preferred read replica."""
    return FetchResponse(
        throttle_time_ms=0, error_code=0, session_id=0,
        responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
            partition_index=PARTITION, error_code=0, high_watermark=100,
            last_stable_offset=-1, log_start_offset=-1,
            aborted_transactions=[],
            preferred_read_replica=preferred_read_replica,
            records=b'')])])


class TestKIP392RackAwareFetching:
    """End-to-end: client_rack arrives on the wire and the broker's
    preferred_read_replica is honored on the next fetch."""

    def test_rack_id_sent_on_fetch_request(self, broker, net, manager, fetcher):
        """FetchRequest carries ``rack_id`` when client_rack is configured,
        and negotiates to v11+ against a modern broker."""
        fetcher.config['client_rack'] = 'us-east-1a'
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', 3))

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['api_version'] = api_version
            decoded = FetchRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['rack_id'] = decoded.rack_id
            return _fetch_response_with_preferred_replica(-1)
        broker.respond_fn(FetchRequest, handler)

        # Build the FetchRequest via the Fetcher (so client_rack is wired in)
        # and send it directly via manager - sidesteps the IO-thread driving
        # complexity of fetcher.send_fetches() in a synchronous test.
        requests = fetcher._create_fetch_requests()
        assert 0 in requests, 'expected one fetch routed to the leader (node 0)'
        request, _ = requests[0]
        future = manager.send(request, node_id=0)
        net.wait_for(future, 2000)

        assert captured['api_version'] >= 11, (
            'KIP-392 requires FetchRequest v11+; got v%s' % captured.get('api_version'))
        assert captured['rack_id'] == 'us-east-1a'

    def test_preferred_replica_cached_and_used_on_next_fetch(
            self, broker, net, manager, fetcher):
        """First fetch goes to the leader; broker returns
        ``preferred_read_replica=N``; second fetch routes to node N."""
        tp = TopicPartition(TOPIC, PARTITION)
        # Make node 5 reachable via metadata (still pointing at the same
        # MockBroker socket so the request actually completes).
        broker.set_metadata(
            brokers=[
                _MetaBroker(node_id=0, host=broker.host, port=broker.port, rack=None),
                _MetaBroker(node_id=5, host=broker.host, port=broker.port, rack=None),
            ],
            topics=[_MetaTopic(
                error_code=0, name=TOPIC, is_internal=False,
                partitions=[_MetaPartition(
                    error_code=0, partition_index=PARTITION,
                    leader_id=0, leader_epoch=3,
                    replica_nodes=[0, 5], isr_nodes=[0, 5],
                    offline_replicas=[])],
            )])
        # Re-pull metadata so the cluster cache knows about node 5.
        net.wait_for(manager.cluster.request_update(), 2000)

        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', 3))

        # Without a cached preferred replica, the first fetch must go to
        # the leader (node 0).
        assert fetcher._select_read_replica(tp) == 0

        # Synthesize a fetch response advertising node 5 as preferred.
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch
        completed = CompletedFetch(
            tp, 0, 11,
            _fetch_response_with_preferred_replica(5).responses[0].partitions[0],
            MagicMock())
        fetcher._parse_fetched_data(completed)

        # Next fetch should route to node 5.
        assert fetcher._select_read_replica(tp) == 5

    def test_preferred_replica_negative_one_means_leader(
            self, broker, fetcher):
        """``preferred_read_replica == -1`` is the broker explicitly telling
        the client to stop using a cached follower."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.assignment[tp].update_preferred_read_replica(
            5, time.monotonic() + 60)

        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch
        completed = CompletedFetch(
            tp, 0, 11,
            _fetch_response_with_preferred_replica(-1).responses[0].partitions[0],
            MagicMock())
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', 3))
        fetcher._parse_fetched_data(completed)
        assert fetcher._subscriptions.assignment[tp].preferred_read_replica() is None


# --------------------------------------------------------------------------- #
# Fetch v12: split request epochs + tagged response fields                    #
# --------------------------------------------------------------------------- #


_EpochEndOffset = _FetchPartition.EpochEndOffset
_LeaderIdAndEpoch = _FetchPartition.LeaderIdAndEpoch


def _fetch_v12_partition_data(error_code=0, high_watermark=100,
                              diverging_epoch=None, current_leader=None,
                              records=b''):
    return _FetchPartition(
        partition_index=PARTITION, error_code=error_code,
        high_watermark=high_watermark, last_stable_offset=-1,
        log_start_offset=-1,
        diverging_epoch=diverging_epoch,
        current_leader=current_leader,
        snapshot_id=None,
        aborted_transactions=[], preferred_read_replica=-1,
        records=records)


class TestFetchV12Epoch:
    """FetchRequest v12 split-epoch request encoding and tagged response handling."""

    def test_negotiates_v12_and_sends_split_epoch_fields(
            self, broker, net, manager, fetcher):
        """current_leader_epoch comes from cluster metadata, last_fetched_epoch
        from the position - and they are sent distinctly on the wire."""
        tp = TopicPartition(TOPIC, PARTITION)
        # position.leader_epoch = 3 (record view), cluster epoch = 5 (metadata view)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        assert manager.cluster.update_partition_leader(
            tp, leader_id=broker.node_id, leader_epoch=5) is True

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['api_version'] = api_version
            decoded = FetchRequest.decode(
                request_bytes, version=api_version, header=True)
            partition = decoded.topics[0].partitions[0]
            captured['current_leader_epoch'] = partition.current_leader_epoch
            captured['last_fetched_epoch'] = partition.last_fetched_epoch
            return _fetch_response_with_preferred_replica(-1)
        broker.respond_fn(FetchRequest, handler)

        requests = fetcher._create_fetch_requests()
        assert broker.node_id in requests
        request, _ = requests[broker.node_id]
        future = manager.send(request, node_id=broker.node_id)
        net.wait_for(future, 2000)

        assert captured['api_version'] >= 12, (
            'expected Fetch v12+ negotiation; got v%s' % captured.get('api_version'))
        assert captured['current_leader_epoch'] == 5
        assert captured['last_fetched_epoch'] == 3

    def test_last_fetched_epoch_is_minus_one_when_position_has_no_epoch(
            self, broker, net, manager, fetcher):
        """A position without a known epoch (e.g. bare seek) sends -1."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', -1))

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['api_version'] = api_version
            decoded = FetchRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['last_fetched_epoch'] = decoded.topics[0].partitions[0].last_fetched_epoch
            return _fetch_response_with_preferred_replica(-1)
        broker.respond_fn(FetchRequest, handler)

        requests = fetcher._create_fetch_requests()
        request, _ = requests[broker.node_id]
        future = manager.send(request, node_id=broker.node_id)
        net.wait_for(future, 2000)

        assert captured['last_fetched_epoch'] == -1

    def test_current_leader_epoch_minus_one_when_metadata_has_no_epoch(
            self, broker, net, manager, fetcher):
        """If the cluster cache has no epoch for the partition, send -1
        (not the position epoch) - we honestly don't know the current leader."""
        tp = TopicPartition(TOPIC, PARTITION)
        # Force the cached partition's leader_epoch to -1 (unknown).
        manager.cluster._partitions[TOPIC][PARTITION].leader_epoch = -1
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', 7))

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = FetchRequest.decode(
                request_bytes, version=api_version, header=True)
            partition = decoded.topics[0].partitions[0]
            captured['current_leader_epoch'] = partition.current_leader_epoch
            captured['last_fetched_epoch'] = partition.last_fetched_epoch
            return _fetch_response_with_preferred_replica(-1)
        broker.respond_fn(FetchRequest, handler)

        requests = fetcher._create_fetch_requests()
        request, _ = requests[broker.node_id]
        future = manager.send(request, node_id=broker.node_id)
        net.wait_for(future, 2000)

        assert captured['current_leader_epoch'] == -1
        assert captured['last_fetched_epoch'] == 7

    def test_diverging_epoch_response_marks_partition_for_validation(
            self, broker, manager, fetcher, mocker):
        """A v12 response carrying diverging_epoch (with empty records)
        routes the partition into KIP-320 validation instead of parsing
        records."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        spy = mocker.spy(manager.cluster, 'request_update')

        partition_data = _fetch_v12_partition_data(
            diverging_epoch=_EpochEndOffset(epoch=3, end_offset=80))
        completed = CompletedFetch(tp, 100, 12, partition_data, MagicMock())

        result = fetcher._parse_fetched_data(completed)

        assert result is None
        assert fetcher._subscriptions.assignment[tp].awaiting_validation
        assert spy.call_count >= 1

    def test_diverging_epoch_with_unset_end_offset_is_ignored(
            self, broker, fetcher):
        """A divergence struct with end_offset = -1 is treated as 'no
        divergence reported' and records are parsed normally."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))

        partition_data = _fetch_v12_partition_data(
            diverging_epoch=_EpochEndOffset(epoch=-1, end_offset=-1),
            high_watermark=100, records=b'')
        completed = CompletedFetch(tp, 100, 12, partition_data, MagicMock())

        fetcher._parse_fetched_data(completed)
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation

    def test_current_leader_hint_updates_cluster_cache_on_known_broker(
            self, broker, net, manager, fetcher, mocker):
        """A leader-change error response carrying current_leader with a
        newer epoch updates the cached leader id+epoch. If the new leader id
        is a broker we already know, no metadata refresh is needed."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        # Seed a second broker (node_id=7) into the cluster cache so the
        # hinted leader_id is already known.
        broker.set_metadata(
            brokers=[
                _MetaBroker(node_id=broker.node_id, host=broker.host,
                            port=broker.port, rack=None),
                _MetaBroker(node_id=7, host=broker.host,
                            port=broker.port, rack=None),
            ],
            topics=[_MetaTopic(
                error_code=0, name=TOPIC, is_internal=False,
                partitions=[_MetaPartition(
                    error_code=0, partition_index=PARTITION,
                    leader_id=broker.node_id, leader_epoch=3,
                    replica_nodes=[broker.node_id, 7],
                    isr_nodes=[broker.node_id, 7],
                    offline_replicas=[])],
            )])
        net.wait_for(manager.cluster.request_update(), 2000)

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        spy = mocker.spy(manager.cluster, 'request_update')

        partition_data = _fetch_v12_partition_data(
            error_code=Errors.NotLeaderForPartitionError.errno,
            current_leader=_LeaderIdAndEpoch(leader_id=7, leader_epoch=9))
        completed = CompletedFetch(tp, 50, 12, partition_data, MagicMock())

        fetcher._parse_fetched_data(completed)

        # Cache was updated with the hinted leader.
        assert manager.cluster.leader_for_partition(tp) == 7
        assert manager.cluster.leader_epoch_for_partition(tp) == 9
        # request_update is still called from the NotLeaderForPartition branch
        # itself (existing behavior); the hint just avoided an *additional*
        # forced refresh for an unknown broker.
        assert spy.call_count >= 1

    def test_current_leader_hint_unknown_broker_requests_metadata_update(
            self, broker, manager, fetcher, mocker):
        """The case we explicitly care about: the broker advertises a new
        leader (node_id=99) that we don't have broker metadata for. The
        fetcher must trigger a metadata refresh so the consumer learns the
        new leader's address (v12 has no node_endpoints)."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        assert manager.cluster.broker_metadata(99) is None, (
            'precondition: node 99 must be unknown')
        spy = mocker.spy(manager.cluster, 'request_update')

        partition_data = _fetch_v12_partition_data(
            error_code=Errors.FencedLeaderEpochError.errno,
            current_leader=_LeaderIdAndEpoch(leader_id=99, leader_epoch=11))
        completed = CompletedFetch(tp, 50, 12, partition_data, MagicMock())

        fetcher._parse_fetched_data(completed)

        # Cache updated with the (unknown) leader id and new epoch.
        assert manager.cluster.leader_for_partition(tp) == 99
        assert manager.cluster.leader_epoch_for_partition(tp) == 11
        # request_update fires twice in this branch: once by the
        # _maybe_update_current_leader helper (because leader 99 is unknown)
        # and once by the FencedLeaderEpoch handler itself. Either way, the
        # important thing is that *at least one* refresh was requested.
        assert spy.call_count >= 1
        # And validation was queued (FencedLeaderEpoch path).
        assert fetcher._subscriptions.assignment[tp].awaiting_validation

    def test_current_leader_hint_with_stale_epoch_is_ignored(
            self, broker, manager, fetcher):
        """A hint whose epoch is not strictly newer than the cache must not
        rewrite the cached leader."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        # Cache currently has leader_id=broker.node_id, leader_epoch=3.
        partition_data = _fetch_v12_partition_data(
            error_code=Errors.NotLeaderForPartitionError.errno,
            current_leader=_LeaderIdAndEpoch(leader_id=99, leader_epoch=3))
        completed = CompletedFetch(tp, 50, 12, partition_data, MagicMock())

        fetcher._parse_fetched_data(completed)

        assert manager.cluster.leader_for_partition(tp) == broker.node_id
        assert manager.cluster.leader_epoch_for_partition(tp) == 3


# --------------------------------------------------------------------------- #
# Deterministic leader-epoch (KIP-320) regression matrix                      #
#                                                                             #
# Every test below is driven by scripted MockBroker replies. Expected states #
# are derived from the protocol semantics and the configured reset policy,   #
# not copied from the implementation:                                        #
#                                                                             #
#   * A partition is fetchable only with a valid position AND no pending     #
#     truncation validation AND no pending reset.                            #
#   * OffsetForLeaderEpoch: end_offset < position means the consumer has     #
#     read into a region the new leader truncated; end_offset/-1 epoch means  #
#     the broker has no record of the requested epoch at all.                #
#   * FENCED/UNKNOWN epoch responses are retriable: the partition stays      #
#     gated until an authoritative reply lands.                              #
#   * With an auto reset policy (earliest/latest) the consumer self-heals;   #
#     with NONE the exception is delivered to the caller and the position is #
#     left untouched.                                                        #
#   * A reply that no longer matches the position (seek/rebalance/epoch      #
#     advance since the request was sent) must be ignored.                   #
#   * No usable epoch (legacy metadata, bare seek) => nothing to validate,   #
#     so the partition stays fetchable without any OffsetForLeaderEpoch RPC. #


class _EventRecorder:
    """Minimal, deterministic event log for a scripted validation flow.

    Events are recorded from the broker handler (which runs on the mock
    transport's event loop) and from the test thread. The selector backend
    used by these tests runs the loop inline (``net.poll`` / ``net.run``
    from the test thread), so ordering is strictly sequential.
    """

    def __init__(self):
        self.events = []

    def add(self, *event):
        self.events.append(event)

    def ofle_requests(self):
        return [e for e in self.events if e[0] == 'ofle_req']


def _record_batch(msgs, offset=0):
    """Build a magic-2 record batch starting at ``offset``.

    ``msgs`` is a list of ``(key, value)`` byte pairs.
    """
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999, offset=offset)
    for key, value in msgs:
        builder.append(key=key, value=value, timestamp=0, headers=[])
    builder.close()
    return builder.buffer()


def _fetch_response_with_records(records, high_watermark=200):
    return FetchResponse(
        throttle_time_ms=0, error_code=0, session_id=0,
        responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
            partition_index=PARTITION, error_code=0,
            high_watermark=high_watermark, last_stable_offset=-1,
            log_start_offset=-1, aborted_transactions=[],
            preferred_read_replica=-1, records=records)])])


def _list_offsets_response(offset, leader_epoch=5):
    Topic = ListOffsetsResponse.ListOffsetsTopicResponse
    Partition = Topic.ListOffsetsPartitionResponse
    return ListOffsetsResponse(
        0,  # throttle_time_ms
        [Topic(name=TOPIC, partitions=[Partition(
            partition_index=PARTITION, error_code=0, old_style_offsets=[],
            timestamp=-1, offset=offset, leader_epoch=leader_epoch)],
            version=7)],
        version=7)


def _pump_until(net, predicate, timeout_ms=2000):
    """Drive the selector loop until ``predicate()`` is true.

    Returns True if the predicate fired before the deadline. Used to wait
    for an in-flight request to reach the broker without sleeping blindly.
    """
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        if predicate():
            return True
        net.poll(timeout_ms=10)
    return predicate()


def _ofle_handler(recorder, response):
    """Broker handler that records the decoded request and returns a fixed
    OffsetForLeaderEpoch response."""
    def handler(api_key, api_version, correlation_id, request_bytes):
        decoded = OffsetForLeaderEpochRequest.decode(
            request_bytes, version=api_version, header=True)
        partition = decoded.topics[0].partitions[0]
        recorder.add('ofle_req',
                     'current_epoch=%s' % partition.current_leader_epoch,
                     'last_fetched_epoch=%s' % partition.leader_epoch)
        return response
    return handler


def _gated_ofle_handler(recorder, net, gate, response):
    """Broker handler that holds the response until ``gate`` completes.

    Simulates a slow broker: the request is recorded immediately, the reply
    is delivered only after the test thread resolves ``gate`` -- the window
    in which the user can seek / a rebalance can revoke / the leader can
    change again.
    """
    async def handler(api_key, api_version, correlation_id, request_bytes):
        decoded = OffsetForLeaderEpochRequest.decode(
            request_bytes, version=api_version, header=True)
        partition = decoded.topics[0].partitions[0]
        recorder.add('ofle_req',
                     'current_epoch=%s' % partition.current_leader_epoch,
                     'last_fetched_epoch=%s' % partition.leader_epoch)
        await net.await_for(gate, timeout_ms=5000, raise_error=False)
        recorder.add('ofle_reply',
                     'end_offset=%s' % response.topics[0].partitions[0].end_offset)
        return response
    return handler


class TestLeaderEpochValidationRegressions:
    """Deterministic, protocol-derived regressions for the KIP-320 path.

    The shared ``fetcher`` fixture subscribes TOPIC/PARTITION with the
    ``earliest`` auto-reset policy. A separate fixture below builds the same
    wiring with reset policy NONE.
    """

    TP = TopicPartition(TOPIC, PARTITION)

    @pytest.fixture
    def recorder(self):
        return _EventRecorder()

    @pytest.fixture
    def reset_none_fetcher(self, broker, client, manager, metrics):
        _broker_metadata(broker, leader_epoch=3)
        manager.bootstrap(timeout_ms=5000)
        subs = SubscriptionState(offset_reset_strategy='none')
        subs.subscribe(topics=[TOPIC])
        subs.assign_from_subscribed([self.TP])
        return Fetcher(client, subs, metrics=metrics, retry_backoff_ms=50)

    def test_leader_changes_again_while_validation_in_flight(
            self, broker, net, manager, fetcher, recorder):
        """Second leader election during an in-flight validation.

        Setup: position (100, record-epoch 3); cluster epoch 3 -> 5 marks
        validation. While the OffsetForLeaderEpoch request is in flight the
        cluster elects yet another leader (epoch 7). The old leader's reply
        (FENCED) cannot clear the partition. Expected end state derives
        from the state machine: the new epoch re-marks validation, the
        partition stays non-fetchable until an authoritative reply against
        epoch 7 confirms it, and the wire carries the updated
        current_leader_epoch on the retry.
        """
        tp = self.TP
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        _broker_metadata(broker, leader_epoch=5)
        net.wait_for(manager.cluster.request_update(), 1000)
        fetcher.maybe_validate_positions()
        assert fetcher._subscriptions.assignment[tp].awaiting_validation
        assert not fetcher._subscriptions.is_fetchable(tp)

        # Stage 1: the old leader's reply (FENCED) is held at the broker.
        # Stage 2: the retry against the new epoch is held too, so the test
        # can observe the re-armed state before it completes.
        gate1, gate2 = net.create_future(), net.create_future()
        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _gated_ofle_handler(
                recorder, net, gate1,
                _ofle_response(Errors.FencedLeaderEpochError.errno, -1, -1)))
        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _gated_ofle_handler(
                recorder, net, gate2,
                _ofle_response(error_code=0, leader_epoch=3, end_offset=100)))

        task = fetcher._manager.call_soon(fetcher._validate_offsets_async, 5000)
        assert _pump_until(net, lambda: bool(recorder.ofle_requests()))
        # Election #2 happens while the first request is still outstanding.
        # Apply it straight to the cluster cache (equivalent to the metadata
        # refresh landing on another connection; the mock serializes requests
        # per connection, so a wire refresh cannot interleave with the gated
        # OffsetForLeaderEpoch exchange). The same node keeps leadership with
        # a strictly higher epoch -- the epoch advance is what must gate.
        assert manager.cluster.update_partition_leader(tp, broker.node_id, 7)
        assert manager.cluster.leader_epoch_for_partition(tp) == 7
        gate1.success(None)

        # The FENCED reply cannot validate against epoch 7: the driver must
        # re-arm and re-issue the request itself (no external poll needed).
        assert _pump_until(net, lambda: len(recorder.ofle_requests()) >= 2)
        state = fetcher._subscriptions.assignment[tp]
        assert state.awaiting_validation
        assert not state.is_fetchable()

        # The retry names the *new* current epoch on the wire.
        requests = recorder.ofle_requests()
        assert 'current_epoch=7' in requests[1]
        assert 'last_fetched_epoch=3' in requests[1]

        # The authoritative reply against epoch 7 reconciles the partition.
        gate2.success(None)
        net.wait_for(task, 5000)
        assert not state.awaiting_validation
        assert state.is_fetchable()
        # No truncation -> offset untouched.
        assert state.position.offset == 100
        assert fetcher._cached_log_truncation is None

    def test_success_reply_landing_after_epoch_bump_does_not_validate(
            self, broker, net, manager, fetcher, recorder):
        """A *successful* reply from the old leader must not validate the
        position against the new epoch.

        The OffsetForLeaderEpoch reply was computed by the epoch-5 leader;
        by the time it is processed the cluster has already moved to epoch
        7. Accepting it would mark the position reconciled against a leader
        that never confirmed it. The driver must re-arm validation and
        re-validate against epoch 7 before the partition becomes fetchable.
        """
        tp = self.TP
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        _broker_metadata(broker, leader_epoch=5)
        net.wait_for(manager.cluster.request_update(), 1000)
        fetcher.maybe_validate_positions()

        gate1, gate2 = net.create_future(), net.create_future()
        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _gated_ofle_handler(
                recorder, net, gate1,
                _ofle_response(error_code=0, leader_epoch=3, end_offset=100)))
        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _gated_ofle_handler(
                recorder, net, gate2,
                _ofle_response(error_code=0, leader_epoch=3, end_offset=100)))

        task = fetcher._manager.call_soon(fetcher._validate_offsets_async, 5000)
        assert _pump_until(net, lambda: bool(recorder.ofle_requests()))
        # Election #2 lands while the success reply is still in flight.
        assert manager.cluster.update_partition_leader(tp, broker.node_id, 7)
        gate1.success(None)

        # The epoch-5 reply must not complete validation: the driver re-arms
        # and issues a second request against epoch 7.
        assert _pump_until(net, lambda: len(recorder.ofle_requests()) >= 2)
        state = fetcher._subscriptions.assignment[tp]
        assert state.awaiting_validation
        assert not state.is_fetchable()
        assert 'current_epoch=7' in recorder.ofle_requests()[1]

        gate2.success(None)
        net.wait_for(task, 5000)
        assert not state.awaiting_validation
        assert state.is_fetchable()
        assert state.position.offset == 100

    def test_retriable_error_retries_within_one_driver_and_gates_fetch(
            self, broker, net, manager, fetcher):
        """A transient error must not deliver records past truncation.

        The partition stays non-fetchable across the retriable failure, the
        driver sleeps retry_backoff_ms and retries by itself, and the
        eventual truncation answer (end_offset 80 < position 100) seeks the
        position to the broker-confirmed divergence point.
        """
        tp = self.TP
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)
        assert not fetcher._subscriptions.is_fetchable(tp)

        broker.respond(OffsetForLeaderEpochRequest, _ofle_response(
            error_code=Errors.NotLeaderForPartitionError.errno,
            leader_epoch=-1, end_offset=-1))
        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=3, end_offset=80))

        start = time.monotonic()
        net.run(fetcher._validate_offsets_async, 5000)
        elapsed = time.monotonic() - start

        state = fetcher._subscriptions.assignment[tp]
        assert not state.awaiting_validation
        assert fetcher._cached_log_truncation is None
        # Protocol: end_offset 80 is the first diverging offset -> seek there.
        assert state.position == OffsetAndMetadata(80, '', 3)
        assert state.is_fetchable()
        assert elapsed >= fetcher.config['retry_backoff_ms'] / 1000 - 0.02
        assert broker.requests_received >= 2

    def test_truncation_does_not_deliver_records_past_divergence(
            self, broker, net, manager, fetcher, recorder):
        """The core regression: when the log is truncated, records buffered
        from the *old* (now-truncated) region must not be delivered, and the
        consumer must continue at the broker-confirmed divergence point.

        Sequence:
          1. A fetch at position 100 returns records 100..104 (old leader's
             log); they sit buffered in the fetcher.
          2. Before the poll drains them, the cluster epoch advances and the
             new leader's OffsetForLeaderEpoch reply says the log diverges
             at offset 80 (records 80.. were rolled back).
          3. The buffered 100..104 records are now garbage: they must not be
             returned. The position must move to 80, and the next fetch must
             request offset 80 -- i.e. the consumer re-reads from the
             divergence, never delivering records at wrong positions.
        """
        tp = self.TP
        fetcher.config['check_crcs'] = False
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))

        # 1. Old leader's fetch response: records 100..104, buffered but
        #    not yet drained by the poll loop.
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch
        partition_data = _fetch_response_with_records(
            _record_batch([(b'k%d' % i, b'v%d' % i) for i in range(100, 105)],
                          offset=100)).responses[0].partitions[0]
        fetcher._completed_fetches.append(
            CompletedFetch(tp, 100, 12, partition_data, MagicMock()))

        # 2. Epoch advances; validation marks the partition and the new
        #    leader reports divergence at 80.
        _broker_metadata(broker, leader_epoch=5)
        net.wait_for(manager.cluster.request_update(), 1000)
        fetcher.maybe_validate_positions()
        assert fetcher._subscriptions.assignment[tp].awaiting_validation

        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _ofle_handler(recorder, _ofle_response(0, 3, 80)))
        net.run(fetcher._validate_offsets_async, 5000)

        state = fetcher._subscriptions.assignment[tp]
        assert state.position == OffsetAndMetadata(80, '', 3)
        assert state.is_fetchable()

        # 3. Draining must not yield the stale 100..104 records: their
        #    next_fetch_offset (100) no longer matches the position (80).
        records, _ = fetcher.fetched_records(10, update_offsets=True)
        assert records == {}, \
            'stale pre-truncation records were delivered at the wrong position'
        assert fetcher._subscriptions.assignment[tp].position.offset == 80

        # And the next fetch request goes out at offset 80.
        requests = fetcher._create_fetch_requests()
        assert broker.node_id in requests
        _, fetch_offsets = requests[broker.node_id]
        assert fetch_offsets[tp] == 80

    def test_truncation_without_reset_policy_raises_and_leaves_position(
            self, broker, net, manager, reset_none_fetcher, recorder):
        """Reset policy NONE: the callback surface gets LogTruncationError
        carrying the divergent offset; the position is NOT moved (the caller
        decides), nothing is auto-reset, and the exception is reported
        exactly once."""
        tp = self.TP
        fetcher = reset_none_fetcher
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _ofle_handler(recorder, _ofle_response(0, 3, 80)))
        net.run(fetcher._validate_offsets_async, 5000)

        with pytest.raises(Errors.LogTruncationError) as exc_info:
            fetcher.validate_offsets_if_needed()
        divergent = exc_info.value.divergent_offsets[tp]
        assert divergent is not None
        assert divergent.offset == 80
        assert divergent.leader_epoch == 3

        state = fetcher._subscriptions.assignment[tp]
        # With no reset policy the fetcher must not invent a recovery point:
        # the position is left where it was, nothing is auto-reset, and the
        # truncation is reported exactly once (the cached exception is consumed
        # by the call that raised it).
        assert state.position.offset == 100
        assert not state.awaiting_reset
        assert fetcher.validate_offsets_if_needed() is None
        assert fetcher._cached_log_truncation is None

    def test_user_seek_during_in_flight_validation_ignores_stale_reply(
            self, broker, net, manager, fetcher, recorder):
        """The user seeks while a validation reply is outstanding.

        The reply computed for the pre-seek position (100, epoch 3) must not
        be applied to the post-seek position (200, epoch 4): a seek is an
        explicit user override, and the reply says nothing about offset 200.
        The new position is then validated on its own against the current
        leader epoch.
        """
        tp = self.TP
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        _broker_metadata(broker, leader_epoch=5)
        net.wait_for(manager.cluster.request_update(), 1000)
        fetcher.maybe_validate_positions()
        assert fetcher._subscriptions.assignment[tp].awaiting_validation

        # Two staged replies: the first is computed for the pre-seek position
        # (truncation to 80); the second validates the post-seek position.
        gate1, gate2 = net.create_future(), net.create_future()
        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _gated_ofle_handler(recorder, net, gate1, _ofle_response(0, 3, 80)))
        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _gated_ofle_handler(recorder, net, gate2, _ofle_response(0, 4, 250)))
        task = fetcher._manager.call_soon(fetcher._validate_offsets_async, 5000)
        assert _pump_until(net, lambda: bool(recorder.ofle_requests()))

        # User seeks while the broker holds the first reply.
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(200, '', 4))
        gate1.success(None)

        # The stale reply (end_offset 80 for the old position) was ignored:
        # the position is still the user's seek target, not 80, and the
        # driver re-arms validation for the new position on its own.
        assert _pump_until(net, lambda: len(recorder.ofle_requests()) >= 2)
        state = fetcher._subscriptions.assignment[tp]
        assert state.position == OffsetAndMetadata(200, '', 4)
        assert not state.awaiting_reset
        assert fetcher._cached_log_truncation is None
        assert state.awaiting_validation
        assert not state.is_fetchable()

        # The retry validates the seeked position's own record epoch.
        assert 'last_fetched_epoch=4' in recorder.ofle_requests()[1]
        gate2.success(None)
        net.wait_for(task, 5000)
        assert not state.awaiting_validation
        assert state.is_fetchable()
        assert state.position.offset == 200
        assert state.position.leader_epoch == 4

    def test_stale_reply_after_revocation_is_ignored(
            self, broker, net, manager, fetcher, recorder):
        """A rebalance revokes the partition while the validation reply is
        outstanding. The late reply must not touch the (now re-assigned)
        partition state, must not be cached as a truncation, and must not
        crash the driver."""
        tp = self.TP
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        gate = net.create_future()
        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _gated_ofle_handler(recorder, net, gate, _ofle_response(0, 3, 80)))
        task = fetcher._manager.call_soon(fetcher._validate_offsets_async, 5000)
        assert _pump_until(net, lambda: bool(recorder.ofle_requests()))

        # Revocation: the partition leaves the assignment entirely.
        fetcher._subscriptions.assign_from_subscribed([])
        assert tp not in fetcher._subscriptions.assignment
        gate.success(None)
        net.wait_for(task, 5000)

        # The truncation reply for the revoked partition was discarded.
        assert fetcher._cached_log_truncation is None

        # Re-assignment starts from a clean state; the old reply must not
        # leak into it.
        fetcher._subscriptions.assign_from_subscribed([tp])
        state = fetcher._subscriptions.assignment[tp]
        assert state.position is None
        assert not state.awaiting_validation
        assert not state.awaiting_reset

    def test_old_broker_without_epoch_info_keeps_partition_fetchable(
            self, broker, net, manager, fetcher, recorder):
        """Legacy brokers (no leader_epoch in metadata, positions without a
        record epoch) cannot be validated -- and must not be gated either.

        KIP-320 only applies when epochs are available; with no epoch
        information the consumer falls back to pre-KIP-320 behavior: fetch
        directly, no OffsetForLeaderEpoch RPC on the wire.
        """
        tp = self.TP
        state = fetcher._subscriptions.assignment[tp]

        # Case 1: metadata carries no epoch (pre-2.4 broker / unreliable
        # epoch), position has one. Nothing to validate against.
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        manager.cluster._partitions[TOPIC][PARTITION].leader_epoch = -1
        fetcher.maybe_validate_positions()
        assert not state.awaiting_validation
        assert state.is_fetchable()

        # Case 2: metadata has an epoch but the position does not (bare seek
        # to an offset). There is no record epoch to validate.
        manager.cluster._partitions[TOPIC][PARTITION].leader_epoch = 5
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', -1))
        fetcher.maybe_validate_positions()
        assert not state.awaiting_validation
        assert state.is_fetchable()

        # The validation driver is a no-op and sends nothing on the wire.
        received_before = broker.requests_received
        net.run(fetcher._validate_offsets_async, 500)
        assert broker.requests_received == received_before
        assert recorder.ofle_requests() == []

    def test_successful_validation_preserves_position_record_epoch(
            self, broker, net, manager, fetcher, recorder):
        """A no-truncation reply confirms the position; it must not rewrite
        the position's record epoch.

        The OffsetForLeaderEpoch response's leader_epoch is the epoch of the
        *requested* point (or, on some brokers, a newer epoch) -- it is NOT
        the epoch of the record at the consumer's offset. The position's
        leader_epoch is sent as last_fetched_epoch on Fetch v12+ for KIP-595
        divergence detection; overwriting it with the response epoch would
        misreport which epoch the consumer's last record belongs to.
        """
        tp = self.TP
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        _broker_metadata(broker, leader_epoch=5)
        net.wait_for(manager.cluster.request_update(), 1000)
        fetcher.maybe_validate_positions()

        # Broker answers with a NEWER epoch than the position's record epoch
        # (e.g. the epoch of the log-end entry), end_offset beyond position.
        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _ofle_handler(recorder, _ofle_response(0, 9, 100)))
        net.run(fetcher._validate_offsets_async, 5000)

        state = fetcher._subscriptions.assignment[tp]
        assert not state.awaiting_validation
        assert state.is_fetchable()
        assert state.position.offset == 50
        assert state.position.leader_epoch == 3, (
            'validation success must not rewrite the record epoch of the '
            'position (it feeds last_fetched_epoch on Fetch v12+)')

        # The partition is reconciled against cluster epoch 5, so the next
        # poll does not re-validate...
        fetcher.maybe_validate_positions()
        assert not state.awaiting_validation
        # ...but a further election (epoch 7) does, and the wire still
        # carries the position's true record epoch as last_fetched_epoch.
        _broker_metadata(broker, leader_epoch=7)
        net.wait_for(manager.cluster.request_update(), 1000)
        fetcher.maybe_validate_positions()
        assert state.awaiting_validation
        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _ofle_handler(recorder, _ofle_response(0, 3, 100)))
        net.run(fetcher._validate_offsets_async, 5000)
        last_request = recorder.ofle_requests()[-1]
        assert 'last_fetched_epoch=3' in last_request
        assert 'current_epoch=7' in last_request

    def test_undefined_epoch_response_makes_reset_allowed_and_resets(
            self, broker, net, manager, fetcher, recorder):
        """UNDEFINED epoch response with a reset policy: the partition moves
        to awaiting-reset (not awaiting-validation), becomes resettable
        immediately, and the ListOffsets reset drives it to the policy
        offset, after which it is fetchable again."""
        tp = self.TP
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond_fn(
            OffsetForLeaderEpochRequest,
            _ofle_handler(recorder, _ofle_response(0, -1, -1)))
        net.run(fetcher._validate_offsets_async, 5000)

        state = fetcher._subscriptions.assignment[tp]
        # No known divergence point -> fall back to the reset policy.
        assert state.awaiting_reset
        assert not state.awaiting_validation
        assert state.is_reset_allowed()
        assert not state.is_fetchable()
        assert fetcher._cached_log_truncation is None
        assert tp in fetcher._subscriptions.partitions_needing_reset()

        # The reset itself goes through ListOffsets (earliest policy -> 5).
        broker.respond(ListOffsetsRequest, _list_offsets_response(5))
        net.run(fetcher._reset_offsets_async, 5000)

        assert not state.awaiting_reset
        assert state.position is not None
        assert state.position.offset == 5
        assert state.is_fetchable()
