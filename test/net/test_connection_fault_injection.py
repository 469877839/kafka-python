"""Fault-injection tests for the request/response correlation chain.

No real network is used: a ``ScriptedTransport`` stands in for the socket and
lets each test arrange the exact order of events (fragmented frames, glued
frames, unknown/duplicate correlation ids, connection loss + reconnect,
partial writes, undecodable responses) and then verify, per scenario:

  * how many times each future completes (exactly once, or not at all),
  * which exception each future is attributed,
  * the in-flight queue contents,
  * the connection recovery state.

Failures on one node must never complete or fail another node's futures --
these tests assert per-connection attribution instead of blanket "everything
is cleared" checks. See ``test/net/request_lifecycle.md`` for the request
lifecycle table this suite pins down.
"""

import time

import pytest

import kafka.errors as Errors
from kafka.net.backend.selector import NetworkSelector
from kafka.net.connection import KafkaConnection
from kafka.protocol.metadata import MetadataRequest, MetadataResponse
from kafka.protocol.schemas.fields.codecs import Int32


REQUEST_TIMEOUT_MS = 40  # keep the regression suite in the sub-second range


class ScriptedTransport:
    """Controllable socket stand-in implementing the NetTransport surface.

    The write side is scripted via ``write_script`` (e.g. accept only a
    partial prefix, then fail). The read side is driven by the test through
    ``deliver()`` / ``drop()``, which invoke the protocol callbacks exactly
    the way KafkaTCPTransport's read/write loops would -- including aborting
    the connection when the protocol raises KafkaProtocolError.
    """

    def __init__(self):
        self.protocol = None
        self.written = bytearray()
        self.write_script = []
        self.abort_errors = []
        self.closed = False

    # -- transport API used by KafkaConnection --

    def get_peer(self):
        return ('scripted', 9092, 0, 0)

    def host_port(self):
        return 'scripted:9092'

    def set_protocol(self, protocol):
        self.protocol = protocol

    def get_protocol(self):
        return self.protocol

    def resume_reading(self):
        pass

    def pause_reading(self):
        pass

    def is_closing(self):
        return self.closed

    def write(self, data):
        data = bytes(data)
        if self.write_script:
            action = self.write_script.pop(0)
            if action[0] == 'partial':
                nbytes = action[1]
                self.written += data[:nbytes]
                raise Errors.KafkaConnectionError(
                    'socket send failed after %d of %d bytes' % (nbytes, len(data)))
            if action[0] == 'fail':
                raise action[1]
            raise AssertionError('unknown write script action: %r' % (action,))
        self.written += data
        return len(data)

    def abort(self, error=None):
        self.abort_errors.append(error)
        self.closed = True
        if self.protocol is not None:
            self.protocol.connection_lost(error)

    def close(self):
        self.closed = True
        if self.protocol is not None:
            self.protocol.connection_lost(None)

    # -- test-side event injection --

    def deliver(self, data):
        """Feed inbound bytes to the connection.

        Mirrors KafkaTCPTransport._read_from_sock: a KafkaProtocolError
        raised by the protocol aborts the connection with that error.
        """
        try:
            self.protocol.data_received(data)
        except Errors.KafkaProtocolError as exc:
            self.abort(exc)

    def drop(self, exc=None):
        """Simulate the peer disappearing (RST / EOF)."""
        if exc is None:
            exc = Errors.KafkaConnectionError('socket disconnected')
        self.closed = True
        self.protocol.connection_lost(exc)


class FutureTracker:
    """Records every completion of a future so tests can assert exactly-once.

    Future.add_both dispatches f(value) on success and f(exception) on
    failure, so the outcome kind is derived from the argument itself.
    """

    def __init__(self, future):
        self.future = future
        self.outcomes = []
        future.add_both(self._record)

    def _record(self, result):
        if isinstance(result, BaseException):
            self.outcomes.append(('error', result))
        else:
            self.outcomes.append(('value', result))

    @property
    def done_count(self):
        return len(self.outcomes)

    @property
    def values(self):
        return [v for kind, v in self.outcomes if kind == 'value']

    @property
    def errors(self):
        return [e for kind, e in self.outcomes if kind == 'error']


class Harness:
    """A connected KafkaConnection over a ScriptedTransport."""

    def __init__(self, net, node_id='node-0', **configs):
        configs.setdefault('request_timeout_ms', REQUEST_TIMEOUT_MS)
        self.conn = KafkaConnection(net, node_id=node_id, **configs)
        self.transport = ScriptedTransport()
        self.conn.connection_made(self.transport)
        self.conn._init_complete()
        assert self.conn.connected

    def send(self):
        future = self.conn.send_request(MetadataRequest[0]([]))
        return future, FutureTracker(future)

    def correlation_id(self, future):
        for cid, fut, *_ in self.conn.in_flight_requests:
            if fut is future:
                return cid
        raise AssertionError('future is not in flight')

    def in_flight_count(self):
        return len(self.conn.in_flight_requests)


def metadata_frame(correlation_id, broker_node_id):
    """A framed MetadataResponse[0] identifiable by its broker node_id."""
    response = MetadataResponse[0](
        throttle_time_ms=0,
        brokers=[(broker_node_id, 'broker-%d' % broker_node_id, 9092)],
        cluster_id=None,
        controller_id=-1,
        topics=[])
    payload = Int32.encode(correlation_id) + response.encode()
    return Int32.encode(len(payload)) + payload


def broker_node_id(response):
    return response.brokers[0].node_id


@pytest.fixture
def net():
    backend = NetworkSelector()
    try:
        yield backend
    finally:
        backend.close()


@pytest.fixture
def harness(net):
    return Harness(net)


def drive_timers(net):
    """Fire any due scheduled tasks (e.g. per-request timeout timers)."""
    net.drain(scheduled=True)


class TestFragmentedLengthPrefix:
    def test_frame_header_and_body_split_across_reads(self, harness):
        future, tracker = harness.send()
        frame = metadata_frame(harness.correlation_id(future), broker_node_id=11)

        # 4-byte length prefix arrives in three pieces, then the body in two.
        for chunk in (frame[:1], frame[1:3], frame[3:4]):
            harness.transport.deliver(chunk)
            assert not future.is_done
            assert harness.in_flight_count() == 1

        mid = 4 + (len(frame) - 4) // 2
        harness.transport.deliver(frame[4:mid])
        assert not future.is_done
        assert harness.in_flight_count() == 1

        harness.transport.deliver(frame[mid:])
        assert tracker.done_count == 1
        assert broker_node_id(tracker.values[0]) == 11
        assert harness.in_flight_count() == 0
        assert harness.conn.connected
        assert not harness.conn.closed

    def test_single_byte_drips(self, harness):
        future, tracker = harness.send()
        frame = metadata_frame(harness.correlation_id(future), broker_node_id=12)
        for i in range(len(frame)):
            harness.transport.deliver(frame[i:i + 1])
        assert tracker.done_count == 1
        assert broker_node_id(tracker.values[0]) == 12
        assert harness.in_flight_count() == 0
        assert harness.conn.connected


class TestGluedResponses:
    def test_two_responses_in_one_read_complete_in_order(self, harness):
        fut1, trk1 = harness.send()
        fut2, trk2 = harness.send()
        cid1 = harness.correlation_id(fut1)
        cid2 = harness.correlation_id(fut2)
        assert cid1 != cid2

        glued = metadata_frame(cid1, 21) + metadata_frame(cid2, 22)
        harness.transport.deliver(glued)

        assert trk1.done_count == 1
        assert trk2.done_count == 1
        # FIFO correlation: first response completes the first request.
        assert broker_node_id(trk1.values[0]) == 21
        assert broker_node_id(trk2.values[0]) == 22
        assert harness.in_flight_count() == 0
        assert harness.conn.connected


class TestUnknownCorrelationId:
    def test_response_with_no_in_flight_requests(self, harness):
        harness.transport.deliver(metadata_frame(99, 31))

        assert harness.conn.closed
        assert isinstance(harness.conn.close_future.exception, Errors.CorrelationIdError)
        assert harness.in_flight_count() == 0

    def test_unrelated_request_on_same_connection_fails_with_cause(self, harness):
        future, tracker = harness.send()
        harness.transport.deliver(metadata_frame(99, 31))

        # The stray response is never attributed to the pending request...
        assert tracker.values == []
        # ...it fails once the transport aborts with the protocol error.
        assert tracker.done_count == 1
        assert isinstance(tracker.errors[0], Errors.CorrelationIdError)
        assert harness.in_flight_count() == 0
        assert harness.conn.closed


class TestDuplicateCorrelationId:
    def test_replayed_response_is_rejected(self, harness):
        fut1, trk1 = harness.send()
        cid1 = harness.correlation_id(fut1)
        harness.transport.deliver(metadata_frame(cid1, 41))
        assert trk1.done_count == 1
        assert broker_node_id(trk1.values[0]) == 41

        # A duplicate of the first response arrives while a second request
        # is in flight: it must not complete the second request.
        fut2, trk2 = harness.send()
        harness.transport.deliver(metadata_frame(cid1, 99))

        assert trk1.done_count == 1  # never completed twice
        assert trk2.values == []
        assert trk2.done_count == 1
        assert isinstance(trk2.errors[0], Errors.CorrelationIdError)
        assert harness.in_flight_count() == 0
        assert harness.conn.closed


class TestMismatchedCorrelationId:
    """Deliberately cross-wired double: proves the assertions detect swapped
    correlation ids instead of silently mis-attributing responses."""

    def test_swapped_response_ids_close_the_connection(self, harness):
        fut1, trk1 = harness.send()
        fut2, trk2 = harness.send()
        cid1 = harness.correlation_id(fut1)
        cid2 = harness.correlation_id(fut2)

        # Swap: fut1's response carries cid2, fut2's carries cid1.
        swapped = metadata_frame(cid2, 51) + metadata_frame(cid1, 52)
        harness.transport.deliver(swapped)

        # Neither future may observe the wrong response as a success.
        assert trk1.values == []
        assert trk2.values == []
        # Both fail exactly once with the connection-level cause.
        assert trk1.done_count == 1
        assert trk2.done_count == 1
        assert isinstance(trk1.errors[0], Errors.CorrelationIdError)
        assert isinstance(trk2.errors[0], Errors.CorrelationIdError)
        assert isinstance(harness.conn.close_future.exception, Errors.CorrelationIdError)
        assert harness.in_flight_count() == 0
        assert harness.conn.closed


class TestConnectionCloseAndReconnect:
    def test_close_fails_in_flight_then_new_connection_recovers(self, net):
        first = Harness(net, node_id='node-0')
        fut1, trk1 = first.send()
        fut2, trk2 = first.send()

        reset = Errors.KafkaConnectionError('connection reset by peer')
        first.transport.drop(reset)

        for tracker in (trk1, trk2):
            assert tracker.done_count == 1
            assert tracker.errors[0] is reset
        assert first.in_flight_count() == 0
        assert first.conn.closed
        assert first.conn.close_future.exception is reset
        assert first.conn.init_future.succeeded()  # init completed before the drop

        # Reconnect: a fresh connection for the same node works independently.
        second = Harness(net, node_id='node-0')
        fut3, trk3 = second.send()
        second.transport.deliver(metadata_frame(second.correlation_id(fut3), 61))
        assert trk3.done_count == 1
        assert broker_node_id(trk3.values[0]) == 61
        assert second.conn.connected

        # Recovery must not retroactively touch the old connection's futures.
        assert trk1.done_count == 1
        assert trk2.done_count == 1

    def test_failure_is_scoped_to_its_own_node(self, net):
        node_a = Harness(net, node_id='node-a')
        node_b = Harness(net, node_id='node-b')
        fut_a, trk_a = node_a.send()
        fut_b, trk_b = node_b.send()

        node_a.transport.drop(Errors.KafkaConnectionError('node-a reset'))

        # Node A's failure must not drain or fail node B's in-flight request.
        assert trk_a.done_count == 1
        assert trk_b.done_count == 0
        assert node_b.in_flight_count() == 1
        assert node_b.conn.connected
        assert not node_b.conn.close_future.is_done

        node_b.transport.deliver(metadata_frame(node_b.correlation_id(fut_b), 62))
        assert trk_b.done_count == 1
        assert broker_node_id(trk_b.values[0]) == 62


class TestPartialWriteFailure:
    def test_write_fails_mid_request(self, harness, net):
        # The socket accepts only a prefix of the request bytes, then fails.
        harness.transport.write_script = [('partial', 5)]
        with pytest.raises(Errors.KafkaConnectionError):
            harness.conn.send_request(MetadataRequest[0]([]))

        # Current behavior, pinned: the future is already registered in
        # flight and is left pending -- the "future never completes" symptom.
        # Only the request-timeout path recovers it.
        assert harness.in_flight_count() == 1
        assert len(harness.transport.written) == 5
        assert harness.conn.connected  # no automatic close on write error

        (_cid, future, _sent, _timeout_at, _task) = harness.conn.in_flight_requests[0]
        tracker = FutureTracker(future)
        time.sleep(REQUEST_TIMEOUT_MS / 1000 * 1.5)
        drive_timers(net)

        assert tracker.done_count == 1
        assert isinstance(tracker.errors[0], Errors.RequestTimedOutError)
        assert harness.in_flight_count() == 0
        assert harness.conn.closed
        assert isinstance(harness.conn.close_future.exception, Errors.RequestTimedOutError)


class TestResponseParseError:
    def test_undecodable_response_body(self, harness):
        future, tracker = harness.send()
        cid = harness.correlation_id(future)

        # Valid frame length and correlation id, but the body claims one
        # broker and then truncates -- decode fails.
        payload = Int32.encode(cid) + Int32.encode(1)
        harness.transport.deliver(Int32.encode(len(payload)) + payload)

        assert tracker.values == []
        assert tracker.done_count == 1
        assert isinstance(tracker.errors[0], Errors.KafkaProtocolError)
        assert isinstance(harness.conn.close_future.exception, Errors.KafkaProtocolError)
        assert harness.in_flight_count() == 0
        assert harness.conn.closed


class TestRequestTimeout:
    def test_unanswered_request_times_out_and_closes(self, harness, net):
        future, tracker = harness.send()
        assert harness.in_flight_count() == 1

        time.sleep(REQUEST_TIMEOUT_MS / 1000 * 1.5)
        drive_timers(net)

        assert tracker.done_count == 1
        assert isinstance(tracker.errors[0], Errors.RequestTimedOutError)
        assert harness.in_flight_count() == 0
        assert harness.conn.closed
        assert isinstance(harness.conn.close_future.exception, Errors.RequestTimedOutError)
        assert harness.transport.abort_errors, 'timeout must abort the transport'
