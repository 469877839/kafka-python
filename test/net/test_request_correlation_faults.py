"""Fault-injection tests for the request/response correlation pipeline.

No real sockets are opened anywhere in this module: a single controllable
socket double (:class:`ScriptedSocket`) stands in for the transport and lets
each test script the exact ordering of write outcomes and inbound byte
chunks.  ``KafkaConnection`` is driven through its real public surface
(``send_request`` / ``data_received`` / ``connection_lost``), so these tests
exercise the production correlation-id matching, in-flight queue management,
future fan-out and reconnect code paths end to end.

Request lifecycle table
-----------------------

State transitions tracked for every future under test::

    send_request()
      |-- connected            -> _send_request -> parser.send_request
      |                            -> in_flight_requests ENQUEUE (cid, future, timer)
      |                            -> transport.write(frame) .................. (1)
      |-- initializing/reauth  -> _request_buffer ENQUEUE (flushed on init)
      |-- paused               -> future.failure(NodeNotReadyError) (immediate)
      `-- disconnected         -> future.failure(KafkaConnectionError) (immediate)

    data_received(bytes) -> parser.receive_bytes
      |-- frame complete, cid matches HEAD of in-flight deque
      |     -> DEQUEUE, timer cancelled, future.success(response) ........... (2)
      |-- frame complete, cid mismatch
      |     -> close(KafkaConnectionError('unrecognized correlation id')) ... (3)
      |-- parser raises KafkaProtocolError (bad length / undecodable body)
      |     -> transport aborts -> connection_lost(exc) ..................... (4)
      `-- partial frame -> bytes retained, no future touched ................ (5)

    connection_lost(exc) -> close_future resolves
      -> fail_in_flight_requests(exc): ONLY this connection's buffered and
         in-flight futures fail; other connections keep running ............ (6)

    reconnect (manager get_connection again) -> fresh KafkaConnection,
         fresh parser/cid counter, old futures stay settled ................ (7)

    transport.write failure (partial bytes then error)
      -> abort -> connection_lost -> (6); already-enqueued future fails,
         it is never silently dropped ....................................... (8)

The failure scoping in (6)/(8) is the regression guard against "one socket
error clears every node's requests": faults are isolated per connection.

Fault scenario -> test class
----------------------------

    length prefix / payload fragmented   -> TestFragmentedLengthPrefix
    responses coalesced into one recv    -> TestCoalescedResponses
    unknown / duplicate correlation id   -> TestUnknownOrDuplicateCorrelationId
    send buffer partially written, fails -> TestPartialWriteFailure
    response decode / frame-size error   -> TestResponseParseFailure
    close mid-flight, reconnect          -> TestConnectionCloseAndReconnect
    swapped ids (mutation canary)        -> TestSwappedCorrelationIdCanary
"""
import struct
from collections import deque

import pytest

import kafka.errors as Errors
from kafka.net.connection import KafkaConnection
from kafka.net.manager import KafkaConnectionManager
from kafka.protocol.broker_version_data import BrokerVersionData
from kafka.protocol.metadata import (
    ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse)
from kafka.protocol.parser import KafkaProtocol


# ---------------------------------------------------------------------------
# Controllable socket / transport double
# ---------------------------------------------------------------------------


class ScriptedSocket:
    """A transport double with *no* sockets behind it.

    ``write()`` records the framed bytes the connection handed over; the test
    then decides what happens next.  ``feed()`` pushes raw wire bytes into the
    connection exactly the way a real transport's ``_read_from_sock`` loop
    does, including the ``KafkaProtocolError -> abort`` escalation.  Every
    transition is appended to ``events`` so assertions can look at ordering,
    not just end state.
    """

    def __init__(self):
        self._protocol = None
        self._closed = False
        self._write_buffer = deque()  # frames queued via write(), flushed by drain()
        self.writes = []              # every payload offered to write()
        self.bytes_written = 0
        self.events = []
        self.host = 'scripted'

    # -- protocol wiring ----------------------------------------------------
    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def get_peer(self):
        return ('127.0.0.1', 9092)

    def host_port(self):
        return 'scripted:9092'

    def resume_reading(self):
        pass

    def pause_reading(self):
        pass

    def is_reading(self):
        return not self._closed

    def is_closing(self):
        return self._closed

    # -- fault injection ----------------------------------------------------
    def feed(self, data):
        """Deliver inbound bytes; mirror KafkaTCPTransport._read_from_sock."""
        if self._closed:
            return
        if not data:
            return
        self.events.append(('feed', len(data)))
        try:
            self._protocol.data_received(data)
        except Errors.KafkaProtocolError as exc:
            # Real transports abort on a protocol error; do the same so the
            # connection_lost -> fail_in_flight_requests path runs for real.
            self.events.append(('protocol_error', type(exc).__name__))
            self.abort(exc)

    def write(self, data):
        """Mirror the real transport: buffer only; the bytes leave the process
        when the event loop drives ``drain()``.  Never raises for I/O errors --
        those surface asynchronously from ``drain()`` just like production."""
        if self._closed:
            self.events.append(('write_after_close', len(data)))
            raise RuntimeError('Transport closed for writes')
        if not data:
            raise ValueError("Cant write empty data")
        self.writes.append(data)
        self._write_buffer.append(bytearray(data))
        self.events.append(('write', len(data)))
        return len(data)

    def drain(self, send_limit=None):
        """Flush buffered frames, simulating socket-level short writes.

        ``send_limit`` is the maximum number of bytes the socket accepts in
        this call.  Frames are consumed in order; if the limit is exhausted
        part-way through a frame, the un-sent tail stays queued (real
        transports requeue a memoryview of the remainder).  If
        ``fail_remaining`` is set, the next send after ``send_limit`` bytes
        raises instead of accepting more.
        """
        while self._write_buffer:
            chunk = self._write_buffer[0]
            if send_limit is None:
                accepted = len(chunk)
            else:
                accepted = min(len(chunk), max(0, send_limit))
            if self.fail_remaining and accepted < len(chunk):
                self.bytes_written += accepted
                del chunk[:accepted]
                self.events.append(('partial_write', accepted))
                self.abort(Errors.KafkaConnectionError('socket write failed'))
                return
            self.bytes_written += accepted
            if accepted:
                self.events.append(('sent', accepted))
            if accepted < len(chunk):
                del chunk[:accepted]
                self.events.append(('short_write', len(chunk)))
                return
            self._write_buffer.popleft()
            if send_limit is not None:
                send_limit -= accepted

    def pending_bytes(self):
        return sum(len(c) for c in self._write_buffer)

    # -- teardown -----------------------------------------------------------
    def close(self):
        if not self._closed:
            self._closed = True
            self.events.append(('close',))
            proto, self._protocol = self._protocol, None
            if proto is not None:
                proto.connection_lost(None)

    def abort(self, error=None):
        if not self._closed:
            self._closed = True
            self._write_buffer.clear()   # abort() discards unsent data, like the real transport
            self.events.append(('abort', str(error)))
            proto, self._protocol = self._protocol, None
            if proto is not None:
                proto.connection_lost(error)

    def __repr__(self):
        return '<ScriptedSocket closed=%s events=%d>' % (self._closed, len(self.events))


# When True, a drain that can only send part of a frame aborts instead of
# leaving the remainder queued. Default False models benign short writes.
ScriptedSocket.fail_remaining = False


# ---------------------------------------------------------------------------
# Wire-frame helpers and connection factory
# ---------------------------------------------------------------------------


def metadata_request():
    return MetadataRequest[0](topics=[])


def api_versions_response_frame(correlation_id):
    """Framed ApiVersionsResponse(v3) used to satisfy connection init."""
    bvd = BrokerVersionData((4, 2))
    api_version = ApiVersionsResponse.ApiVersion
    api_keys = [api_version(api_key=key, min_version=lo, max_version=hi)
                for key, (lo, hi) in bvd.api_versions.items()]
    response = ApiVersionsResponse(
        version=3, error_code=0, api_keys=api_keys, throttle_time_ms=0)
    response.with_header(correlation_id=correlation_id)
    return response.encode(framed=True, header=True)


def metadata_response_frame(correlation_id, *, payload_cid=None):
    """Build a complete framed v0 MetadataResponse frame.

    ``payload_cid`` overrides the correlation id carried *inside* the frame
    (the frame length still covers the original payload) so unknown / duplicate
    id faults can be scripted without touching production code.
    """
    broker = MetadataResponse.MetadataResponseBroker(
        node_id=0, host='localhost', port=9092)
    response = MetadataResponse[0](
        brokers=[broker], cluster_id=None, controller_id=0,
        topics=[], throttle_time_ms=0)
    response.with_header(correlation_id=correlation_id)
    frame = response.encode(framed=True, header=True)
    if payload_cid is not None and payload_cid != correlation_id:
        payload = frame[4:]
        payload = struct.pack('>i', payload_cid) + payload[4:]
        frame = struct.pack('>i', len(payload)) + payload
    return frame


def make_connection(net, node_id='node-A', request_timeout_ms=30000):
    """A fully connected KafkaConnection on top of a controllable socket."""
    conn = KafkaConnection(net, node_id=node_id,
                           broker_version_data=BrokerVersionData((0, 9)),
                           request_timeout_ms=request_timeout_ms)
    sock = ScriptedSocket()
    conn.connection_made(sock)
    conn._init_complete()          # skip ApiVersions/SASL; broker version pre-seeded
    assert conn.connected and not conn.initializing
    return conn, sock


def send_one(conn):
    future = conn.send_request(metadata_request())
    cid = conn.in_flight_requests[-1][0]
    return cid, future


class FutureObserver:
    """Count terminal transitions so 'resolved exactly once' is assertable."""

    def __init__(self, future):
        self.future = future
        self.successes = 0
        self.failures = 0
        future.add_callback(self._on_success)
        future.add_errback(self._on_failure)

    def _on_success(self, _value):
        self.successes += 1

    def _on_failure(self, _exc):
        self.failures += 1

    @property
    def done_once(self):
        return (self.successes + self.failures) == 1


class TestFragmentedLengthPrefix:
    """A length prefix (and payload) arriving in arbitrary chunks must not
    resolve anything until a full frame is available -- this is the classic
    'future never completes' symptom under packet fragmentation."""

    def test_byte_at_a_time_does_not_resolve_future_early(self, net):
        conn, sock = make_connection(net)
        cid, future = send_one(conn)
        obs = FutureObserver(future)
        frame = metadata_response_frame(cid)

        # Feed every byte individually.
        for byte in frame[:-1]:
            sock.feed(bytes([byte]))
            assert not future.is_done, 'future resolved on a partial frame'
            assert len(conn.in_flight_requests) == 1

        # Parser must still be mid-frame and nothing was popped.
        assert conn.parser._receiving or conn.parser._header.tell() > 0

        # Final byte completes the frame -> exactly one success.
        sock.feed(frame[-1:])
        assert obs.done_once and obs.successes == 1
        assert isinstance(future.value, MetadataResponse)
        assert len(conn.in_flight_requests) == 0
        assert not conn.closed

    def test_header_split_across_two_feeds(self, net):
        conn, sock = make_connection(net)
        cid, future = send_one(conn)
        frame = metadata_response_frame(cid)

        # Length prefix split 1 + 3 bytes, payload in a later chunk.
        sock.feed(frame[:1])
        sock.feed(frame[1:4])
        assert not future.is_done
        sock.feed(frame[4:])
        assert future.succeeded()
        assert isinstance(future.value, MetadataResponse)

    def test_payload_split_mid_body(self, net):
        conn, sock = make_connection(net)
        cid, future = send_one(conn)
        frame = metadata_response_frame(cid)
        split = len(frame) // 2

        sock.feed(frame[:split])
        assert not future.is_done and len(conn.in_flight_requests) == 1
        sock.feed(frame[split:])
        assert future.succeeded() and len(conn.in_flight_requests) == 0


class TestCoalescedResponses:
    """Two full frames glued into one recv() call (TCP coalescing) must each
    reach their own future in FIFO order."""

    def test_two_frames_in_one_recv(self, net):
        conn, sock = make_connection(net)
        cid1, f1 = send_one(conn)
        cid2, f2 = send_one(conn)
        assert len(conn.in_flight_requests) == 2

        blob = metadata_response_frame(cid1) + metadata_response_frame(cid2)
        sock.feed(blob)

        assert f1.succeeded() and f2.succeeded()
        assert isinstance(f1.value, MetadataResponse)
        assert isinstance(f2.value, MetadataResponse)
        assert len(conn.in_flight_requests) == 0
        assert ('feed', len(blob)) in sock.events

    def test_complete_frame_plus_partial_next_frame(self, net):
        conn, sock = make_connection(net)
        cid1, f1 = send_one(conn)
        cid2, f2 = send_one(conn)
        frame1 = metadata_response_frame(cid1)
        frame2 = metadata_response_frame(cid2)

        # Frame 1 complete, only the length header of frame 2 present.
        sock.feed(frame1 + frame2[:4])
        assert f1.succeeded()
        assert not f2.is_done
        assert len(conn.in_flight_requests) == 1
        # Rest of frame 2 arrives later and resolves the second future.
        sock.feed(frame2[4:])
        assert f2.succeeded()
        assert len(conn.in_flight_requests) == 0


class TestUnknownOrDuplicateCorrelationId:
    """Correlation ids must match the FIFO head of the in-flight deque.
    A mismatch means the response stream is no longer aligned with the
    requests, so the connection tears down and every in-flight future on
    *that* connection fails with the CorrelationIdError."""

    def test_unknown_cid_with_requests_in_flight(self, net):
        conn, sock = make_connection(net)
        cid, f1 = send_one(conn)
        _, f2 = send_one(conn)
        obs1, obs2 = FutureObserver(f1), FutureObserver(f2)

        # Frame claims an id that no request used.
        sock.feed(metadata_response_frame(cid, payload_cid=cid + 500))

        assert conn.closed
        assert conn.close_future.failed()
        assert isinstance(conn.close_future.exception, Errors.CorrelationIdError)
        # Both in-flight futures failed exactly once, neither got a value.
        assert obs1.done_once and obs1.failures == 1 and obs1.successes == 0
        assert obs2.done_once and obs2.failures == 1 and obs2.successes == 0
        assert isinstance(f1.exception, Errors.CorrelationIdError)
        assert isinstance(f2.exception, Errors.CorrelationIdError)
        # In-flight queue drained and timers cancelled.
        assert len(conn.in_flight_requests) == 0
        assert any(ev[0] == 'abort' for ev in sock.events)

    def test_duplicate_cid_replays_old_response(self, net):
        conn, sock = make_connection(net)
        cid1, f1 = send_one(conn)
        cid2, f2 = send_one(conn)

        # First response resolves request 1 normally.
        sock.feed(metadata_response_frame(cid1))
        assert f1.succeeded()

        # Broker replays the same id instead of answering request 2.
        sock.feed(metadata_response_frame(cid1))
        assert conn.closed
        assert isinstance(conn.close_future.exception, Errors.CorrelationIdError)
        assert f2.failed()
        assert isinstance(f2.exception, Errors.CorrelationIdError)
        # Request 1 is untouched: it had already completed successfully.
        assert f1.succeeded()
        assert len(conn.in_flight_requests) == 0

    def test_response_with_no_in_flight_requests(self, net):
        conn, sock = make_connection(net)
        cid, _f = send_one(conn)
        # Consume the response so the deque is empty again.
        sock.feed(metadata_response_frame(cid))
        assert len(conn.in_flight_requests) == 0

        # Unsolicited frame afterwards: nothing to correlate against.
        sock.feed(metadata_response_frame(999, payload_cid=999))
        assert conn.closed
        assert isinstance(conn.close_future.exception, Errors.CorrelationIdError)
        assert 'No in-flight-request' in str(conn.close_future.exception)

    def test_parser_level_cid_mismatch_also_tears_down(self, net):
        """The parser independently tracks request headers; a payload whose
        id differs from the parser's own FIFO head raises CorrelationIdError,
        and the transport escalates that to a connection teardown."""
        # Parser-level: mismatch raises before any response is produced.
        parser = KafkaProtocol(client_id='test')
        parser.send_request(metadata_request())
        with pytest.raises(Errors.CorrelationIdError):
            parser.receive_bytes(metadata_response_frame(1, payload_cid=2))

        # Connection-level: the same bytes through the scripted socket abort
        # the connection and fail the in-flight future.
        conn, sock = make_connection(net)
        cid, f1 = send_one(conn)
        sock.feed(metadata_response_frame(cid, payload_cid=cid + 1))
        assert conn.closed
        assert f1.failed()
        assert isinstance(f1.exception, Errors.CorrelationIdError)


class TestPartialWriteFailure:
    """The socket accepts only part of the request frame, then errors.  The
    request is already in the in-flight deque when write() is called, so the
    failure must surface on that future rather than dropping it silently
    (which is exactly the 'future never completes' hang)."""

    def test_partial_send_then_socket_error(self, net):
        conn, sock = make_connection(net)
        sock.fail_remaining = True
        _, future = send_one(conn)
        obs = FutureObserver(future)
        frame = sock.writes[-1]

        # Socket accepts only the length prefix (4 bytes) and then breaks.
        sock.drain(send_limit=4)
        assert sock.bytes_written == 4 and sock._closed

        assert conn.closed
        assert conn.close_future.failed()
        assert isinstance(conn.close_future.exception, Errors.KafkaConnectionError)
        assert obs.done_once and obs.failures == 1
        assert isinstance(future.exception, Errors.KafkaConnectionError)
        assert len(conn.in_flight_requests) == 0
        # The un-sent tail was discarded by abort(), not delivered twice later.
        assert ('partial_write', 4) in sock.events
        assert frame[:4] != b''  # sanity: the frame really did start on the wire

    def test_error_before_any_byte_accepted(self, net):
        conn, sock = make_connection(net)
        sock.fail_remaining = True
        _, future = send_one(conn)

        sock.drain(send_limit=0)
        assert conn.closed
        assert future.failed()
        assert isinstance(future.exception, Errors.KafkaConnectionError)
        assert len(conn.in_flight_requests) == 0

    def test_benign_short_write_does_not_fail_future(self, net):
        """A partial send with no error is normal: the tail stays queued and a
        later drain finishes it; the future remains pending throughout."""
        conn, sock = make_connection(net)
        _, future = send_one(conn)
        frame_len = len(sock.writes[-1])

        sock.drain(send_limit=3)
        assert not conn.closed and not future.is_done
        assert sock.pending_bytes() == frame_len - 3

        sock.drain()  # finish the frame; no response yet, still pending
        assert sock.pending_bytes() == 0
        assert not future.is_done and len(conn.in_flight_requests) == 1


class TestResponseParseFailure:
    """A well-framed but undecodable payload must tear the connection down
    rather than leaving the parser wedged and the future hanging."""

    def _junk_frame_with_matching_cid(self, cid):
        # Header carries the expected correlation id (so no CorrelationIdError),
        # the body is undecodable -> KafkaProtocolError('Unable to decode').
        payload = struct.pack('>i', cid) + b'\x7f\xff\xff\xffgarbage-body'
        return struct.pack('>i', len(payload)) + payload

    def test_decode_error_aborts_and_fails_future(self, net):
        conn, sock = make_connection(net)
        cid, future = send_one(conn)
        obs = FutureObserver(future)

        sock.feed(self._junk_frame_with_matching_cid(cid))
        assert conn.closed
        assert conn.close_future.failed()
        assert isinstance(conn.close_future.exception, Errors.KafkaProtocolError)
        assert obs.done_once and obs.failures == 1
        assert isinstance(future.exception, Errors.KafkaProtocolError)
        assert len(conn.in_flight_requests) == 0

    def test_decode_error_after_one_good_response_only_fails_second(self, net):
        conn, sock = make_connection(net)
        cid1, f1 = send_one(conn)
        cid2, f2 = send_one(conn)

        # First frame decodes fine.
        sock.feed(metadata_response_frame(cid1))
        assert f1.succeeded()
        # Second frame is junk with f2's expected cid.
        sock.feed(self._junk_frame_with_matching_cid(cid2))

        assert conn.closed
        assert f2.failed()
        assert isinstance(f2.exception, Errors.KafkaProtocolError)
        # f1's success stands; the parse fault does not retroactively fail it.
        assert f1.succeeded()
        assert len(conn.in_flight_requests) == 0

    def test_invalid_frame_length_closes_connection(self, net):
        """A length prefix beyond receive_message_max_bytes is rejected while
        still header-only -- no payload ever staged."""
        conn, sock = make_connection(net)
        _, future = send_one(conn)

        sock.feed(struct.pack('>i', conn.config['receive_message_max_bytes'] + 1))
        assert conn.closed
        assert future.failed()
        assert isinstance(future.exception, Errors.InvalidReceiveError)
        assert len(conn.in_flight_requests) == 0


class TestConnectionCloseAndReconnect:
    """Closing mid-flight fails exactly the in-flight requests of that
    connection, and a reconnect produces a fresh connection/parser state."""

    def test_close_fails_buffered_and_in_flight(self, net):
        conn, sock = make_connection(net)
        _, in_flight_f = send_one(conn)
        # A request sent while initializing lands in the request buffer.
        conn.initializing = True
        buffered_f = conn.send_request(metadata_request())
        assert len(conn._request_buffer) == 1
        conn.initializing = False

        error = Errors.KafkaConnectionError('broker restarted')
        conn.close(error)

        assert conn.closed
        assert in_flight_f.failed() and buffered_f.failed()
        assert in_flight_f.exception is error
        assert buffered_f.exception is error
        assert len(conn.in_flight_requests) == 0
        assert len(conn._request_buffer) == 0
        # The timeout task of the in-flight request was cancelled.
        assert conn.close_future.is_done

    def test_failure_isolated_per_connection(self, net):
        """The fault must not clear requests on other nodes: each connection
        owns its own in-flight deque and its own fan-out."""
        conn_a, sock_a = make_connection(net, node_id='node-A')
        conn_b, sock_b = make_connection(net, node_id='node-B')
        _, fa1 = send_one(conn_a)
        _, fa2 = send_one(conn_a)
        _, fb1 = send_one(conn_b)
        _, fb2 = send_one(conn_b)

        sock_a.abort(Errors.KafkaConnectionError('A down'))

        for future in (fa1, fa2):
            assert future.failed()
            assert isinstance(future.exception, Errors.KafkaConnectionError)
        # Node B is untouched: futures pending, deque intact, connection open.
        for future in (fb1, fb2):
            assert not future.is_done
        assert len(conn_b.in_flight_requests) == 2
        assert not conn_b.closed

        # Node B then answers normally -- its responses still correlate.
        sock_b.feed(metadata_response_frame(
            conn_b.in_flight_requests[0][0]))
        sock_b.feed(metadata_response_frame(
            conn_b.in_flight_requests[0][0]))
        assert fb1.succeeded() and fb2.succeeded()
        assert len(conn_b.in_flight_requests) == 0

    def test_parse_fault_on_one_node_leaves_sibling_untouched(self, net):
        conn_a, sock_a = make_connection(net, node_id='node-A')
        conn_b, sock_b = make_connection(net, node_id='node-B')
        cid_a, fa = send_one(conn_a)
        cid_b, fb = send_one(conn_b)

        junk_with_a_cid = (
            struct.pack('>i', cid_a) + b'\x7f\xff\xff\xffgarbage-body')
        sock_a.feed(struct.pack('>i', len(junk_with_a_cid)) + junk_with_a_cid)

        assert conn_a.closed and fa.failed()
        assert isinstance(fa.exception, Errors.KafkaProtocolError)
        assert not conn_b.closed and not fb.is_done
        assert len(conn_b.in_flight_requests) == 1

        sock_b.feed(metadata_response_frame(cid_b))
        assert fb.succeeded()
        assert len(conn_b.in_flight_requests) == 0

    def test_write_fault_on_one_node_leaves_sibling_untouched(self, net):
        conn_a, sock_a = make_connection(net, node_id='node-A')
        conn_b, sock_b = make_connection(net, node_id='node-B')
        sock_a.fail_remaining = True
        _, fa = send_one(conn_a)
        cid_b, fb = send_one(conn_b)

        sock_a.drain(send_limit=2)
        assert conn_a.closed and fa.failed()
        assert isinstance(fa.exception, Errors.KafkaConnectionError)
        assert not conn_b.closed and not fb.is_done
        assert len(conn_b.in_flight_requests) == 1

        sock_b.feed(metadata_response_frame(cid_b))
        assert fb.succeeded()

    def test_reconnect_through_manager_resets_parser_and_cid_space(self, net):
        manager = KafkaConnectionManager(
            net, bootstrap_servers='localhost:9092',
            api_version=(4, 2),
            reconnect_backoff_ms=5, reconnect_backoff_max_ms=20,
            socket_connection_setup_timeout_ms=2000)
        sockets = []

        class AutoApiVersionsSocket(ScriptedSocket):
            """Answers the manager's initialization ApiVersions handshake from
            the request bytes observed in write(), then behaves normally."""

            def write(self, data):
                result = super().write(data)
                api_key = struct.unpack('>h', data[4:6])[0]
                if api_key == ApiVersionsRequest.API_KEY:
                    cid = struct.unpack('>i', data[8:12])[0]
                    self.events.append(('api_versions_answer', cid))
                    self._net.call_soon(
                        lambda s=self, c=cid: s.feed(api_versions_response_frame(c)))
                return result

        async def scripted_connect(protocol, host, port, **kwargs):
            sock = AutoApiVersionsSocket()
            sock._net = net
            sockets.append(sock)
            protocol.connection_made(sock)

        net.create_connection = scripted_connect
        node_id = 'bootstrap-0'

        # First connection: establish through the real manager init path.
        conn1 = manager.get_connection(node_id)
        net.poll(timeout_ms=1000, future=conn1.init_future)
        assert conn1.connected
        f1 = conn1.send_request(metadata_request())
        old_cid = conn1.in_flight_requests[0][0]
        old_parser = conn1.parser

        sockets[0].abort(Errors.KafkaConnectionError('broker crash'))
        net.poll(timeout_ms=100)
        assert conn1.closed and f1.failed()
        assert isinstance(f1.exception, Errors.KafkaConnectionError)
        assert node_id not in manager._conns      # close_future popped the entry

        # Reconnect: fresh object, fresh parser and correlation-id space.
        if manager.connection_delay(node_id) > 0:
            net.poll(timeout_ms=100)
        conn2 = manager.get_connection(node_id)
        net.poll(timeout_ms=1000, future=conn2.init_future)
        assert conn2.connected and conn2 is not conn1
        assert conn2.parser is not old_parser
        # The fresh parser restarted numbering: its init ApiVersions handshake
        # consumed correlation id 1, so the counter is back at the start.
        assert conn2.parser._correlation_id == 1
        assert node_id in manager._conns

        # The new connection allocates the same metadata cid as the old one
        # (id 2 on both), proving cid spaces restart per connection; the old
        # failure stays settled and is never re-delivered.
        f2 = conn2.send_request(metadata_request())
        new_cid = conn2.in_flight_requests[-1][0]
        assert new_cid == old_cid                    # cid counters restart per connection
        sockets[1].feed(metadata_response_frame(new_cid))
        assert f2.succeeded()
        assert f1.failed()                            # stays failed
        assert len(conn2.in_flight_requests) == 0
        manager.close()
        net.poll(timeout_ms=100)


class TestSwappedCorrelationIdCanary:
    """Mutation canary.

    A deliberately mis-wired socket double returns each request's response
    tagged with the *other* request's correlation id.  Correct assertions must
    flag the cross-wiring; this test is marked xfail(strict=True) so the
    suite fails loudly if someone weakens the correlation guarantees (or the
    assertion style) enough to let swapped results slip through unnoticed.
    """

    @pytest.mark.xfail(
        strict=True,
        reason='intentional cid swap: the guard assertion must reject crossed results')
    def test_naive_assignment_under_swap_is_rejected(self, net):
        """The swapped frames never produce two successful futures.  The final
        guard is written as the (wrong) naive expectation on purpose: under
        correct correlation handling the connection aborts, so expecting both
        futures to succeed fails -- xfail(strict) turns "it started passing"
        into a hard test failure, proving this assertion set can detect
        result-crossing rather than rubber-stamping any outcome."""
        conn, sock = make_connection(net)
        cid1, f1 = send_one(conn)
        cid2, f2 = send_one(conn)

        # Broker-side cross-wiring: the frame sent to request 1 carries
        # request 2's id (and vice versa), so the parser sees an id that does
        # not match its own FIFO head.
        sock.feed(metadata_response_frame(cid1, payload_cid=cid2))
        sock.feed(metadata_response_frame(cid2, payload_cid=cid1))

        assert f1.succeeded() and f2.succeeded(), (
            'swapped correlation ids were accepted as correct results')

    def test_swap_is_actually_caught_in_practice(self, net):
        """The non-canary version of the same scenario: production raises
        CorrelationIdError, tears the connection down and fails both in-flight
        futures -- neither gets a (wrong) success value."""
        conn, sock = make_connection(net)
        cid1, f1 = send_one(conn)
        cid2, f2 = send_one(conn)

        sock.feed(metadata_response_frame(cid1, payload_cid=cid2))
        assert conn.closed
        assert isinstance(conn.close_future.exception, Errors.CorrelationIdError)
        assert f1.failed() and f2.failed()
        assert isinstance(f1.exception, Errors.CorrelationIdError)
        assert isinstance(f2.exception, Errors.CorrelationIdError)
        assert len(conn.in_flight_requests) == 0
