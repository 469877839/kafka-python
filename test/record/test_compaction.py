# -*- coding: utf-8 -*-
import struct

import pytest

from kafka.errors import UnsupportedVersionError
from kafka.record import MemoryRecords, compact_records
from kafka.record.default_records import (
    DefaultRecordBatch, DefaultRecordBatchBuilder,
)
from kafka.record.legacy_records import LegacyRecordBatchBuilder
from kafka.record.util import calc_crc32c

from test.record.test_records import control_batch_data_v2
from test.testutil import maybe_skip_unsupported_compression


def _build_batch(base_offset, compression_type, producer_id, producer_epoch,
                 base_sequence, transactional, records,
                 timestamp_type=DefaultRecordBatch.CREATE_TIME,
                 leader_epoch=0):
    """Build a magic-v2 batch and patch the broker-assigned header fields."""
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=compression_type,
        is_transactional=transactional,
        producer_id=producer_id, producer_epoch=producer_epoch,
        base_sequence=base_sequence, batch_size=1024 * 1024)
    for relative_offset, timestamp, key, value, headers in records:
        builder.append(relative_offset, timestamp, key, value, headers)
    buffer = bytearray(builder.build())
    struct.pack_into(">q", buffer, 0, base_offset)
    struct.pack_into(
        ">i", buffer, DefaultRecordBatch.AFTER_LEN_OFFSET, leader_epoch)

    if timestamp_type == DefaultRecordBatch.LOG_APPEND_TIME:
        attributes_offset = DefaultRecordBatch.ATTRIBUTES_OFFSET
        attributes, = struct.unpack_from(">h", buffer, attributes_offset)
        struct.pack_into(
            ">h", buffer, attributes_offset,
            attributes | DefaultRecordBatch.TIMESTAMP_TYPE_MASK)
        struct.pack_into(
            ">I", buffer, DefaultRecordBatch.CRC_OFFSET,
            calc_crc32c(memoryview(buffer)[attributes_offset:]))
    return bytes(buffer)


def _control_batch():
    # The upstream hand-built control fixture lives at base offset 0; keep
    # it verbatim so its raw bytes pass through compaction untouched.
    return control_batch_data_v2[0]


# A deliberately small, highly compressible payload so gzip actually shrinks
# the rebuilt batch instead of falling back to uncompressed storage.
BULK = b"z" * 128


def _input_batches(compression_type):
    # Batch A (base offset 10): null key, an in-batch duplicate, a tombstone
    # that is later superseded, another ordinary record and an unsuperseded
    # tombstone. Relative offsets are intentionally non-contiguous at the end.
    batch_a = _build_batch(
        10, compression_type, producer_id=100, producer_epoch=7,
        base_sequence=50, transactional=True, records=[
            (0, 1000, None, b"null-1" + BULK, [("h0", b"v0")]),
            (1, 1001, b"a", b"a1" + BULK, []),
            (2, 1002, b"a", None, []),
            (3, 1003, b"b", b"b1" + BULK, [("hb", None)]),
            (4, 1004, b"a", b"a2" + BULK, [("ha", b"va")]),
            (5, 1005, b"d", None, []),
        ])
    # Batch B (base offset 20): a and b are overwritten across batches, c is
    # new, another null key and a tombstone that stays the last occurrence.
    batch_b = _build_batch(
        20, compression_type, producer_id=100, producer_epoch=7,
        base_sequence=60, transactional=True, records=[
            (0, 2000, b"a", b"a3" + BULK, []),
            (1, 2001, b"b", b"b2" + BULK, []),
            (2, 2002, b"c", b"c1" + BULK, []),
            (3, 2003, None, b"null-2" + BULK, []),
            (4, 2004, b"e", None, []),
        ])
    # Transaction control batch between the data batches: must stay in
    # position and pass through byte-for-byte.
    batch_control = _control_batch()
    # Empty data batch (base offset 30): must be dropped.
    batch_empty = _build_batch(
        30, compression_type, producer_id=-1, producer_epoch=-1,
        base_sequence=-1, transactional=False, records=[])
    return batch_a, batch_control, batch_b, batch_empty


@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
])
def test_compact_records_acceptance(compression_type):
    maybe_skip_unsupported_compression(compression_type)
    raw = b"".join(_input_batches(compression_type))

    compacted = compact_records(MemoryRecords(raw))
    batches = list(compacted)

    # Empty batch is dropped; the control batch stays between both data
    # batches, preserving the original batch ordering.
    assert len(batches) == 3
    batch_a, batch_control, batch_b = batches

    # Every rebuilt data batch must carry a valid CRC and a self-consistent
    # length. (The hand-built upstream control fixture is copied verbatim
    # and never rewritten, so it is only compared byte-for-byte below.)
    for batch in (batch_a, batch_b):
        assert batch.validate_crc()
        assert batch.size_in_bytes == len(batch._buffer)

    # Compression type is preserved on the rebuilt batches.
    assert batch_a.compression_type == compression_type
    assert batch_b.compression_type == compression_type

    # ---- Batch A: only the null-key record and the unsuperseded tombstone
    # survive, because every ordinary key here is overwritten in batch B.
    assert not batch_a.is_control_batch
    assert batch_a.base_offset == 10
    # LastOffsetDelta keeps the original coordinate of the last survivor.
    assert batch_a.last_offset_delta == 5
    assert batch_a.producer_id == 100
    assert batch_a.producer_epoch == 7
    assert batch_a.base_sequence == 50
    assert batch_a.is_transactional is True
    records_a = list(batch_a)
    assert [(r.offset, r.key, r.value) for r in records_a] == [
        (10, None, b"null-1" + BULK),
        (15, b"d", None),
    ]
    # Offsets are original coordinates, not renumbered to 0, 1.
    assert [r.offset - batch_a.base_offset for r in records_a] == [0, 5]
    # Headers and timestamps survive byte-for-byte.
    assert records_a[0].headers == [("h0", b"v0")]
    assert records_a[1].headers == []
    assert [r.timestamp for r in records_a] == [1000, 1005]

    # ---- Batch B: local winners plus null key and the surviving tombstone.
    assert not batch_b.is_control_batch
    assert batch_b.base_offset == 20
    assert batch_b.last_offset_delta == 4
    assert batch_b.producer_id == 100
    assert batch_b.producer_epoch == 7
    assert batch_b.base_sequence == 60
    records_b = list(batch_b)
    assert [(r.offset, r.key, r.value) for r in records_b] == [
        (20, b"a", b"a3" + BULK),
        (21, b"b", b"b2" + BULK),
        (22, b"c", b"c1" + BULK),
        (23, None, b"null-2" + BULK),
        (24, b"e", None),
    ]
    assert [r.timestamp for r in records_b] == [
        2000, 2001, 2002, 2003, 2004]

    # ---- Control batch survives verbatim and still decodes as a marker.
    assert batch_control.is_control_batch is True
    control_records = list(batch_control)
    assert len(control_records) == 1
    assert control_records[0].offset == 0
    assert control_records[0].abort is True
    assert bytes(batch_control._buffer) == control_batch_data_v2[0]


def test_compact_roundtrip_through_reader():
    # The compacted buffer must be re-readable by the unmodified reader and
    # expose the exact same records on a second decode.
    compacted = compact_records(MemoryRecords(b"".join(_input_batches(
        DefaultRecordBatch.CODEC_NONE))))
    reread = MemoryRecords(bytes(compacted._buffer))
    offsets = []
    for batch in reread:
        if not batch.is_control_batch:
            assert batch.validate_crc()
        offsets.extend(record.offset for record in batch)
    assert offsets == [10, 15, 0, 20, 21, 22, 23, 24]


def test_compact_drops_batch_compacted_to_empty():
    older = _build_batch(
        50, DefaultRecordBatch.CODEC_NONE, -1, -1, -1, False, [
            (0, 1, b"k", b"old", []),
            (1, 2, b"k", b"newer", []),
        ])
    newer = _build_batch(
        60, DefaultRecordBatch.CODEC_NONE, -1, -1, -1, False, [
            (0, 3, b"k", b"newest", []),
        ])
    batches = list(compact_records(MemoryRecords(older + newer)))
    assert len(batches) == 1
    assert batches[0].base_offset == 60
    assert batches[0].validate_crc()
    assert [(r.offset, r.value) for r in batches[0]] == [(60, b"newest")]


def test_compact_preserves_log_append_time():
    raw = _build_batch(
        70, DefaultRecordBatch.CODEC_NONE, -1, -1, -1, False, [
            (0, 1000, b"k", b"v1", []),
            (2, 2000, b"k", b"v2", []),
        ], timestamp_type=DefaultRecordBatch.LOG_APPEND_TIME)
    batch = list(compact_records(MemoryRecords(raw)))[0]
    assert batch.timestamp_type == DefaultRecordBatch.LOG_APPEND_TIME
    assert batch.validate_crc()
    records = list(batch)
    assert [(r.offset, r.timestamp) for r in records] == [
        (72, batch.max_timestamp)]


def test_compact_accepts_bytes_input():
    raw = _build_batch(
        0, DefaultRecordBatch.CODEC_NONE, -1, -1, -1, False, [
            (0, 1, b"k", b"v", []),
        ])
    batches = list(compact_records(raw))
    assert len(batches) == 1
    assert list(batches[0])[0].value == b"v"


def test_compact_empty_input():
    assert list(compact_records(b"")) == []
    assert list(compact_records(MemoryRecords(b""))) == []


def test_compact_rejects_legacy_format():
    builder = LegacyRecordBatchBuilder(
        magic=1, compression_type=0, batch_size=1024 * 1024)
    builder.append(0, timestamp=1000, key=b"k", value=b"v")
    raw = bytes(builder.build())
    with pytest.raises(UnsupportedVersionError):
        compact_records(MemoryRecords(raw))
