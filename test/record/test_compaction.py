# -*- coding: utf-8 -*-

import struct

import pytest

from kafka.errors import IllegalArgumentError, UnsupportedVersionError
from kafka.record import MemoryRecords, compact_batches, compact_records
from kafka.record.default_records import (
    ControlRecord,
    DefaultRecord,
    DefaultRecordBatch,
    DefaultRecordBatchBuilder,
)
from kafka.record.util import calc_crc32c


def _build_batch(records, base_offset, compression_type=0,
                 is_transactional=False, producer_id=-1, producer_epoch=-1,
                 base_sequence=-1, timestamp_type=0, control=False):
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=compression_type,
        is_transactional=is_transactional,
        producer_id=producer_id,
        producer_epoch=producer_epoch,
        base_sequence=base_sequence,
        batch_size=999999,
        timestamp_type=timestamp_type,
    )
    builder.set_batch_coordinates(base_offset, base_offset)
    last_offset = base_offset
    for offset, timestamp, key, value, headers in records:
        metadata = builder.append(offset, timestamp, key, value, headers)
        assert metadata is not None
        last_offset = offset
    builder.set_batch_coordinates(base_offset, last_offset)
    buffer = builder.build(force_compression=bool(compression_type))
    if control:
        buffer[DefaultRecordBatch.ATTRIBUTES_OFFSET + 1] |= (
            DefaultRecordBatch.CONTROL_MASK
        )
        struct.pack_into(
            ">I",
            buffer,
            DefaultRecordBatch.CRC_OFFSET,
            calc_crc32c(
                memoryview(buffer)[DefaultRecordBatch.ATTRIBUTES_OFFSET:]
            ),
        )
    return bytes(buffer)


def _source_batches(compression_type):
    first = _build_batch(
        [
            (0, 1000, b"key-a", b"old-a", [("source", b"batch-0")]),
            (1, 1001, b"key-b", b"old-b", []),
            (2, 1002, None, b"no-key", [("source", b"batch-0")]),
            (3, 1003, b"key-a", None, []),
        ],
        base_offset=0,
        is_transactional=True,
        producer_id=99,
        producer_epoch=3,
        base_sequence=1000,
        compression_type=compression_type,
    )
    second = _build_batch(
        [
            (4, 1004, b"key-a", b"new-a", [("source", b"batch-1")]),
            (5, 1005, b"", b"empty-key", []),
            (6, 1006, b"key-b", None, []),
        ],
        base_offset=4,
        is_transactional=True,
        producer_id=99,
        producer_epoch=3,
        base_sequence=1004,
        compression_type=compression_type,
    )
    return [first, second]


@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
])
def test_compaction_preserves_offsets_headers_timestamps(compression_type):
    compacted = compact_batches(_source_batches(compression_type))

    first = DefaultRecordBatch(bytes(compacted[0]))
    second = DefaultRecordBatch(bytes(compacted[1]))
    for batch, buffer in zip((first, second), compacted):
        assert batch.validate_crc() is True
        assert batch.size_in_bytes == len(buffer)
        assert batch.compression_type == compression_type
        assert batch.is_transactional is True
        assert batch.producer_id == 99
        assert batch.producer_epoch == 3

    # key-a's old value at 0 is superseded; its tombstone at 3 remains.
    assert [(r.offset, r.key, r.value, r.headers) for r in first] == [
        (2, None, b"no-key", [("source", b"batch-0")]),
        (3, b"key-a", None, []),
    ]
    assert first.base_offset == 0
    assert first.last_offset == 3
    assert first.base_sequence == 1000
    assert first.last_sequence == 1003

    # key-b's old value at 1 is superseded by the tombstone at 6.
    assert [(r.offset, r.key, r.value, r.headers) for r in second] == [
        (4, b"key-a", b"new-a", [("source", b"batch-1")]),
        (5, b"", b"empty-key", []),
        (6, b"key-b", None, []),
    ]
    assert second.base_offset == 4
    assert second.last_offset == 6
    assert second.base_sequence == 1004
    assert second.last_sequence == 1006

    for record in list(first) + list(second):
        assert isinstance(record, DefaultRecord)
        assert record.timestamp == 1000 + record.offset


def test_compaction_retains_control_and_empty_batches():
    control = _build_batch(
        [(7, 1007, b"\x00\x00\x00\x00", b"abort-marker", [])],
        base_offset=7,
        control=True,
    )
    empty = _build_batch([], base_offset=8)
    source = [control, empty]

    compacted = compact_batches(source)
    assert len(compacted) == 2
    assert compacted[0] == control
    assert compacted[1] == empty

    control_batch = DefaultRecordBatch(bytes(compacted[0]))
    assert control_batch.is_control_batch is True
    assert control_batch.validate_crc() is True
    control_record = list(control_batch)[0]
    assert isinstance(control_record, ControlRecord)
    assert control_record.offset == 7
    assert control_record.abort is True
    assert control_record.value == b"abort-marker"

    empty_batch = DefaultRecordBatch(bytes(compacted[1]))
    assert empty_batch.records_count == 0
    assert empty_batch.base_offset == 8
    assert empty_batch.validate_crc() is True


def test_compaction_retains_compressed_control_batch():
    control = _build_batch(
        [(3, 1003, b"\x00\x00\x00\x01", b"commit-marker", [])],
        base_offset=3,
        compression_type=DefaultRecordBatch.CODEC_GZIP,
        control=True,
    )

    compacted = compact_batches([control])
    assert len(compacted) == 1
    assert compacted[0] == control

    batch = DefaultRecordBatch(bytes(compacted[0]))
    assert batch.validate_crc() is True
    assert batch.is_control_batch is True
    assert batch.compression_type == DefaultRecordBatch.CODEC_GZIP
    record = list(batch)[0]
    assert isinstance(record, ControlRecord)
    assert record.commit is True
    assert record.offset == 3


def test_compaction_rejects_iterated_batch():
    source = _build_batch([(0, 1000, b"key", b"value", [])], base_offset=0)
    batch = DefaultRecordBatch(source)
    list(batch)
    with pytest.raises(IllegalArgumentError):
        compact_batches([batch])


def test_compaction_can_drop_fully_superseded_batches():
    first = _build_batch(
        [(0, 1000, b"key", b"v1", [])],
        base_offset=0,
        producer_id=10,
        producer_epoch=2,
        base_sequence=500,
    )
    second = _build_batch(
        [(1, 1001, b"key", b"v2", [])],
        base_offset=1,
        producer_id=10,
        producer_epoch=2,
        base_sequence=501,
    )

    compacted = compact_batches(
        [first, second], preserve_empty_batches=False
    )
    assert len(compacted) == 1

    batch = DefaultRecordBatch(bytes(compacted[0]))
    assert batch.validate_crc() is True
    assert batch.base_offset == 1
    assert [record.offset for record in batch] == [1]
    assert batch.last_offset == 1
    assert batch.base_sequence == 501
    assert batch.last_sequence == 501


def test_compaction_preserves_last_offset_when_last_record_removed():
    first = _build_batch(
        [
            (0, 1000, None, b"no-key", []),
            (1, 1001, b"key", b"old", []),
            (2, 1002, b"key", b"new", []),
        ],
        base_offset=0,
        producer_id=21,
        producer_epoch=1,
        base_sequence=400,
    )
    second = _build_batch(
        [(3, 1003, b"key", b"latest", [])],
        base_offset=3,
        producer_id=21,
        producer_epoch=1,
        base_sequence=403,
    )

    compacted = compact_batches([first, second])
    batch = DefaultRecordBatch(bytes(compacted[0]))
    assert batch.validate_crc() is True
    # Only the null-key record remains, but the last offset stays at 2.
    assert [record.offset for record in batch] == [0]
    assert batch.base_offset == 0
    assert batch.last_offset == 2
    assert batch.last_offset_delta == 2
    assert batch.base_sequence == 400
    assert batch.last_sequence == 402


def test_compaction_preserves_append_time_and_headers():
    source = _build_batch(
        [
            (10, 111, b"key", b"v1", [("first", b"1")]),
            (11, 222, b"key", b"v2", [("second", None)]),
            (12, 333, b"other", b"v3", [("third", b"3")]),
        ],
        base_offset=10,
        timestamp_type=DefaultRecordBatch.LOG_APPEND_TIME,
        is_transactional=True,
        producer_id=77,
        producer_epoch=4,
        base_sequence=900,
    )

    compacted = compact_batches([source])[0]
    batch = DefaultRecordBatch(bytes(compacted))
    assert batch.timestamp_type == DefaultRecordBatch.LOG_APPEND_TIME
    assert batch.first_timestamp == 111
    assert batch.max_timestamp == 333
    assert batch.base_offset == 10
    assert batch.last_offset == 12
    assert batch.base_sequence == 900
    assert batch.last_sequence == 902

    assert [
        (r.offset, r.key, r.value, r.headers, r.timestamp)
        for r in batch
    ] == [
        (11, b"key", b"v2", [("second", None)], 333),
        (12, b"other", b"v3", [("third", b"3")], 333),
    ]


def test_compact_records_round_trips_through_memory_records():
    first = _build_batch(
        [(0, 1000, b"key", b"v1", []), (1, 1001, b"key", b"v2", [])],
        base_offset=0,
    )
    second = _build_batch(
        [(2, 1002, b"key", b"v3", []), (3, 1003, b"other", b"v4", [])],
        base_offset=2,
        compression_type=DefaultRecordBatch.CODEC_GZIP,
    )

    compacted = compact_records(first + second)
    records = MemoryRecords(compacted)
    batches = []
    while records.has_next():
        batches.append(records.next_batch())

    assert len(batches) == 2
    assert [[record.offset for record in batch] for batch in batches] == [
        [],
        [2, 3],
    ]
    assert batches[1].compression_type == DefaultRecordBatch.CODEC_GZIP


def test_compaction_rejects_legacy_magic():
    from kafka.record.legacy_records import LegacyRecordBatchBuilder

    builder = LegacyRecordBatchBuilder(
        magic=1, compression_type=0, batch_size=999999
    )
    builder.append(0, 1000, b"key", b"value")
    with pytest.raises(UnsupportedVersionError):
        compact_batches([bytes(builder.build())])


def test_compaction_can_validate_input_crc():
    source = _build_batch([(0, 1000, b"key", b"value", [])], base_offset=0)
    corrupt = bytearray(source)
    corrupt[DefaultRecordBatch.CRC_OFFSET] ^= 0xFF
    with pytest.raises(IllegalArgumentError):
        compact_batches([bytes(corrupt)], validate_crcs=True)
