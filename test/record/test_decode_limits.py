# -*- coding: utf-8 -*-
"""Tests for optional decompressed-byte / record-count decode limits.

Batches are built with repeated payloads so a tiny compressed fixture expands
to a large amount of output. No large fixture files or external codec
services are required: the limit-bearing codecs exercised here (none / gzip)
are pure Python.
"""

import struct

import pytest

from kafka.codec import has_lz4, has_snappy, has_zstd
from kafka.errors import (
    CorruptRecordError,
    DecompressedSizeLimitExceededError,
    RecordChecksumError,
    RecordCountLimitExceededError,
    RecordDecodeLimitError,
    TruncatedRecordError,
    UnsupportedCodecError,
    UnsupportedDecompressionLimitError,
)
from kafka.record.default_records import (
    DefaultRecordBatch, DefaultRecordBatchBuilder,
)
from kafka.record.legacy_records import (
    LegacyRecordBatch, LegacyRecordBatchBuilder,
)
from kafka.record.memory_records import MemoryRecords


def build_v2(compression_type, value, count=1, headers=None):
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=compression_type, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=10 ** 9)
    for offset in range(count):
        builder.append(
            offset, timestamp=1, key=None, value=value,
            headers=headers or [])
    return bytes(builder.build())


def build_legacy(magic, compression_type, value, count=1):
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=compression_type,
        batch_size=10 ** 9)
    for offset in range(count):
        builder.append(offset, timestamp=1, key=None, value=value)
    return bytes(builder.build())


def _decompressed_size_v2(buffer):
    # Exact size of the batch's record payload as the decoder sees it.
    batch = DefaultRecordBatch(bytes(buffer))
    if batch.compression_type == DefaultRecordBatch.CODEC_GZIP:
        from kafka.codec import gzip_decode
        return len(gzip_decode(buffer[DefaultRecordBatch.HEADER_STRUCT.size:]))
    return len(buffer) - DefaultRecordBatch.HEADER_STRUCT.size


# ---------------------------------------------------------------------------
# Happier paths: limits at/above real output keep decoding exact.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
])
def test_v2_limits_at_or_above_real_size_decode_normally(compression_type):
    # 64 KiB of repeated data: high compression ratio, tiny fixture.
    payload = b"A" * (64 * 1024)
    buffer = build_v2(compression_type, payload)
    # The decompressed output includes record framing on top of the payload.
    decompressed_size = _decompressed_size_v2(buffer)
    assert decompressed_size > len(payload)

    batch = DefaultRecordBatch(
        buffer, max_decompressed_bytes=decompressed_size, max_records=1)
    records = list(batch)
    assert len(records) == 1
    assert records[0].value == payload
    # Limit exactly at the output size must not trip.
    assert len(records[0].value) == len(payload)


def test_v2_defaults_remain_unlimited():
    payload = b"B" * (256 * 1024)
    buffer = build_v2(DefaultRecordBatch.CODEC_GZIP, payload)
    records = list(DefaultRecordBatch(buffer))
    assert records[0].value == payload
    # No limits configured == unchanged behavior.


# ---------------------------------------------------------------------------
# High-ratio gzip input is rejected before the whole payload is materialized.
# ---------------------------------------------------------------------------

def test_v2_gzip_decompressed_byte_limit():
    # ~64 KiB raw -> ~200 byte compressed fixture.
    payload = b"C" * (64 * 1024)
    buffer = build_v2(DefaultRecordBatch.CODEC_GZIP, payload)
    assert len(buffer) < 2048

    limit = 4096
    batch = DefaultRecordBatch(buffer, max_decompressed_bytes=limit)
    with pytest.raises(DecompressedSizeLimitExceededError) as exc_info:
        list(batch)
    # Reported measurement is past the limit but well below the real size,
    # proving the bound stopped the decode early.
    assert exc_info.value.limit == limit
    assert limit < exc_info.value.observed < len(payload)


def test_v2_uncompressed_decompressed_byte_limit():
    payload = b"D" * (64 * 1024)
    buffer = build_v2(DefaultRecordBatch.CODEC_NONE, payload)

    batch = DefaultRecordBatch(buffer, max_decompressed_bytes=4096)
    with pytest.raises(DecompressedSizeLimitExceededError) as exc_info:
        list(batch)
    # Uncompressed output is the payload plus record framing.
    assert exc_info.value.observed == _decompressed_size_v2(buffer)
    assert exc_info.value.observed > len(payload)


# ---------------------------------------------------------------------------
# Record-count limits: header is checked before any record is produced.
# ---------------------------------------------------------------------------

def test_v2_record_count_limit_checked_before_decoding():
    buffer = build_v2(DefaultRecordBatch.CODEC_GZIP, b"E", count=5)
    builder_byte = DefaultRecordBatch(buffer)

    with pytest.raises(RecordCountLimitExceededError) as exc_info:
        DefaultRecordBatch(buffer, max_records=3)
    assert exc_info.value.limit == 3
    assert exc_info.value.observed == 5

    # The unlimited batch still decodes all five records.
    assert len(list(builder_byte)) == 5


def test_v2_record_count_limit_does_not_yield_partial_batch():
    buffer = build_v2(DefaultRecordBatch.CODEC_NONE, b"E", count=5)
    # The header check fires at construction time: no record (complete or
    # otherwise) is ever handed to an iterator.
    with pytest.raises(RecordCountLimitExceededError):
        DefaultRecordBatch(buffer, max_records=3)


# ---------------------------------------------------------------------------
# Malicious declared lengths: truncation, not runaway allocation.
# ---------------------------------------------------------------------------

def test_v2_declared_value_length_beyond_buffer_is_truncation():
    buffer = bytearray(build_v2(DefaultRecordBatch.CODEC_NONE, b"hello"))
    header_size = DefaultRecordBatch.HEADER_STRUCT.size

    # Walk varints to the value length: record length, attrs, timestamp
    # delta, offset delta, key length, value length.
    pos = header_size
    for _ in range(5):
        _, pos = _decode_varint(buffer, pos)
    # Replace the 1-byte value-length varint with a huge declared length.
    huge = _encode_varint(10 ** 9)
    buffer[pos:pos + 1] = huge

    batch = DefaultRecordBatch(bytes(buffer))
    with pytest.raises(TruncatedRecordError):
        list(batch)


def test_v2_declared_record_length_mismatch_is_corruption():
    buffer = bytearray(build_v2(DefaultRecordBatch.CODEC_NONE, b"hello"))
    header_size = DefaultRecordBatch.HEADER_STRUCT.size

    # Inflate the outer record-length varint to a value far larger than the
    # batch payload. Decoding must fail deterministically, without allocating
    # based on the declared size.
    pos = header_size
    _, original_end = _decode_varint(buffer, pos)
    buffer[pos:original_end] = _encode_varint(10 ** 6)

    batch = DefaultRecordBatch(bytes(buffer))
    with pytest.raises(CorruptRecordError):
        list(batch)


# ---------------------------------------------------------------------------
# Truncated and corrupt gzip streams are distinct from limit errors.
# ---------------------------------------------------------------------------

def test_v2_truncated_gzip_is_truncation_error():
    buffer = bytearray(build_v2(DefaultRecordBatch.CODEC_GZIP, b"F" * 4096))
    length_offset = struct.calcsize(">q")
    # Keep the 61-byte header plus a fragment of the compressed payload.
    truncated = buffer[:DefaultRecordBatch.HEADER_STRUCT.size + 16]
    struct.pack_into(
        ">i", truncated, length_offset,
        len(truncated) - struct.calcsize(">qi"))

    batch = DefaultRecordBatch(bytes(truncated), max_decompressed_bytes=10 ** 9)
    with pytest.raises(TruncatedRecordError):
        list(batch)


def test_v2_corrupt_gzip_stream_is_corruption_error():
    buffer = bytearray(build_v2(DefaultRecordBatch.CODEC_GZIP, b"G" * 4096))
    # Flip a byte well inside the compressed payload (past the 10-byte gzip
    # header) so the frame checksum / deflate stream is invalid.
    buffer[DefaultRecordBatch.HEADER_STRUCT.size + 15] ^= 0xFF

    batch = DefaultRecordBatch(bytes(buffer), max_decompressed_bytes=10 ** 9)
    with pytest.raises(CorruptRecordError):
        list(batch)


def test_v2_checksum_failure_is_its_own_error():
    buffer = bytearray(build_v2(DefaultRecordBatch.CODEC_NONE, b"hello"))
    crc_offset = struct.calcsize(">qiib")
    buffer[crc_offset] ^= 0xFF

    batch = DefaultRecordBatch(bytes(buffer))
    assert batch.validate_crc() is False

    # The fetcher surfaces this as RecordChecksumError; the type is distinct
    # from limit/truncation errors while remaining a CorruptRecordError.
    assert issubclass(RecordChecksumError, CorruptRecordError)
    assert not issubclass(RecordChecksumError, RecordDecodeLimitError)


# ---------------------------------------------------------------------------
# Unsupported codecs with limits are refused, not silently unbounded.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_SNAPPY,
    DefaultRecordBatch.CODEC_LZ4,
    DefaultRecordBatch.CODEC_ZSTD,
])
def test_v2_unlimitable_codec_with_limit_is_rejected(compression_type):
    # Patch the batch attributes directly: no codec library needed to build
    # the header, and the rejection happens before any decoding is attempted.
    buffer = bytearray(build_v2(DefaultRecordBatch.CODEC_NONE, b"hello"))
    attrs_offset = struct.calcsize(">qiibI")
    struct.pack_into(">h", buffer, attrs_offset, compression_type)

    with pytest.raises(UnsupportedDecompressionLimitError) as exc_info:
        DefaultRecordBatch(bytes(buffer), max_decompressed_bytes=1024)
    assert isinstance(exc_info.value, UnsupportedCodecError)

    # Without a decompressed-byte limit the batch is still constructed lazily
    # and only fails at decode time if the library is missing (or decodes with
    # the library present).
    batch = DefaultRecordBatch(bytes(buffer))
    if compression_type == DefaultRecordBatch.CODEC_SNAPPY and not has_snappy():
        with pytest.raises(UnsupportedCodecError):
            list(batch)
    elif compression_type == DefaultRecordBatch.CODEC_LZ4 and not has_lz4():
        with pytest.raises(UnsupportedCodecError):
            list(batch)
    elif compression_type == DefaultRecordBatch.CODEC_ZSTD and not has_zstd():
        with pytest.raises(UnsupportedCodecError):
            list(batch)


def test_legacy_unlimitable_codec_with_limit_is_rejected():
    buffer = bytearray(build_legacy(1, LegacyRecordBatch.CODEC_NONE, b"hello"))
    magic_offset = LegacyRecordBatch.MAGIC_OFFSET
    attrs_offset = magic_offset + 1
    # magic byte is 1; compression bits live in attributes.
    struct.pack_into(">b", buffer, attrs_offset, LegacyRecordBatch.CODEC_SNAPPY)

    with pytest.raises(UnsupportedDecompressionLimitError):
        LegacyRecordBatch(bytes(buffer), 1, max_decompressed_bytes=1024)


# ---------------------------------------------------------------------------
# MemoryRecords: a limit error in one batch must not lose the next position.
# ---------------------------------------------------------------------------

def test_memory_records_preserves_next_batch_after_limit_error():
    bomb = build_v2(DefaultRecordBatch.CODEC_GZIP, b"H" * (64 * 1024))
    plain = build_v2(DefaultRecordBatch.CODEC_NONE, b"next-batch")

    records = MemoryRecords(bomb + plain, max_decompressed_bytes=4096)
    bomb_batch = records.next_batch()
    assert bomb_batch is not None
    with pytest.raises(DecompressedSizeLimitExceededError):
        list(bomb_batch)

    # The next batch's position was advanced before decoding, so it is still
    # readable and is delivered complete.
    assert records.has_next()
    next_batch = records.next_batch()
    decoded = list(next_batch)
    assert len(decoded) == 1
    assert decoded[0].value == b"next-batch"
    assert records.has_next() is False


def test_memory_records_count_limit_preserves_next_batch():
    five = build_v2(DefaultRecordBatch.CODEC_NONE, b"I", count=5)
    plain = build_v2(DefaultRecordBatch.CODEC_NONE, b"tail")

    records = MemoryRecords(five + plain, max_records=2)
    # The count limit is checked when the batch is constructed; the read
    # position has already advanced past it.
    with pytest.raises(RecordCountLimitExceededError):
        records.next_batch()

    assert records.has_next()
    tail = list(records.next_batch())
    assert [r.value for r in tail] == [b"tail"]


def test_memory_records_limit_does_not_regress_valid_bytes():
    data = build_v2(DefaultRecordBatch.CODEC_NONE, b"J")
    records = MemoryRecords(data, max_records=100)
    assert records.valid_bytes() == len(data)


@pytest.mark.parametrize("magic", [0, 1])
def test_memory_records_propagates_limits_to_legacy(magic):
    payload = b"K" * (64 * 1024)
    data = build_legacy(magic, LegacyRecordBatch.CODEC_GZIP, payload)

    records = MemoryRecords(data, max_decompressed_bytes=4096)
    batch = records.next_batch()
    assert isinstance(batch, LegacyRecordBatch)
    with pytest.raises(DecompressedSizeLimitExceededError):
        list(batch)


# ---------------------------------------------------------------------------
# Legacy batches: gzip limits work, uncompressed payload limit applies.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_gzip_decompressed_byte_limit(magic):
    payload = b"L" * (64 * 1024)
    buffer = build_legacy(magic, LegacyRecordBatch.CODEC_GZIP, payload)
    assert len(buffer) < 2048

    batch = LegacyRecordBatch(bytes(buffer), magic, max_decompressed_bytes=4096)
    with pytest.raises(DecompressedSizeLimitExceededError) as exc_info:
        list(batch)
    assert exc_info.value.limit == 4096
    assert exc_info.value.observed > 4096


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_gzip_limit_above_output_round_trip(magic):
    payload = b"M" * (64 * 1024)
    buffer = build_legacy(magic, LegacyRecordBatch.CODEC_GZIP, payload)
    # Decompressed output is the inner message set: payload plus framing.
    batch = LegacyRecordBatch(
        bytes(buffer), magic, max_decompressed_bytes=len(payload) + 1024)
    records = list(batch)
    assert len(records) == 1
    assert records[0].value == payload


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_uncompressed_decompressed_byte_limit(magic):
    payload = b"N" * (64 * 1024)
    buffer = build_legacy(magic, LegacyRecordBatch.CODEC_NONE, payload)
    batch = LegacyRecordBatch(bytes(buffer), magic, max_decompressed_bytes=4096)
    with pytest.raises(DecompressedSizeLimitExceededError):
        list(batch)


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_record_count_limit(magic):
    buffer = build_legacy(magic, LegacyRecordBatch.CODEC_GZIP, b"O", count=5)
    with pytest.raises(RecordCountLimitExceededError) as exc_info:
        list(LegacyRecordBatch(bytes(buffer), magic, max_records=2))
    assert exc_info.value.limit == 2
    assert exc_info.value.observed == 5


def test_legacy_truncated_gzip_is_truncation_error():
    buffer = bytearray(build_legacy(1, LegacyRecordBatch.CODEC_GZIP, b"P" * 4096))
    # Keep the wrapper message header and a fragment of the compressed value.
    truncated = buffer[:LegacyRecordBatch.KEY_OFFSET_V1 + 32]
    length_offset = struct.calcsize(">q")
    struct.pack_into(
        ">i", truncated, length_offset,
        len(truncated) - struct.calcsize(">qi"))

    batch = LegacyRecordBatch(bytes(truncated), 1, max_decompressed_bytes=10 ** 9)
    with pytest.raises(TruncatedRecordError):
        list(batch)


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------

def _decode_varint(buffer, pos):
    from kafka.record.util import decode_varint
    return decode_varint(buffer, pos)


def _encode_varint(value):
    from kafka.record.util import encode_varint
    out = bytearray()
    encode_varint(value, out.append)
    return bytes(out)
