import struct
from unittest.mock import patch

import pytest

import kafka.codec as codecs
import kafka.record.default_records as default_records
from kafka.consumer.fetcher import Fetcher
from kafka.errors import (
    DecompressionBytesLimitError,
    RecordCountLimitError,
    RecordCRCError,
    RecordTruncatedError,
    UnsupportedRecordLimitsError,
)
from kafka.record.default_records import DefaultRecordBatch
from kafka.record.memory_records import MemoryRecords, MemoryRecordsBuilder
from kafka.record.util import encode_varint
from kafka.structs import TopicPartition


def _build_batch(compression_type, values):
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=compression_type, batch_size=10 ** 9)
    for value in values:
        builder.append(timestamp=1, key=None, value=value, headers=[])
    builder.close()
    return builder.buffer()


def _varint(value):
    encoded = bytearray()
    encode_varint(value, encoded.append)
    return bytes(encoded)


def test_gzip_byte_limit_is_bounded_and_next_batch_readable():
    bomb = _build_batch(DefaultRecordBatch.CODEC_GZIP, [b"A" * 200000])
    normal = _build_batch(DefaultRecordBatch.CODEC_NONE, [b"next"])
    records = MemoryRecords(
        bytes(bomb) + bytes(normal), max_decompressed_bytes=1024)

    batch = records.next_batch()
    with pytest.raises(DecompressionBytesLimitError) as err:
        list(batch)
    # The bounded decoder stops after reading one byte beyond the budget.
    assert err.value.limit == 1024
    assert err.value.actual == 1025
    assert err.value.decompressed_bytes == 1025
    assert batch.decompressed_bytes == 1025

    next_batch = records.next_batch()
    assert [record.value for record in next_batch] == [b"next"]


def test_gzip_limit_equal_to_actual_output_is_allowed():
    payload = b"repeat me " * 5000
    raw = _build_batch(DefaultRecordBatch.CODEC_GZIP, [payload])
    decompressed_size = len(codecs.gzip_decode(
        raw[DefaultRecordBatch.HEADER_STRUCT.size:]))

    batch = DefaultRecordBatch(
        bytes(raw), max_decompressed_bytes=decompressed_size)
    assert [record.value for record in batch] == [payload]
    assert batch.decompressed_bytes == decompressed_size


def test_uncompressed_byte_limit_does_not_consume_next_batch():
    oversized = _build_batch(DefaultRecordBatch.CODEC_NONE, [b"X" * 4096])
    normal = _build_batch(DefaultRecordBatch.CODEC_NONE, [b"ok"])
    records = MemoryRecords(
        bytes(oversized) + bytes(normal), max_decompressed_bytes=128)

    with pytest.raises(DecompressionBytesLimitError):
        list(records.next_batch())
    assert [record.value for record in records.next_batch()] == [b"ok"]


def test_record_count_limit_rejects_batch_without_partial_delivery():
    raw = _build_batch(DefaultRecordBatch.CODEC_NONE, [b"1", b"2", b"3"])
    batch = DefaultRecordBatch(bytes(raw), max_records=2)

    with pytest.raises(RecordCountLimitError) as err:
        list(batch)
    assert err.value.limit == 2
    assert err.value.actual == 3


def test_malicious_declared_record_length_is_truncation_not_limit():
    raw = _build_batch(DefaultRecordBatch.CODEC_NONE, [b"abc"])
    payload_start = DefaultRecordBatch.HEADER_STRUCT.size
    corrupted = (
        bytes(raw[:payload_start]) + _varint(100000) + b"\x00" * 10)
    batch = DefaultRecordBatch(
        corrupted, max_decompressed_bytes=1024)

    with pytest.raises(RecordTruncatedError):
        list(batch)


def test_crc_error_and_truncation_are_distinct():
    raw = bytearray(_build_batch(DefaultRecordBatch.CODEC_NONE, [b"value"]))
    crc_offset = DefaultRecordBatch.CRC_OFFSET
    crc = struct.unpack_from(">I", raw, crc_offset)[0]
    struct.pack_into(">I", raw, crc_offset, crc ^ 0xFFFFFFFF)
    batch = DefaultRecordBatch(bytes(raw))
    assert batch.validate_crc() is False

    tp = TopicPartition("topic", 0)
    part = Fetcher.PartitionRecords(0, tp, MemoryRecords(bytes(raw)))
    with pytest.raises(RecordCRCError):
        part.take(1)

    truncated = _build_batch(DefaultRecordBatch.CODEC_GZIP, [b"abc"])
    batch = DefaultRecordBatch(bytes(truncated[:-8]))
    with pytest.raises(RecordTruncatedError):
        list(batch)


def test_byte_limit_rejects_non_incremental_codec_before_decoding():
    raw = bytearray(_build_batch(DefaultRecordBatch.CODEC_NONE, [b"value"]))
    attributes_offset = DefaultRecordBatch.ATTRIBUTES_OFFSET
    struct.pack_into(
        ">h", raw, attributes_offset, DefaultRecordBatch.CODEC_ZSTD)
    batch = DefaultRecordBatch(
        bytes(raw), max_decompressed_bytes=1024)

    with patch.object(codecs, "has_zstd", return_value=True), \
            patch.object(default_records, "zstd_decode") as decode:
        with pytest.raises(UnsupportedRecordLimitsError, match="incremental"):
            list(batch)
        decode.assert_not_called()
