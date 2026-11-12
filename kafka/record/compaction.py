"""Offline key compaction for magic-v2 record batches.

This module rewrites a finite, caller-supplied sequence of record batches so
that only the last record for each non-empty key is retained. It is intended
for offline archival workflows and does not implement broker retention,
tombstone expiration, or transaction visibility policies.
"""

import struct

from kafka.errors import IllegalArgumentError, UnsupportedVersionError
from kafka.record.default_records import (
    DefaultRecordBatch,
    DefaultRecordBatchBuilder,
)
from kafka.record.memory_records import MemoryRecords
from kafka.record.util import calc_crc32c

# Offset of the magic byte within a record batch (after offset and length).
_MAGIC_OFFSET = struct.calcsize(">qii")


def compact_batches(batches, preserve_empty_batches=True, validate_crcs=False):
    """Compact keyed records from a finite sequence of magic-v2 batches.

    The last record for each non-empty key in the supplied range is retained.
    Records with null or zero-length keys, tombstones, and transaction control
    batches are retained rather than treated as ordinary overwritable records.
    Original record offsets are retained, including gaps inside rebuilt
    batches; batch base offsets, last offset deltas, producer sequences,
    timestamps, headers, compression codec, and control markers are preserved.

    This is an offline, input-scoped rewrite only. It does not implement
    broker retention windows, tombstone expiration, or transaction visibility.

    Arguments:
        batches: Iterable of ``DefaultRecordBatch`` objects or bytes-like
            values, each containing one magic-v2 record batch. Passed batch
            objects are consumed by the compaction scan and must not have
            been iterated before.
        preserve_empty_batches (bool): Retain batches that contain no records
            after compaction, preserving their producer sequence coordinates.
            When ``False``, such batches are omitted from the result.
        validate_crcs (bool): Validate each input batch CRC before reading it.

    Returns:
        list of bytearray: Rebuilt batch buffers, in input order.
    """
    parsed = [_coerce_batch(batch, validate_crcs) for batch in batches]
    retained_offsets = _retained_offsets([batch for batch, _ in parsed])

    compacted = []
    for batch, source_buffer in parsed:
        if batch.is_control_batch or batch.records_count == 0:
            compacted.append(bytearray(source_buffer))
            continue

        source_batch = DefaultRecordBatch(source_buffer)
        retained = [
            record for record in source_batch
            if record.offset in retained_offsets
        ]

        if retained:
            compacted.append(_rebuild_batch(batch, retained))
        elif preserve_empty_batches:
            compacted.append(_write_empty_batch(batch))

    return compacted


def compact_records(data, preserve_empty_batches=True, validate_crcs=False):
    """Compact a bytes-like value containing one or more magic-v2 batches.

    See :func:`compact_batches` for retention semantics. The returned value is
    the concatenation of the compacted batch buffers and can be decoded with
    :class:`~kafka.record.memory_records.MemoryRecords`.
    """
    records = MemoryRecords(data)
    batches = []
    while records.has_next():
        batches.append(records.next_batch())
    return b"".join(
        compact_batches(
            batches,
            preserve_empty_batches=preserve_empty_batches,
            validate_crcs=validate_crcs,
        )
    )


def _coerce_batch(batch, validate_crcs):
    if isinstance(batch, DefaultRecordBatch):
        if batch._decompressed:
            raise IllegalArgumentError(
                "Cannot compact an already-iterated record batch; "
                "pass the original batch bytes instead"
            )
    else:
        magic = struct.unpack_from(">b", memoryview(batch), _MAGIC_OFFSET)[0]
        if magic < 2:
            raise UnsupportedVersionError(
                "Offline key compaction requires magic-v2 record batches; "
                "got magic {}".format(magic)
            )
        batch = DefaultRecordBatch(batch)
    if validate_crcs and not batch.validate_crc():
        raise IllegalArgumentError(
            "Invalid CRC for record batch at base offset {}".format(
                batch.base_offset
            )
        )
    source_buffer = bytes(batch._buffer[:batch.size_in_bytes])
    return batch, source_buffer


def _retained_offsets(batches):
    latest_key_offset = {}
    protected_offsets = set()

    for batch in batches:
        if batch.is_control_batch:
            for record in batch:
                protected_offsets.add(record.offset)
            continue

        for record in batch:
            key = record.key
            if key is None or len(key) == 0:
                protected_offsets.add(record.offset)
            elif record.value is None:
                # A tombstone supersedes earlier records for its key, but is
                # never itself removed by a later record.
                protected_offsets.add(record.offset)
                latest_key_offset[key] = record.offset
            else:
                latest_key_offset[key] = record.offset

    return protected_offsets.union(latest_key_offset.values())


def _rebuild_batch(batch, records):
    builder = DefaultRecordBatchBuilder(
        magic=batch.magic,
        compression_type=batch.compression_type,
        is_transactional=batch.is_transactional,
        producer_id=batch.producer_id,
        producer_epoch=batch.producer_epoch,
        base_sequence=batch.base_sequence,
        batch_size=DefaultRecordBatch.MAX_INT,
        timestamp_type=batch.timestamp_type,
    )
    builder.set_batch_coordinates(batch.base_offset, batch.base_offset)
    builder.override_timestamps(batch.first_timestamp, batch.max_timestamp)

    for record in records:
        metadata = builder.append(
            record.offset,
            record.timestamp,
            record.key,
            record.value,
            record.headers,
        )
        if metadata is None:
            raise IllegalArgumentError(
                "Record at offset {} exceeds the compacted batch "
                "capacity".format(record.offset)
            )

    # The original last offset (and thus last sequence) must survive even if
    # the last record in the batch was removed.
    builder.override_last_offset(batch.last_offset)
    return builder.build(
        force_compression=batch.compression_type != DefaultRecordBatch.CODEC_NONE
    )


def _write_empty_batch(batch):
    attributes = batch.attributes
    buffer = bytearray(DefaultRecordBatch.HEADER_STRUCT.size)
    DefaultRecordBatch.HEADER_STRUCT.pack_into(
        buffer,
        0,
        batch.base_offset,
        len(buffer) - DefaultRecordBatch.AFTER_LEN_OFFSET,
        batch.leader_epoch,
        batch.magic,
        0,
        attributes,
        batch.last_offset_delta,
        batch.first_timestamp,
        batch.max_timestamp,
        batch.producer_id,
        batch.producer_epoch,
        batch.base_sequence,
        0,
    )
    crc = calc_crc32c(memoryview(buffer)[DefaultRecordBatch.ATTRIBUTES_OFFSET:])
    DefaultRecordBatch.HEADER_STRUCT.pack_into(
        buffer,
        0,
        batch.base_offset,
        len(buffer) - DefaultRecordBatch.AFTER_LEN_OFFSET,
        batch.leader_epoch,
        batch.magic,
        crc,
        attributes,
        batch.last_offset_delta,
        batch.first_timestamp,
        batch.max_timestamp,
        batch.producer_id,
        batch.producer_epoch,
        batch.base_sequence,
        0,
    )
    return buffer
