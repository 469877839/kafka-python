"""Offline key compaction for finite magic-v2 record batch sequences.

This is a deliberately limited, in-process compaction entry point built on
top of the existing :class:`MemoryRecords` reader and
:class:`DefaultRecordBatchBuilder`. It operates strictly on the supplied
input range and does not attempt to emulate broker side retention:

* Records carrying a non-null key and a non-null value are ordinary
  overwritable records; when the same key appears more than once (including
  across different batches), only the record at the highest offset is kept.
* Records with a null key can never overwrite anything and are always kept.
* Tombstones (non-null key, null value) never overwrite earlier entries for
  their key. They are kept while they remain the last occurrence of the
  key, but a later ordinary record supersedes them. Broker delete-retention
  would eventually drop a surviving tombstone; that policy is intentionally
  out of scope here.
* Transaction control batches are passed through byte-for-byte and their
  control records never participate in the key map.

Surviving records keep their original absolute offsets: the batch base
offset is preserved and records are rewritten with their original offset
deltas instead of being renumbered contiguously. Producer id, producer
epoch, base sequence, timestamps and headers are preserved, and every
rebuilt batch gets a fresh, valid CRC and length.
"""

import struct

from kafka.errors import UnsupportedVersionError
from kafka.record.default_records import (
    DefaultRecordBatch, DefaultRecordBatchBuilder,
)
from kafka.record.memory_records import MemoryRecords
from kafka.record.util import calc_crc32c


def compact_records(records):
    """Compact a finite sequence of record batches by key.

    Arguments:
        records (MemoryRecords or bytes-like): the input batch sequence.
            Only magic v2 (default record format) batches are supported.

    Returns:
        MemoryRecords: a new buffer containing, in original order, the
        rebuilt non-empty data batches and the untouched control batches.
        Input batches that end up with no surviving records are dropped.
    """
    if not isinstance(records, MemoryRecords):
        records = MemoryRecords(records)

    # First pass: decode every record up front (MemoryRecords batches are
    # single-pass iterators). We track:
    #   * last_in_batch per batch: the highest ordinary-record offset for
    #     each key inside that batch, so earlier same-batch duplicates are
    #     dropped.
    #   * last_offsets globally: the highest ordinary-record offset for each
    #     key across the whole input. A batch-local winner is kept only when
    #     it still holds the global last position, which gives the
    #     cross-batch last-write-wins behavior. The same table decides
    #     whether a tombstone is still the last occurrence of its key.
    # Null keys never participate and tombstones do not overwrite.
    last_offsets = {}
    # (batch_or_raw_bytes, records_or_None); a None records entry marks a
    # control batch whose raw bytes are passed through untouched.
    planned_batches = []

    for batch in records:
        if batch.magic < 2:
            raise UnsupportedVersionError(
                "Offline compaction requires record format v2 (magic 2),"
                " got magic {}".format(batch.magic))
        if isinstance(batch, DefaultRecordBatch) and batch.is_control_batch:
            # Iterating a control batch would decompress/mutate its buffer,
            # so capture its raw bytes before touching anything else.
            planned_batches.append((bytes(batch._buffer), None, None))
            continue

        kept = [record for record in batch]
        last_in_batch = {}
        for record in kept:
            if record.key is not None and record.value is not None:
                last_offsets[record.key] = max(
                    last_offsets.get(record.key, record.offset),
                    record.offset)
                last_in_batch[record.key] = max(
                    last_in_batch.get(record.key, record.offset),
                    record.offset)
        planned_batches.append((batch, kept, last_in_batch))

    output = bytearray()

    # Second pass: rebuild each data batch with the surviving records.
    for batch, kept, last_in_batch in planned_batches:
        if kept is None:
            output.extend(batch)
            continue

        def _survives(record):
            if record.key is None:
                # Null keys never participate in compaction.
                return True
            if record.value is None:
                # A tombstone survives only while no later ordinary record
                # exists for its key; it never removes an earlier one.
                return record.offset > last_offsets.get(record.key, -1)
            # An ordinary record must be the last ordinary occurrence of its
            # key inside this batch; the global check then decides whether
            # that batch-local winner is still last across the whole input.
            return (last_in_batch[record.key] == record.offset
                    and last_offsets[record.key] == record.offset)

        surviving = [record for record in kept if _survives(record)]
        if not surviving:
            continue

        # The builder measures its uncompressed buffer against batch_size,
        # so size it from the surviving records themselves rather than the
        # (possibly compressed) source batch, which can be far smaller and
        # would make append() reject everything past the first record.
        target_size = DefaultRecordBatchBuilder.HEADER_STRUCT.size
        for record in surviving:
            target_size += DefaultRecordBatchBuilder.size_in_bytes(
                offset_delta=record.offset - batch.base_offset,
                timestamp_delta=record.timestamp - batch.first_timestamp,
                key=record.key,
                value=record.value,
                headers=record.headers)

        builder = DefaultRecordBatchBuilder(
            magic=batch.magic,
            compression_type=batch.compression_type,
            is_transactional=batch.is_transactional,
            producer_id=batch.producer_id,
            producer_epoch=batch.producer_epoch,
            base_sequence=batch.base_sequence,
            batch_size=target_size)

        for record in surviving:
            builder.append(
                offset=record.offset - batch.base_offset,
                timestamp=record.timestamp,
                key=record.key,
                value=record.value,
                headers=list(record.headers))

        rebuilt = bytearray(builder.build())

        # BaseOffset and PartitionLeaderEpoch are not covered by the CRC.
        struct.pack_into(">q", rebuilt, 0, batch.base_offset)
        struct.pack_into(
            ">i", rebuilt, DefaultRecordBatch.AFTER_LEN_OFFSET,
            batch.leader_epoch)

        # The builder does not model the broker-assigned timestamp type
        # bit, so restore it from the original attributes and refresh CRC.
        original_attributes, = struct.unpack_from(
            ">h", rebuilt, DefaultRecordBatch.ATTRIBUTES_OFFSET)
        attributes = original_attributes | (
            batch.attributes & DefaultRecordBatch.TIMESTAMP_TYPE_MASK)
        if attributes != original_attributes:
            struct.pack_into(
                ">h", rebuilt, DefaultRecordBatch.ATTRIBUTES_OFFSET,
                attributes)
            crc = calc_crc32c(
                memoryview(rebuilt)[DefaultRecordBatch.ATTRIBUTES_OFFSET:])
            struct.pack_into(
                ">I", rebuilt, DefaultRecordBatch.CRC_OFFSET, crc)

        output.extend(rebuilt)

    return MemoryRecords(output)
