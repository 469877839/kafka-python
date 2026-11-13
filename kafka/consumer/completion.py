"""Explicit processing-completion tracking for parallel consumers.

When records from a partition are processed in parallel (or otherwise
complete out of order), committing the fetch position directly would
skip records whose processing has not finished yet. This module tracks,
per partition, which delivered offsets are still pending and computes
the highest offset that is safe to commit: the next position such that
every *delivered* record below it has been explicitly completed.

Offsets in a Kafka partition are not guaranteed to be contiguous:
log compaction and transaction control records leave holes that are
never delivered to the consumer. The tracker therefore advances based
on the actually delivered offsets only -- it never waits for offsets
that were never handed to the application.

Note: this provides "don't commit past unprocessed records" semantics
within Kafka only. It does NOT provide exactly-once semantics across
external systems (e.g. a database): after a crash or rebalance, records
between the committed offset and the last completed offset will be
redelivered, so processing must be idempotent or made transactional by
the application.
"""

import logging
import threading

from kafka.structs import OffsetAndMetadata

log = logging.getLogger(__name__)


class ProcessingCompletionTracker:
    """Tracks delivered-but-uncompleted offsets per assigned partition.

    Delivery is registered with :meth:`note_delivered` and processing
    completion with :meth:`complete`. :meth:`committable_offsets` then
    computes, per partition, the next offset that may be committed:

    * if any delivered offsets are still pending, the committable
      position is the smallest pending offset (everything below it was
      either completed or never delivered -- compaction / control-record
      holes are skipped automatically);
    * otherwise it is the partition's current fetch position.

    Each (re)assignment of a partition starts a new, monotonically
    increasing epoch. Seeks, revocations and reassignments discard the
    pending state of the affected partition, and completion
    notifications carrying a stale epoch are rejected, so completions
    from a previous assignment can never advance the new assignment's
    state.
    """

    def __init__(self):
        self._lock = threading.RLock()
        # tp -> current assignment epoch. Monotonic per partition and
        # kept across revocations so a re-assignment always starts a
        # strictly newer epoch than any previous one.
        self._epochs = {}
        # tp -> {offset: epoch} for records delivered under the current
        # assignment but not yet completed.
        self._pending = {}

    def note_delivered(self, tp, offset):
        """Register a record handed to the application as pending.

        Returns True if the delivery was tracked, False if the
        partition is not part of the current tracked assignment.
        """
        with self._lock:
            pending = self._pending.get(tp)
            if pending is None:
                log.debug("Ignoring delivery of %s offset %s: partition not "
                          "in the tracked assignment", tp, offset)
                return False
            pending[offset] = self._epochs[tp]
            return True

    def complete(self, tp, offset, epoch=None):
        """Mark a delivered record as fully processed.

        Duplicate completions and completions for offsets that are not
        pending (never delivered, already completed, or delivered under
        a revoked/reset assignment) are ignored. If ``epoch`` is given,
        the completion is only honored when it matches the partition's
        current assignment epoch, so a notification issued for a record
        of a previous assignment cannot complete a redelivery under the
        new assignment.

        Returns True if a pending entry was cleared, False otherwise.
        """
        with self._lock:
            pending = self._pending.get(tp)
            if not pending or offset not in pending:
                return False
            current_epoch = self._epochs[tp]
            if epoch is not None and epoch != current_epoch:
                log.debug("Ignoring completion of %s offset %s for stale "
                          "epoch %s (current epoch %s)",
                          tp, offset, epoch, current_epoch)
                return False
            del pending[offset]
            return True

    def committable_offsets(self, subscription):
        """Compute the offsets safe to commit for the current assignment.

        Arguments:
            subscription (SubscriptionState): provides the current
                assignment and fetch positions.

        Returns:
            dict: {TopicPartition: OffsetAndMetadata} for every assigned
            partition with a valid position.
        """
        offsets = {}
        with self._lock:
            for tp, state in subscription.assignment.items():
                if not state.has_valid_position:
                    continue
                pending = self._pending.get(tp)
                current_epoch = self._epochs.get(tp)
                if pending:
                    outstanding = [offset for offset, epoch in pending.items()
                                   if epoch == current_epoch]
                else:
                    outstanding = []
                if outstanding:
                    next_offset = min(outstanding)
                else:
                    next_offset = state.position.offset
                offsets[tp] = OffsetAndMetadata(
                    next_offset, state.position.metadata,
                    state.position.leader_epoch)
        return offsets

    def current_epoch(self, tp):
        """Return the partition's current assignment epoch, or None."""
        with self._lock:
            return self._epochs.get(tp)

    def pending_offsets(self, tp):
        """Return the set of delivered-but-uncompleted offsets for tp."""
        with self._lock:
            return set(self._pending.get(tp, ()))

    # SubscriptionState assignment-listener callbacks.

    def on_partitions_assigned(self, partitions):
        with self._lock:
            for tp in partitions:
                self._epochs[tp] = self._epochs.get(tp, -1) + 1
                self._pending[tp] = {}

    def on_partitions_revoked(self, partitions):
        with self._lock:
            for tp in partitions:
                # The epoch counter is kept so a later re-assignment
                # starts a strictly newer epoch.
                self._pending.pop(tp, None)

    def on_position_reset(self, tp):
        with self._lock:
            if tp in self._pending:
                self._epochs[tp] += 1
                self._pending[tp] = {}
