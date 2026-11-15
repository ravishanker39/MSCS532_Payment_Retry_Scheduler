"""Indexed binary min-heap supporting explicit deadline changes and removal.

The hash table maps IDs to entries, and each entry stores its heap position.
Heap swaps update positions directly, avoiding a hash lookup at every level.
Only one entry exists per pending ID; cancellation leaves no stale heap item.
This is a single-threaded, volatile scheduling core, not a payment processor.
"""

from collections.abc import Callable
from dataclasses import dataclass, replace

from .hash_table import ChainedHashTable


@dataclass(frozen=True, slots=True)
class RetryJob:
    """Immutable public view; timestamps are nonnegative integer milliseconds."""

    job_id: str
    due_at_ms: int
    sequence: int


@dataclass(slots=True)
class _Entry:
    job: RetryJob
    position: int

    @property
    def priority(self) -> tuple[int, int]:
        # Compare the deadline first. When two deadlines match, the job
        # with the smaller arrival sequence gets the first turn.
        return self.job.due_at_ms, self.job.sequence


class RetryScheduler:
    """Keep pending jobs in increasing (deadline, original arrival) order.

    Rescheduling preserves original arrival order among equal deadlines.
    Re-adding a removed ID is a new arrival. Pending-ID uniqueness does not
    implement durable idempotency or exactly-once payment execution.
    """

    def __init__(self, *, hasher: Callable[[str], int] = hash) -> None:
        self._heap: list[_Entry] = []
        self._by_id: ChainedHashTable[_Entry] = ChainedHashTable(hasher)
        self._next_sequence = 0

    def __len__(self) -> int:
        return len(self._heap)

    @staticmethod
    def _validate_id(job_id: str) -> None:
        if type(job_id) is not str or not 1 <= len(job_id) <= 128:
            raise ValueError("job_id must be a string of 1 to 128 characters")

    @staticmethod
    def _validate_time(value: int) -> None:
        if type(value) is not int or not 0 <= value <= 2**63 - 1:
            raise ValueError("time must be a nonnegative signed 64-bit integer")

    def schedule(self, job_id: str, due_at_ms: int) -> RetryJob:
        """Add a new pending ID; reject duplicates without altering the queue."""
        self._validate_id(job_id)
        self._validate_time(due_at_ms)
        if job_id in self._by_id:
            raise ValueError(f"job already pending: {job_id}")
        # The new entry starts at the end of the heap. Store that same
        # entry under its ID so later deadline changes can find it directly.
        job = RetryJob(job_id, due_at_ms, self._next_sequence)
        entry = _Entry(job, len(self._heap))
        self._heap.append(entry)
        self._by_id[job_id] = entry
        self._next_sequence += 1
        self._sift_up(entry.position)
        return job

    def get(self, job_id: str) -> RetryJob:
        """Return an immutable snapshot, or raise KeyError for an unknown ID."""
        self._validate_id(job_id)
        return self._by_id[job_id].job

    def peek(self) -> RetryJob | None:
        return self._heap[0].job if self._heap else None

    def reschedule(self, job_id: str, due_at_ms: int) -> RetryJob:
        """Move a deadline earlier or later and immediately repair heap order."""
        self._validate_id(job_id)
        self._validate_time(due_at_ms)
        entry = self._by_id[job_id]
        # Keep the original sequence when changing the deadline. Otherwise
        # editing a waiting job could send it to the back of a tie.
        entry.job = replace(entry.job, due_at_ms=due_at_ms)
        self._repair(entry.position)
        return entry.job

    def cancel(self, job_id: str) -> RetryJob:
        """Remove any pending job in expected amortized O(log n) time."""
        self._validate_id(job_id)
        return self._remove_at(self._by_id[job_id].position)

    def pop_due(self, now_ms: int) -> RetryJob | None:
        """Remove one eligible job; leave future jobs queued."""
        self._validate_time(now_ms)
        # The root has the earliest deadline. If even that job is in the
        # future, all jobs must wait. A deadline equal to now is eligible.
        if not self._heap or self._heap[0].job.due_at_ms > now_ms:
            return None
        return self._remove_at(0)

    def _swap(self, first: int, second: int) -> None:
        self._heap[first], self._heap[second] = self._heap[second], self._heap[first]
        # The ID table points to these entry objects. Fix their positions
        # now so a later cancellation removes the intended job.
        self._heap[first].position = first
        self._heap[second].position = second

    def _sift_up(self, index: int) -> None:
        while index:
            parent = (index - 1) // 2
            # An earlier job trades places with its parent. Once the parent
            # already comes first, the job is high enough in the heap.
            if self._heap[parent].priority <= self._heap[index].priority:
                break
            self._swap(parent, index)
            index = parent

    def _sift_down(self, index: int) -> None:
        size = len(self._heap)
        while 2 * index + 1 < size:
            child = 2 * index + 1
            # Start with the left child. If the right child is due sooner,
            # promote it instead so both children still follow their parent.
            if child + 1 < size and self._heap[child + 1].priority < self._heap[child].priority:
                child += 1
            if self._heap[index].priority <= self._heap[child].priority:
                break
            self._swap(index, child)
            index = child

    def _repair(self, index: int) -> None:
        # One priority changed. It can cross its parent or one of its
        # children, so only one direction needs repair.
        if index and self._heap[index].priority < self._heap[(index - 1) // 2].priority:
            self._sift_up(index)
        else:
            self._sift_down(index)

    def _remove_at(self, index: int) -> RetryJob:
        removed = self._heap[index]
        last = self._heap.pop()
        del self._by_id[removed.job.job_id]
        # Use the last entry to fill the gap, then restore heap order from
        # that slot. Removing the last entry itself leaves no gap.
        if index < len(self._heap):
            self._heap[index] = last
            last.position = index
            self._repair(index)
        removed.position = -1
        return removed.job

    def assert_valid(self) -> None:
        """Check both heap order and the ID-to-entry bijection, outside timing."""
        self._by_id.assert_valid()
        assert len(self._heap) == len(self._by_id)
        for index, entry in enumerate(self._heap):
            # Looking up any heap entry by ID must lead back to this exact
            # object and slot. Otherwise cancelling that ID could remove a neighbor.
            assert entry.position == index
            assert self._by_id[entry.job.job_id] is entry
            if index:
                assert self._heap[(index - 1) // 2].priority <= entry.priority

