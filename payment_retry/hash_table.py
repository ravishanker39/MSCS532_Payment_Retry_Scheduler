"""Resizable separate chaining with list buckets, implemented without dict.

Expected lookup/update cost is O(1) under well-distributed hashes and bounded
key lengths. A collision chain can make an operation O(n). Resizing costs
O(n + capacity) for that operation; geometric resizing amortizes that work.
This class is internal to the scheduler and requires immutable string keys.
"""

from collections.abc import Callable, Iterator
from typing import Generic, TypeVar

V = TypeVar("V")


class ChainedHashTable(Generic[V]):
    """List buckets avoid allocating a separate linked-list node per collision.

    Growth above 0.75 and shrinkage below 0.20 leave a gap that prevents
    repeated growth/shrinkage when operations alternate near a threshold.
    The injected hasher is used only to test collision behavior.
    """

    MIN_CAPACITY = 8

    def __init__(self, hasher: Callable[[str], int] = hash) -> None:
        self._buckets: list[list[tuple[str, V]]] = [
            [] for _ in range(self.MIN_CAPACITY)
        ]
        self._size = 0
        self._hasher = hasher

    def __len__(self) -> int:
        return self._size

    @property
    def capacity(self) -> int:
        return len(self._buckets)

    def __getitem__(self, key: str) -> V:
        # A hash picks the bucket. Two IDs can land in the same bucket,
        # so compare their full strings to find the requested entry.
        for stored_key, value in self._buckets[self._hasher(key) % self.capacity]:
            if stored_key == key:
                return value
        raise KeyError(key)

    def __contains__(self, key: str) -> bool:
        try:
            self[key]
            return True
        except KeyError:
            return False

    def __setitem__(self, key: str, value: V) -> None:
        bucket = self._buckets[self._hasher(key) % self.capacity]
        for index, (stored_key, _) in enumerate(bucket):
            if stored_key == key:
                # The ID already has a slot. Updating it keeps the size the same.
                bucket[index] = (key, value)
                return
        # With eight buckets, six keys fit at the 75% limit. The seventh
        # triggers a resize, so look up its bucket again before inserting.
        if (self._size + 1) * 4 > self.capacity * 3:
            self._resize(self.capacity * 2)
            bucket = self._buckets[self._hasher(key) % self.capacity]
        bucket.append((key, value))
        self._size += 1

    def __delitem__(self, key: str) -> None:
        bucket = self._buckets[self._hasher(key) % self.capacity]
        for index, (stored_key, _) in enumerate(bucket):
            if stored_key == key:
                # Fill the gap with the bucket's last pair. Lookups search by
                # ID, so moving this pair leaves the other keys accessible.
                bucket[index] = bucket[-1]
                bucket.pop()
                self._size -= 1
                # Shrink once the key count falls below 20% of the bucket
                # count. One deletion just after growth won't undo it.
                if self.capacity > self.MIN_CAPACITY and self._size * 5 < self.capacity:
                    self._resize(max(self.MIN_CAPACITY, self.capacity // 2))
                return
        raise KeyError(key)

    def items(self) -> Iterator[tuple[str, V]]:
        """Iterate without mutation; no ordering is promised."""
        for bucket in self._buckets:
            yield from bucket

    def _resize(self, capacity: int) -> None:
        replacement: list[list[tuple[str, V]]] = [[] for _ in range(capacity)]
        # The bucket number depends on capacity. Redistribute every key
        # but keep its value pointing to the same heap entry.
        for key, value in self.items():
            replacement[self._hasher(key) % capacity].append((key, value))
        self._buckets = replacement

    def assert_valid(self) -> None:
        """Expensive diagnostic; never included in operation timings."""
        assert self.capacity >= self.MIN_CAPACITY
        # Starting at eight and always doubling or halving keeps capacity
        # a power of two. Such a number has exactly one bit set.
        assert self.capacity & (self.capacity - 1) == 0
        keys = set()
        for index, bucket in enumerate(self._buckets):
            for key, _ in bucket:
                assert self._hasher(key) % self.capacity == index
                assert key not in keys
                keys.add(key)
        assert len(keys) == self._size

