from __future__ import annotations

from typing import Generic, Hashable, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class _Node(Generic[K, V]):
    __slots__ = ("key", "value", "prev", "next")

    def __init__(self, key: K | None = None, value: V | None = None) -> None:
        self.key = key
        self.value = value
        self.prev: _Node | None = None
        self.next: _Node | None = None


class LRUCache(Generic[K, V]):
    """Least-recently-used cache.

    A dict gives O(1) key lookup. A doubly linked list keeps recency order:
    the most recently used node sits just after `_head`, the least recently
    used just before `_tail`. Every get/put is O(1) because moving or removing
    a node only touches its neighbours.
    """

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self._capacity = capacity
        self._index: dict[K, _Node[K, V]] = {}
        self._head: _Node[K, V] = _Node()  # sentinel: MRU side
        self._tail: _Node[K, V] = _Node()  # sentinel: LRU side
        self._head.next = self._tail
        self._tail.prev = self._head
        self.hits = 0
        self.misses = 0

    def __len__(self) -> int:
        return len(self._index)

    def __contains__(self, key: object) -> bool:
        return key in self._index

    def get(self, key: K) -> V | None:
        node = self._index.get(key)
        if node is None:
            self.misses += 1
            return None
        self.hits += 1
        self._move_to_front(node)
        return node.value

    def put(self, key: K, value: V) -> None:
        node = self._index.get(key)
        if node is not None:
            node.value = value
            self._move_to_front(node)
            return
        if len(self._index) >= self._capacity:
            self._evict_least_recent()
        node = _Node(key, value)
        self._index[key] = node
        self._insert_front(node)

    def _evict_least_recent(self) -> None:
        node = self._tail.prev
        assert node is not None and node is not self._head
        self._unlink(node)
        del self._index[node.key]

    def _unlink(self, node: _Node[K, V]) -> None:
        assert node.prev is not None and node.next is not None
        node.prev.next = node.next
        node.next.prev = node.prev

    def _insert_front(self, node: _Node[K, V]) -> None:
        node.prev = self._head
        node.next = self._head.next
        self._head.next.prev = node
        self._head.next = node

    def _move_to_front(self, node: _Node[K, V]) -> None:
        self._unlink(node)
        self._insert_front(node)
