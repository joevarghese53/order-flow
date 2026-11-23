from __future__ import annotations

from typing import Generic, TypeVar

T = TypeVar("T")


class DeadlineHeap(Generic[T]):
    """Binary min-heap keyed by a numeric deadline.

    Entries with equal deadlines pop in insertion order (FIFO). A sequence
    number breaks ties, so items themselves never need to be comparable.
    push and pop are O(log n); peek is O(1).
    """

    def __init__(self) -> None:
        self._items: list[tuple[float, int, T]] = []
        self._counter = 0

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return bool(self._items)

    def push(self, deadline: float, item: T) -> None:
        self._items.append((deadline, self._counter, item))
        self._counter += 1
        self._sift_up(len(self._items) - 1)

    def peek(self) -> T:
        if not self._items:
            raise IndexError("peek from empty heap")
        return self._items[0][2]

    def pop(self) -> T:
        if not self._items:
            raise IndexError("pop from empty heap")
        top = self._items[0]
        last = self._items.pop()
        if self._items:
            self._items[0] = last
            self._sift_down(0)
        return top[2]

    def _less(self, i: int, j: int) -> bool:
        return self._items[i][:2] < self._items[j][:2]

    def _swap(self, i: int, j: int) -> None:
        self._items[i], self._items[j] = self._items[j], self._items[i]

    def _sift_up(self, i: int) -> None:
        while i > 0:
            parent = (i - 1) // 2
            if not self._less(i, parent):
                return
            self._swap(i, parent)
            i = parent

    def _sift_down(self, i: int) -> None:
        n = len(self._items)
        while True:
            smallest = i
            left, right = 2 * i + 1, 2 * i + 2
            if left < n and self._less(left, smallest):
                smallest = left
            if right < n and self._less(right, smallest):
                smallest = right
            if smallest == i:
                return
            self._swap(i, smallest)
            i = smallest
