import random

import pytest

from orders.ds.deadline_heap import DeadlineHeap
from orders.ds.graph import WeightedGraph
from orders.ds.lru import LRUCache


# ---------- LRU ----------

def test_lru_evicts_least_recently_used():
    cache = LRUCache(2)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)  # evicts "a"
    assert cache.get("a") is None
    assert cache.get("b") == 2
    assert cache.get("c") == 3


def test_lru_get_refreshes_recency():
    cache = LRUCache(2)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.get("a")  # "b" is now the least recently used
    cache.put("c", 3)
    assert "b" not in cache
    assert "a" in cache and "c" in cache


def test_lru_put_existing_key_updates_value_without_evicting():
    cache = LRUCache(2)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("a", 10)
    assert len(cache) == 2
    assert cache.get("a") == 10
    assert "b" in cache


def test_lru_counts_hits_and_misses():
    cache = LRUCache(1)
    cache.put("x", 1)
    cache.get("x")
    cache.get("y")
    assert (cache.hits, cache.misses) == (1, 1)


def test_lru_capacity_must_be_positive():
    with pytest.raises(ValueError):
        LRUCache(0)


def test_lru_matches_reference_model_under_random_workload():
    rng = random.Random(7)
    capacity = 5
    cache = LRUCache(capacity)
    model: list[tuple[int, int]] = []  # ordered oldest -> newest

    for _ in range(2000):
        key = rng.randint(0, 9)
        if rng.random() < 0.5:
            value = rng.randint(0, 999)
            cache.put(key, value)
            model = [(k, v) for k, v in model if k != key]
            model.append((key, value))
            if len(model) > capacity:
                model.pop(0)
        else:
            expected = next((v for k, v in model if k == key), None)
            if expected is not None:
                model = [(k, v) for k, v in model if k != key]
                model.append((key, expected))
            assert cache.get(key) == expected


# ---------- Deadline heap ----------

def test_heap_pops_in_deadline_order():
    rng = random.Random(1)
    deadlines = [rng.randint(0, 10_000) for _ in range(500)]
    heap: DeadlineHeap[int] = DeadlineHeap()
    for d in deadlines:
        heap.push(d, d)
    popped = [heap.pop() for _ in range(len(deadlines))]
    assert popped == sorted(deadlines)
    assert not heap


def test_heap_breaks_ties_in_insertion_order():
    heap: DeadlineHeap[str] = DeadlineHeap()
    for name in ["first", "second", "third"]:
        heap.push(100, name)
    assert [heap.pop() for _ in range(3)] == ["first", "second", "third"]


def test_heap_peek_and_empty_errors():
    heap: DeadlineHeap[str] = DeadlineHeap()
    with pytest.raises(IndexError):
        heap.pop()
    with pytest.raises(IndexError):
        heap.peek()
    heap.push(5, "urgent")
    heap.push(9, "later")
    assert heap.peek() == "urgent"
    assert len(heap) == 2


# ---------- Graph ----------

def test_dijkstra_prefers_cheaper_multi_hop_route():
    graph = WeightedGraph()
    graph.add_edge("A", "B", 10)
    graph.add_edge("A", "C", 2)
    graph.add_edge("C", "B", 3)
    dist, prev = graph.shortest_paths("A")
    assert dist["B"] == 5
    assert WeightedGraph.path(prev, "A", "B") == ["A", "C", "B"]


def test_nearest_skips_unreachable_candidates_and_sorts_by_distance():
    graph = WeightedGraph()
    graph.add_edge("home", "far", 9)
    graph.add_edge("home", "near", 1)
    graph.add_edge("island", "island2", 1)  # not connected to "home"
    result = graph.nearest("home", ["far", "island", "near"])
    assert result == [("near", 1), ("far", 9)]


def test_add_edge_rejects_negative_weight():
    with pytest.raises(ValueError):
        WeightedGraph().add_edge("a", "b", -1)


def test_shortest_paths_unknown_source_raises():
    with pytest.raises(KeyError):
        WeightedGraph().shortest_paths("nope")
