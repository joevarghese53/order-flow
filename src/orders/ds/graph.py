from __future__ import annotations

import math
from collections.abc import Iterable

from orders.ds.deadline_heap import DeadlineHeap


class WeightedGraph:
    """Undirected weighted graph with Dijkstra's shortest-path search."""

    def __init__(self) -> None:
        self._adj: dict[str, dict[str, float]] = {}

    def add_edge(self, a: str, b: str, weight: float) -> None:
        if weight < 0:
            raise ValueError("Dijkstra requires non-negative weights")
        self._adj.setdefault(a, {})[b] = weight
        self._adj.setdefault(b, {})[a] = weight

    def shortest_paths(self, source: str) -> tuple[dict[str, float], dict[str, str]]:
        """Return (distance, previous-node) maps for every node reachable from source."""
        if source not in self._adj:
            raise KeyError(f"unknown node {source!r}")
        dist: dict[str, float] = {source: 0.0}
        prev: dict[str, str] = {}
        settled: set[str] = set()
        frontier: DeadlineHeap[str] = DeadlineHeap()
        frontier.push(0.0, source)
        while frontier:
            node = frontier.pop()
            if node in settled:
                continue  # stale entry left behind by a later, cheaper relaxation
            settled.add(node)
            for neighbour, weight in self._adj[node].items():
                candidate = dist[node] + weight
                if candidate < dist.get(neighbour, math.inf):
                    dist[neighbour] = candidate
                    prev[neighbour] = node
                    frontier.push(candidate, neighbour)
        return dist, prev

    def nearest(self, source: str, candidates: Iterable[str]) -> list[tuple[str, float]]:
        """Candidates reachable from source, closest first. Ties break alphabetically."""
        dist, _ = self.shortest_paths(source)
        reachable = [(c, dist[c]) for c in candidates if c in dist]
        return sorted(reachable, key=lambda pair: (pair[1], pair[0]))

    @staticmethod
    def path(prev: dict[str, str], source: str, target: str) -> list[str]:
        """Rebuild the route from source to target using a predecessor map."""
        route = [target]
        while route[-1] != source:
            route.append(prev[route[-1]])
        return list(reversed(route))
