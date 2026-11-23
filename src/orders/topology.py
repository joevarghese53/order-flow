"""Shipping network: regions and warehouses as nodes, transit hours as edges.

The worker asks the graph which warehouse is closest to the customer's
region (Dijkstra) and tries them in that order until one has stock.
"""

from orders.ds.graph import WeightedGraph

REGIONS = ("NORTH", "EAST", "SOUTH", "WEST")
WAREHOUSES = ("WH-NORTH", "WH-EAST", "WH-WEST")

# (from, to, transit hours)
EDGES = [
    ("NORTH", "WH-NORTH", 1),
    ("NORTH", "EAST", 3),
    ("EAST", "WH-EAST", 1),
    ("EAST", "SOUTH", 2),
    ("SOUTH", "WEST", 3),
    ("WEST", "WH-WEST", 1),
    ("NORTH", "WEST", 6),
]


def build_shipping_graph() -> WeightedGraph:
    graph = WeightedGraph()
    for a, b, hours in EDGES:
        graph.add_edge(a, b, hours)
    return graph


class WarehouseRanker:
    def __init__(self, graph: WeightedGraph) -> None:
        self._graph = graph

    def warehouses_by_distance(self, region: str) -> list[str]:
        return [warehouse for warehouse, _ in self._graph.nearest(region, WAREHOUSES)]
