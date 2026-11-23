import pytest

from orders.catalog import ProductCatalog
from orders.ds.lru import LRUCache
from orders.models import OrderStatus
from orders.placement import OrderPlacer
from orders.topology import WarehouseRanker, build_shipping_graph
from orders.worker import OrderWorker
from tests.fakes import FakeNotifier, FakeOrderRepo, FakeQueue, sqs_record

NOW_MS = 1_700_000_000_000
PRODUCTS = {"SKU-1": {"sku": "SKU-1"}, "SKU-2": {"sku": "SKU-2"}}


class Harness:
    def __init__(self):
        self.repo = FakeOrderRepo()
        self.queue = FakeQueue()
        self.notifier = FakeNotifier()
        catalog = ProductCatalog(PRODUCTS.get, LRUCache(10))
        self.placer = OrderPlacer(self.repo, catalog, self.queue, clock=lambda: NOW_MS, new_id=self._next_id())
        self.worker = OrderWorker(
            self.repo, catalog, self.notifier, WarehouseRanker(build_shipping_graph()), clock=lambda: NOW_MS + 500
        )
        self.repo.inventory = {("SKU-1", "WH-NORTH"): 5, ("SKU-1", "WH-EAST"): 5, ("SKU-1", "WH-WEST"): 5}

    @staticmethod
    def _next_id():
        counter = iter(range(1, 1000))
        return lambda: f"order-{next(counter)}"

    def place(self, key, lines, region="north", ship_by=NOW_MS // 1000 + 3600):
        payload = {"customer_id": "c", "region": region, "lines": lines, "ship_by": ship_by}
        return self.placer.place(payload, key).body["order_id"]

    def run(self, order_ids, message_prefix="m"):
        records = [sqs_record(f"{message_prefix}{i}", oid) for i, oid in enumerate(order_ids)]
        return self.worker.handle_batch(records)


@pytest.fixture
def h():
    return Harness()


def test_order_is_confirmed_at_nearest_warehouse_with_stock(h):
    order_id = h.place("k1", [{"sku": "SKU-1", "quantity": 2}], region="north")
    assert h.run([order_id]) == {"batchItemFailures": []}

    order = h.repo.orders[order_id]
    assert order.status is OrderStatus.CONFIRMED
    assert order.warehouse_id == "WH-NORTH"
    assert h.repo.inventory[("SKU-1", "WH-NORTH")] == 3
    assert [o.status for o in h.notifier.published] == [OrderStatus.CONFIRMED]


def test_falls_back_to_next_closest_warehouse_when_nearest_is_empty(h):
    h.repo.inventory[("SKU-1", "WH-NORTH")] = 0
    order_id = h.place("k1", [{"sku": "SKU-1", "quantity": 1}], region="north")
    h.run([order_id])
    assert h.repo.orders[order_id].warehouse_id == "WH-EAST"  # NORTH->EAST->WH-EAST = 4h, the next closest


def test_order_is_rejected_when_no_warehouse_has_stock(h):
    for key in list(h.repo.inventory):
        h.repo.inventory[key] = 0
    order_id = h.place("k1", [{"sku": "SKU-1", "quantity": 1}])
    h.run([order_id])
    order = h.repo.orders[order_id]
    assert order.status is OrderStatus.REJECTED
    assert order.reason == "OUT_OF_STOCK"
    assert h.notifier.published[-1].status is OrderStatus.REJECTED


def test_stock_is_never_oversold_across_competing_orders(h):
    h.repo.inventory = {("SKU-1", "WH-NORTH"): 3, ("SKU-1", "WH-EAST"): 0, ("SKU-1", "WH-WEST"): 0}
    ids = [h.place(f"k{i}", [{"sku": "SKU-1", "quantity": 1}]) for i in range(5)]
    h.run(ids)
    statuses = [h.repo.orders[i].status for i in ids]
    assert statuses.count(OrderStatus.CONFIRMED) == 3
    assert statuses.count(OrderStatus.REJECTED) == 2
    assert h.repo.inventory[("SKU-1", "WH-NORTH")] == 0


def test_earliest_deadline_is_processed_first(h):
    late = h.place("late", [{"sku": "SKU-1", "quantity": 1}], ship_by=NOW_MS // 1000 + 9000)
    soon = h.place("soon", [{"sku": "SKU-1", "quantity": 1}], ship_by=NOW_MS // 1000 + 100)
    middle = h.place("mid", [{"sku": "SKU-1", "quantity": 1}], ship_by=NOW_MS // 1000 + 5000)
    h.run([late, soon, middle])
    assert [oid for oid, _ in h.repo.reserve_log] == [soon, middle, late]


def test_redelivered_message_does_not_reserve_twice(h):
    order_id = h.place("k1", [{"sku": "SKU-1", "quantity": 2}])
    h.run([order_id], "first")
    h.run([order_id], "second")  # SQS redelivery of the same order
    assert h.repo.inventory[("SKU-1", "WH-NORTH")] == 3


def test_failed_notification_is_retried_without_double_reserving(h):
    order_id = h.place("k1", [{"sku": "SKU-1", "quantity": 2}])
    h.notifier.failures_remaining = 1

    result = h.run([order_id])
    assert result == {"batchItemFailures": [{"itemIdentifier": "m0"}]}
    assert h.repo.inventory[("SKU-1", "WH-NORTH")] == 3  # stock reserved despite the failed publish

    assert h.run([order_id], "retry") == {"batchItemFailures": []}
    assert h.repo.inventory[("SKU-1", "WH-NORTH")] == 3  # still reserved exactly once
    assert [o.status for o in h.notifier.published] == [OrderStatus.CONFIRMED]  # notification eventually sent


def test_unknown_message_body_is_reported_as_failure_not_raised(h):
    result = h.worker.handle_batch([{"messageId": "bad", "body": "{not json"}])
    assert result == {"batchItemFailures": [{"itemIdentifier": "bad"}]}


def test_missing_order_is_reported_as_failure(h):
    result = h.run(["order-does-not-exist"])
    assert result == {"batchItemFailures": [{"itemIdentifier": "m0"}]}
