import pytest

from orders.catalog import ProductCatalog
from orders.ds.lru import LRUCache
from orders.messaging import OrderQueue
from orders.placement import OrderPlacer, ValidationError
from orders.models import OrderStatus
from tests.fakes import FakeOrderRepo, FakeQueue

NOW_MS = 1_700_000_000_000
PRODUCTS = {"SKU-1": {"sku": "SKU-1", "name": "Mug"}, "SKU-2": {"sku": "SKU-2", "name": "Lamp"}}


def make_placer(repo=None, queue=None):
    repo = repo or FakeOrderRepo()
    queue = queue or FakeQueue()
    ids = iter(f"order-{n}" for n in range(1, 100))
    catalog = ProductCatalog(PRODUCTS.get, LRUCache(10))
    placer = OrderPlacer(repo, catalog, queue, clock=lambda: NOW_MS, new_id=lambda: next(ids))
    return placer, repo, queue


def body(**overrides):
    base = {"customer_id": "cust-1", "region": "north", "lines": [{"sku": "SKU-1", "quantity": 2}]}
    base.update(overrides)
    return base


def test_new_order_is_stored_enqueued_and_accepted():
    placer, repo, queue = make_placer()
    result = placer.place(body(), "key-1")
    assert result.status_code == 202
    assert result.body == {"order_id": "order-1", "status": "PENDING"}
    assert repo.orders["order-1"].region == "NORTH"
    assert queue.sent == ["order-1"]


def test_same_idempotency_key_and_body_returns_same_order_without_duplicating():
    placer, repo, queue = make_placer()
    first = placer.place(body(), "key-1")
    second = placer.place(body(), "key-1")
    assert second.body["order_id"] == first.body["order_id"]
    assert len(repo.orders) == 1
    # Re-enqueued because the order is still pending; the worker tolerates duplicates.
    assert queue.sent == ["order-1", "order-1"]


def test_retry_after_order_was_processed_does_not_enqueue_again():
    placer, repo, queue = make_placer()
    placer.place(body(), "key-1")
    repo.orders["order-1"].status = OrderStatus.CONFIRMED
    result = placer.place(body(), "key-1")
    assert result.status_code == 200
    assert result.body["status"] == "CONFIRMED"
    assert queue.sent == ["order-1"]


def test_reusing_idempotency_key_with_different_body_is_rejected():
    placer, repo, _ = make_placer()
    placer.place(body(), "key-1")
    result = placer.place(body(lines=[{"sku": "SKU-2", "quantity": 1}]), "key-1")
    assert result.status_code == 422
    assert len(repo.orders) == 1


def test_missing_idempotency_key_is_a_validation_error():
    placer, _, _ = make_placer()
    with pytest.raises(ValidationError, match="Idempotency-Key"):
        placer.place(body(), "")


@pytest.mark.parametrize(
    "bad_body",
    [
        body(customer_id=""),
        body(region="MARS"),
        body(lines=[]),
        body(lines=[{"sku": "SKU-1", "quantity": 0}]),
        body(lines=[{"sku": "SKU-1", "quantity": True}]),
        body(lines=[{"sku": "SKU-1", "quantity": 101}]),
        body(lines=[{"sku": "NOPE", "quantity": 1}]),
        body(lines=[{"sku": "SKU-1", "quantity": 1}, {"sku": "SKU-1", "quantity": 2}]),
        body(ship_by=NOW_MS // 1000 - 10),
        "not an object",
    ],
)
def test_invalid_orders_are_rejected(bad_body):
    placer, repo, queue = make_placer()
    with pytest.raises(ValidationError):
        placer.place(bad_body, "key-1")
    assert repo.orders == {} and queue.sent == []


def test_default_ship_by_is_two_days_out():
    placer, repo, _ = make_placer()
    placer.place(body(), "key-1")
    assert repo.orders["order-1"].ship_by == NOW_MS // 1000 + 2 * 24 * 3600


def test_order_queue_sends_only_the_order_id():
    sent = []

    class FakeSqs:
        def send_message(self, **kwargs):
            sent.append(kwargs)

    OrderQueue(FakeSqs(), "https://sqs.example/queue").send("order-9")
    assert sent == [{"QueueUrl": "https://sqs.example/queue", "MessageBody": '{"order_id": "order-9"}'}]
