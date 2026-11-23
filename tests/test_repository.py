"""Checks how the DynamoDB repository interprets transaction cancellations.

These use a hand-built client rather than AWS, so they run offline. The
error shape matches what boto3 returns for TransactionCanceledException.
"""

from botocore.exceptions import ClientError

from orders.models import Order, OrderLine, OrderStatus, ReserveOutcome
from orders.repository import DynamoOrderRepository

PENDING_ORDER = Order(
    order_id="o1",
    customer_id="c",
    region="NORTH",
    lines=[OrderLine("SKU-1", 2), OrderLine("SKU-2", 1)],
    ship_by=2_000_000_000,
    created_at=1_700_000_000_000,
)


def cancelled(reasons):
    return ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [{"Code": code} for code in reasons],
        },
        "TransactWriteItems",
    )


class FakeClient:
    def __init__(self, transact_error=None, existing_idem=None):
        self.transact_error = transact_error
        self.existing_idem = existing_idem
        self.transact_calls = []

    def transact_write_items(self, TransactItems):
        self.transact_calls.append(TransactItems)
        if self.transact_error:
            raise self.transact_error

    def get_item(self, **kwargs):
        return {"Item": self.existing_idem} if self.existing_idem else {}


def repo_with(client):
    return DynamoOrderRepository(client, orders_table="orders", inventory_table="inventory")


def test_reserve_success_conditions_every_line_and_the_order_status():
    client = FakeClient()
    outcome = repo_with(client).reserve(PENDING_ORDER, "WH-NORTH")

    assert outcome is ReserveOutcome.RESERVED
    operations = client.transact_calls[0]
    assert len(operations) == 3  # two inventory lines + the order update
    assert operations[0]["Update"]["ConditionExpression"] == "qty >= :requested"
    assert operations[-1]["Update"]["ConditionExpression"] == "#status = :pending"


def test_reserve_maps_order_condition_failure_to_already_processed():
    client = FakeClient(transact_error=cancelled(["None", "None", "ConditionalCheckFailed"]))
    assert repo_with(client).reserve(PENDING_ORDER, "WH-NORTH") is ReserveOutcome.ALREADY_PROCESSED


def test_reserve_maps_inventory_condition_failure_to_out_of_stock():
    client = FakeClient(transact_error=cancelled(["ConditionalCheckFailed", "None", "None"]))
    assert repo_with(client).reserve(PENDING_ORDER, "WH-NORTH") is ReserveOutcome.OUT_OF_STOCK


def test_reserve_reraises_unexpected_cancellations():
    client = FakeClient(transact_error=cancelled(["TransactionConflict", "None", "None"]))
    try:
        repo_with(client).reserve(PENDING_ORDER, "WH-NORTH")
    except RuntimeError as err:
        assert "unexpected" in str(err)
    else:
        raise AssertionError("expected RuntimeError")


def test_create_order_returns_existing_record_when_idempotency_key_is_taken():
    stored = {"pk": {"S": "IDEM#k"}, "order_id": {"S": "o0"}, "request_hash": {"S": "abc"}}
    client = FakeClient(transact_error=cancelled(["ConditionalCheckFailed", "ConditionalCheckFailed"]), existing_idem=stored)

    created, record = repo_with(client).create_order("k", "abc", PENDING_ORDER, 3600)

    assert created is False
    assert record == {"pk": "IDEM#k", "order_id": "o0", "request_hash": "abc"}


def test_create_order_writes_idempotency_and_order_in_one_transaction():
    client = FakeClient()
    created, record = repo_with(client).create_order("k", "abc", PENDING_ORDER, 3600)

    assert (created, record) == (True, None)
    puts = client.transact_calls[0]
    assert [p["Put"]["Item"]["pk"] for p in puts] == [{"S": "IDEM#k"}, {"S": "ORDER#o1"}]
    assert all(p["Put"]["ConditionExpression"] == "attribute_not_exists(pk)" for p in puts)
    assert OrderStatus.PENDING.value == puts[1]["Put"]["Item"]["status"]["S"]
