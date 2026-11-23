"""DynamoDB access.

Orders and idempotency records share the Orders table, keyed by `pk`:
    ORDER#<order_id>        the order document
    IDEM#<idempotency_key>  maps a client key to its order (expires via `ttl`)

Stock lives in the Inventory table keyed by (sku, warehouse_id). Reserving
stock and confirming the order happen in one TransactWriteItems call, so
either both succeed or neither does.
"""

from __future__ import annotations

import time

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from orders.models import Order, OrderStatus, ReserveOutcome

_serialize = TypeSerializer().serialize
_deserialize = TypeDeserializer().deserialize


def _to_av(values: dict) -> dict:
    return {key: _serialize(value) for key, value in values.items()}


def _from_av(item: dict) -> dict:
    return {key: _deserialize(value) for key, value in item.items()}


def order_pk(order_id: str) -> str:
    return f"ORDER#{order_id}"


def idempotency_pk(key: str) -> str:
    return f"IDEM#{key}"


class DynamoOrderRepository:
    def __init__(self, client, orders_table: str, inventory_table: str) -> None:
        self._client = client
        self._orders = orders_table
        self._inventory = inventory_table

    def create_order(
        self, idempotency_key: str, request_hash: str, order: Order, ttl_seconds: int
    ) -> tuple[bool, dict | None]:
        """Store the order and its idempotency record atomically.

        Returns (True, None) when created. If the key was already used, returns
        (False, existing_record) and writes nothing.
        """
        record = {
            "pk": idempotency_pk(idempotency_key),
            "order_id": order.order_id,
            "request_hash": request_hash,
            "ttl": int(time.time()) + ttl_seconds,
        }
        order_item = {"pk": order_pk(order.order_id), **order.to_dict()}
        try:
            self._client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": self._orders,
                            "Item": _to_av(record),
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": self._orders,
                            "Item": _to_av(order_item),
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    },
                ]
            )
            return True, None
        except ClientError as err:
            if err.response["Error"]["Code"] != "TransactionCanceledException":
                raise

        existing = self._client.get_item(
            TableName=self._orders,
            Key=_to_av({"pk": idempotency_pk(idempotency_key)}),
            ConsistentRead=True,
        ).get("Item")
        if existing is None:
            # Cancelled for another reason (for example a transaction conflict). The caller retries.
            raise RuntimeError("transaction cancelled and no idempotency record exists")
        return False, _from_av(existing)

    def get_order(self, order_id: str) -> Order | None:
        item = self._client.get_item(
            TableName=self._orders,
            Key=_to_av({"pk": order_pk(order_id)}),
            ConsistentRead=True,
        ).get("Item")
        return Order.from_dict(_from_av(item)) if item else None

    def reserve(self, order: Order, warehouse_id: str) -> ReserveOutcome:
        """Atomically decrement stock for every line and mark the order CONFIRMED.

        Each inventory update is conditional on `qty >= requested`, so two
        orders can never take the same last unit. The order update requires
        status PENDING, so a redelivered SQS message cannot reserve twice.
        """
        now_ms = int(time.time() * 1000)
        operations = [
            {
                "Update": {
                    "TableName": self._inventory,
                    "Key": _to_av({"sku": line.sku, "warehouse_id": warehouse_id}),
                    "UpdateExpression": "SET qty = qty - :requested",
                    "ConditionExpression": "qty >= :requested",
                    "ExpressionAttributeValues": _to_av({":requested": line.quantity}),
                }
            }
            for line in order.lines
        ]
        operations.append(
            {
                "Update": {
                    "TableName": self._orders,
                    "Key": _to_av({"pk": order_pk(order.order_id)}),
                    "UpdateExpression": "SET #status = :confirmed, warehouse_id = :wh, updated_at = :now",
                    "ConditionExpression": "#status = :pending",
                    "ExpressionAttributeNames": {"#status": "status"},
                    "ExpressionAttributeValues": _to_av(
                        {
                            ":confirmed": OrderStatus.CONFIRMED.value,
                            ":pending": OrderStatus.PENDING.value,
                            ":wh": warehouse_id,
                            ":now": now_ms,
                        }
                    ),
                }
            }
        )
        try:
            self._client.transact_write_items(TransactItems=operations)
            return ReserveOutcome.RESERVED
        except ClientError as err:
            if err.response["Error"]["Code"] != "TransactionCanceledException":
                raise
            reasons = [reason.get("Code") for reason in err.response.get("CancellationReasons", [])]

        # CancellationReasons line up with TransactItems; the order update is last.
        if reasons[-1:] == ["ConditionalCheckFailed"]:
            return ReserveOutcome.ALREADY_PROCESSED
        if "ConditionalCheckFailed" in reasons:
            return ReserveOutcome.OUT_OF_STOCK
        raise RuntimeError(f"reservation cancelled for an unexpected reason: {reasons}")

    def reject(self, order_id: str, reason: str) -> bool:
        """Mark a PENDING order REJECTED. Returns False if it was already processed."""
        try:
            self._client.update_item(
                TableName=self._orders,
                Key=_to_av({"pk": order_pk(order_id)}),
                UpdateExpression="SET #status = :rejected, reason = :reason, updated_at = :now",
                ConditionExpression="#status = :pending",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues=_to_av(
                    {
                        ":rejected": OrderStatus.REJECTED.value,
                        ":pending": OrderStatus.PENDING.value,
                        ":reason": reason,
                        ":now": int(time.time() * 1000),
                    }
                ),
            )
            return True
        except ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise


class DynamoProductRepository:
    def __init__(self, client, products_table: str) -> None:
        self._client = client
        self._table = products_table

    def get(self, sku: str) -> dict | None:
        item = self._client.get_item(
            TableName=self._table,
            Key=_to_av({"sku": sku}),
        ).get("Item")
        return _from_av(item) if item else None
