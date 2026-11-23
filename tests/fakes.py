"""In-memory stand-ins for DynamoDB, SQS and SNS with the same semantics the real code relies on."""

import copy
from dataclasses import dataclass, field

from orders.models import Order, OrderStatus, ReserveOutcome


@dataclass
class FakeOrderRepo:
    orders: dict[str, Order] = field(default_factory=dict)
    idempotency: dict[str, dict] = field(default_factory=dict)
    inventory: dict[tuple[str, str], int] = field(default_factory=dict)
    reserve_log: list[tuple[str, str]] = field(default_factory=list)  # (order_id, warehouse) per attempt

    def create_order(self, idempotency_key, request_hash, order, ttl_seconds):
        if idempotency_key in self.idempotency:
            return False, dict(self.idempotency[idempotency_key])
        self.idempotency[idempotency_key] = {"order_id": order.order_id, "request_hash": request_hash}
        self.orders[order.order_id] = copy.deepcopy(order)
        return True, None

    def get_order(self, order_id):
        order = self.orders.get(order_id)
        return copy.deepcopy(order) if order else None

    def reserve(self, order, warehouse_id):
        self.reserve_log.append((order.order_id, warehouse_id))
        stored = self.orders[order.order_id]
        if stored.status is not OrderStatus.PENDING:
            return ReserveOutcome.ALREADY_PROCESSED
        for line in order.lines:
            if self.inventory.get((line.sku, warehouse_id), 0) < line.quantity:
                return ReserveOutcome.OUT_OF_STOCK
        for line in order.lines:
            self.inventory[(line.sku, warehouse_id)] -= line.quantity
        stored.status = OrderStatus.CONFIRMED
        stored.warehouse_id = warehouse_id
        return ReserveOutcome.RESERVED

    def reject(self, order_id, reason):
        stored = self.orders[order_id]
        if stored.status is not OrderStatus.PENDING:
            return False
        stored.status = OrderStatus.REJECTED
        stored.reason = reason
        return True


@dataclass
class FakeQueue:
    sent: list[str] = field(default_factory=list)

    def send(self, order_id):
        self.sent.append(order_id)


@dataclass
class FakeNotifier:
    published: list[Order] = field(default_factory=list)
    failures_remaining: int = 0

    def publish(self, order):
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise ConnectionError("SNS unavailable")
        self.published.append(copy.deepcopy(order))


def sqs_record(message_id: str, order_id: str) -> dict:
    import json

    return {"messageId": message_id, "body": json.dumps({"order_id": order_id})}
