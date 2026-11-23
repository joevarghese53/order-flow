"""Order intake: validate, persist once per Idempotency-Key, then enqueue."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from orders import config
from orders.catalog import ProductCatalog
from orders.messaging import OrderQueue
from orders.metrics import emit
from orders.models import Order, OrderLine, OrderStatus
from orders.repository import DynamoOrderRepository
from orders.topology import REGIONS

MAX_LINES = 20
MAX_QUANTITY = 100
MAX_IDEMPOTENCY_KEY_LENGTH = 128


class ValidationError(ValueError):
    """The request is invalid. The API returns 400 with the message."""


@dataclass(frozen=True)
class PlacementResult:
    status_code: int
    body: dict


def request_hash(body: dict) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _text(body: dict, field: str, max_length: int) -> str:
    value = body.get(field)
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > max_length:
        raise ValidationError(f"'{field}' must be a non-empty string of at most {max_length} characters")
    return value.strip()


def _integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_order(body: object, catalog: ProductCatalog, order_id: str, now_ms: int) -> Order:
    if not isinstance(body, dict):
        raise ValidationError("body must be a JSON object")

    customer_id = _text(body, "customer_id", 64)
    region = _text(body, "region", 32).upper()
    if region not in REGIONS:
        raise ValidationError(f"unknown region '{region}'")

    raw_lines = body.get("lines")
    if not isinstance(raw_lines, list) or not 1 <= len(raw_lines) <= MAX_LINES:
        raise ValidationError(f"'lines' must be a list of 1 to {MAX_LINES} items")

    lines: list[OrderLine] = []
    seen_skus: set[str] = set()
    for raw in raw_lines:
        if not isinstance(raw, dict):
            raise ValidationError("each line must be an object with 'sku' and 'quantity'")
        sku = _text(raw, "sku", 64)
        quantity = raw.get("quantity")
        if not _integer(quantity) or not 1 <= quantity <= MAX_QUANTITY:
            raise ValidationError(f"quantity for {sku} must be an integer from 1 to {MAX_QUANTITY}")
        if sku in seen_skus:
            raise ValidationError(f"duplicate sku '{sku}'; combine the quantities")
        if catalog.get(sku) is None:
            raise ValidationError(f"unknown sku '{sku}'")
        seen_skus.add(sku)
        lines.append(OrderLine(sku=sku, quantity=quantity))

    now_s = now_ms // 1000
    ship_by = body.get("ship_by", now_s + config.DEFAULT_SHIP_WINDOW_SECONDS)
    if not _integer(ship_by) or ship_by <= now_s:
        raise ValidationError("'ship_by' must be a future epoch timestamp in seconds")

    return Order(
        order_id=order_id,
        customer_id=customer_id,
        region=region,
        lines=lines,
        ship_by=ship_by,
        created_at=now_ms,
    )


class OrderPlacer:
    def __init__(
        self,
        repo: DynamoOrderRepository,
        catalog: ProductCatalog,
        queue: OrderQueue,
        clock: Callable[[], int] = lambda: int(time.time() * 1000),
        new_id: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        self._repo = repo
        self._catalog = catalog
        self._queue = queue
        self._clock = clock
        self._new_id = new_id

    def place(self, body: object, idempotency_key: str) -> PlacementResult:
        if not idempotency_key or len(idempotency_key) > MAX_IDEMPOTENCY_KEY_LENGTH:
            raise ValidationError(f"Idempotency-Key header is required (1 to {MAX_IDEMPOTENCY_KEY_LENGTH} chars)")

        order = parse_order(body, self._catalog, self._new_id(), self._clock())
        digest = request_hash(body)

        created, existing = self._repo.create_order(
            idempotency_key, digest, order, config.IDEMPOTENCY_TTL_SECONDS
        )
        if created:
            emit("OrdersReceived", 1)
            self._queue.send(order.order_id)
            return PlacementResult(202, {"order_id": order.order_id, "status": OrderStatus.PENDING.value})

        if existing["request_hash"] != digest:
            return PlacementResult(422, {"error": "Idempotency-Key was already used with a different request"})

        emit("DuplicateOrderRequests", 1)
        current = self._repo.get_order(existing["order_id"])
        if current.status is OrderStatus.PENDING:
            # The first call may have failed after the DB write but before the
            # enqueue. Re-sending is safe: the worker's conditional update makes
            # duplicate messages harmless.
            self._queue.send(current.order_id)
            return PlacementResult(202, {"order_id": current.order_id, "status": current.status.value})
        return PlacementResult(200, {"order_id": current.order_id, "status": current.status.value})
