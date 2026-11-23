"""SQS worker: reserves stock for each order, earliest deadline first."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable

from orders.catalog import ProductCatalog
from orders.ds.deadline_heap import DeadlineHeap
from orders.metrics import emit
from orders.models import Order, OrderStatus, ReserveOutcome
from orders.topology import WarehouseRanker

log = logging.getLogger(__name__)


class OrderWorker:
    def __init__(
        self,
        repo,
        catalog: ProductCatalog,
        notifier,
        ranker: WarehouseRanker,
        clock: Callable[[], int] = lambda: int(time.time() * 1000),
    ) -> None:
        self._repo = repo
        self._catalog = catalog
        self._notifier = notifier
        self._ranker = ranker
        self._clock = clock

    def handle_batch(self, records: list[dict]) -> dict:
        """Process an SQS batch. Returns the partial-failure response.

        Failed messages are reported back so SQS retries only those. After
        maxReceiveCount attempts SQS moves them to the dead-letter queue.
        """
        failures: list[dict] = []
        deadlines: DeadlineHeap[tuple[str, Order]] = DeadlineHeap()

        for record in records:
            message_id = record["messageId"]
            try:
                order_id = json.loads(record["body"])["order_id"]
                order = self._repo.get_order(order_id)
                if order is None:
                    raise LookupError(f"order {order_id} does not exist")
            except Exception:
                log.exception("could not load order for message %s", message_id)
                failures.append({"itemIdentifier": message_id})
                continue
            deadlines.push(order.ship_by, (message_id, order))

        # Urgent orders first. This ordering only applies within a batch: SQS
        # standard queues give no global ordering.
        while deadlines:
            message_id, order = deadlines.pop()
            try:
                self.process(order)
            except Exception:
                log.exception("failed to process order %s", order.order_id)
                failures.append({"itemIdentifier": message_id})

        return {"batchItemFailures": failures}

    def process(self, order: Order) -> None:
        final = self._decide(order)
        # Notify after the DB commit. If this publish fails, the message is
        # retried, the reservation reports ALREADY_PROCESSED, and the
        # notification is sent again. Consumers dedupe on order_id + status.
        self._notifier.publish(final)

    def _decide(self, order: Order) -> Order:
        if any(self._catalog.get(line.sku) is None for line in order.lines):
            return self._reject(order, "UNKNOWN_SKU")

        for warehouse in self._ranker.warehouses_by_distance(order.region):
            outcome = self._repo.reserve(order, warehouse)
            if outcome is ReserveOutcome.OUT_OF_STOCK:
                continue
            if outcome is ReserveOutcome.ALREADY_PROCESSED:
                return self._current(order.order_id)
            order.status = OrderStatus.CONFIRMED
            order.warehouse_id = warehouse
            emit("OrdersConfirmed", 1)
            self._record_latency(order)
            return order

        return self._reject(order, "OUT_OF_STOCK")

    def _reject(self, order: Order, reason: str) -> Order:
        if not self._repo.reject(order.order_id, reason):
            return self._current(order.order_id)
        order.status = OrderStatus.REJECTED
        order.reason = reason
        emit("OrdersRejected", 1)
        self._record_latency(order)
        return order

    def _current(self, order_id: str) -> Order:
        current = self._repo.get_order(order_id)
        if current is None:
            raise LookupError(f"order {order_id} disappeared")
        return current

    def _record_latency(self, order: Order) -> None:
        emit("EndToEndLatency", self._clock() - order.created_at, unit="Milliseconds")
