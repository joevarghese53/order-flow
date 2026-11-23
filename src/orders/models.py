from __future__ import annotations

import enum
from dataclasses import dataclass


class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"


class ReserveOutcome(str, enum.Enum):
    RESERVED = "RESERVED"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    ALREADY_PROCESSED = "ALREADY_PROCESSED"


@dataclass(frozen=True)
class OrderLine:
    sku: str
    quantity: int


@dataclass
class Order:
    order_id: str
    customer_id: str
    region: str
    lines: list[OrderLine]
    ship_by: int  # epoch seconds: the fulfillment deadline
    created_at: int  # epoch milliseconds
    status: OrderStatus = OrderStatus.PENDING
    warehouse_id: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict:
        data = {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "region": self.region,
            "lines": [{"sku": line.sku, "quantity": line.quantity} for line in self.lines],
            "ship_by": self.ship_by,
            "created_at": self.created_at,
            "status": self.status.value,
            "warehouse_id": self.warehouse_id,
            "reason": self.reason,
        }
        return {key: value for key, value in data.items() if value is not None}

    @classmethod
    def from_dict(cls, data: dict) -> "Order":
        return cls(
            order_id=data["order_id"],
            customer_id=data["customer_id"],
            region=data["region"],
            lines=[OrderLine(sku=line["sku"], quantity=int(line["quantity"])) for line in data["lines"]],
            ship_by=int(data["ship_by"]),
            created_at=int(data["created_at"]),
            status=OrderStatus(data["status"]),
            warehouse_id=data.get("warehouse_id"),
            reason=data.get("reason"),
        )
