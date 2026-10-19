import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from app.enums import OrderStatus


@dataclass(frozen=True, slots=True)
class NewOrderItem:
    product_id: uuid.UUID
    quantity: int
    price: Decimal


@dataclass(frozen=True, slots=True)
class OrderItemEntity:
    id: uuid.UUID
    product_id: uuid.UUID
    quantity: int
    price: Decimal


@dataclass(frozen=True, slots=True)
class OrderEntity:
    id: uuid.UUID
    user_id: uuid.UUID
    status: OrderStatus
    total_amount: Decimal
    shipping_address: str | None
    created_at: datetime
    updated_at: datetime
    items: list[OrderItemEntity]
    cancellation_reason: str | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class StockItemCheckResult:
    product_id: uuid.UUID
    status: str
    catalog_price: str | None = None
    available_stock: int = 0
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class StockAvailabilityResult:
    eligible: bool
    overall_status: str
    reason: str | None = None
    items: list[StockItemCheckResult] | None = None
