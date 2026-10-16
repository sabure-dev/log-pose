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
