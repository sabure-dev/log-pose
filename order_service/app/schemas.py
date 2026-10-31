import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from app.enums import OrderStatus


class OrderItemCreate(BaseModel):
    product_id: uuid.UUID
    quantity: Annotated[int, Field(ge=1, le=10000)]
    price: Annotated[
        Decimal,
        Field(
            gt=0,
            max_digits=10,
            decimal_places=2,
            validation_alias=AliasChoices("price", "expected_price"),
        ),
    ]


class OrderCreate(BaseModel):
    items: Annotated[list[OrderItemCreate], Field(min_length=1, max_length=100)]
    shipping_address: Annotated[str | None, Field(max_length=500)] = None

    @field_validator("items")
    @classmethod
    def validate_items(cls, items: list[OrderItemCreate]) -> list[OrderItemCreate]:
        product_ids = [item.product_id for item in items]
        if len(product_ids) != len(set(product_ids)):
            raise ValueError("Duplicate product_id found in items")
        total = sum((item.price * item.quantity for item in items), Decimal("0.00"))
        if total >= Decimal("100000000.00"):
            raise ValueError("Total order amount exceeds limit")
        return items


class OrderItemRead(BaseModel):
    id: uuid.UUID
    product_id: uuid.UUID
    quantity: int
    price: Decimal

    model_config = ConfigDict(from_attributes=True)


class OrderRead(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    status: OrderStatus
    total_amount: Decimal
    shipping_address: str | None = None
    cancellation_reason: str | None = None
    created_at: datetime
    updated_at: datetime
    items: list[OrderItemRead]

    model_config = ConfigDict(from_attributes=True)


class InventoryResultPayload(BaseModel):
    event_id: uuid.UUID | None = None
    correlation_id: str | None = None
    traceparent: str | None = None
    event_type: str | None = None
    order_id: uuid.UUID
    status: Literal["RESERVED", "FAILED"]
    reason: str | None = None
