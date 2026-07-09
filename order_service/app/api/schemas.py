import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, ConfigDict


class OrderItemCreate(BaseModel):
    product_id: uuid.UUID
    quantity: int = Field(default=1, ge=1)
    price: Decimal = Field(ge=0, decimal_places=2)


class OrderItemRead(BaseModel):
    id: uuid.UUID
    product_id: uuid.UUID
    quantity: int
    price: Decimal

    model_config = ConfigDict(from_attributes=True)


class OrderCreate(BaseModel):
    items: list[OrderItemCreate] = Field(min_length=1)


class OrderRead(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    status: str
    total_amount: Decimal
    created_at: datetime
    updated_at: datetime
    items: list[OrderItemRead]

    model_config = ConfigDict(from_attributes=True)
