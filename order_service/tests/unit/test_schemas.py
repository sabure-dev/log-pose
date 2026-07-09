import uuid
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.api.schemas import OrderCreate, OrderItemCreate


def test_valid_order_item_creation():
    item = OrderItemCreate(product_id=uuid.uuid4(), quantity=2, price=Decimal("150.00"))
    assert item.quantity == 2
    assert item.price == Decimal("150.00")


def test_invalid_order_item_negative_price():
    with pytest.raises(ValidationError):
        OrderItemCreate(product_id=uuid.uuid4(), quantity=1, price=Decimal("-50.00"))


def test_invalid_order_item_zero_quantity():
    with pytest.raises(ValidationError):
        OrderItemCreate(product_id=uuid.uuid4(), quantity=0, price=Decimal("100.00"))


def test_empty_order_items_list():
    with pytest.raises(ValidationError):
        OrderCreate(items=[])
