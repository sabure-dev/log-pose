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


def test_invalid_order_item_zero_price():
    with pytest.raises(ValidationError):
        OrderItemCreate(product_id=uuid.uuid4(), quantity=1, price=Decimal("0.00"))


def test_invalid_order_item_zero_quantity():
    with pytest.raises(ValidationError):
        OrderItemCreate(product_id=uuid.uuid4(), quantity=0, price=Decimal("100.00"))


def test_invalid_order_item_excessive_quantity():
    with pytest.raises(ValidationError):
        OrderItemCreate(
            product_id=uuid.uuid4(), quantity=10001, price=Decimal("100.00")
        )


def test_empty_order_items_list():
    with pytest.raises(ValidationError):
        OrderCreate(items=[])


def test_duplicate_products_in_order_create():
    product_id = uuid.uuid4()
    with pytest.raises(ValidationError):
        OrderCreate(
            items=[
                OrderItemCreate(
                    product_id=product_id, quantity=1, price=Decimal("10.00")
                ),
                OrderItemCreate(
                    product_id=product_id, quantity=2, price=Decimal("10.00")
                ),
            ]
        )
