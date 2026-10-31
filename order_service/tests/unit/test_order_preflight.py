import uuid
from decimal import Decimal
from typing import Sequence

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.deps import get_inventory_client
from app.database import get_db
from app.domain import (
    NewOrderItem,
    StockAvailabilityResult,
    StockItemCheckResult,
)
from app.exceptions import (
    InsufficientStock,
    InvalidQuantity,
    InventoryError,
    PriceMismatch,
    ProductNotFound,
)
from app.interfaces import AbstractInventoryClient
from app.main import app
from app.services.order import OrderService
from tests.unit.test_order_service import FakeUnitOfWork


class MockStockClient(AbstractInventoryClient):
    def __init__(
        self,
        eligible: bool = True,
        status: str = "OK",
        reason: str | None = None,
        exception: Exception | None = None,
    ):
        self.eligible = eligible
        self.status = status
        self.reason = reason
        self.exception = exception
        self.called = False

    async def check_stock_availability(
        self, items: Sequence[NewOrderItem]
    ) -> StockAvailabilityResult:
        self.called = True
        if self.exception:
            raise self.exception
        return StockAvailabilityResult(
            eligible=self.eligible,
            overall_status=self.status,
            reason=self.reason,
            items=[
                StockItemCheckResult(
                    product_id=it.product_id,
                    status=self.status,
                    catalog_price=f"{it.price:.2f}",
                    available_stock=100 if self.eligible else 0,
                    reason=self.reason,
                )
                for it in items
            ],
        )


@pytest.mark.asyncio
async def test_order_service_preflight_success():
    uow = FakeUnitOfWork()
    client = MockStockClient(eligible=True, status="OK")
    service = OrderService(uow=uow, inventory_client=client)

    user_id = uuid.uuid4()
    items = [
        NewOrderItem(
            product_id=uuid.uuid4(),
            quantity=2,
            price=Decimal("500.00"),
        )
    ]

    order = await service.create_new_order(user_id=user_id, items=items)
    assert client.called is True
    assert order is not None
    assert order.total_amount == Decimal("1000.00")
    assert len(uow.outbox.events) == 1
    assert uow.outbox.events[0]["topic"] == "order.created"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status_code,reason,expected_exc",
    [
        (
            "PRODUCT_NOT_FOUND",
            "product not found in catalog",
            ProductNotFound,
        ),
        (
            "PRICE_MISMATCH",
            "price mismatch",
            PriceMismatch,
        ),
        (
            "INSUFFICIENT_STOCK",
            "insufficient stock",
            InsufficientStock,
        ),
        (
            "INVALID_QUANTITY",
            "invalid quantity",
            InvalidQuantity,
        ),
        (
            "UNKNOWN_ERROR",
            "inventory service internal failure",
            InventoryError,
        ),
    ],
)
async def test_order_service_preflight_rejections(status_code, reason, expected_exc):
    uow = FakeUnitOfWork()
    client = MockStockClient(eligible=False, status=status_code, reason=reason)
    service = OrderService(uow=uow, inventory_client=client)

    user_id = uuid.uuid4()
    items = [
        NewOrderItem(
            product_id=uuid.uuid4(),
            quantity=1,
            price=Decimal("100.00"),
        )
    ]

    with pytest.raises(expected_exc):
        await service.create_new_order(user_id=user_id, items=items)

    assert client.called is True
    assert len(uow.orders.orders) == 0
    assert len(uow.outbox.events) == 0


@pytest.mark.asyncio
async def test_order_service_preflight_idempotent_replay_does_not_call_inventory():
    uow = FakeUnitOfWork()
    client = MockStockClient(eligible=True, status="OK")
    service = OrderService(uow=uow, inventory_client=client)

    user_id = uuid.uuid4()
    prod_id = uuid.uuid4()
    items = [
        NewOrderItem(
            product_id=prod_id,
            quantity=1,
            price=Decimal("100.00"),
        )
    ]

    order1 = await service.create_new_order(
        user_id=user_id, items=items, idempotency_key="key-123"
    )
    assert client.called is True

    client.called = False
    client.eligible = False
    client.status = "INSUFFICIENT_STOCK"

    order2 = await service.create_new_order(
        user_id=user_id, items=items, idempotency_key="key-123"
    )
    assert client.called is False
    assert order1.id == order2.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status_code,reason,expected_http_status",
    [
        ("PRODUCT_NOT_FOUND", "product not found in catalog", 400),
        ("PRICE_MISMATCH", "price does not match catalog", 409),
        ("INSUFFICIENT_STOCK", "insufficient stock", 409),
        ("INVALID_QUANTITY", "invalid quantity", 400),
        ("UNSPECIFIED", "service unavailable", 422),
    ],
)
async def test_api_preflight_error_responses(
    status_code, reason, expected_http_status, db_session
):
    mock_client = MockStockClient(eligible=False, status=status_code, reason=reason)

    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_inventory_client] = lambda: mock_client

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as ac:
        resp = await ac.post(
            "/orders/",
            json={
                "items": [
                    {
                        "product_id": str(uuid.uuid4()),
                        "quantity": 1,
                        "price": "100.00",
                    }
                ]
            },
            headers={"X-User-Id": str(uuid.uuid4())},
        )
        assert resp.status_code == expected_http_status
        assert reason in resp.json()["detail"]

    app.dependency_overrides.clear()
