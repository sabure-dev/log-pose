import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from app.clients.inventory import GrpcInventoryClient
from app.domain import NewOrderItem
from app.exceptions import InventoryError
from app.pb import (
    AvailabilityStatus,
    CheckStockAvailabilityResponse,
    StockItemResult,
)


@pytest.mark.asyncio
async def test_grpc_inventory_client_success():
    mock_channel = MagicMock()
    mock_channel.close = AsyncMock()
    client = GrpcInventoryClient(channel=mock_channel)

    prod_id = uuid.uuid4()
    mock_stub = MagicMock()
    mock_resp = CheckStockAvailabilityResponse(
        eligible=True,
        overall_status=AvailabilityStatus.AVAILABILITY_STATUS_OK,
        reason="",
        items=[
            StockItemResult(
                product_id=str(prod_id),
                status=AvailabilityStatus.AVAILABILITY_STATUS_OK,
                catalog_price="1500.50",
                available_stock=25,
                reason="",
            )
        ],
    )
    mock_stub.CheckStockAvailability = AsyncMock(return_value=mock_resp)
    client._stub = mock_stub

    items = [
        NewOrderItem(
            product_id=prod_id,
            quantity=2,
            price=Decimal("1500.50"),
        )
    ]

    result = await client.check_stock_availability(items)
    assert result.eligible is True
    assert result.overall_status == "OK"
    assert len(result.items) == 1
    assert result.items[0].product_id == prod_id
    assert result.items[0].available_stock == 25
    assert result.items[0].status == "OK"


@pytest.mark.asyncio
async def test_grpc_inventory_client_rejections():
    mock_channel = MagicMock()
    client = GrpcInventoryClient(channel=mock_channel)

    prod_id = uuid.uuid4()
    mock_stub = MagicMock()
    mock_resp = CheckStockAvailabilityResponse(
        eligible=False,
        overall_status=AvailabilityStatus.AVAILABILITY_STATUS_PRICE_MISMATCH,
        reason="price mismatch",
        items=[
            StockItemResult(
                product_id=str(prod_id),
                status=AvailabilityStatus.AVAILABILITY_STATUS_PRICE_MISMATCH,
                catalog_price="2000.00",
                available_stock=10,
                reason="price mismatch",
            )
        ],
    )
    mock_stub.CheckStockAvailability = AsyncMock(return_value=mock_resp)
    client._stub = mock_stub

    items = [
        NewOrderItem(
            product_id=prod_id,
            quantity=1,
            price=Decimal("1500.00"),
        )
    ]

    result = await client.check_stock_availability(items)
    assert result.eligible is False
    assert result.overall_status == "PRICE_MISMATCH"
    assert result.reason == "price mismatch"


@pytest.mark.asyncio
async def test_grpc_inventory_client_rpc_error():
    mock_channel = MagicMock()
    client = GrpcInventoryClient(channel=mock_channel)

    mock_stub = MagicMock()
    rpc_error = grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=grpc.aio.Metadata(),
        trailing_metadata=grpc.aio.Metadata(),
        details="connection refused",
    )
    mock_stub.CheckStockAvailability = AsyncMock(side_effect=rpc_error)
    client._stub = mock_stub

    items = [
        NewOrderItem(
            product_id=uuid.uuid4(),
            quantity=1,
            price=Decimal("100.00"),
        )
    ]

    with pytest.raises(InventoryError) as exc_info:
        await client.check_stock_availability(items)

    assert "Unavailable" in str(exc_info.value) or "connection refused" in str(
        exc_info.value
    )
