import uuid
from typing import Sequence

import grpc
import grpc.aio

from app.domain import NewOrderItem, StockAvailabilityResult, StockItemCheckResult
from app.exceptions import InventoryError
from app.interfaces import AbstractInventoryClient
from app.logging import correlation_id_ctx
from app.pb import (
    AvailabilityStatus,
    CheckStockAvailabilityRequest,
    InventoryServiceStub,
    StockItemRequest,
)
from app.telemetry import get_current_traceparent


class GrpcInventoryClient(AbstractInventoryClient):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        channel: grpc.aio.Channel | None = None,
        timeout: float = 5.0,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._custom_channel = channel
        self._channel: grpc.aio.Channel | None = channel
        self._stub: InventoryServiceStub | None = None

    def _get_stub(self) -> InventoryServiceStub:
        if self._stub is None:
            if self._channel is None:
                target = f"{self.host}:{self.port}"
                self._channel = grpc.aio.insecure_channel(target)
            self._stub = InventoryServiceStub(self._channel)
        return self._stub

    async def check_stock_availability(
        self, items: Sequence[NewOrderItem]
    ) -> StockAvailabilityResult:
        stub = self._get_stub()
        req_items = [
            StockItemRequest(
                product_id=str(item.product_id),
                quantity=item.quantity,
                expected_price=f"{item.price:.2f}",
            )
            for item in items
        ]
        request = CheckStockAvailabilityRequest(items=req_items)
        metadata = []
        correlation_id = correlation_id_ctx.get()
        if correlation_id:
            metadata.append(("x-correlation-id", correlation_id))
        traceparent = get_current_traceparent()
        if traceparent:
            metadata.append(("traceparent", traceparent))

        try:
            resp = await stub.CheckStockAvailability(
                request,
                metadata=metadata if metadata else None,
                timeout=self.timeout,
            )
        except grpc.aio.AioRpcError as exc:
            raise InventoryError(
                f"Inventory gRPC service error ({exc.code()}): {exc.details()}"
            ) from exc

        status_name = AvailabilityStatus.Name(resp.overall_status).removeprefix(
            "AVAILABILITY_STATUS_"
        )

        item_results = []
        for it in resp.items:
            try:
                prod_uuid = uuid.UUID(it.product_id)
            except (ValueError, TypeError, AttributeError):
                prod_uuid = uuid.UUID(int=0)

            item_results.append(
                StockItemCheckResult(
                    product_id=prod_uuid,
                    status=AvailabilityStatus.Name(it.status).removeprefix(
                        "AVAILABILITY_STATUS_"
                    ),
                    catalog_price=it.catalog_price if it.catalog_price else None,
                    available_stock=it.available_stock,
                    reason=it.reason if it.reason else None,
                )
            )

        return StockAvailabilityResult(
            eligible=resp.eligible,
            overall_status=status_name,
            reason=resp.reason if resp.reason else None,
            items=item_results,
        )

    async def close(self) -> None:
        if self._channel and self._custom_channel is None:
            await self._channel.close()
