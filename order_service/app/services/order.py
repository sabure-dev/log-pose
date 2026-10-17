import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Sequence

from sqlalchemy.exc import IntegrityError

from app.domain import NewOrderItem, OrderEntity
from app.enums import OrderStatus
from app.exceptions import InvalidTransition, OrderNotFound
from app.interfaces import AbstractUnitOfWork
from app.logging import correlation_id_ctx

VALID_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.PENDING: {OrderStatus.RESERVED, OrderStatus.CANCELLED},
    OrderStatus.RESERVED: {OrderStatus.READY_FOR_SHIPPING, OrderStatus.CANCELLED},
    OrderStatus.READY_FOR_SHIPPING: {OrderStatus.COMPLETED, OrderStatus.CANCELLED},
    OrderStatus.COMPLETED: set(),
    OrderStatus.CANCELLED: set(),
}


class OrderService:
    def __init__(self, uow: AbstractUnitOfWork):
        self.uow = uow

    async def create_new_order(
        self,
        user_id: uuid.UUID,
        items: Sequence[NewOrderItem],
        shipping_address: str | None = None,
        idempotency_key: str | None = None,
    ) -> OrderEntity:
        total_amount = sum(
            (item.price * item.quantity for item in items), Decimal("0.00")
        )

        async with self.uow:
            if idempotency_key:
                existing = await self.uow.orders.get_by_user_and_idempotency_key(
                    user_id, idempotency_key
                )
                if existing:
                    return existing

            try:
                created_order = await self.uow.orders.add(
                    user_id,
                    total_amount,
                    items,
                    shipping_address,
                    idempotency_key=idempotency_key,
                )
            except IntegrityError:
                await self.uow.rollback()
                if idempotency_key:
                    existing = await self.uow.orders.get_by_user_and_idempotency_key(
                        user_id, idempotency_key
                    )
                    if existing:
                        return existing
                raise

            event_id = uuid.uuid4()
            correlation_id = correlation_id_ctx.get() or str(uuid.uuid4())
            event_payload = {
                "event_id": str(event_id),
                "correlation_id": correlation_id,
                "event_type": "OrderCreated",
                "order_id": str(created_order.id),
                "user_id": str(user_id),
                "total_amount": f"{total_amount:.2f}",
                "shipping_address": shipping_address,
                "items": [
                    {
                        "product_id": str(item.product_id),
                        "quantity": item.quantity,
                        "price": f"{item.price:.2f}",
                    }
                    for item in items
                ],
                "occurred_at": datetime.now(timezone.utc).isoformat(),
            }

            await self.uow.outbox.add(
                topic="order.created", payload=event_payload, event_id=event_id
            )

            await self.uow.commit()
            return created_order

    async def get_order(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> OrderEntity | None:
        async with self.uow:
            return await self.uow.orders.get_by_id_and_user(order_id, user_id)

    async def get_orders(
        self, user_id: uuid.UUID, skip: int = 0, limit: int = 100
    ) -> Sequence[OrderEntity]:
        async with self.uow:
            return await self.uow.orders.get_all_by_user(user_id, skip, limit)

    async def cancel_order(
        self,
        order_id: uuid.UUID,
        user_id: uuid.UUID,
        reason: str = "cancelled by user",
    ) -> OrderEntity:
        async with self.uow:
            order = await self.uow.orders.get_for_update(order_id)
            if not order or order.user_id != user_id:
                raise OrderNotFound(f"Order {order_id} not found")

            current_status = order.status
            if current_status == OrderStatus.CANCELLED:
                return order

            if OrderStatus.CANCELLED not in VALID_TRANSITIONS.get(
                current_status, set()
            ):
                raise InvalidTransition(
                    f"Cannot cancel order with status {current_status.value}"
                )

            updated_order = await self.uow.orders.update_status(
                order_id, OrderStatus.CANCELLED, cancellation_reason=reason
            )
            await self._emit_cancelled(order)

            await self.uow.commit()
            return updated_order or order

    async def ship_order(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> OrderEntity:
        async with self.uow:
            order = await self.uow.orders.get_for_update(order_id)
            if not order or order.user_id != user_id:
                raise OrderNotFound(f"Order {order_id} not found")

            current_status = order.status
            if current_status == OrderStatus.READY_FOR_SHIPPING:
                return order

            if OrderStatus.READY_FOR_SHIPPING not in VALID_TRANSITIONS.get(
                current_status, set()
            ):
                raise InvalidTransition(
                    f"Invalid status transition from "
                    f"{current_status.value} to {OrderStatus.READY_FOR_SHIPPING.value}"
                )

            updated_order = await self.uow.orders.update_status(
                order_id, OrderStatus.READY_FOR_SHIPPING
            )
            await self.uow.commit()
            return updated_order or order

    async def complete_order(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> OrderEntity:
        async with self.uow:
            order = await self.uow.orders.get_for_update(order_id)
            if not order or order.user_id != user_id:
                raise OrderNotFound(f"Order {order_id} not found")

            current_status = order.status
            if current_status == OrderStatus.COMPLETED:
                return order

            if OrderStatus.COMPLETED not in VALID_TRANSITIONS.get(
                current_status, set()
            ):
                raise InvalidTransition(
                    f"Invalid status transition from "
                    f"{current_status.value} to {OrderStatus.COMPLETED.value}"
                )

            updated_order = await self.uow.orders.update_status(
                order_id, OrderStatus.COMPLETED
            )
            await self._emit_completed(order)
            await self.uow.commit()
            return updated_order or order

    async def update_order_status(
        self,
        order_id: uuid.UUID,
        status: OrderStatus,
        reason: str | None = None,
    ) -> OrderEntity | None:
        async with self.uow:
            order = await self.uow.orders.get_for_update(order_id)
            if not order:
                return None

            current_status = order.status
            if current_status == status:
                return order

            if status not in VALID_TRANSITIONS.get(current_status, set()):
                raise InvalidTransition(
                    f"Invalid status transition from "
                    f"{current_status.value} to {status.value}"
                )

            cancellation_reason = reason if status == OrderStatus.CANCELLED else None
            updated_order = await self.uow.orders.update_status(
                order_id, status, cancellation_reason=cancellation_reason
            )

            if status == OrderStatus.CANCELLED and current_status in (
                OrderStatus.RESERVED,
                OrderStatus.READY_FOR_SHIPPING,
            ):
                await self._emit_cancelled(order)
            elif status == OrderStatus.COMPLETED:
                await self._emit_completed(order)

            await self.uow.commit()
            return updated_order

    async def expire_stale_pending_orders(
        self, timeout_seconds: int = 900, batch_size: int = 50
    ) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout_seconds)
        async with self.uow:
            stale_orders = await self.uow.orders.get_stale_pending_orders(
                older_than=cutoff, limit=batch_size
            )
            if not stale_orders:
                return 0

            for order in stale_orders:
                await self.uow.orders.update_status(
                    order.id,
                    OrderStatus.CANCELLED,
                    cancellation_reason="reservation timeout",
                )
                await self._emit_cancelled(order)

            await self.uow.commit()
            return len(stale_orders)

    async def _emit_cancelled(self, order: OrderEntity) -> None:
        cancel_event_id = uuid.uuid4()
        correlation_id = correlation_id_ctx.get() or str(uuid.uuid4())
        cancel_payload = {
            "event_id": str(cancel_event_id),
            "correlation_id": correlation_id,
            "event_type": "OrderCancelled",
            "order_id": str(order.id),
            "user_id": str(order.user_id),
            "items": [
                {
                    "product_id": str(item.product_id),
                    "quantity": item.quantity,
                }
                for item in order.items
            ],
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        }
        await self.uow.outbox.add(
            topic="order.cancelled",
            payload=cancel_payload,
            event_id=cancel_event_id,
        )

    async def _emit_completed(self, order: OrderEntity) -> None:
        complete_event_id = uuid.uuid4()
        correlation_id = correlation_id_ctx.get() or str(uuid.uuid4())
        complete_payload = {
            "event_id": str(complete_event_id),
            "correlation_id": correlation_id,
            "event_type": "OrderCompleted",
            "order_id": str(order.id),
            "user_id": str(order.user_id),
            "items": [
                {
                    "product_id": str(item.product_id),
                    "quantity": item.quantity,
                }
                for item in order.items
            ],
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        }
        await self.uow.outbox.add(
            topic="order.completed",
            payload=complete_payload,
            event_id=complete_event_id,
        )
