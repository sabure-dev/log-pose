import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence

from app.enums import OrderStatus
from app.exceptions import InvalidTransition, OrderNotFound
from app.interfaces import AbstractUnitOfWork
from app.logging import correlation_id_ctx
from app.models import Order
from app.schemas import OrderCreate, OrderRead

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
        self, user_id: uuid.UUID, order_in: OrderCreate
    ) -> OrderRead:
        total_amount = sum(
            (item.price * item.quantity for item in order_in.items), Decimal("0.00")
        )

        async with self.uow:
            draft_order = await self.uow.orders.add(
                user_id, total_amount, order_in.items, order_in.shipping_address
            )

            event_id = uuid.uuid4()
            correlation_id = correlation_id_ctx.get() or str(uuid.uuid4())
            event_payload = {
                "event_id": str(event_id),
                "correlation_id": correlation_id,
                "event_type": "OrderCreated",
                "order_id": str(draft_order.id),
                "user_id": str(user_id),
                "total_amount": str(total_amount.quantize(Decimal("0.01"))),
                "shipping_address": order_in.shipping_address,
                "items": [
                    {
                        "product_id": str(item.product_id),
                        "quantity": item.quantity,
                        "price": str(item.price.quantize(Decimal("0.01"))),
                    }
                    for item in order_in.items
                ],
                "occurred_at": datetime.now(timezone.utc).isoformat(),
            }

            await self.uow.outbox.add(
                topic="order.created", payload=event_payload, event_id=event_id
            )

            await self.uow.commit()
            final_order = await self.uow.orders.get(draft_order.id)
            return OrderRead.model_validate(final_order)

    async def get_order(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> OrderRead | None:
        async with self.uow:
            order = await self.uow.orders.get_by_id_and_user(order_id, user_id)
            if not order:
                return None
            return OrderRead.model_validate(order)

    async def get_orders(
        self, user_id: uuid.UUID, skip: int = 0, limit: int = 100
    ) -> Sequence[OrderRead]:
        async with self.uow:
            orders = await self.uow.orders.get_all_by_user(user_id, skip, limit)
            return [OrderRead.model_validate(order) for order in orders]

    async def _emit_cancelled(self, order: Order) -> None:
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

    async def cancel_order(self, order_id: uuid.UUID, user_id: uuid.UUID) -> OrderRead:
        async with self.uow:
            order = await self.uow.orders.get_for_update(order_id)
            if not order or order.user_id != user_id:
                raise OrderNotFound(f"Order {order_id} not found")

            current_status = OrderStatus(order.status)
            if current_status == OrderStatus.CANCELLED:
                return OrderRead.model_validate(order)

            if OrderStatus.CANCELLED not in VALID_TRANSITIONS.get(
                current_status, set()
            ):
                raise InvalidTransition(
                    f"Cannot cancel order with status {current_status.value}"
                )

            was_reserved = current_status == OrderStatus.RESERVED
            order.status = OrderStatus.CANCELLED.value

            if was_reserved:
                await self._emit_cancelled(order)

            await self.uow.commit()
            updated_order = await self.uow.orders.get(order_id)
            return OrderRead.model_validate(updated_order)

    async def update_order_status(
        self, order_id: uuid.UUID, status: OrderStatus
    ) -> OrderRead | None:
        async with self.uow:
            order = await self.uow.orders.get_for_update(order_id)
            if not order:
                return None

            current_status = OrderStatus(order.status)
            if current_status == status:
                return OrderRead.model_validate(order)

            if status not in VALID_TRANSITIONS.get(current_status, set()):
                raise InvalidTransition(
                    f"Invalid status transition from "
                    f"{current_status.value} to {status.value}"
                )

            was_reserved = current_status == OrderStatus.RESERVED
            order.status = status.value

            if status == OrderStatus.CANCELLED and was_reserved:
                await self._emit_cancelled(order)

            await self.uow.commit()
            updated_order = await self.uow.orders.get(order_id)
            return OrderRead.model_validate(updated_order)
