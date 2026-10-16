import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence

from app.enums import OrderStatus
from app.interfaces import AbstractUnitOfWork
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
            event_payload = {
                "event_id": str(event_id),
                "event_type": "OrderCreated",
                "order_id": str(draft_order.id),
                "user_id": str(user_id),
                "total_amount": str(total_amount),
                "shipping_address": order_in.shipping_address,
                "items": [
                    {
                        "product_id": str(item.product_id),
                        "quantity": item.quantity,
                        "price": str(item.price),
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

    async def update_order_status(
        self, order_id: uuid.UUID, status: OrderStatus
    ) -> OrderRead | None:
        async with self.uow:
            order = await self.uow.orders.get(order_id)
            if not order:
                return None

            current_status = OrderStatus(order.status)
            if current_status == status:
                return OrderRead.model_validate(order)

            if status not in VALID_TRANSITIONS.get(current_status, set()):
                raise ValueError(
                    f"Invalid status transition from {current_status.value} "
                    f"to {status.value}"
                )

            order.status = status.value

            if status == OrderStatus.CANCELLED:
                cancel_event_id = uuid.uuid4()
                cancel_payload = {
                    "event_id": str(cancel_event_id),
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

            await self.uow.commit()
            updated_order = await self.uow.orders.get(order_id)
            return OrderRead.model_validate(updated_order)
