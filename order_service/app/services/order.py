import uuid
from typing import Sequence

from app.api.schemas import OrderCreate
from app.enums import OrderStatus
from app.interfaces import AbstractUnitOfWork
from app.models import Order


class OrderService:
    def __init__(self, uow: AbstractUnitOfWork):
        self.uow = uow

    async def create_new_order(self, user_id: uuid.UUID, order_in: OrderCreate):
        total_amount = sum(item.price * item.quantity for item in order_in.items)

        async with self.uow:
            draft_order = await self.uow.orders.add(
                user_id, total_amount, order_in.items
            )
            await self.uow.commit()
            final_order = await self.uow.orders.get(draft_order.id)

        return final_order

    async def get_order(self, order_id: uuid.UUID) -> Order | None:
        async with self.uow:
            return await self.uow.orders.get(order_id)

    async def get_orders(self, skip: int = 0, limit: int = 100) -> Sequence[Order]:
        async with self.uow:
            return await self.uow.orders.get_all(skip, limit)

    async def update_order_status(
        self, order_id: uuid.UUID, status: OrderStatus
    ) -> Order | None:
        async with self.uow:
            order = await self.uow.orders.get(order_id)
            if not order:
                return None

            order.status = status.value
            await self.uow.commit()
            updated_order = await self.uow.orders.get(order_id)
            return updated_order
