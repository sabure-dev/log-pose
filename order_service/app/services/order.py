import uuid

from app.api.schemas import OrderCreate
from app.interfaces import AbstractUnitOfWork


class OrderService:
    def __init__(self, uow: AbstractUnitOfWork):
        self.uow = uow

    async def create_new_order(self, user_id: uuid.UUID, order_in: OrderCreate):
        total_amount = sum(item.price * item.quantity for item in order_in.items)

        async with self.uow:
            draft_order = await self.uow.orders.add(user_id, total_amount, order_in.items)
            await self.uow.commit()
            final_order = await self.uow.orders.get(draft_order.id)

        return final_order
