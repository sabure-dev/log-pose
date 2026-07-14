import uuid
from decimal import Decimal
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.schemas import OrderItemCreate
from app.interfaces import AbstractOrderRepository
from app.models import Order, OrderItem


class SqlAlchemyOrderRepository(AbstractOrderRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(
        self,
        user_id: uuid.UUID,
        total_amount: Decimal,
        items_data: list[OrderItemCreate],
    ) -> Order:
        new_order = Order(user_id=user_id, total_amount=total_amount)
        self.session.add(new_order)
        await self.session.flush()

        for item in items_data:
            new_item = OrderItem(
                order_id=new_order.id,
                product_id=item.product_id,
                quantity=item.quantity,
                price=item.price,
            )
            self.session.add(new_item)

        return new_order

    async def get(self, order_id: uuid.UUID) -> Order | None:
        query = (
            select(Order).where(Order.id == order_id).options(selectinload(Order.items))
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_all(self, skip: int = 0, limit: int = 100) -> Sequence[Order]:
        query = (
            select(Order).options(selectinload(Order.items)).offset(skip).limit(limit)
        )
        result = await self.session.execute(query)
        return result.scalars().all()
