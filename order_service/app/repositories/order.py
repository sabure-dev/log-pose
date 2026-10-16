import uuid
from decimal import Decimal
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.interfaces import AbstractOrderRepository
from app.models import Order, OrderItem
from app.schemas import OrderItemCreate


class SqlAlchemyOrderRepository(AbstractOrderRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(
        self,
        user_id: uuid.UUID,
        total_amount: Decimal,
        items_data: list[OrderItemCreate],
        shipping_address: str | None = None,
    ) -> Order:
        new_order = Order(
            user_id=user_id,
            total_amount=total_amount,
            shipping_address=shipping_address,
            items=[
                OrderItem(
                    product_id=item.product_id,
                    quantity=item.quantity,
                    price=item.price,
                )
                for item in items_data
            ],
        )
        self.session.add(new_order)
        await self.session.flush()
        return new_order

    async def get(self, order_id: uuid.UUID) -> Order | None:
        query = (
            select(Order).where(Order.id == order_id).options(selectinload(Order.items))
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_by_id_and_user(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> Order | None:
        query = (
            select(Order)
            .where(Order.id == order_id, Order.user_id == user_id)
            .options(selectinload(Order.items))
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_all(self, skip: int = 0, limit: int = 100) -> Sequence[Order]:
        query = (
            select(Order)
            .options(selectinload(Order.items))
            .order_by(Order.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_all_by_user(
        self, user_id: uuid.UUID, skip: int = 0, limit: int = 100
    ) -> Sequence[Order]:
        query = (
            select(Order)
            .where(Order.user_id == user_id)
            .options(selectinload(Order.items))
            .order_by(Order.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await self.session.execute(query)
        return result.scalars().all()
