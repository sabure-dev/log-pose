import uuid
from datetime import datetime
from decimal import Decimal
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.domain import NewOrderItem, OrderEntity, OrderItemEntity
from app.enums import OrderStatus
from app.interfaces import AbstractOrderRepository
from app.models import Order, OrderItem


def _to_entity(order: Order) -> OrderEntity:
    return OrderEntity(
        id=order.id,
        user_id=order.user_id,
        status=OrderStatus(order.status),
        total_amount=order.total_amount,
        shipping_address=order.shipping_address,
        cancellation_reason=order.cancellation_reason,
        idempotency_key=order.idempotency_key,
        created_at=order.created_at,
        updated_at=order.updated_at,
        items=[
            OrderItemEntity(
                id=item.id,
                product_id=item.product_id,
                quantity=item.quantity,
                price=item.price,
            )
            for item in order.items
        ],
    )


class SqlAlchemyOrderRepository(AbstractOrderRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(
        self,
        user_id: uuid.UUID,
        total_amount: Decimal,
        items_data: Sequence[NewOrderItem],
        shipping_address: str | None = None,
        idempotency_key: str | None = None,
    ) -> OrderEntity:
        new_order = Order(
            user_id=user_id,
            total_amount=total_amount,
            shipping_address=shipping_address,
            idempotency_key=idempotency_key,
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
        await self.session.refresh(
            new_order, attribute_names=["items", "created_at", "updated_at"]
        )
        return _to_entity(new_order)

    async def get(self, order_id: uuid.UUID) -> OrderEntity | None:
        query = (
            select(Order).where(Order.id == order_id).options(selectinload(Order.items))
        )
        result = await self.session.execute(query)
        order = result.scalar_one_or_none()
        return _to_entity(order) if order else None

    async def get_for_update(self, order_id: uuid.UUID) -> OrderEntity | None:
        query = (
            select(Order)
            .where(Order.id == order_id)
            .with_for_update()
            .options(selectinload(Order.items))
        )
        result = await self.session.execute(query)
        order = result.scalar_one_or_none()
        return _to_entity(order) if order else None

    async def get_by_user_and_idempotency_key(
        self, user_id: uuid.UUID, idempotency_key: str
    ) -> OrderEntity | None:
        query = (
            select(Order)
            .where(
                Order.user_id == user_id,
                Order.idempotency_key == idempotency_key,
            )
            .options(selectinload(Order.items))
        )
        result = await self.session.execute(query)
        order = result.scalar_one_or_none()
        return _to_entity(order) if order else None

    async def update_status(
        self,
        order_id: uuid.UUID,
        status: OrderStatus,
        cancellation_reason: str | None = None,
    ) -> OrderEntity | None:
        query = (
            select(Order).where(Order.id == order_id).options(selectinload(Order.items))
        )
        result = await self.session.execute(query)
        order = result.scalar_one_or_none()
        if not order:
            return None
        order.status = status.value
        if cancellation_reason is not None:
            order.cancellation_reason = cancellation_reason
        await self.session.flush()
        await self.session.refresh(order, attribute_names=["updated_at"])
        return _to_entity(order)

    async def get_by_id_and_user(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> OrderEntity | None:
        query = (
            select(Order)
            .where(Order.id == order_id, Order.user_id == user_id)
            .options(selectinload(Order.items))
        )
        result = await self.session.execute(query)
        order = result.scalar_one_or_none()
        return _to_entity(order) if order else None

    async def get_all_by_user(
        self, user_id: uuid.UUID, skip: int = 0, limit: int = 100
    ) -> Sequence[OrderEntity]:
        query = (
            select(Order)
            .where(Order.user_id == user_id)
            .options(selectinload(Order.items))
            .order_by(Order.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await self.session.execute(query)
        return [_to_entity(o) for o in result.scalars().all()]

    async def get_stale_pending_orders(
        self, older_than: datetime, limit: int = 50
    ) -> Sequence[OrderEntity]:
        query = (
            select(Order)
            .where(
                Order.status == OrderStatus.PENDING.value,
                Order.created_at <= older_than,
            )
            .order_by(Order.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
            .options(selectinload(Order.items))
        )
        result = await self.session.execute(query)
        return [_to_entity(o) for o in result.scalars().all()]
