import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence

import pytest

from app.domain import NewOrderItem, OrderEntity, OrderItemEntity
from app.enums import OrderStatus
from app.exceptions import InvalidTransition, OrderNotFound
from app.interfaces import (
    AbstractOrderRepository,
    AbstractOutboxRepository,
    AbstractUnitOfWork,
)
from app.services.order import OrderService


class FakeOrderRepository(AbstractOrderRepository):
    def __init__(self):
        self.orders: dict[uuid.UUID, OrderEntity] = {}

    async def add(
        self,
        user_id: uuid.UUID,
        total_amount: Decimal,
        items_data: Sequence[NewOrderItem],
        shipping_address: str | None = None,
    ) -> OrderEntity:
        now = datetime.now(timezone.utc)
        order_items = [
            OrderItemEntity(
                id=uuid.uuid4(),
                product_id=it.product_id,
                quantity=it.quantity,
                price=it.price,
            )
            for it in items_data
        ]
        new_order = OrderEntity(
            id=uuid.uuid4(),
            user_id=user_id,
            status=OrderStatus.PENDING,
            total_amount=total_amount,
            shipping_address=shipping_address,
            created_at=now,
            updated_at=now,
            items=order_items,
        )
        self.orders[new_order.id] = new_order
        return new_order

    async def get(self, order_id: uuid.UUID) -> OrderEntity | None:
        return self.orders.get(order_id)

    async def get_for_update(self, order_id: uuid.UUID) -> OrderEntity | None:
        return self.orders.get(order_id)

    async def update_status(
        self, order_id: uuid.UUID, status: OrderStatus
    ) -> OrderEntity | None:
        existing = self.orders.get(order_id)
        if not existing:
            return None
        updated = OrderEntity(
            id=existing.id,
            user_id=existing.user_id,
            status=status,
            total_amount=existing.total_amount,
            shipping_address=existing.shipping_address,
            created_at=existing.created_at,
            updated_at=datetime.now(timezone.utc),
            items=existing.items,
        )
        self.orders[order_id] = updated
        return updated

    async def get_by_id_and_user(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> OrderEntity | None:
        order = self.orders.get(order_id)
        if order and order.user_id == user_id:
            return order
        return None

    async def get_all_by_user(
        self, user_id: uuid.UUID, skip: int = 0, limit: int = 100
    ) -> Sequence[OrderEntity]:
        user_orders = [o for o in self.orders.values() if o.user_id == user_id]
        return user_orders[skip : skip + limit]


class FakeOutboxRepository(AbstractOutboxRepository):
    def __init__(self):
        self.events = []

    async def add(
        self, topic: str, payload: dict, event_id: uuid.UUID | None = None
    ) -> None:
        self.events.append({"topic": topic, "payload": payload, "event_id": event_id})


class FakeUnitOfWork(AbstractUnitOfWork):
    def __init__(self):
        self.orders = FakeOrderRepository()
        self.outbox = FakeOutboxRepository()
        self.committed = False
        self.rollbacked = False

    async def commit(self):
        self.committed = True

    async def rollback(self):
        self.rollbacked = True


async def test_create_order_calculates_total_and_commits():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)

    user_id = uuid.uuid4()
    items = [
        NewOrderItem(product_id=uuid.uuid4(), quantity=2, price=Decimal("150.00")),
        NewOrderItem(product_id=uuid.uuid4(), quantity=1, price=Decimal("100.00")),
    ]

    order = await service.create_new_order(
        user_id=user_id, items=items, shipping_address="Test Street 1"
    )

    assert order.total_amount == Decimal("400.00")
    assert order.user_id == user_id
    assert order.shipping_address == "Test Street 1"
    assert uow.committed is True
    assert uow.rollbacked is False

    assert len(uow.outbox.events) == 1
    event = uow.outbox.events[0]
    assert event["topic"] == "order.created"
    assert event["payload"]["order_id"] == str(order.id)
    assert event["payload"]["total_amount"] == "400.00"


async def test_cancel_pending_order_emits_cancelled_event():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()
    product_id = uuid.uuid4()

    draft_order = await uow.orders.add(
        user_id,
        Decimal("100.00"),
        [NewOrderItem(product_id=product_id, quantity=2, price=Decimal("50.00"))],
    )

    cancelled = await service.cancel_order(draft_order.id, user_id=user_id)
    assert cancelled.status == OrderStatus.CANCELLED

    cancelled_events = [e for e in uow.outbox.events if e["topic"] == "order.cancelled"]
    assert len(cancelled_events) == 1
    assert cancelled_events[0]["payload"]["order_id"] == str(draft_order.id)
    assert cancelled_events[0]["payload"]["items"] == [
        {"product_id": str(product_id), "quantity": 2}
    ]


async def test_system_cancel_from_pending_does_not_emit_cancelled_event():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("100.00"), [])
    updated = await service.update_order_status(draft_order.id, OrderStatus.CANCELLED)

    assert updated is not None
    assert updated.status == OrderStatus.CANCELLED
    cancelled_events = [e for e in uow.outbox.events if e["topic"] == "order.cancelled"]
    assert len(cancelled_events) == 0


async def test_update_reserved_order_to_cancelled_publishes_outbox_event():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("100.00"), [])
    await service.update_order_status(draft_order.id, OrderStatus.RESERVED)

    updated_order = await service.update_order_status(
        draft_order.id, OrderStatus.CANCELLED
    )

    assert updated_order is not None
    assert updated_order.status == OrderStatus.CANCELLED
    cancelled_events = [e for e in uow.outbox.events if e["topic"] == "order.cancelled"]
    assert len(cancelled_events) == 1


async def test_invalid_status_transition_raises_error():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("100.00"), [])
    await service.update_order_status(draft_order.id, OrderStatus.CANCELLED)

    with pytest.raises(InvalidTransition):
        await service.update_order_status(draft_order.id, OrderStatus.RESERVED)
