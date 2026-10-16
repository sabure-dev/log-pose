import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence

import pytest

from app.enums import OrderStatus
from app.exceptions import InvalidTransition, OrderNotFound
from app.interfaces import (
    AbstractOrderRepository,
    AbstractOutboxRepository,
    AbstractUnitOfWork,
)
from app.models import Order, OrderItem
from app.schemas import OrderCreate, OrderItemCreate
from app.services.order import OrderService


class FakeOrderRepository(AbstractOrderRepository):
    def __init__(self):
        self.orders: dict[uuid.UUID, Order] = {}

    async def add(
        self,
        user_id: uuid.UUID,
        total_amount: Decimal,
        items: list,
        shipping_address: str | None = None,
    ) -> Order:
        now = datetime.now(timezone.utc)
        order_items = []
        for it in items:
            if hasattr(it, "product_id"):
                order_items.append(
                    OrderItem(
                        id=uuid.uuid4(),
                        order_id=uuid.uuid4(),
                        product_id=it.product_id,
                        quantity=it.quantity,
                        price=it.price,
                    )
                )

        new_order = Order(
            id=uuid.uuid4(),
            user_id=user_id,
            total_amount=total_amount,
            shipping_address=shipping_address,
            created_at=now,
            updated_at=now,
            items=order_items,
        )
        new_order.status = OrderStatus.PENDING.value
        self.orders[new_order.id] = new_order
        return new_order

    async def get(self, order_id: uuid.UUID) -> Order | None:
        return self.orders.get(order_id)

    async def get_for_update(self, order_id: uuid.UUID) -> Order | None:
        return self.orders.get(order_id)

    async def get_by_id_and_user(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> Order | None:
        order = self.orders.get(order_id)
        if order and order.user_id == user_id:
            return order
        return None

    async def get_all(self, skip: int = 0, limit: int = 100) -> Sequence[Order]:
        return list(self.orders.values())[skip : skip + limit]

    async def get_all_by_user(
        self, user_id: uuid.UUID, skip: int = 0, limit: int = 100
    ) -> Sequence[Order]:
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
    item1 = OrderItemCreate(
        product_id=uuid.uuid4(), quantity=2, price=Decimal("150.00")
    )
    item2 = OrderItemCreate(
        product_id=uuid.uuid4(), quantity=1, price=Decimal("100.00")
    )
    order_in = OrderCreate(items=[item1, item2], shipping_address="Test Street 1")

    order = await service.create_new_order(user_id=user_id, order_in=order_in)

    assert order.total_amount == Decimal("400.00")
    assert order.user_id == user_id
    assert order.shipping_address == "Test Street 1"
    assert uow.committed is True
    assert uow.rollbacked is False
    assert order.id in uow.orders.orders

    assert len(uow.outbox.events) == 1
    event = uow.outbox.events[0]
    assert event["topic"] == "order.created"
    assert event["payload"]["order_id"] == str(order.id)
    assert event["payload"]["total_amount"] == "400.00"
    assert event["payload"]["shipping_address"] == "Test Street 1"
    assert event["payload"]["items"][0]["price"] == "150.00"
    assert event["payload"]["items"][1]["price"] == "100.00"
    assert "event_id" in event["payload"]
    assert "occurred_at" in event["payload"]
    assert str(event["event_id"]) == event["payload"]["event_id"]


async def test_get_orders_pagination_filtered_by_user():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()
    other_user_id = uuid.uuid4()

    for _ in range(5):
        await uow.orders.add(user_id, Decimal("100.00"), [])

    for _ in range(3):
        await uow.orders.add(other_user_id, Decimal("50.00"), [])

    orders_page_1 = await service.get_orders(user_id, skip=0, limit=3)
    orders_page_2 = await service.get_orders(user_id, skip=3, limit=3)

    assert len(orders_page_1) == 3
    assert len(orders_page_2) == 2
    assert all(o.user_id == user_id for o in orders_page_1)
    assert all(o.user_id == user_id for o in orders_page_2)


async def test_get_order_by_id_scoped_to_user():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()
    other_user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("250.00"), [])

    found_order = await service.get_order(draft_order.id, user_id=user_id)
    assert found_order is not None
    assert found_order.id == draft_order.id

    unauthorized_order = await service.get_order(draft_order.id, user_id=other_user_id)
    assert unauthorized_order is None


async def test_update_order_status_to_cancelled_publishes_outbox_event():
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
    assert uow.committed is True

    cancelled_events = [e for e in uow.outbox.events if e["topic"] == "order.cancelled"]
    assert len(cancelled_events) == 1
    cancel_payload = cancelled_events[0]["payload"]
    assert cancel_payload["order_id"] == str(draft_order.id)
    assert cancel_payload["event_type"] == "OrderCancelled"


async def test_cancel_order_scoped_to_user():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()
    other_user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("100.00"), [])
    await service.update_order_status(draft_order.id, OrderStatus.RESERVED)

    cancelled_order = await service.cancel_order(draft_order.id, user_id=user_id)
    assert cancelled_order.status == OrderStatus.CANCELLED

    with pytest.raises(OrderNotFound):
        await service.cancel_order(draft_order.id, user_id=other_user_id)


async def test_invalid_status_transition_raises_error():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("100.00"), [])
    await service.update_order_status(draft_order.id, OrderStatus.CANCELLED)

    with pytest.raises(InvalidTransition):
        await service.update_order_status(draft_order.id, OrderStatus.RESERVED)


async def test_idempotent_status_update():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("100.00"), [])
    order_reserved = await service.update_order_status(
        draft_order.id, OrderStatus.RESERVED
    )
    order_again = await service.update_order_status(
        draft_order.id, OrderStatus.RESERVED
    )

    assert order_again.id == order_reserved.id
    assert order_again.status == OrderStatus.RESERVED
