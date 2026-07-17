import uuid
from decimal import Decimal
from typing import Sequence

from app.api.schemas import OrderCreate, OrderItemCreate
from app.enums import OrderStatus
from app.interfaces import (
    AbstractOrderRepository,
    AbstractOutboxRepository,
    AbstractUnitOfWork,
)
from app.models import Order
from app.services.order import OrderService


class FakeOrderRepository(AbstractOrderRepository):
    def __init__(self):
        self.orders: dict[uuid.UUID, Order] = {}

    async def add(
        self, user_id: uuid.UUID, total_amount: Decimal, items: list
    ) -> Order:
        new_order = Order(id=uuid.uuid4(), user_id=user_id, total_amount=total_amount)
        new_order.status = OrderStatus.PENDING.value
        self.orders[new_order.id] = new_order
        return new_order

    async def get(self, order_id: uuid.UUID) -> Order | None:
        return self.orders.get(order_id)

    async def get_all(self, skip: int = 0, limit: int = 100) -> Sequence[Order]:
        return list(self.orders.values())[skip : skip + limit]


class FakeOutboxRepository(AbstractOutboxRepository):
    def __init__(self):
        self.events = []

    async def add(self, topic: str, payload: dict) -> None:
        self.events.append({"topic": topic, "payload": payload})


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
    order_in = OrderCreate(items=[item1, item2])

    order = await service.create_new_order(user_id=user_id, order_in=order_in)

    assert order.total_amount == Decimal("400.00")

    assert order.user_id == user_id

    assert uow.committed is True
    assert uow.rollbacked is False

    assert order.id in uow.orders.orders

    assert len(uow.outbox.events) == 1
    event = uow.outbox.events[0]
    assert event["topic"] == "order.created"
    assert event["payload"]["order_id"] == str(order.id)
    assert event["payload"]["total_amount"] == 400.0


async def test_get_orders_pagination():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    for _ in range(5):
        await uow.orders.add(user_id, Decimal("100.00"), [])

    orders_page_1 = await service.get_orders(skip=0, limit=3)
    orders_page_2 = await service.get_orders(skip=3, limit=3)

    assert len(orders_page_1) == 3
    assert len(orders_page_2) == 2


async def test_get_order_by_id():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("250.00"), [])

    found_order = await service.get_order(draft_order.id)

    assert found_order is not None
    assert found_order.id == draft_order.id
    assert found_order.total_amount == Decimal("250.00")


async def test_update_order_status():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("100.00"), [])

    updated_order = await service.update_order_status(
        draft_order.id, OrderStatus.RESERVED
    )

    assert updated_order is not None
    assert updated_order.status == OrderStatus.RESERVED.value
    assert uow.committed is True
