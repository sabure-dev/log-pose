import uuid
from decimal import Decimal

from app.api.schemas import OrderCreate, OrderItemCreate
from app.interfaces import AbstractOrderRepository, AbstractUnitOfWork
from app.models import Order
from app.services.order import OrderService


class FakeOrderRepository(AbstractOrderRepository):
    def __init__(self):
        self.orders: dict[uuid.UUID, Order] = {}

    async def add(self, user_id: uuid.UUID, total_amount: Decimal, items: list) -> Order:
        new_order = Order(id=uuid.uuid4(), user_id=user_id, total_amount=total_amount)
        self.orders[new_order.id] = new_order
        return new_order

    async def get(self, order_id: uuid.UUID) -> Order | None:
        return self.orders.get(order_id)


class FakeUnitOfWork(AbstractUnitOfWork):
    def __init__(self):
        self.orders = FakeOrderRepository()
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
    item1 = OrderItemCreate(product_id=uuid.uuid4(), quantity=2, price=Decimal("150.00"))
    item2 = OrderItemCreate(product_id=uuid.uuid4(), quantity=1, price=Decimal("50.00"))
    order_in = OrderCreate(items=[item1, item2])

    order = await service.create_new_order(user_id=user_id, order_in=order_in)

    assert order.total_amount == Decimal("350.00")

    assert order.user_id == user_id

    assert uow.committed is True
    assert uow.rollbacked is False

    assert order.id in uow.orders.orders
