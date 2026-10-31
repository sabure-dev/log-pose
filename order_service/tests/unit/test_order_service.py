import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Sequence

import pytest

from app.domain import NewOrderItem, OrderEntity, OrderItemEntity
from app.enums import OrderStatus
from app.exceptions import IdempotencyConflict, InvalidQuantity, InvalidTransition
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
        idempotency_key: str | None = None,
        created_at: datetime | None = None,
    ) -> OrderEntity:
        now = created_at or datetime.now(timezone.utc)
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
            idempotency_key=idempotency_key,
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

    async def get_by_user_and_idempotency_key(
        self, user_id: uuid.UUID, idempotency_key: str
    ) -> OrderEntity | None:
        for order in self.orders.values():
            if order.user_id == user_id and order.idempotency_key == idempotency_key:
                return order
        return None

    async def update_status(
        self,
        order_id: uuid.UUID,
        status: OrderStatus,
        cancellation_reason: str | None = None,
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
            cancellation_reason=(
                cancellation_reason[:500]
                if cancellation_reason is not None
                else existing.cancellation_reason
            ),
            idempotency_key=existing.idempotency_key,
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

    async def get_stale_pending_orders(
        self, older_than: datetime, limit: int = 50
    ) -> Sequence[OrderEntity]:
        stale = [
            o
            for o in self.orders.values()
            if o.status == OrderStatus.PENDING and o.created_at <= older_than
        ]
        return stale[:limit]


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


async def test_create_order_with_idempotency_key_deduplicates_and_detects_conflict():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()
    product_id = uuid.uuid4()
    items = [NewOrderItem(product_id=product_id, quantity=1, price=Decimal("99.50"))]

    first = await service.create_new_order(
        user_id=user_id, items=items, idempotency_key="idem-key-1"
    )
    second = await service.create_new_order(
        user_id=user_id, items=items, idempotency_key="idem-key-1"
    )

    assert first.id == second.id
    assert len(uow.outbox.events) == 1

    # Different quantity with same Idempotency-Key must raise IdempotencyConflict
    different_items = [
        NewOrderItem(product_id=product_id, quantity=2, price=Decimal("99.50"))
    ]
    with pytest.raises(IdempotencyConflict):
        await service.create_new_order(
            user_id=user_id, items=different_items, idempotency_key="idem-key-1"
        )

    # Blank/whitespace idempotency_key is normalized to None
    blank_order = await service.create_new_order(
        user_id=user_id, items=items, idempotency_key="   "
    )
    assert blank_order.idempotency_key is None


async def test_cancel_pending_order_emits_cancelled_event_and_sets_reason():
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
    assert cancelled.cancellation_reason == "cancelled by user"

    cancelled_events = [e for e in uow.outbox.events if e["topic"] == "order.cancelled"]
    assert len(cancelled_events) == 1
    assert cancelled_events[0]["payload"]["order_id"] == str(draft_order.id)
    assert cancelled_events[0]["payload"]["items"] == [
        {"product_id": str(product_id), "quantity": 2}
    ]


async def test_system_cancel_from_pending_records_truncated_reason_and_skips_event():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    draft_order = await uow.orders.add(user_id, Decimal("100.00"), [])
    long_reason = "x" * 700
    updated = await service.update_order_status(
        draft_order.id, OrderStatus.CANCELLED, reason=long_reason
    )

    assert updated is not None
    assert updated.status == OrderStatus.CANCELLED
    assert len(updated.cancellation_reason or "") == 500
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


async def test_ship_and_complete_order_emits_order_completed_event():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()
    product_id = uuid.uuid4()

    order = await uow.orders.add(
        user_id,
        Decimal("300.00"),
        [NewOrderItem(product_id=product_id, quantity=3, price=Decimal("100.00"))],
    )
    await service.update_order_status(order.id, OrderStatus.RESERVED)

    shipped = await service.ship_order(order.id, user_id=user_id)
    assert shipped.status == OrderStatus.READY_FOR_SHIPPING

    completed = await service.complete_order(order.id, user_id=user_id)
    assert completed.status == OrderStatus.COMPLETED

    completed_events = [e for e in uow.outbox.events if e["topic"] == "order.completed"]
    assert len(completed_events) == 1
    assert completed_events[0]["payload"]["event_type"] == "OrderCompleted"
    assert completed_events[0]["payload"]["order_id"] == str(order.id)
    assert completed_events[0]["payload"]["items"] == [
        {"product_id": str(product_id), "quantity": 3}
    ]


async def test_expire_stale_pending_orders_cancels_and_emits_event():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()
    product_id = uuid.uuid4()

    stale_time = datetime.now(timezone.utc) - timedelta(minutes=30)
    stale_order = await uow.orders.add(
        user_id,
        Decimal("50.00"),
        [NewOrderItem(product_id=product_id, quantity=1, price=Decimal("50.00"))],
        created_at=stale_time,
    )

    expired_count = await service.expire_stale_pending_orders(timeout_seconds=900)
    assert expired_count == 1

    updated = await uow.orders.get(stale_order.id)
    assert updated is not None
    assert updated.status == OrderStatus.CANCELLED
    assert updated.cancellation_reason == "reservation timeout"

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


async def test_create_new_order_with_empty_items_raises_invalid_quantity():
    uow = FakeUnitOfWork()
    service = OrderService(uow=uow)
    user_id = uuid.uuid4()

    with pytest.raises(InvalidQuantity, match="Order must contain at least one item"):
        await service.create_new_order(user_id=user_id, items=[])

