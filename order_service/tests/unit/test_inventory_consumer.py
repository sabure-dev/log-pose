import json
import uuid

import pytest

from app.enums import OrderStatus
from app.models import Order
from app.workers.inventory_consumer import handle_inventory_message


class DummyMessage:
    def __init__(self, body: bytes, routing_key: str):
        self.body = body
        self.routing_key = routing_key
        self.acked = False
        self.rejected = False
        self.nacked = False

    async def ack(self):
        self.acked = True

    async def reject(self, requeue: bool = False):
        self.rejected = True

    async def nack(self, requeue: bool = True):
        self.nacked = True


@pytest.mark.asyncio
async def test_handle_inventory_reserved_updates_order_status(db_session):
    order_id = uuid.uuid4()
    order = Order(
        id=order_id,
        user_id=uuid.uuid4(),
        status=OrderStatus.PENDING.value,
        total_amount=100.0,
    )
    db_session.add(order)
    await db_session.commit()

    payload = {
        "event_id": str(uuid.uuid4()),
        "order_id": str(order_id),
        "status": "RESERVED",
    }
    msg = DummyMessage(
        body=json.dumps(payload).encode("utf-8"),
        routing_key="inventory.reserved",
    )

    class DummySessionFactory:
        def __call__(self):
            class DummySessionContext:
                async def __aenter__(self):
                    return db_session

                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass

            return DummySessionContext()

    await handle_inventory_message(msg, DummySessionFactory())

    await db_session.refresh(order)
    assert order.status == OrderStatus.RESERVED.value
    assert msg.acked is True


@pytest.mark.asyncio
async def test_handle_inventory_failed_cancels_order(db_session):
    order_id = uuid.uuid4()
    order = Order(
        id=order_id,
        user_id=uuid.uuid4(),
        status=OrderStatus.PENDING.value,
        total_amount=100.0,
    )
    db_session.add(order)
    await db_session.commit()

    payload = {
        "event_id": str(uuid.uuid4()),
        "order_id": str(order_id),
        "status": "FAILED",
        "reason": "insufficient stock",
    }
    msg = DummyMessage(
        body=json.dumps(payload).encode("utf-8"),
        routing_key="inventory.failed",
    )

    class DummySessionFactory:
        def __call__(self):
            class DummySessionContext:
                async def __aenter__(self):
                    return db_session

                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass

            return DummySessionContext()

    await handle_inventory_message(msg, DummySessionFactory())

    await db_session.refresh(order)
    assert order.status == OrderStatus.CANCELLED.value
    assert msg.acked is True


@pytest.mark.asyncio
async def test_handle_inventory_malformed_message_rejects():
    msg = DummyMessage(
        body=b"invalid json",
        routing_key="inventory.reserved",
    )
    await handle_inventory_message(msg, None)
    assert msg.rejected is True


@pytest.mark.asyncio
async def test_handle_inventory_unknown_status_rejects():
    payload = {
        "order_id": str(uuid.uuid4()),
        "status": "UNKNOWN_STATUS",
    }
    msg = DummyMessage(
        body=json.dumps(payload).encode("utf-8"),
        routing_key="inventory.reserved",
    )
    await handle_inventory_message(msg, None)
    assert msg.rejected is True
