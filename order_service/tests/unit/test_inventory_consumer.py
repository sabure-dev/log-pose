import json
import uuid
from unittest.mock import AsyncMock

import pytest

from app.enums import OrderStatus
from app.models import Order
from app.workers.inventory_consumer import (
    RETRY_QUEUE_NAME,
    handle_inventory_message,
)


class DummyMessage:
    def __init__(
        self,
        body: bytes,
        routing_key: str,
        headers: dict | None = None,
    ):
        self.body = body
        self.routing_key = routing_key
        self.headers = headers or {}
        self.acked = False
        self.rejected = False
        self.nacked = False

    async def ack(self):
        self.acked = True

    async def reject(self, requeue: bool = False):
        self.rejected = True

    async def nack(self, requeue: bool = True):
        self.nacked = True


class DummyChannel:
    def __init__(self):
        self.default_exchange = AsyncMock()


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
        "correlation_id": "corr-123",
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
async def test_handle_inventory_failed_cancels_order_and_stores_reason(db_session):
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
        "correlation_id": "corr-456",
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
    assert order.cancellation_reason == "insufficient stock"
    assert msg.acked is True


@pytest.mark.asyncio
async def test_handle_inventory_db_error_retries_and_dead_letters():
    class FailingSessionFactory:
        def __call__(self):
            class FailingContext:
                async def __aenter__(self):
                    raise RuntimeError("db unavailable")

                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass

            return FailingContext()

    payload = {
        "event_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "status": "RESERVED",
    }

    msg1 = DummyMessage(
        body=json.dumps(payload).encode("utf-8"),
        routing_key="inventory.reserved",
    )
    ch1 = DummyChannel()
    await handle_inventory_message(msg1, FailingSessionFactory(), channel=ch1)
    assert msg1.acked is True
    assert ch1.default_exchange.publish.call_count == 1
    assert (
        ch1.default_exchange.publish.call_args.kwargs["routing_key"] == RETRY_QUEUE_NAME
    )

    msg3 = DummyMessage(
        body=json.dumps(payload).encode("utf-8"),
        routing_key="inventory.reserved",
        headers={"x-retry-count": 2},
    )
    ch3 = DummyChannel()
    await handle_inventory_message(msg3, FailingSessionFactory(), channel=ch3)
    assert msg3.rejected is True
    assert ch3.default_exchange.publish.call_count == 0


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
