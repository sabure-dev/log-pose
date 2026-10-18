import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

from app.models import OutboxEvent
from app.workers.outbox_relay import (
    MAX_OUTBOX_ATTEMPTS,
    cleanup_processed_outbox_events,
    count_parked_outbox_events,
    fetch_unprocessed_events,
    process_outbox_batch,
    requeue_parked_outbox_events,
)


async def test_fetch_unprocessed_events_and_requeue_parked(db_session):
    event_unprocessed = OutboxEvent(
        id=uuid.uuid4(),
        topic="order.created",
        payload={"order_id": str(uuid.uuid4())},
        processed=False,
    )
    event_processed = OutboxEvent(
        id=uuid.uuid4(),
        topic="order.created",
        payload={"order_id": str(uuid.uuid4())},
        processed=True,
    )
    event_parked = OutboxEvent(
        id=uuid.uuid4(),
        topic="order.created",
        payload={"order_id": str(uuid.uuid4())},
        processed=False,
        attempts=MAX_OUTBOX_ATTEMPTS,
        last_error="broker down",
    )

    db_session.add_all([event_unprocessed, event_processed, event_parked])
    await db_session.commit()

    parked_count = await count_parked_outbox_events(db_session)
    assert parked_count == 1

    events = await fetch_unprocessed_events(db_session, batch_size=10)

    assert len(events) == 1
    assert events[0].id == event_unprocessed.id
    assert events[0].processed is False

    requeued = await requeue_parked_outbox_events(db_session)
    assert requeued == 1

    parked_after = await count_parked_outbox_events(db_session)
    assert parked_after == 0

    events_after = await fetch_unprocessed_events(db_session, batch_size=10)
    assert len(events_after) == 2


async def test_process_outbox_batch_publishes_and_marks_processed(db_session):
    event = OutboxEvent(
        id=uuid.uuid4(),
        topic="order.created",
        payload={"order_id": str(uuid.uuid4()), "total_amount": "150.00"},
        processed=False,
    )

    db_session.add(event)
    await db_session.commit()

    mock_exchange = AsyncMock()

    processed_count = await process_outbox_batch(
        db_session, mock_exchange, batch_size=10
    )

    assert processed_count == 1
    assert mock_exchange.publish.call_count == 1

    call_args = mock_exchange.publish.call_args
    published_msg = call_args[0][0]
    routing_key = call_args[1]["routing_key"]

    assert routing_key == "order.created"
    assert published_msg.message_id == str(event.id)
    assert published_msg.content_type == "application/json"

    await db_session.refresh(event)
    assert event.processed is True


async def test_process_outbox_batch_partial_failure_does_not_rollback_previous(
    db_session,
):
    ev1 = OutboxEvent(
        id=uuid.uuid4(),
        topic="order.created",
        payload={"order_id": str(uuid.uuid4())},
        processed=False,
    )
    ev2 = OutboxEvent(
        id=uuid.uuid4(),
        topic="order.cancelled",
        payload={"order_id": str(uuid.uuid4())},
        processed=False,
    )
    db_session.add_all([ev1, ev2])
    await db_session.commit()

    async def side_effect(msg, routing_key, mandatory, timeout):
        if msg.message_id == str(ev2.id):
            raise RuntimeError("unroutable message")

    mock_exchange = AsyncMock()
    mock_exchange.publish.side_effect = side_effect

    published_count = await process_outbox_batch(
        db_session, mock_exchange, batch_size=10
    )

    assert published_count == 1

    await db_session.refresh(ev1)
    await db_session.refresh(ev2)

    assert ev1.processed is True
    assert ev2.processed is False
    assert ev2.attempts == 1
    assert "unroutable message" in (ev2.last_error or "")
    assert ev2.next_retry_at is not None


async def test_cleanup_processed_outbox_events_respects_batch_limit(db_session):
    old_time = datetime.now(timezone.utc) - timedelta(days=10)
    for _ in range(3):
        db_session.add(
            OutboxEvent(
                id=uuid.uuid4(),
                topic="order.created",
                payload={"order_id": str(uuid.uuid4())},
                processed=True,
                created_at=old_time,
            )
        )
    await db_session.commit()

    deleted = await cleanup_processed_outbox_events(
        db_session, retention_days=7, batch_limit=2
    )
    assert deleted == 2
