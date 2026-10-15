import uuid
from unittest.mock import AsyncMock

from app.models import OutboxEvent
from app.workers.outbox_relay import fetch_unprocessed_events, process_outbox_batch


async def test_fetch_unprocessed_events(db_session):
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

    db_session.add_all([event_unprocessed, event_processed])
    await db_session.commit()

    events = await fetch_unprocessed_events(db_session, batch_size=10)

    assert len(events) == 1
    assert events[0].id == event_unprocessed.id
    assert events[0].processed is False


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
