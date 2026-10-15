import uuid

from sqlalchemy import select

from app.models import OutboxEvent


async def test_unprocessed_outbox_events_query(db_session):
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

    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.processed.is_(False))
        .order_by(OutboxEvent.created_at.asc())
    )
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].id == event_unprocessed.id
    assert events[0].processed is False
