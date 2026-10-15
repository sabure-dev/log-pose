import asyncio
import json
import logging
from typing import Sequence

import aio_pika
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import async_session_maker
from app.models import OutboxEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def fetch_unprocessed_events(
    session: AsyncSession, batch_size: int = 50
) -> Sequence[OutboxEvent]:
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.processed.is_(False))
        .order_by(OutboxEvent.created_at.asc())
        .limit(batch_size)
    )
    bind = session.get_bind()
    if bind is not None and bind.dialect.name == "postgresql":
        stmt = stmt.with_for_update(skip_locked=True)

    result = await session.execute(stmt)
    return result.scalars().all()


async def process_outbox_batch(
    session: AsyncSession,
    exchange: aio_pika.abc.AbstractExchange,
    batch_size: int = 50,
) -> int:
    events = await fetch_unprocessed_events(session, batch_size=batch_size)
    if not events:
        return 0

    for event in events:
        body = json.dumps(event.payload, default=str).encode("utf-8")
        msg = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(event.id),
            content_type="application/json",
        )

        await exchange.publish(msg, routing_key=event.topic)
        event.processed = True
        logger.info(f"Event sent: {event.topic} (ID: {event.id})")

    await session.commit()
    return len(events)


async def run_outbox_relay():
    logger.info("Starting Outbox Relay Worker...")

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        name="logpose_events", type=aio_pika.ExchangeType.TOPIC, durable=True
    )

    while True:
        try:
            async with async_session_maker() as session:
                processed_count = await process_outbox_batch(session, exchange)
                if processed_count == 0:
                    await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"Outbox worker error: {e}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(run_outbox_relay())
