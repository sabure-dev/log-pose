import asyncio
import json
import logging
import signal
from typing import Sequence

import aio_pika
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import async_session_maker, engine
from app.logging import setup_logging
from app.models import OutboxEvent

logger = logging.getLogger(__name__)


async def fetch_unprocessed_events(
    session: AsyncSession, batch_size: int = 50
) -> Sequence[OutboxEvent]:
    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.processed.is_(False))
        .order_by(OutboxEvent.created_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
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

        await exchange.publish(msg, routing_key=event.topic, mandatory=True)
        event.processed = True
        logger.info(f"Event sent: {event.topic} (ID: {event.id})")

    await session.commit()
    return len(events)


async def run_outbox_relay():
    setup_logging()
    logger.info("Starting Outbox Relay Worker...")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.declare_exchange(
            name="logpose_events", type=aio_pika.ExchangeType.TOPIC, durable=True
        )

        while not stop_event.is_set():
            try:
                async with async_session_maker() as session:
                    processed_count = await process_outbox_batch(session, exchange)
                    if processed_count == 0:
                        try:
                            await asyncio.wait_for(stop_event.wait(), timeout=2.0)
                        except asyncio.TimeoutError:
                            pass
            except Exception as e:
                logger.error(f"Outbox worker error: {e}")
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
    finally:
        await connection.close()
        await engine.dispose()
        logger.info("Outbox Relay Worker stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run_outbox_relay())
