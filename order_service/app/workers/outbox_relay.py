import asyncio
import json
import logging

import aio_pika
from sqlalchemy import select

from app.config import settings
from app.database import async_session_maker
from app.models import OutboxEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
                stmt = (
                    select(OutboxEvent)
                    .where(not OutboxEvent.processed)
                    .order_by(OutboxEvent.created_at)
                    .limit(50)
                    .with_for_update(skip_locked=True)
                )
                result = await session.execute(stmt)
                events = result.scalars().all()

                if not events:
                    await asyncio.sleep(2)
                    continue

                for event in events:
                    body = json.dumps(event.payload, default=str).encode("utf-8")
                    msg = aio_pika.Message(
                        body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    )

                    await exchange.publish(msg, routing_key=event.topic)

                    event.processed = True
                    logger.info(f"Event sent: {event.topic} (ID: {event.id})")

                await session.commit()

        except Exception as e:
            logger.error(f"Outbox worker error: {e}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(run_outbox_relay())
