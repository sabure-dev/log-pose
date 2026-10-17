import asyncio
import json
import logging
import signal
from datetime import datetime, timedelta, timezone
from typing import Sequence

import aio_pika
from sqlalchemy import delete, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import async_session_maker, engine
from app.logging import correlation_id_ctx, setup_logging
from app.models import OutboxEvent
from app.repositories.uow import SqlAlchemyUnitOfWork
from app.services.order import OrderService

logger = logging.getLogger(__name__)

MAX_OUTBOX_ATTEMPTS = 20
MAX_BACKOFF_SECONDS = 300
CLAIM_LEASE_SECONDS = 30
PUBLISH_TIMEOUT_SECONDS = 5.0
CLEANUP_BATCH_LIMIT = 1000


async def fetch_unprocessed_events(
    session: AsyncSession, batch_size: int = 50
) -> Sequence[OutboxEvent]:
    now = datetime.now(timezone.utc)
    stmt = (
        select(OutboxEvent)
        .where(
            OutboxEvent.processed.is_(False),
            OutboxEvent.attempts < MAX_OUTBOX_ATTEMPTS,
            or_(
                OutboxEvent.next_retry_at.is_(None),
                OutboxEvent.next_retry_at <= now,
            ),
        )
        .order_by(OutboxEvent.created_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    return result.scalars().all()


async def claim_unprocessed_events(
    session: AsyncSession, batch_size: int = 50
) -> Sequence[OutboxEvent]:
    events = await fetch_unprocessed_events(session, batch_size=batch_size)
    if not events:
        return []

    lease_until = datetime.now(timezone.utc) + timedelta(seconds=CLAIM_LEASE_SECONDS)
    for event in events:
        event.next_retry_at = lease_until

    # Commit the short lease immediately so row locks are NOT held during network I/O.
    await session.commit()
    return events


async def process_outbox_batch(
    session: AsyncSession,
    exchange: aio_pika.abc.AbstractExchange,
    batch_size: int = 50,
) -> int:
    events = await claim_unprocessed_events(session, batch_size=batch_size)
    if not events:
        return 0

    published_count = 0
    for event in events:
        corr_id = (
            event.payload.get("correlation_id", "")
            if isinstance(event.payload, dict)
            else ""
        )
        token = correlation_id_ctx.set(corr_id)
        try:
            body = json.dumps(event.payload, default=str).encode("utf-8")
            msg = aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=str(event.id),
                content_type="application/json",
            )

            await exchange.publish(
                msg,
                routing_key=event.topic,
                mandatory=True,
                timeout=PUBLISH_TIMEOUT_SECONDS,
            )
            event.processed = True
            event.last_error = None
            event.next_retry_at = None
            published_count += 1
            logger.info(f"Event sent: {event.topic} (ID: {event.id})")
        except Exception as exc:
            next_attempt = (event.attempts or 0) + 1
            backoff_seconds = min(1 << min(next_attempt, 9), MAX_BACKOFF_SECONDS)
            event.attempts = next_attempt
            event.last_error = str(exc)
            event.next_retry_at = datetime.now(timezone.utc) + timedelta(
                seconds=backoff_seconds
            )
            if next_attempt >= MAX_OUTBOX_ATTEMPTS:
                logger.error(
                    f"Outbox event {event.id} ({event.topic}) parked after "
                    f"{next_attempt} attempts: {exc}"
                )
            else:
                logger.warning(
                    f"Failed to publish outbox event {event.id} ({event.topic}), "
                    f"attempt {next_attempt}/{MAX_OUTBOX_ATTEMPTS}: {exc}"
                )
        finally:
            correlation_id_ctx.reset(token)

    await session.commit()
    return published_count


async def requeue_parked_outbox_events(session: AsyncSession) -> int:
    stmt = (
        update(OutboxEvent)
        .where(
            OutboxEvent.processed.is_(False),
            OutboxEvent.attempts >= MAX_OUTBOX_ATTEMPTS,
        )
        .values(attempts=0, last_error=None, next_retry_at=None)
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount or 0


async def cleanup_processed_outbox_events(
    session: AsyncSession,
    retention_days: int = 7,
    batch_limit: int = CLEANUP_BATCH_LIMIT,
) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    subq = (
        select(OutboxEvent.id)
        .where(
            OutboxEvent.processed.is_(True),
            OutboxEvent.created_at < cutoff,
        )
        .order_by(OutboxEvent.created_at.asc())
        .limit(batch_limit)
        .scalar_subquery()
    )
    stmt = delete(OutboxEvent).where(OutboxEvent.id.in_(subq))
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount or 0


async def run_outbox_relay():
    setup_logging()
    logger.info("Starting Outbox Relay Worker...")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    maintenance_counter = 0
    try:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.declare_exchange(
            name="logpose_events", type=aio_pika.ExchangeType.TOPIC, durable=True
        )

        while not stop_event.is_set():
            try:
                async with async_session_maker() as session:
                    processed_count = await process_outbox_batch(session, exchange)

                    maintenance_counter += 1
                    if maintenance_counter >= 30:
                        maintenance_counter = 0
                        uow = SqlAlchemyUnitOfWork(session=session)
                        order_service = OrderService(uow=uow)
                        expired = await order_service.expire_stale_pending_orders(
                            timeout_seconds=settings.pending_order_timeout_seconds
                        )
                        if expired > 0:
                            logger.info(f"Expired {expired} stale pending orders")
                        await cleanup_processed_outbox_events(session)

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
