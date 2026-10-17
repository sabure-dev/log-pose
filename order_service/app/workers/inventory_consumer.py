import asyncio
import functools
import json
import logging
import signal
import uuid

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage

from app.config import settings
from app.database import async_session_maker, engine
from app.enums import OrderStatus
from app.exceptions import DomainError
from app.logging import correlation_id_ctx, setup_logging
from app.repositories.uow import SqlAlchemyUnitOfWork
from app.services.order import OrderService

logger = logging.getLogger(__name__)

MAX_DELIVERY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2.0
MAIN_QUEUE_NAME = "orders_inventory_events_queue"
RETRY_QUEUE_NAME = "orders_inventory_events_retry_queue"
DLQ_QUEUE_NAME = "orders_inventory_events_dlq"


def _get_retry_count(message: AbstractIncomingMessage) -> int:
    headers = getattr(message, "headers", None) or {}
    raw = headers.get("x-retry-count", 0)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


async def _retry_or_dead_letter(
    message: AbstractIncomingMessage,
    channel: AbstractChannel | None,
    order_id_str: str,
    err: Exception,
) -> None:
    attempts = _get_retry_count(message) + 1
    if attempts >= MAX_DELIVERY_ATTEMPTS:
        logger.error(
            f"Max delivery attempts ({attempts}) reached for order {order_id_str}, "
            f"sending to DLQ: {err}"
        )
        await message.reject(requeue=False)
        return

    logger.warning(
        f"Transient error processing order {order_id_str} "
        f"(attempt {attempts}/{MAX_DELIVERY_ATTEMPTS}), "
        f"scheduling retry in {RETRY_DELAY_SECONDS}s: {err}"
    )

    if channel is None:
        await message.nack(requeue=True)
        return

    headers = dict(getattr(message, "headers", None) or {})
    headers["x-retry-count"] = attempts
    headers["x-original-routing-key"] = headers.get(
        "x-original-routing-key", getattr(message, "routing_key", "")
    )

    try:
        retry_msg = aio_pika.Message(
            body=message.body,
            headers=headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            expiration=RETRY_DELAY_SECONDS,
        )
        await channel.default_exchange.publish(
            retry_msg,
            routing_key=RETRY_QUEUE_NAME,
            timeout=5.0,
        )
    except Exception as pub_err:
        logger.error(f"Failed to publish message to retry queue: {pub_err}")
        await message.nack(requeue=True)
        return

    await message.ack()


async def handle_inventory_message(
    message: AbstractIncomingMessage,
    session_factory,
    channel: AbstractChannel | None = None,
) -> None:
    try:
        payload = json.loads(message.body.decode("utf-8"))
    except Exception as exc:
        logger.error(f"Malformed json payload: {exc}")
        await message.reject(requeue=False)
        return

    if not isinstance(payload, dict):
        logger.error("Payload is not a JSON object")
        await message.reject(requeue=False)
        return

    correlation_id = payload.get("correlation_id") or ""
    token = correlation_id_ctx.set(str(correlation_id))

    try:
        order_id_raw = payload.get("order_id")
        status_raw = payload.get("status")
        reason = payload.get("reason")
        if not order_id_raw or status_raw not in ("RESERVED", "FAILED"):
            logger.warning(f"Unprocessable payload or status: {payload}")
            await message.reject(requeue=False)
            return

        try:
            order_id = uuid.UUID(str(order_id_raw))
        except ValueError:
            logger.error(f"Invalid UUID: {order_id_raw}")
            await message.reject(requeue=False)
            return

        target_status = (
            OrderStatus.RESERVED if status_raw == "RESERVED" else OrderStatus.CANCELLED
        )

        try:
            async with session_factory() as session:
                uow = SqlAlchemyUnitOfWork(session=session)
                service = OrderService(uow=uow)
                try:
                    await service.update_order_status(
                        order_id, target_status, reason=reason
                    )
                except DomainError as domain_err:
                    logger.warning(f"Domain transition rejected: {domain_err}")
        except Exception as db_err:
            logger.error(f"Database error in inventory consumer: {db_err}")
            await _retry_or_dead_letter(message, channel, str(order_id), db_err)
            return

        await message.ack()
    finally:
        correlation_id_ctx.reset(token)


async def run_inventory_consumer() -> None:
    setup_logging()
    logger.info("Starting Inventory Events Consumer Worker...")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel(publisher_confirms=True)
        await channel.set_qos(prefetch_count=10)

        dlx_exchange = await channel.declare_exchange(
            name="logpose_dlx", type=aio_pika.ExchangeType.TOPIC, durable=True
        )
        dlq = await channel.declare_queue(name=DLQ_QUEUE_NAME, durable=True)
        for dlq_key in (DLQ_QUEUE_NAME, MAIN_QUEUE_NAME, "inventory.#"):
            await dlq.bind(dlx_exchange, routing_key=dlq_key)

        await channel.declare_queue(
            name=RETRY_QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": MAIN_QUEUE_NAME,
            },
        )

        exchange = await channel.declare_exchange(
            name="logpose_events", type=aio_pika.ExchangeType.TOPIC, durable=True
        )

        queue = await channel.declare_queue(
            name=MAIN_QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "logpose_dlx",
                "x-dead-letter-routing-key": DLQ_QUEUE_NAME,
            },
        )

        await queue.bind(exchange, routing_key="inventory.reserved")
        await queue.bind(exchange, routing_key="inventory.failed")

        handler = functools.partial(
            handle_inventory_message,
            session_factory=async_session_maker,
            channel=channel,
        )
        await queue.consume(handler)
        logger.info("Inventory Events Consumer is running and listening...")

        await stop_event.wait()
    finally:
        await connection.close()
        await engine.dispose()
        logger.info("Inventory Events Consumer stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run_inventory_consumer())
