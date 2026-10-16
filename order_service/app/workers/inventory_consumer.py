import asyncio
import json
import logging
import uuid

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.config import settings
from app.database import async_session_maker
from app.enums import OrderStatus
from app.exceptions import DomainError
from app.repositories.uow import SqlAlchemyUnitOfWork
from app.services.order import OrderService

logger = logging.getLogger(__name__)


async def handle_inventory_message(
    message: AbstractIncomingMessage, session_factory
) -> None:
    try:
        payload = json.loads(message.body.decode("utf-8"))
    except Exception as exc:
        logger.error(f"Malformed json payload: {exc}")
        await message.reject(requeue=False)
        return

    order_id_raw = payload.get("order_id")
    status_raw = payload.get("status")
    if not order_id_raw or status_raw not in ("RESERVED", "FAILED"):
        logger.warning(f"Unprocessable payload or status: {payload}")
        await message.reject(requeue=False)
        return

    try:
        order_id = uuid.UUID(order_id_raw)
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
                await service.update_order_status(order_id, target_status)
            except DomainError as domain_err:
                logger.warning(f"Domain transition rejected: {domain_err}")
        await message.ack()
    except Exception as db_err:
        logger.error(f"Database error in inventory consumer: {db_err}")
        await message.nack(requeue=True)


async def run_inventory_consumer() -> None:
    logger.info("Starting Inventory Events Consumer Worker...")

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    exchange = await channel.declare_exchange(
        name="logpose_events", type=aio_pika.ExchangeType.TOPIC, durable=True
    )

    queue = await channel.declare_queue(
        name="orders_inventory_events_queue", durable=True
    )

    await queue.bind(exchange, routing_key="inventory.reserved")
    await queue.bind(exchange, routing_key="inventory.failed")

    await queue.consume(lambda msg: handle_inventory_message(msg, async_session_maker))
    logger.info("Inventory Events Consumer is running and listening...")

    try:
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_inventory_consumer())
