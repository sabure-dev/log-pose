import asyncio
import json
import logging
import uuid

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.config import settings
from app.database import async_session_maker
from app.enums import OrderStatus
from app.repositories.uow import SqlAlchemyUnitOfWork
from app.services.order import OrderService

logger = logging.getLogger(__name__)


async def handle_inventory_message(
    message: AbstractIncomingMessage, session_factory
) -> None:
    async with message.process(requeue=False):
        try:
            payload = json.loads(message.body.decode("utf-8"))
        except Exception as exc:
            logger.error(f"Failed to decode message: {exc}")
            return

        order_id_raw = payload.get("order_id")
        status_raw = payload.get("status")
        if not order_id_raw or not status_raw:
            logger.warning(f"Malformed message payload: {payload}")
            return

        try:
            order_id = uuid.UUID(order_id_raw)
        except ValueError:
            logger.error(f"Invalid UUID for order_id: {order_id_raw}")
            return

        target_status = (
            OrderStatus.RESERVED if status_raw == "RESERVED" else OrderStatus.CANCELLED
        )

        async with session_factory() as session:
            uow = SqlAlchemyUnitOfWork(session=session)
            service = OrderService(uow=uow)
            try:
                updated_order = await service.update_order_status(
                    order_id, target_status
                )
                if updated_order:
                    logger.info(
                        f"Order {order_id} status updated to {target_status.value} "
                        f"from {message.routing_key}"
                    )
                else:
                    logger.warning(f"Order {order_id} not found")
            except ValueError as val_err:
                logger.warning(f"Order {order_id} transition rejected: {val_err}")


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
