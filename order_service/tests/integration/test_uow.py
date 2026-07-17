import json
import uuid
from decimal import Decimal

from sqlalchemy import text

from app.api.schemas import OrderItemCreate
from app.repositories.uow import SqlAlchemyUnitOfWork


async def test_uow_can_save_order_and_outbox_event_atomically(db_session):
    uow = SqlAlchemyUnitOfWork(session=db_session)
    user_id = uuid.uuid4()
    product_id = uuid.uuid4()
    items = [
        OrderItemCreate(product_id=product_id, quantity=1, price=Decimal("100.00"))
    ]

    async with uow:
        order = await uow.orders.add(
            user_id=user_id, total_amount=Decimal("100.00"), items_data=items
        )

        await uow.outbox.add(
            topic="test.topic",
            payload={"test_key": "test_value", "order_id": str(order.id)},
        )

        await uow.commit()

    saved_order = await uow.orders.get(order.id)
    assert saved_order is not None
    assert saved_order.total_amount == Decimal("100.00")

    result = await db_session.execute(text("SELECT * FROM outbox_events"))
    events = result.mappings().all()

    assert len(events) == 1
    assert events[0]["topic"] == "test.topic"
    assert not events[0]["processed"]

    payload_data = json.loads(events[0]["payload"])

    assert payload_data["test_key"] == "test_value"
    assert payload_data["order_id"] == str(order.id)
