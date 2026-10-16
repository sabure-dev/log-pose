import uuid
from decimal import Decimal

from app.repositories.order import SqlAlchemyOrderRepository
from app.schemas import OrderItemCreate


async def test_repository_can_save_and_retrieve_order(db_session):
    repo = SqlAlchemyOrderRepository(session=db_session)

    user_id = uuid.uuid4()
    product_id = uuid.uuid4()
    items_data = [
        OrderItemCreate(product_id=product_id, quantity=2, price=Decimal("150.00"))
    ]

    draft_order = await repo.add(
        user_id=user_id,
        total_amount=Decimal("300.00"),
        items_data=items_data,
        shipping_address="Delivery St 10",
    )

    await db_session.commit()

    saved_order = await repo.get(draft_order.id)

    assert saved_order is not None
    assert saved_order.id == draft_order.id
    assert saved_order.user_id == user_id
    assert saved_order.total_amount == Decimal("300.00")
    assert saved_order.shipping_address == "Delivery St 10"

    assert len(saved_order.items) == 1
    assert saved_order.items[0].product_id == product_id
    assert saved_order.items[0].quantity == 2


async def test_repository_can_get_orders_filtered_by_user(db_session):
    repo = SqlAlchemyOrderRepository(session=db_session)
    user_id = uuid.uuid4()
    other_user_id = uuid.uuid4()

    for _ in range(3):
        items_data = [
            OrderItemCreate(
                product_id=uuid.uuid4(), quantity=1, price=Decimal("100.00")
            )
        ]
        await repo.add(
            user_id=user_id, total_amount=Decimal("100.00"), items_data=items_data
        )

    for _ in range(2):
        items_data = [
            OrderItemCreate(
                product_id=uuid.uuid4(), quantity=1, price=Decimal("50.00")
            )
        ]
        await repo.add(
            user_id=other_user_id, total_amount=Decimal("50.00"), items_data=items_data
        )

    await db_session.commit()

    user_orders = await repo.get_all_by_user(user_id=user_id, skip=0, limit=10)
    other_orders = await repo.get_all_by_user(user_id=other_user_id, skip=0, limit=10)

    assert len(user_orders) == 3
    assert len(other_orders) == 2
    assert all(o.user_id == user_id for o in user_orders)
    assert all(o.user_id == other_user_id for o in other_orders)
