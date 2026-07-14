import uuid
from decimal import Decimal

from app.api.schemas import OrderItemCreate
from app.repositories.order import SqlAlchemyOrderRepository


async def test_repository_can_save_and_retrieve_order(db_session):
    repo = SqlAlchemyOrderRepository(session=db_session)

    user_id = uuid.uuid4()
    product_id = uuid.uuid4()
    items_data = [
        OrderItemCreate(product_id=product_id, quantity=2, price=Decimal("150.00"))
    ]

    draft_order = await repo.add(
        user_id=user_id, total_amount=Decimal("300.00"), items_data=items_data
    )

    await db_session.commit()

    saved_order = await repo.get(draft_order.id)

    assert saved_order is not None
    assert saved_order.id == draft_order.id
    assert saved_order.user_id == user_id
    assert saved_order.total_amount == Decimal("300.00")

    assert len(saved_order.items) == 1
    assert saved_order.items[0].product_id == product_id
    assert saved_order.items[0].quantity == 2


async def test_repository_can_get_all_orders_with_pagination(db_session):
    repo = SqlAlchemyOrderRepository(session=db_session)
    user_id = uuid.uuid4()

    for _ in range(3):
        items_data = [
            OrderItemCreate(
                product_id=uuid.uuid4(), quantity=1, price=Decimal("100.00")
            )
        ]
        await repo.add(
            user_id=user_id, total_amount=Decimal("100.00"), items_data=items_data
        )

    await db_session.commit()

    page_1 = await repo.get_all(skip=0, limit=2)
    page_2 = await repo.get_all(skip=2, limit=2)

    assert len(page_1) == 2
    assert len(page_2) == 1

    assert len(page_1[0].items) == 1
