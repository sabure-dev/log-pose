import uuid
from typing import Annotated

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.interfaces import AbstractUnitOfWork
from app.repositories.uow import SqlAlchemyUnitOfWork
from app.services.order import OrderService

DEFAULT_TEST_USER_ID = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")


def get_uow(session: AsyncSession = Depends(get_db)) -> AbstractUnitOfWork:
    return SqlAlchemyUnitOfWork(session=session)


def get_order_service(uow: AbstractUnitOfWork = Depends(get_uow)) -> OrderService:
    return OrderService(uow=uow)


def get_current_user_id(
    x_user_id: Annotated[
        uuid.UUID | None,
        Header(
            alias="X-User-Id",
            description="UUID текущего пользователя",
            examples=["123e4567-e89b-12d3-a456-426614174000"],
        ),
    ] = None,
) -> uuid.UUID:
    if x_user_id is not None:
        return x_user_id
    return DEFAULT_TEST_USER_ID


OrderServiceDep = Annotated[OrderService, Depends(get_order_service)]
UserIdDep = Annotated[uuid.UUID, Depends(get_current_user_id)]
