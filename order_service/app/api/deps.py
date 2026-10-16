import uuid
from typing import Annotated

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.interfaces import AbstractUnitOfWork
from app.repositories.uow import SqlAlchemyUnitOfWork
from app.services.order import OrderService


def get_uow(session: AsyncSession = Depends(get_db)) -> AbstractUnitOfWork:
    return SqlAlchemyUnitOfWork(session=session)


def get_order_service(uow: AbstractUnitOfWork = Depends(get_uow)) -> OrderService:
    return OrderService(uow=uow)


def get_current_user_id(
    x_user_id: Annotated[
        uuid.UUID,
        Header(
            alias="X-User-Id",
            description="UUID текущего пользователя",
        ),
    ],
) -> uuid.UUID:
    return x_user_id


OrderServiceDep = Annotated[OrderService, Depends(get_order_service)]
UserIdDep = Annotated[uuid.UUID, Depends(get_current_user_id)]
