from app.database import get_db
from app.interfaces import AbstractUnitOfWork
from app.repositories.uow import SqlAlchemyUnitOfWork
from app.services.order import OrderService
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession


def get_uow(session: AsyncSession = Depends(get_db)) -> AbstractUnitOfWork:
    return SqlAlchemyUnitOfWork(session=session)


def get_order_service(uow: AbstractUnitOfWork = Depends(get_uow)) -> OrderService:
    return OrderService(uow=uow)
