from sqlalchemy.ext.asyncio import AsyncSession

from app.interfaces import AbstractUnitOfWork
from app.repositories.order import SqlAlchemyOrderRepository


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def __aenter__(self):
        self.orders = SqlAlchemyOrderRepository(self.session)
        return await super().__aenter__()

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()
