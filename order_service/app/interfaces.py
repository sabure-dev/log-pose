import uuid
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Sequence

from app.api.schemas import OrderItemCreate
from app.models import Order


class AbstractOrderRepository(ABC):
    @abstractmethod
    async def add(
        self, user_id: uuid.UUID, total_amount: Decimal, items: list[OrderItemCreate]
    ) -> Order:
        raise NotImplementedError

    @abstractmethod
    async def get(self, order_id: uuid.UUID) -> Order | None:
        raise NotImplementedError

    @abstractmethod
    async def get_all(self, skip: int = 0, limit: int = 100) -> Sequence[Order]:
        raise NotImplementedError


class AbstractUnitOfWork(ABC):
    orders: AbstractOrderRepository

    async def __aenter__(self) -> "AbstractUnitOfWork":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()

    @abstractmethod
    async def commit(self):
        raise NotImplementedError

    @abstractmethod
    async def rollback(self):
        raise NotImplementedError
