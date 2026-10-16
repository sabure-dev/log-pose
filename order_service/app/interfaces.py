import uuid
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, Sequence

from app.models import Order
from app.schemas import OrderItemCreate


class AbstractOrderRepository(ABC):
    @abstractmethod
    async def add(
        self,
        user_id: uuid.UUID,
        total_amount: Decimal,
        items_data: list[OrderItemCreate],
        shipping_address: str | None = None,
    ) -> Order:
        raise NotImplementedError

    @abstractmethod
    async def get(self, order_id: uuid.UUID) -> Order | None:
        raise NotImplementedError

    @abstractmethod
    async def get_by_id_and_user(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> Order | None:
        raise NotImplementedError

    @abstractmethod
    async def get_all(self, skip: int = 0, limit: int = 100) -> Sequence[Order]:
        raise NotImplementedError

    @abstractmethod
    async def get_all_by_user(
        self, user_id: uuid.UUID, skip: int = 0, limit: int = 100
    ) -> Sequence[Order]:
        raise NotImplementedError


class AbstractOutboxRepository(ABC):
    @abstractmethod
    async def add(
        self,
        topic: str,
        payload: dict[str, Any],
        event_id: uuid.UUID | None = None,
    ) -> None:
        raise NotImplementedError


class AbstractUnitOfWork(ABC):
    orders: AbstractOrderRepository
    outbox: AbstractOutboxRepository

    async def __aenter__(self) -> "AbstractUnitOfWork":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self.rollback()

    @abstractmethod
    async def commit(self):
        raise NotImplementedError

    @abstractmethod
    async def rollback(self):
        raise NotImplementedError
