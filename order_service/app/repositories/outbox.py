from sqlalchemy.ext.asyncio import AsyncSession

from app.interfaces import AbstractOutboxRepository
from app.models import OutboxEvent


class SqlAlchemyOutboxRepository(AbstractOutboxRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, topic: str, payload: dict) -> None:
        event = OutboxEvent(topic=topic, payload=payload)
        self.session.add(event)
