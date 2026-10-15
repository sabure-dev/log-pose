import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.interfaces import AbstractOutboxRepository
from app.models import OutboxEvent


class SqlAlchemyOutboxRepository(AbstractOutboxRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(
        self,
        topic: str,
        payload: dict[str, Any],
        event_id: uuid.UUID | None = None,
    ) -> OutboxEvent:
        event = OutboxEvent(
            id=event_id or uuid.uuid4(),
            topic=topic,
            payload=payload,
        )
        self.session.add(event)
        return event
