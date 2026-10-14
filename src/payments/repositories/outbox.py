import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, *, aggregate_id: uuid.UUID, event_type: str, payload: dict[str, Any]) -> None:
        self._session.add(
            OutboxEvent(aggregate_id=aggregate_id, event_type=event_type, payload=payload)
        )

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxEvent]:
        """Locks a batch of unpublished events.

        SKIP LOCKED lets several relay replicas work in parallel: each one grabs its own
        rows and never blocks on (or double-publishes) rows another replica is handling.
        """
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(stmt)).all()

    @staticmethod
    def mark_published(event: OutboxEvent) -> None:
        event.published_at = datetime.now(UTC)
        event.attempts += 1
        event.last_error = None

    @staticmethod
    def mark_failed(event: OutboxEvent, error: str) -> None:
        event.attempts += 1
        event.last_error = error
