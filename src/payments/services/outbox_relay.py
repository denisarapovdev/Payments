import asyncio
import contextlib
import logging
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import OutboxEvent
from payments.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    async def publish(self, event: OutboxEvent) -> None:
        """Returns only after the broker has durably accepted the message."""
        ...


class OutboxRelay:
    """Moves committed outbox rows to RabbitMQ.

    A row is marked published only after the broker confirmed the message, inside the same
    transaction that holds its row lock. If the process dies between confirm and commit, the row
    is published again on the next pass: delivery is at-least-once and consumers are idempotent.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        *,
        batch_size: int,
        poll_interval: float,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval

    async def run_once(self) -> int:
        """Publishes one batch. Returns the number of published events."""
        published = 0
        async with self._session_factory() as session, session.begin():
            events = await OutboxRepository(session).lock_unpublished(self._batch_size)
            for event in events:
                try:
                    await self._publisher.publish(event)
                except Exception as exc:
                    logger.warning("outbox event %s publish failed: %r", event.id, exc)
                    OutboxRepository.mark_failed(event, repr(exc))
                    # Most likely the broker is unavailable: stop the batch, retry next tick.
                    break
                OutboxRepository.mark_published(event)
                published += 1
        if published:
            logger.info("outbox: published %d event(s)", published)
        return published

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("outbox relay started")
        while not stop.is_set():
            try:
                published = await self.run_once()
            except Exception:
                logger.exception("outbox relay iteration failed")
                published = 0
            if published < self._batch_size:  # backlog drained -> wait for new rows
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)
        logger.info("outbox relay stopped")
