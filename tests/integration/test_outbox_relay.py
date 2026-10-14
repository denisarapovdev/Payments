import asyncio
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import OutboxEvent
from payments.repositories.outbox import OutboxRepository
from payments.services.outbox_relay import OutboxRelay


class RecordingPublisher:
    def __init__(self, fail: bool = False, delay: float = 0) -> None:
        self.fail = fail
        self.delay = delay
        self.published: list[uuid.UUID] = []

    async def publish(self, event: OutboxEvent) -> None:
        await asyncio.sleep(self.delay)
        if self.fail:
            raise ConnectionError("broker unavailable")
        self.published.append(event.id)


async def add_events(sf: async_sessionmaker[AsyncSession], n: int) -> None:
    async with sf() as session, session.begin():
        repo = OutboxRepository(session)
        for _ in range(n):
            repo.add(aggregate_id=uuid.uuid4(), event_type="payments.new", payload={})


async def all_events(sf: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with sf() as session:
        return list((await session.scalars(select(OutboxEvent))).all())


def relay(sf: async_sessionmaker[AsyncSession], publisher: RecordingPublisher) -> OutboxRelay:
    return OutboxRelay(sf, publisher, batch_size=10, poll_interval=0.01)


async def test_publishes_and_marks_events(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await add_events(session_factory, 3)
    publisher = RecordingPublisher()

    assert await relay(session_factory, publisher).run_once() == 3
    assert await relay(session_factory, publisher).run_once() == 0  # nothing left

    events = await all_events(session_factory)
    assert sorted(publisher.published) == sorted(e.id for e in events)
    assert all(e.published_at is not None and e.attempts == 1 for e in events)


async def test_failed_publish_keeps_event_for_next_pass(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await add_events(session_factory, 2)

    assert await relay(session_factory, RecordingPublisher(fail=True)).run_once() == 0

    events = await all_events(session_factory)
    assert all(e.published_at is None for e in events)
    assert sorted(e.attempts for e in events) == [0, 1]  # the batch stops at the first failure
    assert any(e.last_error and "broker unavailable" in e.last_error for e in events)

    # broker is back: everything gets published
    assert await relay(session_factory, RecordingPublisher()).run_once() == 2


async def test_parallel_relays_never_publish_the_same_event_twice(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await add_events(session_factory, 30)
    publishers = [RecordingPublisher(delay=0.01) for _ in range(3)]

    for _ in range(4):
        await asyncio.gather(*(relay(session_factory, p).run_once() for p in publishers))

    published = [event_id for p in publishers for event_id in p.published]
    assert len(published) == 30
    assert len(set(published)) == 30
    assert sum(1 for p in publishers if p.published) > 1  # work was actually shared
