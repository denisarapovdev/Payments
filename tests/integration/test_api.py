import asyncio
import uuid
from typing import Any

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import OutboxEvent, Payment

URL = "/api/v1/payments"
BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://example.com/webhook",
}


def key() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def count(sf: async_sessionmaker[AsyncSession], model: type[Any]) -> int:
    async with sf() as session:
        return (await session.scalar(select(func.count()).select_from(model))) or 0


async def test_create_payment_writes_payment_and_outbox_event(
    client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(URL, json=BODY, headers=key())

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert {"payment_id", "status", "created_at"} <= data.keys()

    async with session_factory() as session:
        events = (await session.scalars(select(OutboxEvent))).all()
    assert len(events) == 1
    assert events[0].event_type == "payments.new"
    assert events[0].payload["payment_id"] == data["payment_id"]
    assert events[0].published_at is None


async def test_get_payment(client: AsyncClient) -> None:
    created = (await client.post(URL, json=BODY, headers=key())).json()

    response = await client.get(f"{URL}/{created['payment_id']}")

    assert response.status_code == 200
    data = response.json()
    assert data["amount"] == "1500.00"
    assert data["currency"] == "RUB"
    assert data["metadata"] == {"order_id": 42}
    assert data["processed_at"] is None


async def test_get_unknown_payment_is_404(client: AsyncClient) -> None:
    response = await client.get(f"{URL}/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_same_idempotency_key_replays_response(
    client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = key()
    first = await client.post(URL, json=BODY, headers=headers)
    second = await client.post(URL, json=BODY | {"amount": "1500"}, headers=headers)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_same_idempotency_key_with_other_body_is_409(client: AsyncClient) -> None:
    headers = key()
    await client.post(URL, json=BODY, headers=headers)

    response = await client.post(URL, json=BODY | {"amount": "1.00"}, headers=headers)

    assert response.status_code == 409


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = key()
    responses = await asyncio.gather(
        *(client.post(URL, json=BODY, headers=headers) for _ in range(10))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_missing_idempotency_key_is_422(client: AsyncClient) -> None:
    response = await client.post(URL, json=BODY)
    assert response.status_code == 422


async def test_invalid_body_is_422(client: AsyncClient) -> None:
    response = await client.post(URL, json=BODY | {"currency": "GBP"}, headers=key())
    assert response.status_code == 422


async def test_api_key_required(client: AsyncClient) -> None:
    for headers in ({"X-API-Key": ""}, {"X-API-Key": "wrong"}):
        assert (await client.post(URL, json=BODY, headers=key() | headers)).status_code == 401
        assert (await client.get(f"{URL}/{uuid.uuid4()}", headers=headers)).status_code == 401
