"""End-to-end smoke test against a running `docker compose up` stack.

1. happy path: N payments reach a final status and their webhooks are received
2. idempotency: same key -> same payment, other body -> 409
3. retry + DLQ: receiver always fails -> 3 attempts with backoff -> message lands in DLQ
"""

import asyncio
import os
import sys
import time
import uuid
from typing import Any

import httpx

API = os.environ.get("E2E_API", "http://localhost:8000")
MOCK = os.environ.get("E2E_MOCK", "http://localhost:9000")
RABBIT = os.environ.get("E2E_RABBIT", "http://localhost:15672")
API_KEY = os.environ.get("API_KEY", "dev-secret-api-key")
WEBHOOK_URL = "http://webhook-mock:9000/webhook"  # as seen from inside the compose network
DLQ = "payments.new.dlq"


def body(**overrides: Any) -> dict[str, Any]:
    return {
        "amount": "199.99",
        "currency": "USD",
        "description": "e2e",
        "metadata": {"run": "e2e"},
        "webhook_url": WEBHOOK_URL,
    } | overrides


async def create(
    api: httpx.AsyncClient, key: str | None = None, **overrides: Any
) -> httpx.Response:
    headers = {"Idempotency-Key": key or str(uuid.uuid4())}
    return await api.post("/api/v1/payments", json=body(**overrides), headers=headers)


async def wait_final(
    api: httpx.AsyncClient, payment_id: str, limit_s: float = 30
) -> dict[str, Any]:
    deadline = time.monotonic() + limit_s
    while time.monotonic() < deadline:
        data: dict[str, Any] = (await api.get(f"/api/v1/payments/{payment_id}")).json()
        if data["status"] != "pending":
            return data
        await asyncio.sleep(0.5)
    raise AssertionError(f"payment {payment_id} still pending after {limit_s}s")


async def dlq_depth(rabbit: httpx.AsyncClient) -> int:
    response = await rabbit.get(f"/api/queues/%2F/{DLQ}")
    response.raise_for_status()
    return int(response.json().get("messages", 0))


async def happy_path(api: httpx.AsyncClient, mock: httpx.AsyncClient) -> None:
    ids = [(await create(api)).json()["payment_id"] for _ in range(10)]
    results = await asyncio.gather(*(wait_final(api, pid) for pid in ids))
    statuses = [r["status"] for r in results]
    print(f"  statuses: {statuses.count('succeeded')} succeeded, {statuses.count('failed')} failed")

    await asyncio.sleep(1)
    received = {r["body"]["payment_id"] for r in (await mock.get("/received")).json()}
    assert set(ids) <= received, "not all webhooks were received"
    assert all(r["webhook_delivered_at"] for r in [await wait_final(api, pid) for pid in ids])
    print("  all webhooks delivered and signature-checked by the mock")


async def idempotency(api: httpx.AsyncClient) -> None:
    key = str(uuid.uuid4())
    first, second = await create(api, key), await create(api, key)
    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert (await create(api, key, amount="1.00")).status_code == 409
    print("  replay returns the same payment, other body -> 409")


async def retry_and_dlq(
    api: httpx.AsyncClient, mock: httpx.AsyncClient, rabbit: httpx.AsyncClient
) -> None:
    await mock.put("/config", json={"fail_rate": 1.0})
    try:
        before = await dlq_depth(rabbit)
        payment_id = (await create(api)).json()["payment_id"]
        await wait_final(api, payment_id)

        deadline = time.monotonic() + 40
        while await dlq_depth(rabbit) <= before:
            assert time.monotonic() < deadline, "message did not reach the DLQ"
            await asyncio.sleep(1)

        attempts = [
            r for r in (await mock.get("/received")).json() if r["body"]["payment_id"] == payment_id
        ]
        payment = (await api.get(f"/api/v1/payments/{payment_id}")).json()
        assert len(attempts) == 3, f"expected 3 webhook attempts, got {len(attempts)}"
        assert payment["webhook_delivered_at"] is None
        print(
            f"  3 webhook attempts, then message moved to {DLQ}; payment stays {payment['status']}"
        )
    finally:
        await mock.put("/config", json={"fail_rate": 0.0})


async def main() -> None:
    async with (
        httpx.AsyncClient(base_url=API, headers={"X-API-Key": API_KEY}) as api,
        httpx.AsyncClient(base_url=MOCK) as mock,
        httpx.AsyncClient(base_url=RABBIT, auth=("guest", "guest")) as rabbit,
    ):
        for name, step in (
            ("happy path", happy_path(api, mock)),
            ("idempotency", idempotency(api)),
            ("retry + DLQ", retry_and_dlq(api, mock, rabbit)),
        ):
            print(f"[{name}]")
            await step
    print("E2E OK")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except AssertionError as exc:
        print(f"E2E FAILED: {exc}")
        sys.exit(1)
