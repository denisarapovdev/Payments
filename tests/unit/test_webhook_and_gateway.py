import random
import uuid
from decimal import Decimal

import httpx
import pytest

from payments.db.models import Payment
from payments.domain.enums import Currency, PaymentStatus
from payments.domain.exceptions import NonRetryableError, RetryableError
from payments.schemas.events import PaymentWebhookPayload
from payments.services.gateway import EmulatedGateway
from payments.services.webhook import HttpWebhookSender, raise_for_webhook_status, sign

PAYLOAD = PaymentWebhookPayload(
    event_id="evt-1",
    payment_id=uuid.uuid4(),
    status=PaymentStatus.SUCCEEDED,
    amount=Decimal("10.00"),
    currency=Currency.USD,
    processed_at=None,
)


@pytest.mark.parametrize("code", [200, 201, 204])
def test_success_codes(code: int) -> None:
    raise_for_webhook_status(code)


@pytest.mark.parametrize("code", [500, 502, 503, 504, 408, 425, 429])
def test_retryable_codes(code: int) -> None:
    with pytest.raises(RetryableError):
        raise_for_webhook_status(code)


@pytest.mark.parametrize("code", [301, 400, 401, 404, 410, 422])
def test_permanent_codes(code: int) -> None:
    with pytest.raises(NonRetryableError):
        raise_for_webhook_status(code)


async def test_sender_signs_body_and_sets_webhook_id() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await HttpWebhookSender(client, "secret").send("https://hook.test/x", PAYLOAD)

    (request,) = seen
    assert request.headers["X-Webhook-Id"] == "evt-1"
    assert request.headers["X-Signature"] == sign(b"secret", request.content)
    assert request.headers["Content-Type"] == "application/json"


async def test_sender_maps_transport_errors_to_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(RetryableError):
            await HttpWebhookSender(client, "secret").send("https://hook.test/x", PAYLOAD)


async def test_emulated_gateway_respects_delay_and_success_rate() -> None:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    gateway = EmulatedGateway(
        min_delay=2, max_delay=5, success_rate=0.9, rng=random.Random(42), sleep=fake_sleep
    )
    results = [await gateway.charge(Payment()) for _ in range(1000)]

    assert all(2 <= s <= 5 for s in sleeps)
    success_share = results.count(PaymentStatus.SUCCEEDED) / len(results)
    assert 0.85 < success_share < 0.95
    assert set(results) == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}
