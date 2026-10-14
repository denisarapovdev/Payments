import uuid
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import Payment
from payments.domain.enums import Currency, PaymentStatus
from payments.domain.exceptions import NonRetryableError, RetryableError
from payments.schemas.events import PaymentWebhookPayload
from payments.services.processing import PaymentProcessor


class FakeGateway:
    def __init__(self, result: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self.result = result
        self.calls = 0

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.calls += 1
        return self.result


class FakeWebhooks:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.sent: list[PaymentWebhookPayload] = []

    async def send(self, url: str, payload: PaymentWebhookPayload) -> None:
        if self.failures:
            self.failures -= 1
            raise RetryableError("HTTP 503")
        self.sent.append(payload)


async def create_payment(sf: async_sessionmaker[AsyncSession]) -> uuid.UUID:
    async with sf() as session, session.begin():
        payment = Payment(
            amount=Decimal("10.00"),
            currency=Currency.EUR,
            description="test",
            meta={},
            idempotency_key=str(uuid.uuid4()),
            request_hash="x" * 64,
            webhook_url="https://example.com/hook",
        )
        session.add(payment)
    return payment.id


async def load(sf: async_sessionmaker[AsyncSession], payment_id: uuid.UUID) -> Payment:
    async with sf() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_happy_path(
    session_factory: async_sessionmaker[AsyncSession], result: PaymentStatus
) -> None:
    payment_id = await create_payment(session_factory)
    gateway, webhooks = FakeGateway(result), FakeWebhooks()

    await PaymentProcessor(session_factory, gateway, webhooks).process(payment_id)

    payment = await load(session_factory, payment_id)
    assert payment.status is result
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert payment.webhook_attempts == 1
    (sent,) = webhooks.sent
    assert sent.status is result
    assert sent.event_id == f"{payment_id}:{result}"


async def test_redelivery_is_a_noop(session_factory: async_sessionmaker[AsyncSession]) -> None:
    payment_id = await create_payment(session_factory)
    gateway, webhooks = FakeGateway(), FakeWebhooks()
    processor = PaymentProcessor(session_factory, gateway, webhooks)

    await processor.process(payment_id)
    await processor.process(payment_id)

    assert gateway.calls == 1
    assert len(webhooks.sent) == 1


async def test_webhook_retry_does_not_charge_again(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await create_payment(session_factory)
    gateway, webhooks = FakeGateway(), FakeWebhooks(failures=1)
    processor = PaymentProcessor(session_factory, gateway, webhooks)

    with pytest.raises(RetryableError):
        await processor.process(payment_id)

    payment = await load(session_factory, payment_id)
    assert payment.status is PaymentStatus.SUCCEEDED  # result is persisted before the webhook
    assert payment.webhook_delivered_at is None
    assert payment.webhook_attempts == 1
    assert payment.webhook_last_error == "HTTP 503"

    await processor.process(payment_id)  # the retry

    payment = await load(session_factory, payment_id)
    assert gateway.calls == 1
    assert payment.webhook_delivered_at is not None
    assert payment.webhook_attempts == 2
    assert payment.webhook_last_error is None


async def test_concurrent_finalization_keeps_first_result(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await create_payment(session_factory)
    first = PaymentProcessor(session_factory, FakeGateway(PaymentStatus.FAILED), FakeWebhooks())
    await first.process(payment_id)

    # A duplicate that read the row while it was still pending must not overwrite the result
    second = PaymentProcessor(session_factory, FakeGateway(PaymentStatus.SUCCEEDED), FakeWebhooks())
    finalized = await second._finalize(payment_id, PaymentStatus.SUCCEEDED)

    assert finalized.status is PaymentStatus.FAILED


async def test_unknown_payment_is_not_retryable(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    processor = PaymentProcessor(session_factory, FakeGateway(), FakeWebhooks())
    with pytest.raises(NonRetryableError):
        await processor.process(uuid.uuid4())
