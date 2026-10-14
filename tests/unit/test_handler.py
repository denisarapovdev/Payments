import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from payments.domain.exceptions import NonRetryableError, RetryableError
from payments.messaging.handler import ATTEMPT_HEADER, IncomingMessage, PaymentEventHandler
from payments.schemas.events import PaymentCreatedEvent
from payments.services.processing import PaymentProcessor

MAX_ATTEMPTS = 3


@dataclass
class FakeMessage:
    body: bytes
    headers: dict[str, Any] = field(default_factory=dict)
    message_id: str = "msg-1"
    correlation_id: str = "corr-1"
    content_type: str | None = "application/json"
    acked: bool = False
    rejected: bool = False

    async def ack(self) -> None:
        self.acked = True

    async def reject(self, requeue: bool = False) -> None:
        assert requeue is False
        self.rejected = True


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls.append(payment_id)
        if self.error:
            raise self.error


class FakeRetrier:
    def __init__(self) -> None:
        self.scheduled: list[int] = []

    async def schedule(self, message: IncomingMessage, attempt: int) -> None:
        self.scheduled.append(attempt)


def event_message(attempt: int | None = None) -> FakeMessage:
    event = PaymentCreatedEvent(
        event_id=uuid.uuid4(), payment_id=uuid.uuid4(), occurred_at=datetime.now(UTC)
    )
    headers = {} if attempt is None else {ATTEMPT_HEADER: attempt}
    return FakeMessage(body=event.model_dump_json().encode(), headers=headers)


def make_handler(error: Exception | None = None) -> tuple[PaymentEventHandler, FakeRetrier]:
    retrier = FakeRetrier()
    processor = cast(PaymentProcessor, FakeProcessor(error))
    return PaymentEventHandler(processor, retrier, MAX_ATTEMPTS), retrier


async def test_success_acks() -> None:
    handler, retrier = make_handler()
    message = event_message()
    await handler.handle(message)
    assert message.acked
    assert not message.rejected
    assert retrier.scheduled == []


@pytest.mark.parametrize("error", [RetryableError("503"), ConnectionError("db down")])
@pytest.mark.parametrize(("attempt", "next_attempt"), [(None, 2), (1, 2), (2, 3)])
async def test_transient_failure_schedules_retry_then_acks(
    error: Exception, attempt: int | None, next_attempt: int
) -> None:
    handler, retrier = make_handler(error)
    message = event_message(attempt)
    await handler.handle(message)
    assert retrier.scheduled == [next_attempt]
    assert message.acked
    assert not message.rejected


async def test_last_attempt_goes_to_dlq() -> None:
    handler, retrier = make_handler(RetryableError("503"))
    message = event_message(MAX_ATTEMPTS)
    await handler.handle(message)
    assert retrier.scheduled == []
    assert message.rejected
    assert not message.acked


async def test_permanent_failure_goes_to_dlq_immediately() -> None:
    handler, retrier = make_handler(NonRetryableError("404"))
    message = event_message()
    await handler.handle(message)
    assert retrier.scheduled == []
    assert message.rejected


async def test_malformed_message_goes_to_dlq() -> None:
    handler, retrier = make_handler()
    message = FakeMessage(body=b'{"not": "an event"}')
    await handler.handle(message)
    assert message.rejected
    assert retrier.scheduled == []
