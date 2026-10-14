import logging
from collections.abc import Sequence
from typing import Any, Protocol

from faststream.rabbit import RabbitBroker
from pydantic import ValidationError

from payments.domain.exceptions import NonRetryableError
from payments.messaging.topology import RETRY_EXCHANGE, retry_queue
from payments.schemas.events import PaymentCreatedEvent
from payments.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "x-attempt"
# Added by RabbitMQ on dead-lettering; meaningless on a freshly published copy.
_BROKER_HEADERS = ("x-death", "x-first-death-", "x-last-death-")


class IncomingMessage(Protocol):
    """The part of faststream's RabbitMessage the handler relies on (keeps it testable)."""

    body: bytes
    headers: dict[str, Any]
    message_id: str
    correlation_id: str
    content_type: str | None

    async def ack(self) -> None: ...

    async def reject(self, requeue: bool = False) -> None: ...


class Retrier(Protocol):
    async def schedule(self, message: IncomingMessage, attempt: int) -> None: ...


class BrokerRetrier:
    """Re-publishes a message into the retry queue matching the next attempt's delay."""

    def __init__(self, broker: RabbitBroker, delays_ms: Sequence[int]) -> None:
        self._broker = broker
        self._delays_ms = delays_ms

    async def schedule(self, message: IncomingMessage, attempt: int) -> None:
        # attempt 2 waits delays[0], attempt 3 waits delays[1], ...
        queue = retry_queue(self._delays_ms[attempt - 2])
        headers = {k: v for k, v in message.headers.items() if not k.startswith(_BROKER_HEADERS)}
        headers[ATTEMPT_HEADER] = attempt
        await self._broker.publish(
            message.body,
            exchange=RETRY_EXCHANGE,
            routing_key=queue.routing_key,
            persist=True,
            mandatory=True,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            content_type=message.content_type or "application/json",
            headers=headers,
        )


class PaymentEventHandler:
    """Turns processing outcomes into broker actions.

    success              -> ack
    permanent failure    -> reject(requeue=False) -> DLX -> payments.new.dlq
    transient failure    -> publish copy to retry queue (with confirm), then ack the original
    attempts exhausted   -> reject(requeue=False) -> DLQ

    Publishing the retry copy *before* acking keeps at-least-once semantics: a crash in between
    yields a duplicate, which the idempotent processor absorbs.
    """

    def __init__(self, processor: PaymentProcessor, retrier: Retrier, max_attempts: int) -> None:
        self._processor = processor
        self._retrier = retrier
        self._max_attempts = max_attempts

    async def handle(self, message: IncomingMessage) -> None:
        attempt = int(message.headers.get(ATTEMPT_HEADER, 1))

        try:
            event = PaymentCreatedEvent.model_validate_json(message.body)
        except ValidationError:
            logger.exception("malformed message %s, sending to DLQ", message.message_id)
            await message.reject(requeue=False)
            return

        log_ctx = (event.payment_id, attempt, self._max_attempts)
        try:
            await self._processor.process(event.payment_id)
        except NonRetryableError as exc:
            logger.error("payment %s attempt %d/%d permanent failure: %s -> DLQ", *log_ctx, exc)
            await message.reject(requeue=False)
        except Exception as exc:
            # Transient errors and unexpected bugs alike get a bounded number of retries.
            if attempt < self._max_attempts:
                logger.warning("payment %s attempt %d/%d failed: %r -> retry", *log_ctx, exc)
                await self._retrier.schedule(message, attempt + 1)
                await message.ack()
            else:
                logger.error("payment %s attempt %d/%d failed: %r -> DLQ", *log_ctx, exc)
                await message.reject(requeue=False)
        else:
            await message.ack()
