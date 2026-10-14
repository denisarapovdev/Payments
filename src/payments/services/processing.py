import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import Payment
from payments.domain.enums import PaymentStatus
from payments.domain.exceptions import NonRetryableError, PaymentsError
from payments.repositories.payments import PaymentRepository
from payments.schemas.events import PaymentWebhookPayload
from payments.services.gateway import PaymentGateway
from payments.services.webhook import WebhookSender

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """Drives a payment to its final state. Safe to call any number of times for the same payment.

    The progress is stored on the payment row, not in the message, so a redelivered or retried
    event resumes from where the previous attempt stopped:

        pending ──gateway──► succeeded / failed ──webhook──► webhook_delivered_at set

    Each step runs in its own short transaction; no DB transaction or row lock is held
    while waiting for the gateway or the webhook receiver.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhooks: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, payment_id: uuid.UUID) -> None:
        payment = await self._load(payment_id)

        if payment.status is PaymentStatus.PENDING:
            status = await self._gateway.charge(payment)
            payment = await self._finalize(payment_id, status)
        else:
            logger.info("payment %s already %s, skipping gateway", payment_id, payment.status)

        if payment.webhook_delivered_at is None:
            await self._notify(payment)
        else:
            logger.info("payment %s webhook already delivered, skipping", payment_id)

    async def _load(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            # The outbox row is written in the same transaction as the payment,
            # so this means a foreign/corrupted message: retrying won't help.
            raise NonRetryableError(f"payment {payment_id} not found")
        return payment

    async def _finalize(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment:
        async with self._session_factory() as session, session.begin():
            repo = PaymentRepository(session)
            updated = await repo.complete_pending(payment_id, status)
            if updated is not None:
                logger.info("payment %s -> %s", payment_id, status)
                return updated
            # A concurrent duplicate delivery finalized it first: its result wins.
            current = await repo.get(payment_id)
        if current is None:  # pragma: no cover - rows are never deleted
            raise NonRetryableError(f"payment {payment_id} disappeared")
        logger.info("payment %s finalized concurrently as %s", payment_id, current.status)
        return current

    async def _notify(self, payment: Payment) -> None:
        payload = PaymentWebhookPayload(
            event_id=f"{payment.id}:{payment.status}",
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=payment.processed_at,
        )
        try:
            await self._webhooks.send(payment.webhook_url, payload)
        except PaymentsError as exc:
            await self._record_webhook_attempt(payment.id, error=str(exc))
            raise
        await self._record_webhook_attempt(payment.id, error=None)
        logger.info("payment %s webhook delivered to %s", payment.id, payment.webhook_url)

    async def _record_webhook_attempt(self, payment_id: uuid.UUID, *, error: str | None) -> None:
        async with self._session_factory() as session, session.begin():
            await PaymentRepository(session).record_webhook_attempt(payment_id, error=error)
