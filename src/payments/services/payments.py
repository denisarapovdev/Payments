import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import Payment
from payments.domain.enums import EventType
from payments.domain.exceptions import IdempotencyConflictError, PaymentNotFoundError
from payments.repositories.outbox import OutboxRepository
from payments.repositories.payments import PaymentRepository
from payments.schemas.events import PaymentCreatedEvent
from payments.schemas.payment import CreatePaymentRequest


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    replayed: bool


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(self, data: CreatePaymentRequest, idempotency_key: str) -> CreatePaymentResult:
        fingerprint = data.fingerprint()

        async with self._session.begin():
            payment = await self._payments.insert_if_absent(
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                meta=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
            )
            if payment is None:
                return CreatePaymentResult(
                    payment=await self._replay(idempotency_key, fingerprint), replayed=True
                )

            # Same transaction as the payment row: either both are committed or neither is.
            event = PaymentCreatedEvent(
                event_id=uuid.uuid4(), payment_id=payment.id, occurred_at=datetime.now(UTC)
            )
            self._outbox.add(
                aggregate_id=payment.id,
                event_type=EventType.PAYMENT_CREATED,
                payload=event.model_dump(mode="json"),
            )

        return CreatePaymentResult(payment=payment, replayed=False)

    async def get(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def _replay(self, idempotency_key: str, fingerprint: str) -> Payment:
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is None:  # pragma: no cover - conflict implies the row exists
            raise RuntimeError("idempotency conflict without existing row")
        if existing.request_hash != fingerprint:
            raise IdempotencyConflictError(idempotency_key)
        return existing
