import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from payments.domain.enums import Currency, PaymentStatus


class PaymentCreatedEvent(BaseModel):
    """Thin event: the consumer re-reads the payment from the DB (the source of truth)."""

    event_id: uuid.UUID
    payment_id: uuid.UUID
    occurred_at: datetime


class PaymentWebhookPayload(BaseModel):
    event_id: str
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None
