import hashlib
import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from payments.domain.enums import Currency, PaymentStatus

MAX_METADATA_BYTES = 16 * 1024

Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, examples=["1500.00"])]


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("amount")
    @classmethod
    def _normalize_amount(cls, v: Decimal) -> Decimal:
        # 100 / 100.0 / 100.00 are the same money and must produce the same request hash
        return v.quantize(Decimal("0.01"))

    @field_validator("metadata")
    @classmethod
    def _limit_metadata(cls, v: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(v, ensure_ascii=False).encode()) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must not exceed {MAX_METADATA_BYTES} bytes")
        return v

    def fingerprint(self) -> str:
        """Stable hash of the request body, used to detect idempotency key reuse."""
        canonical = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentAcceptedResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="meta")
    status: PaymentStatus
    webhook_url: str
    webhook_delivered_at: datetime | None
    created_at: datetime
    processed_at: datetime | None
