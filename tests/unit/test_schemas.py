from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from payments.core.config import Settings
from payments.schemas.payment import MAX_METADATA_BYTES, CreatePaymentRequest


def make(**overrides: Any) -> CreatePaymentRequest:
    data: dict[str, Any] = {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #1",
        "metadata": {"order_id": 1, "tags": ["a", "b"]},
        "webhook_url": "https://example.com/hook",
    }
    return CreatePaymentRequest.model_validate(data | overrides)


def test_fingerprint_is_stable_across_equivalent_bodies() -> None:
    a = make(amount="100", metadata={"x": 1, "y": 2})
    b = make(amount="100.00", metadata={"y": 2, "x": 1})
    assert a.amount == Decimal("100.00")
    assert a.fingerprint() == b.fingerprint()


def test_fingerprint_changes_with_body() -> None:
    assert make(amount="100").fingerprint() != make(amount="101").fingerprint()


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "1e30", "abc"])
def test_invalid_amount_rejected(amount: str) -> None:
    with pytest.raises(ValidationError):
        make(amount=amount)


def test_unknown_currency_rejected() -> None:
    with pytest.raises(ValidationError):
        make(currency="GBP")


def test_unknown_fields_rejected() -> None:
    with pytest.raises(ValidationError):
        make(status="succeeded")


def test_metadata_size_limited() -> None:
    with pytest.raises(ValidationError):
        make(metadata={"blob": "x" * MAX_METADATA_BYTES})


def test_retry_delays_are_exponential() -> None:
    settings = Settings(max_attempts=4, retry_base_delay=2, retry_multiplier=2)
    assert settings.retry_delays_ms == [2000, 4000, 8000]
    assert Settings(max_attempts=1).retry_delays_ms == []
