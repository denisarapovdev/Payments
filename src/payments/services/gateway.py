import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import Protocol

from payments.db.models import Payment
from payments.domain.enums import PaymentStatus


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus:
        """Returns the final payment status. Raises on transient (retryable) failures.

        A real gateway call must pass `payment.id` as the provider-side idempotency key,
        so a redelivered event can never charge the customer twice.
        """
        ...


class EmulatedGateway:
    """Emulates an external acquirer: 2-5 s latency, ~90% approvals, ~10% declines."""

    def __init__(
        self,
        *,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()  # noqa: S311 - emulation, not crypto
        self._sleep = sleep

    async def charge(self, payment: Payment) -> PaymentStatus:
        await self._sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
