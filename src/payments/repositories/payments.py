import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import Payment
from payments.domain.enums import PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, **values: Any) -> Payment | None:
        """INSERT ... ON CONFLICT (idempotency_key) DO NOTHING.

        Returns the new row, or None if the key already exists. A concurrent transaction holding
        the same key makes this statement wait on the unique index until it commits or rolls back,
        so the race between two identical requests is resolved by PostgreSQL, not by app code.
        """
        stmt = (
            insert(Payment)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == key)
        return (await self._session.scalars(stmt)).one_or_none()

    async def complete_pending(
        self, payment_id: uuid.UUID, status: PaymentStatus
    ) -> Payment | None:
        """Moves pending -> final status. Returns None if the payment was already finalized.

        The `status = pending` predicate makes the transition happen exactly once even if
        the same event is processed concurrently by two consumers.
        """
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=datetime.now(UTC))
            .returning(Payment)
            .execution_options(synchronize_session=False)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def record_webhook_attempt(self, payment_id: uuid.UUID, *, error: str | None) -> None:
        values: dict[str, Any] = {
            "webhook_attempts": Payment.webhook_attempts + 1,
            "webhook_last_error": error,
        }
        if error is None:
            values["webhook_delivered_at"] = datetime.now(UTC)
        await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(**values)
            .execution_options(synchronize_session=False)
        )
