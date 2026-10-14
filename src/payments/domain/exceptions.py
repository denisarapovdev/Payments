class PaymentsError(Exception):
    """Base class for domain errors."""


class PaymentNotFoundError(PaymentsError):
    pass


class IdempotencyConflictError(PaymentsError):
    """Idempotency key was already used with a different request body."""


class RetryableError(PaymentsError):
    """Transient failure: the message should be retried later."""


class NonRetryableError(PaymentsError):
    """Permanent failure: retrying will not help, message goes straight to DLQ."""
