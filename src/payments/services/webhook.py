import hashlib
import hmac
from typing import Protocol

import httpx

from payments.domain.exceptions import NonRetryableError, RetryableError
from payments.schemas.events import PaymentWebhookPayload

RETRYABLE_STATUS_CODES = frozenset({408, 425, 429})


class WebhookSender(Protocol):
    async def send(self, url: str, payload: PaymentWebhookPayload) -> None:
        """Delivers the webhook. Raises RetryableError / NonRetryableError on failure."""
        ...


def sign(secret: bytes, body: bytes) -> str:
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def raise_for_webhook_status(status_code: int) -> None:
    """2xx is success; 5xx/408/425/429 are worth retrying; any other code is permanent."""
    if 200 <= status_code < 300:
        return
    if status_code >= 500 or status_code in RETRYABLE_STATUS_CODES:
        raise RetryableError(f"webhook responded with HTTP {status_code}")
    raise NonRetryableError(f"webhook responded with HTTP {status_code}")


class HttpWebhookSender:
    def __init__(self, client: httpx.AsyncClient, secret: str) -> None:
        self._client = client
        self._secret = secret.encode()

    async def send(self, url: str, payload: PaymentWebhookPayload) -> None:
        body = payload.model_dump_json().encode()
        headers = {
            "Content-Type": "application/json",
            # Stable across retries: lets the receiver deduplicate at-least-once deliveries
            "X-Webhook-Id": payload.event_id,
            # Lets the receiver verify the request really came from us
            "X-Signature": sign(self._secret, body),
        }
        try:
            response = await self._client.post(url, content=body, headers=headers)
        except httpx.HTTPError as exc:  # timeouts, DNS, connection refused/reset, ...
            raise RetryableError(f"webhook transport error: {exc!r}") from exc
        raise_for_webhook_status(response.status_code)
