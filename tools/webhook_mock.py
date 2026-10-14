"""Tiny webhook receiver for local demos (NOT part of the service).

POST /webhook       - receives a webhook; fails with 503 with probability `fail_rate`
GET  /received      - last received webhooks (with signature check result)
PUT  /config        - {"fail_rate": 1.0} switches failure rate at runtime

Run: uvicorn --app-dir tools webhook_mock:app --port 9000
"""

import hashlib
import hmac
import logging
import os
import random
from collections import deque
from typing import Any

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, Field

logger = logging.getLogger("uvicorn.error")

SECRET = os.environ.get("WEBHOOK_SECRET", "change-me-too").encode()


class Config(BaseModel):
    fail_rate: float = Field(ge=0, le=1)


state: dict[str, Any] = {
    "fail_rate": float(os.environ.get("WEBHOOK_MOCK_FAIL_RATE", "0")),
    "received": deque(maxlen=200),
}

app = FastAPI(title="Webhook mock")


@app.post("/webhook")
async def webhook(request: Request) -> Response:
    body = await request.body()
    expected = "sha256=" + hmac.new(SECRET, body, hashlib.sha256).hexdigest()
    signature_ok = hmac.compare_digest(expected, request.headers.get("X-Signature", ""))
    fail = random.random() < state["fail_rate"]  # noqa: S311
    webhook_id = request.headers.get("X-Webhook-Id")
    state["received"].append(
        {
            "webhook_id": webhook_id,
            "signature_ok": signature_ok,
            "responded": 503 if fail else 200,
            "body": await request.json(),
        }
    )
    logger.info("webhook %s signature_ok=%s -> %s", webhook_id, signature_ok, 503 if fail else 200)
    return Response(status_code=503 if fail else 200)


@app.get("/received")
async def received() -> list[dict[str, Any]]:
    return list(state["received"])


@app.put("/config")
async def set_config(config: Config) -> Config:
    state["fail_rate"] = config.fail_rate
    return config
