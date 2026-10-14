"""Consumer of `payments.new`: gateway emulation -> status update -> webhook, with retries + DLQ.

Run: python -m payments.workers.consumer
"""

import asyncio

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit.annotations import RabbitMessage

from payments.core.config import Settings, get_settings
from payments.core.logging import setup_logging
from payments.db.session import create_engine, create_session_factory
from payments.messaging.broker import create_broker
from payments.messaging.handler import BrokerRetrier, PaymentEventHandler
from payments.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE, declare_topology
from payments.services.gateway import EmulatedGateway
from payments.services.processing import PaymentProcessor
from payments.services.webhook import HttpWebhookSender


def create_app(settings: Settings | None = None) -> FastStream:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    broker = create_broker(settings)
    engine = create_engine(settings)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout, follow_redirects=False)

    processor = PaymentProcessor(
        create_session_factory(engine),
        gateway=EmulatedGateway(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
        ),
        webhooks=HttpWebhookSender(http_client, settings.webhook_secret.get_secret_value()),
    )
    handler = PaymentEventHandler(
        processor,
        retrier=BrokerRetrier(broker, settings.retry_delays_ms),
        max_attempts=settings.max_attempts,
    )

    # MANUAL: the handler itself decides between ack / retry / reject(-> DLQ)
    @broker.subscriber(PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL)
    async def on_payment_created(message: RabbitMessage) -> None:
        await handler.handle(message)

    async def declare() -> None:
        await broker.connect()
        await declare_topology(broker, settings.retry_delays_ms)

    async def close_resources() -> None:
        await http_client.aclose()
        await engine.dispose()

    return FastStream(broker, on_startup=[declare], after_shutdown=[close_resources])


if __name__ == "__main__":
    asyncio.run(create_app().run())
