"""Outbox relay: polls the `outbox` table and publishes events to RabbitMQ.

Run: python -m payments.workers.outbox_relay
"""

import asyncio
import signal

from payments.core.config import get_settings
from payments.core.logging import setup_logging
from payments.db.session import create_engine, create_session_factory
from payments.messaging.broker import create_broker
from payments.messaging.publisher import RabbitEventPublisher
from payments.messaging.topology import declare_topology
from payments.services.outbox_relay import OutboxRelay


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)

    broker = create_broker(settings)
    engine = create_engine(settings)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await broker.connect()
        await declare_topology(broker, settings.retry_delays_ms)
        relay = OutboxRelay(
            create_session_factory(engine),
            RabbitEventPublisher(broker),
            batch_size=settings.outbox_batch_size,
            poll_interval=settings.outbox_poll_interval,
        )
        await relay.run(stop)  # finishes the in-flight batch before returning
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
