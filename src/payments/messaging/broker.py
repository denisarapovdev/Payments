from faststream.rabbit import Channel, RabbitBroker

from payments.core.config import Settings


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbit_url,
        default_channel=Channel(
            prefetch_count=settings.consumer_prefetch,
            # publish() returns only after the broker confirmed the message is persisted...
            publisher_confirms=True,
            # ...and raises if a `mandatory` message could not be routed to any queue,
            # so we never mark an outbox row as published when it went nowhere.
            on_return_raises=True,
        ),
    )
