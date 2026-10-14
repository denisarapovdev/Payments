from faststream.rabbit import RabbitBroker

from payments.db.models import OutboxEvent
from payments.messaging.topology import PAYMENTS_EXCHANGE

PUBLISH_TIMEOUT = 5.0


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            event.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=event.event_type,
            persist=True,  # delivery_mode=2: survives a broker restart
            mandatory=True,  # unroutable -> error instead of silent drop
            timeout=PUBLISH_TIMEOUT,
            message_id=str(event.id),
            correlation_id=str(event.aggregate_id),
            message_type=event.event_type,
        )
