"""RabbitMQ topology.

    payments (direct) ──payments.new──► [payments.new] ──consumer
                                             │  reject(requeue=False)
                                             ▼
    payments.dlx (direct) ──payments.new.dlq──► [payments.new.dlq]

    payments.retry (direct) ──payments.new.retry.<N>ms──► [payments.new.retry.<N>ms]
                                             │  x-message-ttl expires
                                             ▼
                          dead-lettered back to payments / payments.new

Retry queues have no consumers: a message simply waits there for its TTL and is then
dead-lettered back into the main queue. This gives delayed retries without the
delayed-message plugin and without blocking a consumer slot with asyncio.sleep().
Queue names contain the delay, so changing the retry config creates new queues instead of
failing with PRECONDITION_FAILED on re-declaration with different arguments.
"""

from collections.abc import Sequence

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from payments.domain.enums import EventType

PAYMENTS_ROUTING_KEY = EventType.PAYMENT_CREATED.value
DLQ_ROUTING_KEY = f"{PAYMENTS_ROUTING_KEY}.dlq"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

PAYMENTS_QUEUE = RabbitQueue(
    PAYMENTS_ROUTING_KEY,
    durable=True,
    routing_key=PAYMENTS_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

DEAD_LETTER_QUEUE = RabbitQueue(DLQ_ROUTING_KEY, durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_queue(delay_ms: int) -> RabbitQueue:
    name = f"{PAYMENTS_ROUTING_KEY}.retry.{delay_ms}ms"
    return RabbitQueue(
        name,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_ROUTING_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker, retry_delays_ms: Sequence[int]) -> None:
    """Idempotently declares all exchanges, queues and bindings.

    Called by both the relay and the consumer, so whichever starts first leaves the broker
    in a state where published messages are routable (publishes are `mandatory`).
    """
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry_exchange = await broker.declare_exchange(RETRY_EXCHANGE)
    dead_letter_exchange = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    main = await broker.declare_queue(PAYMENTS_QUEUE)
    await main.bind(payments_exchange, routing_key=PAYMENTS_QUEUE.routing_key)

    dlq = await broker.declare_queue(DEAD_LETTER_QUEUE)
    await dlq.bind(dead_letter_exchange, routing_key=DEAD_LETTER_QUEUE.routing_key)

    for delay_ms in retry_delays_ms:
        queue = retry_queue(delay_ms)
        declared = await broker.declare_queue(queue)
        await declared.bind(retry_exchange, routing_key=queue.routing_key)
