"""
RabbitMQ sink adapter.

Instead of writing directly to MinIO, the consumer publishes
messages to a RabbitMQ queue. A separate worker process reads
from the queue and writes to MinIO.

This decoupling means:
  - If MinIO is down, messages buffer in RabbitMQ
  - If the consumer crashes, unacknowledged messages are redelivered
  - Multiple workers can drain the queue in parallel if needed

Messages are published with the source name as the routing key,
so the worker knows which MinIO path to write to.
"""

import json
import logging
from datetime import datetime, timezone

import pika

logger = logging.getLogger(__name__)


class RabbitMQSink:

    def __init__(
        self,
        host: str = "rabbitmq",
        port: int = 5672,
        username: str = "pipeline",
        password: str = "pipeline123",
        exchange: str = "raw_events",
        source_name: str = "",
    ):
        self.host = host
        self.port = port
        self.exchange = exchange
        self.source_name = source_name
        self._credentials = pika.PlainCredentials(username, password)
        self._connection = None
        self._channel = None
        self._stats = {"messages_published": 0, "errors": 0}

        self._connect()

    def _connect(self):
        """Establish connection and declare the exchange."""
        params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=self._credentials,
            heartbeat=30,
            connection_attempts=5,
            retry_delay=3,
        )
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()

        # Declare a topic exchange so workers can bind by source
        self._channel.exchange_declare(
            exchange=self.exchange,
            exchange_type="topic",
            durable=True,
        )
        logger.info(
            f"RabbitMQ connected: {self.host}:{self.port}, "
            f"exchange={self.exchange}"
        )

    def _ensure_connected(self):
        """Reconnect if the connection dropped."""
        if self._connection is None or self._connection.is_closed:
            logger.warning("RabbitMQ connection lost, reconnecting...")
            self._connect()

    def write_single(self, raw_message: str) -> None:
        """Publish a single message to RabbitMQ."""
        self._publish(raw_message)

    def write_batch(self, messages: list[str]) -> None:
        """Publish a batch of messages individually to RabbitMQ."""
        for msg in messages:
            self._publish(msg)
        logger.info(
            f"Published batch of {len(messages)} messages "
            f"(routing_key={self.source_name})"
        )

    def _publish(self, raw_message: str):
        """Publish one message with metadata envelope."""
        self._ensure_connected()

        envelope = json.dumps({
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source_name": self.source_name,
            "payload": _safe_parse(raw_message),
        })

        try:
            self._channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.source_name,
                body=envelope.encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # persistent (survives RabbitMQ restart)
                    content_type="application/json",
                ),
            )
            self._stats["messages_published"] += 1
        except pika.exceptions.AMQPError as e:
            self._stats["errors"] += 1
            logger.error(f"Failed to publish: {e}")
            self._connect()  # try to reconnect
            raise

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    def close(self):
        if self._connection and not self._connection.is_closed:
            self._connection.close()
            logger.info("RabbitMQ connection closed")


def _safe_parse(raw: str):
    """Try to parse as JSON; if it fails, store as raw string."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw