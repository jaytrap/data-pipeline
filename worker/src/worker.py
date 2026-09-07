"""
Queue worker: reads messages from RabbitMQ, writes to MinIO.

This is the bridge between the consumer and the data lake.
It runs as a separate process so that:
  - Consumers never block on MinIO writes
  - If MinIO is slow, messages buffer in RabbitMQ
  - Multiple workers can run in parallel for throughput

Each message arrives with a source_name in the envelope.
The worker uses that to determine the MinIO path:
  raw/{source_name}/{date}/{timestamp}_{uuid}.json
"""

import io
import json
import logging
import os
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from threading import Event

import pika
from minio import Minio

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [worker] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("worker")


class MinIOWriter:
    """Handles writing to MinIO with batching."""

    def __init__(self):
        self.client = Minio(
            os.environ.get("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.environ.get("MINIO_ACCESS_KEY", "minio"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "minio123"),
            secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        )
        self.bucket = os.environ.get("SINK_BUCKET", "raw")
        self._ensure_bucket()
        self.stats = {"files_written": 0, "bytes_written": 0}

    def _ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            logger.info(f"Created bucket: {self.bucket}")

    def write(self, source_name: str, body: bytes) -> str:
        """Write a single message to MinIO. Returns the object path."""
        now = datetime.now(timezone.utc)
        date_part = now.strftime("%Y-%m-%d")
        timestamp = int(now.timestamp() * 1000)
        unique_id = uuid.uuid4().hex[:8]
        object_path = f"{source_name}/{date_part}/{timestamp}_{unique_id}.json"

        stream = io.BytesIO(body)
        self.client.put_object(
            self.bucket,
            object_path,
            stream,
            length=len(body),
            content_type="application/json",
        )
        self.stats["files_written"] += 1
        self.stats["bytes_written"] += len(body)
        return object_path

    def write_batch(self, source_name: str, messages: list[bytes]) -> str:
        """Write multiple messages as a single NDJSON file."""
        now = datetime.now(timezone.utc)
        date_part = now.strftime("%Y-%m-%d")
        timestamp = int(now.timestamp() * 1000)
        unique_id = uuid.uuid4().hex[:8]
        object_path = f"{source_name}/{date_part}/{timestamp}_{unique_id}.json"

        ndjson = b"\n".join(messages) + b"\n"
        stream = io.BytesIO(ndjson)
        self.client.put_object(
            self.bucket,
            object_path,
            stream,
            length=len(ndjson),
            content_type="application/x-ndjson",
        )
        self.stats["files_written"] += 1
        self.stats["bytes_written"] += len(ndjson)
        logger.info(
            f"Batch wrote {len(messages)} messages to {object_path}"
        )
        return object_path


class Worker:
    """Consumes from RabbitMQ and writes to MinIO with batching."""

    def __init__(self):
        self.writer = MinIOWriter()
        self.batch_size = int(os.environ.get("WORKER_BATCH_SIZE", "50"))
        self.batch_timeout = int(os.environ.get("WORKER_BATCH_TIMEOUT", "10"))
        self.queue_name = os.environ.get("RABBITMQ_QUEUE", "raw_events_queue")
        self.exchange = os.environ.get("RABBITMQ_EXCHANGE", "raw_events")
        self.binding_keys = os.environ.get(
            "RABBITMQ_BINDING_KEYS", "#"
        ).split(",")

        self._batches = defaultdict(list)
        self._delivery_tags = []
        self._last_flush = time.monotonic()
        self._shutdown = Event()
        self._connection = None
        self._channel = None

    def connect(self):
        """Connect to RabbitMQ and set up the queue."""
        credentials = pika.PlainCredentials(
            os.environ.get("RABBITMQ_USER", "pipeline"),
            os.environ.get("RABBITMQ_PASS", "pipeline123"),
        )
        params = pika.ConnectionParameters(
            host=os.environ.get("RABBITMQ_HOST", "rabbitmq"),
            port=int(os.environ.get("RABBITMQ_PORT", "5672")),
            credentials=credentials,
            heartbeat=30,
            connection_attempts=5,
            retry_delay=3,
        )
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()

        # Declare the exchange (must match what consumers declare)
        self._channel.exchange_declare(
            exchange=self.exchange,
            exchange_type="topic",
            durable=True,
        )

        # Declare a durable queue
        self._channel.queue_declare(
            queue=self.queue_name,
            durable=True,
        )

        # Bind queue to exchange for each source pattern
        for key in self.binding_keys:
            self._channel.queue_bind(
                exchange=self.exchange,
                queue=self.queue_name,
                routing_key=key.strip(),
            )
            logger.info(f"Bound queue to {self.exchange} with key '{key.strip()}'")

        # Prefetch: don't give us more than batch_size unacked messages
        self._channel.basic_qos(prefetch_count=self.batch_size)

        logger.info(
            f"Connected to RabbitMQ, queue={self.queue_name}, "
            f"batch_size={self.batch_size}"
        )

    def run(self):
        """Main loop: consume messages, batch, flush to MinIO."""
        self.connect()

        self._channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self._on_message,
            auto_ack=False,  # we ack after successful MinIO write
        )

        logger.info("Worker running. Waiting for messages...")

        while not self._shutdown.is_set():
            # Process messages with a timeout so we can check flush
            self._connection.process_data_events(time_limit=1)

            # Flush on timeout if there are pending messages
            elapsed = time.monotonic() - self._last_flush
            if elapsed >= self.batch_timeout and self._delivery_tags:
                self._flush("timeout")

        # Final flush on shutdown
        if self._delivery_tags:
            self._flush("shutdown")

        self._connection.close()
        logger.info(f"Worker stopped. Stats: {self.writer.stats}")

    def _on_message(self, channel, method, properties, body):
        """Callback for each message from RabbitMQ."""
        try:
            envelope = json.loads(body)
            source_name = envelope.get("source_name", "unknown")
        except json.JSONDecodeError:
            source_name = "unknown"

        self._batches[source_name].append(body)
        self._delivery_tags.append(method.delivery_tag)

        total_pending = sum(len(v) for v in self._batches.values())
        if total_pending >= self.batch_size:
            self._flush("batch_size")

    def _flush(self, reason: str):
        """Write all pending batches to MinIO and ack the messages."""
        if not self._delivery_tags:
            return

        total = sum(len(v) for v in self._batches.values())
        logger.debug(f"Flushing {total} messages (reason: {reason})")

        try:
            for source_name, messages in self._batches.items():
                if not messages:
                    continue
                if len(messages) == 1:
                    self.writer.write(source_name, messages[0])
                else:
                    self.writer.write_batch(source_name, messages)

            # Ack all messages up to the highest delivery tag
            last_tag = self._delivery_tags[-1]
            self._channel.basic_ack(
                delivery_tag=last_tag,
                multiple=True,  # ack everything up to this tag
            )

        except Exception as e:
            logger.error(f"Flush failed: {e}. Messages will be redelivered.")
            # Don't ack. RabbitMQ will redeliver after timeout.
            self._channel.basic_nack(
                delivery_tag=self._delivery_tags[-1],
                multiple=True,
                requeue=True,
            )

        self._batches = defaultdict(list)
        self._delivery_tags = []
        self._last_flush = time.monotonic()

    def stop(self):
        logger.info("Shutdown signal received")
        self._shutdown.set()


def main():
    worker = Worker()

    def handle_signal(sig, frame):
        worker.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    worker.run()


if __name__ == "__main__":
    main()