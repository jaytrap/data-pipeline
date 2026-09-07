"""
Main consumer loop.

This is the core of the application. It:
  1. Creates the right source adapter based on config
  2. Creates the MinIO sink
  3. Reads from source, batches messages, flushes to sink
  4. Handles graceful shutdown on SIGINT/SIGTERM

The consumer is intentionally dumb about data content.
It does not parse, validate, or transform payloads.
That is Airflow's job downstream.
"""

import asyncio
import logging
import signal
import sys
import time
import os

from config import load_config, ConsumerConfig
from sources import create_source
from sinks import MinIOSink

logger = logging.getLogger("consumer")


class Consumer:

    def __init__(self, config: ConsumerConfig):
        self.config = config
        self._running = False
        self._batch: list[str] = []
        self._last_flush = time.monotonic()
        self._message_count = 0

        # Build source adapter
        self.source = create_source(
            source_type=config.source.type,
            url=config.source.url,
            poll_interval=config.source.poll_interval,
            subscribe_message=config.source.subscribe_message,
            headers=config.source.headers,
        )

        # Build sink based on config
        if config.sink_type == "rabbitmq":
            from sinks import RabbitMQSink
            self.sink = RabbitMQSink(
                host=os.environ.get("RABBITMQ_HOST", "rabbitmq"),
                port=int(os.environ.get("RABBITMQ_PORT", "5672")),
                username=os.environ.get("RABBITMQ_USER", "pipeline"),
                password=os.environ.get("RABBITMQ_PASS", "pipeline123"),
                exchange=os.environ.get("RABBITMQ_EXCHANGE", "raw_events"),
                source_name=config.source.name,
            )
        else:
            self.sink = MinIOSink(
                endpoint=config.sink.endpoint,
                access_key=config.sink.access_key,
                secret_key=config.sink.secret_key,
                bucket=config.sink.bucket,
                path_prefix=config.sink.path_prefix,
                secure=config.sink.secure,
            )

    async def run(self) -> None:
        """Main consumer loop."""
        self._running = True
        logger.info(
            f"Consumer starting: source={self.config.source.name} "
            f"type={self.config.source.type} "
            f"url={self.config.source.url} "
            f"sink={self.config.sink.bucket}/{self.config.sink.path_prefix}"
        )

        # Start the flush timer as a background task
        flush_task = asyncio.create_task(self._flush_timer())

        try:
            async for message in self.source.consume():
                if not self._running:
                    break

                self._batch.append(message)
                self._message_count += 1

                # Flush if batch is full
                if len(self._batch) >= self.config.batch_size:
                    self._flush("batch_size")

                # Log throughput periodically
                if self._message_count % 500 == 0:
                    logger.info(
                        f"Received {self._message_count} messages total. "
                        f"Sink stats: {self.sink.stats}"
                    )

        except asyncio.CancelledError:
            logger.info("Consumer cancelled")
        finally:
            flush_task.cancel()
            # Flush remaining messages before shutdown
            if self._batch:
                self._flush("shutdown")
            await self.source.disconnect()
            logger.info(
                f"Consumer stopped. Total messages: {self._message_count}. "
                f"Sink stats: {self.sink.stats}"
            )

    async def _flush_timer(self) -> None:
        """Background task: flush the batch every batch_timeout seconds."""
        while self._running:
            await asyncio.sleep(1)
            elapsed = time.monotonic() - self._last_flush
            if elapsed >= self.config.batch_timeout and self._batch:
                self._flush("timeout")

    def _flush(self, reason: str) -> None:
        """Write the current batch to MinIO and reset."""
        if not self._batch:
            return

        count = len(self._batch)
        logger.debug(f"Flushing {count} messages (reason: {reason})")

        try:
            if count == 1:
                self.sink.write_single(self._batch[0])
            else:
                self.sink.write_batch(self._batch)
        except Exception as e:
            logger.error(f"Flush failed: {e}. {count} messages may be lost.")

        self._batch = []
        self._last_flush = time.monotonic()

    def stop(self) -> None:
        """Signal the consumer to stop gracefully."""
        logger.info("Shutdown signal received")
        self._running = False


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def main():
    config = load_config()
    setup_logging(config.log_level)

    consumer = Consumer(config)

    # Handle graceful shutdown
    loop = asyncio.new_event_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, consumer.stop)

    try:
        loop.run_until_complete(consumer.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
