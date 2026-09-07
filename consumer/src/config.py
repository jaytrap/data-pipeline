"""
Consumer configuration loaded from environment variables.

Every consumer instance uses the same image but different config.
This module defines what each instance needs to know:
  - Where to connect (source)
  - How to connect (websocket | rest_poll | sse)
  - Where to store (MinIO bucket and path)
"""

import os
from dataclasses import dataclass, field


@dataclass
class SourceConfig:
    """Defines how to connect to a data source."""

    name: str
    type: str  # websocket | rest_poll | sse
    url: str
    poll_interval: int = 60  # seconds, only used for rest_poll
    subscribe_message: str = ""  # optional JSON message to send on WS connect
    headers: dict = field(default_factory=dict)


@dataclass
class SinkConfig:
    """Defines where to store raw data in MinIO."""

    endpoint: str
    access_key: str
    secret_key: str
    bucket: str = "raw"
    path_prefix: str = ""  # e.g. "binance/trades/"
    secure: bool = False


@dataclass
class ConsumerConfig:

    source: SourceConfig
    sink: SinkConfig
    sink_type: str = "minio"  # "minio" or "rabbitmq"
    batch_size: int = 50  # flush to MinIO every N messages
    batch_timeout: int = 10  # or every N seconds, whichever comes first
    log_level: str = "INFO"


def load_config() -> ConsumerConfig:
    """Build config from environment variables."""

    source = SourceConfig(
        name=os.environ.get("SOURCE_NAME", "unknown"),
        type=os.environ.get("SOURCE_TYPE", "rest_poll"),
        url=os.environ["SOURCE_URL"],
        poll_interval=int(os.environ.get("SOURCE_POLL_INTERVAL", "60")),
        subscribe_message=os.environ.get("SOURCE_SUBSCRIBE_MSG", ""),
        headers=_parse_headers(os.environ.get("SOURCE_HEADERS", "")),
    )

    sink = SinkConfig(
        endpoint=os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minio"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minio123"),
        bucket=os.environ.get("SINK_BUCKET", "raw"),
        path_prefix=os.environ.get("SINK_PATH", source.name + "/"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    )

    return ConsumerConfig(
        source=source,
        sink=sink,
        sink_type=os.environ.get("SINK_TYPE", "minio"),
        batch_size=int(os.environ.get("BATCH_SIZE", "50")),
        batch_timeout=int(os.environ.get("BATCH_TIMEOUT", "10")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
    )


def _parse_headers(raw: str) -> dict:
    """Parse 'Key1:Value1,Key2:Value2' into a dict."""
    if not raw:
        return {}
    headers = {}
    for pair in raw.split(","):
        if ":" in pair:
            key, value = pair.split(":", 1)
            headers[key.strip()] = value.strip()
    return headers
