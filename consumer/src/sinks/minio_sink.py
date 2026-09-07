"""
MinIO sink adapter.

Receives raw messages and writes them to MinIO. Supports two modes:

1. Individual writes: each message becomes its own JSON file.
   Good for low-frequency sources (REST polling every 60s).

2. Batch writes: messages are buffered and flushed as a single
   NDJSON (newline-delimited JSON) file. Good for high-frequency
   sources like WebSocket trades (hundreds per second).

The flush happens when either batch_size or batch_timeout is reached,
whichever comes first.

File naming: {path_prefix}{date}/{timestamp}_{uuid}.json
  e.g. binance/trades/2026-09-07/1694100000123_a1b2c3.json
"""

import io
import json
import logging
import uuid
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOSink:

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str = "raw",
        path_prefix: str = "",
        secure: bool = False,
    ):
        self.bucket = bucket
        self.path_prefix = path_prefix.rstrip("/") + "/" if path_prefix else ""

        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

        self._ensure_bucket()
        self._stats = {"files_written": 0, "bytes_written": 0, "errors": 0}

    def _ensure_bucket(self) -> None:
        """Create the bucket if it doesn't exist."""
        try:
            if not self._client.bucket_exists(self.bucket):
                self._client.make_bucket(self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
            else:
                logger.info(f"Bucket exists: {self.bucket}")
        except S3Error as e:
            logger.error(f"Failed to ensure bucket '{self.bucket}': {e}")
            raise

    def _build_object_path(self) -> str:
        """Generate a unique object path with date partitioning."""
        now = datetime.now(timezone.utc)
        date_part = now.strftime("%Y-%m-%d")
        timestamp = int(now.timestamp() * 1000)
        unique_id = uuid.uuid4().hex[:8]
        return f"{self.path_prefix}{date_part}/{timestamp}_{unique_id}.json"

    def write_single(self, raw_message: str) -> str:
        """Write a single message as a JSON file. Returns the object path."""
        object_path = self._build_object_path()

        # Wrap raw message in an envelope with metadata
        envelope = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source_path": self.path_prefix.rstrip("/"),
            "payload": _safe_parse(raw_message),
        }

        data = json.dumps(envelope, default=str).encode("utf-8")
        stream = io.BytesIO(data)

        try:
            self._client.put_object(
                self.bucket,
                object_path,
                stream,
                length=len(data),
                content_type="application/json",
            )
            self._stats["files_written"] += 1
            self._stats["bytes_written"] += len(data)
            logger.debug(f"Wrote {len(data)} bytes to {object_path}")
            return object_path

        except S3Error as e:
            self._stats["errors"] += 1
            logger.error(f"Failed to write to MinIO: {e}")
            raise

    def write_batch(self, messages: list[str]) -> str:
        """
        Write a batch of messages as a single NDJSON file.
        Each line is a JSON envelope with metadata + original payload.
        Returns the object path.
        """
        if not messages:
            return ""

        object_path = self._build_object_path()
        now = datetime.now(timezone.utc).isoformat()

        lines = []
        for msg in messages:
            envelope = {
                "ingested_at": now,
                "source_path": self.path_prefix.rstrip("/"),
                "payload": _safe_parse(msg),
            }
            lines.append(json.dumps(envelope, default=str))

        data = ("\n".join(lines) + "\n").encode("utf-8")
        stream = io.BytesIO(data)

        try:
            self._client.put_object(
                self.bucket,
                object_path,
                stream,
                length=len(data),
                content_type="application/x-ndjson",
            )
            self._stats["files_written"] += 1
            self._stats["bytes_written"] += len(data)
            logger.info(
                f"Batch wrote {len(messages)} messages "
                f"({len(data)} bytes) to {object_path}"
            )
            return object_path

        except S3Error as e:
            self._stats["errors"] += 1
            logger.error(f"Failed to write batch to MinIO: {e}")
            raise

    @property
    def stats(self) -> dict:
        return dict(self._stats)


def _safe_parse(raw: str):
    """Try to parse as JSON; if it fails, store as a raw string."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw
