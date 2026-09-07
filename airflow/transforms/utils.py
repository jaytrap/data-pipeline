"""
Shared utilities for all Airflow transforms.

Handles:
  - Reading raw files from MinIO
  - Watermark tracking (which files have been processed)
  - Time dimension upserts
  - Database connection helpers
"""

import io
import json
import logging
from datetime import datetime, timezone

from minio import Minio
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# -- Connection helpers --

MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minio",
    "secret_key": "minio123",
    "secure": False,
}

POSTGRES_CONFIG = {
    "host": "warehouse-db",
    "port": 5432,
    "dbname": "warehouse",
    "user": "warehouse_user",
    "password": "warehouse_pass",
}


def get_minio_client() -> Minio:
    return Minio(**MINIO_CONFIG)


def get_db_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


# -- MinIO raw file reading --


def list_new_raw_files(
    source_path: str,
    since: datetime = None,
    bucket: str = "raw",
) -> list[str]:
    """
    List raw files in MinIO that haven't been processed yet.
    Uses the watermark timestamp to filter.
    """
    client = get_minio_client()

    if since is None:
        since = datetime(2000, 1, 1, tzinfo=timezone.utc)

    objects = client.list_objects(bucket, prefix=source_path, recursive=True)

    new_files = []
    for obj in objects:
        if obj.last_modified and obj.last_modified > since:
            new_files.append(obj.object_name)

    new_files.sort()
    logger.info(
        f"Found {len(new_files)} new files in {bucket}/{source_path} "
        f"since {since.isoformat()}"
    )
    return new_files


def read_raw_file(object_name: str, bucket: str = "raw") -> list[dict]:
    """
    Read a raw file from MinIO. Handles both:
      - Single JSON (one envelope per file)
      - NDJSON (multiple envelopes per file, newline-delimited)

    Returns a list of parsed payloads.
    """
    client = get_minio_client()
    response = client.get_object(bucket, object_name)
    content = response.read().decode("utf-8")
    response.close()
    response.release_conn()

    payloads = []
    for line in content.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            envelope = json.loads(line)
            # The consumer wraps everything in {ingested_at, source_path, payload}
            payload = envelope.get("payload", envelope)
            payloads.append(payload)
        except json.JSONDecodeError:
            logger.warning(f"Skipping malformed line in {object_name}")

    return payloads


# -- Watermark tracking --


def get_watermark(source_name: str) -> datetime:
    """Get the last processed timestamp for a source."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_processed FROM pipeline_watermark "
                "WHERE source_name = %s",
                (source_name,),
            )
            row = cur.fetchone()
            if row:
                return row[0]
            return datetime(2000, 1, 1, tzinfo=timezone.utc)
    finally:
        conn.close()


def update_watermark(source_name: str, timestamp: datetime, file_count: int):
    """Update the watermark after successful processing."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_watermark
                    (source_name, last_processed, files_processed, last_run_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (source_name) DO UPDATE SET
                    last_processed = EXCLUDED.last_processed,
                    files_processed = pipeline_watermark.files_processed
                        + EXCLUDED.files_processed,
                    last_run_at = NOW()
                """,
                (source_name, timestamp, file_count),
            )
        conn.commit()
    finally:
        conn.close()


# -- Time dimension helper --


def ensure_time_dimension(ts: datetime) -> int:
    """
    Insert a timestamp into dim_time if it doesn't exist.
    Returns the time_id.

    Truncates to the minute so we get one row per minute,
    not one row per second.
    """
    truncated = ts.replace(second=0, microsecond=0)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT time_id FROM dim_time WHERE full_timestamp = %s",
                (truncated,),
            )
            row = cur.fetchone()
            if row:
                return row[0]

            cur.execute(
                """
                INSERT INTO dim_time
                    (full_timestamp, date, hour, minute,
                     day_of_week, day_name, is_weekend, week_of_year)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (full_timestamp) DO UPDATE
                    SET full_timestamp = EXCLUDED.full_timestamp
                RETURNING time_id
                """,
                (
                    truncated,
                    truncated.date(),
                    truncated.hour,
                    truncated.minute,
                    truncated.weekday(),
                    truncated.strftime("%A"),
                    truncated.weekday() >= 5,
                    truncated.isocalendar()[1],
                ),
            )
            conn.commit()
            return cur.fetchone()[0]
    finally:
        conn.close()


# -- Coin dimension helper --


def ensure_coin_dimension(symbol: str, name: str = None) -> int:
    """Insert a coin into dim_coin if it doesn't exist. Returns coin_id."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT coin_id FROM dim_coin WHERE symbol = %s",
                (symbol.lower(),),
            )
            row = cur.fetchone()
            if row:
                return row[0]

            cur.execute(
                """
                INSERT INTO dim_coin (symbol, name)
                VALUES (%s, %s)
                ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol
                RETURNING coin_id
                """,
                (symbol.lower(), name),
            )
            conn.commit()
            return cur.fetchone()[0]
    finally:
        conn.close()


# -- Audit logging --


def log_audit(dag_id, task_id, run_id, status, records_in=0,
              records_out=0, details=None):
    """Write an entry to the pipeline audit log."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_audit_log
                    (dag_id, task_id, run_id, status,
                     records_in, records_out, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dag_id, task_id, run_id, status,
                    records_in, records_out,
                    json.dumps(details) if details else None,
                ),
            )
        conn.commit()
    finally:
        conn.close()
