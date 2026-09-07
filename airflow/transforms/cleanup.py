"""
Data retention cleanup.

Removes data older than the configured retention period from:
  1. MinIO raw zone (delete old JSON files)
  2. PostgreSQL warehouse (delete old fact rows)
  3. PostgreSQL audit log (keep it from growing forever)

Runs on a schedule. Default retention: 3 hours for raw data
and fact tables, 24 hours for audit logs.

This is a dev/local concern. In production you'd move old data
to cold storage (S3 Glacier, cheaper disks) rather than deleting.
"""

import logging
from datetime import datetime, timezone, timedelta

from transforms.utils import get_db_connection, get_minio_client, log_audit

logger = logging.getLogger(__name__)

# How long to keep data
RAW_RETENTION_HOURS = 3
FACT_RETENTION_HOURS = 3
AUDIT_RETENTION_HOURS = 24


def cleanup_minio(dag_id: str = "", run_id: str = "", **kwargs):
    """Delete raw files older than retention period from MinIO."""

    log_audit(dag_id, "cleanup_minio", run_id, "started")

    client = get_minio_client()
    bucket = "raw"
    cutoff = datetime.now(timezone.utc) - timedelta(hours=RAW_RETENTION_HOURS)

    deleted = 0
    errors = 0

    try:
        objects = client.list_objects(bucket, recursive=True)

        for obj in objects:
            if obj.last_modified and obj.last_modified < cutoff:
                try:
                    client.remove_object(bucket, obj.object_name)
                    deleted += 1
                except Exception as e:
                    errors += 1
                    logger.error(
                        f"Failed to delete {obj.object_name}: {e}"
                    )

    except Exception as e:
        logger.error(f"Failed to list MinIO objects: {e}")
        log_audit(dag_id, "cleanup_minio", run_id, "failed",
                  details={"error": str(e)})
        return

    log_audit(dag_id, "cleanup_minio", run_id, "success",
              records_in=deleted + errors, records_out=deleted,
              details={
                  "deleted": deleted,
                  "errors": errors,
                  "cutoff": cutoff.isoformat(),
              })
    logger.info(
        f"MinIO cleanup: deleted {deleted} files older than "
        f"{RAW_RETENTION_HOURS}h ({errors} errors)"
    )


def cleanup_warehouse(dag_id: str = "", run_id: str = "", **kwargs):
    """Delete old rows from fact tables."""

    log_audit(dag_id, "cleanup_warehouse", run_id, "started")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=FACT_RETENTION_HOURS)
    audit_cutoff = datetime.now(timezone.utc) - timedelta(
        hours=AUDIT_RETENTION_HOURS
    )

    conn = get_db_connection()
    total_deleted = 0

    try:
        with conn.cursor() as cur:
            # Clean each fact table
            tables = [
                ("fact_trade_candle", "ingested_at"),
                ("fact_price_snapshot", "ingested_at"),
                ("fact_earthquake", "ingested_at"),
                ("fact_btc_transaction", "ingested_at"),
                ("fact_wiki_edit", "ingested_at"),
                ("fact_exchange_ticker", "ingested_at"),
                ("fact_neo_approach", "ingested_at"),
            ]

            for table, time_col in tables:
                try:
                    cur.execute(
                        f"DELETE FROM {table} WHERE {time_col} < %s",
                        (cutoff,),
                    )
                    count = cur.rowcount
                    total_deleted += count
                    if count > 0:
                        logger.info(f"  {table}: deleted {count} rows")
                except Exception as e:
                    logger.warning(f"  {table}: skip ({e})")

            # Clean orphaned dimension rows (no fact references)
            cur.execute("""
                DELETE FROM dim_time
                WHERE time_id NOT IN (
                    SELECT DISTINCT time_id FROM fact_trade_candle
                    UNION SELECT DISTINCT time_id FROM fact_price_snapshot
                    UNION SELECT DISTINCT time_id FROM fact_earthquake
                    UNION SELECT DISTINCT time_id FROM fact_btc_transaction
                    UNION SELECT DISTINCT time_id FROM fact_wiki_edit
                    UNION SELECT DISTINCT time_id FROM fact_exchange_ticker
                    UNION SELECT DISTINCT time_id FROM fact_neo_approach
                )
            """)
            dim_deleted = cur.rowcount
            if dim_deleted > 0:
                logger.info(f"  dim_time: deleted {dim_deleted} orphaned rows")

            cur.execute("""
                DELETE FROM dim_location
                WHERE location_id NOT IN (
                    SELECT DISTINCT location_id FROM fact_earthquake
                )
            """)
            loc_deleted = cur.rowcount
            if loc_deleted > 0:
                logger.info(
                    f"  dim_location: deleted {loc_deleted} orphaned rows"
                )

            # Clean old audit logs
            cur.execute(
                "DELETE FROM pipeline_audit_log WHERE created_at < %s",
                (audit_cutoff,),
            )
            audit_deleted = cur.rowcount

            # Reset watermarks if they point to deleted data
            cur.execute(
                "UPDATE pipeline_watermark SET files_processed = 0"
            )

        conn.commit()

    finally:
        conn.close()

    log_audit(dag_id, "cleanup_warehouse", run_id, "success",
              records_out=total_deleted,
              details={
                  "fact_rows_deleted": total_deleted,
                  "dim_time_deleted": dim_deleted,
                  "dim_location_deleted": loc_deleted,
                  "audit_deleted": audit_deleted,
                  "cutoff": cutoff.isoformat(),
              })
    logger.info(
        f"Warehouse cleanup: deleted {total_deleted} fact rows, "
        f"{dim_deleted} dim_time, {loc_deleted} dim_location, "
        f"{audit_deleted} audit logs"
    )