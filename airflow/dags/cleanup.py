"""
Cleanup DAG: data retention enforcement.

Runs every hour. Removes data older than the retention period
from both MinIO and PostgreSQL to prevent disk exhaustion
on local development machines.

In production, replace deletion with archival to cold storage.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).parent.parent / "transforms"))

from transforms.alerting import on_task_failure


def run_minio_cleanup(**kwargs):
    from transforms.cleanup import cleanup_minio
    cleanup_minio(dag_id="cleanup", run_id=kwargs.get("run_id", ""))


def run_warehouse_cleanup(**kwargs):
    from transforms.cleanup import cleanup_warehouse
    cleanup_warehouse(dag_id="cleanup", run_id=kwargs.get("run_id", ""))


def log_disk_usage(**kwargs):
    """Log current data sizes for monitoring."""
    from transforms.utils import get_db_connection, get_minio_client

    # Check PostgreSQL sizes
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            tables = [
                "fact_trade_candle", "fact_price_snapshot",
                "fact_earthquake", "fact_btc_transaction",
                "fact_wiki_edit", "fact_exchange_ticker",
                "fact_neo_approach", "dim_time", "dim_location",
                "dim_coin", "pipeline_audit_log",
            ]
            print("\nWarehouse table sizes:")
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    cur.execute(
                        "SELECT pg_size_pretty(pg_total_relation_size(%s))",
                        (table,),
                    )
                    size = cur.fetchone()[0]
                    print(f"  {table}: {count} rows ({size})")
                except Exception:
                    print(f"  {table}: not found")
    finally:
        conn.close()

    # Check MinIO sizes
    client = get_minio_client()
    print("\nMinIO raw zone:")
    sources = ["binance", "coingecko", "usgs", "blockchain",
               "wikipedia", "coinbase", "nasa"]
    for source in sources:
        objects = list(client.list_objects("raw", prefix=f"{source}/",
                                          recursive=True))
        total_size = sum(o.size for o in objects if o.size)
        size_mb = total_size / (1024 * 1024)
        print(f"  {source}: {len(objects)} files ({size_mb:.1f} MB)")

    print("\nCleanup complete")


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_task_failure,
}

with DAG(
    dag_id="cleanup",
    default_args=default_args,
    description="Remove data older than retention period",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "cleanup"],
) as dag:

    minio_cleanup = PythonOperator(
        task_id="cleanup_minio",
        python_callable=run_minio_cleanup,
    )

    warehouse_cleanup = PythonOperator(
        task_id="cleanup_warehouse",
        python_callable=run_warehouse_cleanup,
    )

    disk_report = PythonOperator(
        task_id="disk_usage_report",
        python_callable=log_disk_usage,
        trigger_rule="all_done",
    )

    [minio_cleanup, warehouse_cleanup] >> disk_report