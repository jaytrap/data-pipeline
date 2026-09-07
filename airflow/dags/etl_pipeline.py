"""
Main ETL DAG: raw MinIO -> transform -> PostgreSQL warehouse.

Runs every 5 minutes. Each source gets its own transform task.
Tasks run in parallel where possible, with a shared completion gate.

Dependency graph:

  check_minio_health
        |
        v
  ┌─────┼──────────┐
  v     v           v
binance coingecko  usgs     (parallel transforms)
  |     |           |
  └─────┼──────────┘
        v
  log_completion

Each transform task:
  1. Reads the watermark (last processed timestamp)
  2. Lists new files in MinIO since that watermark
  3. Reads and transforms the raw data
  4. Loads into the warehouse
  5. Updates the watermark

If a transform fails, it retries 3 times. If it still fails,
downstream tasks are skipped and Slack/email alerts fire.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from transforms.alerting import on_task_failure, on_task_retry, on_dag_success, check_data_freshness
from transforms.anomalies import check_all_anomalies
# Add transforms directory to Python path so Airflow workers can import them
sys.path.insert(0, str(Path(__file__).parent.parent / "transforms"))


# -- Health check callable --

def check_minio_health(**kwargs):
    """Verify MinIO is reachable before running transforms."""
    from transforms.utils import get_minio_client

    client = get_minio_client()
    if not client.bucket_exists("raw"):
        raise Exception("MinIO 'raw' bucket not found. Is the consumer running?")
    print("MinIO health check passed: 'raw' bucket exists")


# -- Transform callables --

def run_binance_transform(**kwargs):
    from transforms.transform_binance import run
    run(
        dag_id=kwargs.get("dag").dag_id if kwargs.get("dag") else "etl_pipeline",
        run_id=kwargs.get("run_id", ""),
    )


def run_coingecko_transform(**kwargs):
    from transforms.transform_coingecko import run
    run(
        dag_id=kwargs.get("dag").dag_id if kwargs.get("dag") else "etl_pipeline",
        run_id=kwargs.get("run_id", ""),
    )


def run_usgs_transform(**kwargs):
    from transforms.transform_usgs import run
    run(
        dag_id=kwargs.get("dag").dag_id if kwargs.get("dag") else "etl_pipeline",
        run_id=kwargs.get("run_id", ""),
    )

def run_blockchain_transform(**kwargs):
    from transforms.transform_blockchain import run
    run(
        dag_id=kwargs.get("dag").dag_id if kwargs.get("dag") else "etl_pipeline",
        run_id=kwargs.get("run_id", ""),
    )

def run_wikipedia_transform(**kwargs):
    from transforms.transform_wikipedia import run
    run(
        dag_id=kwargs.get("dag").dag_id if kwargs.get("dag") else "etl_pipeline",
        run_id=kwargs.get("run_id", ""),
    )


def run_coinbase_transform(**kwargs):
    from transforms.transform_coinbase import run
    run(
        dag_id=kwargs.get("dag").dag_id if kwargs.get("dag") else "etl_pipeline",
        run_id=kwargs.get("run_id", ""),
    )


def run_nasa_transform(**kwargs):
    from transforms.transform_nasa import run
    run(
        dag_id=kwargs.get("dag").dag_id if kwargs.get("dag") else "etl_pipeline",
        run_id=kwargs.get("run_id", ""),
    )


# -- Completion callable --

def log_completion(**kwargs):
    from transforms.utils import get_db_connection
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Print summary stats
            for table in [
                "fact_trade_candle",
                "fact_price_snapshot",
                "fact_earthquake",
            ]:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"  {table}: {count} rows")

            cur.execute("SELECT source_name, last_processed, files_processed "
                        "FROM pipeline_watermark ORDER BY source_name")
            rows = cur.fetchall()
            print("\nWatermarks:")
            for source, ts, count in rows:
                print(f"  {source}: last={ts}, files={count}")
    finally:
        conn.close()

    print("\nETL pipeline run complete")


# ================================================================
# DAG DEFINITION
# ================================================================

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Transform raw MinIO data and load into PostgreSQL warehouse",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # prevent overlapping runs
    tags=["etl", "warehouse"],
    on_success_callback=on_dag_success,
) as dag:

    # Task 1: Health check
    health_check = PythonOperator(
        task_id="check_minio_health",
        python_callable=check_minio_health,
    )

    # Task 2a: Binance trades -> 1-min candles
    transform_binance = PythonOperator(
        task_id="transform_binance",
        python_callable=run_binance_transform,
        execution_timeout=timedelta(minutes=5),
    )

    # Task 2b: CoinGecko prices -> price snapshots
    transform_coingecko = PythonOperator(
        task_id="transform_coingecko",
        python_callable=run_coingecko_transform,
        execution_timeout=timedelta(minutes=3),
    )

    # Task 2c: USGS quakes -> earthquake facts
    transform_usgs = PythonOperator(
        task_id="transform_usgs",
        python_callable=run_usgs_transform,
        execution_timeout=timedelta(minutes=3),
    )

    # Task 2d: Blockchain transactions -> BTC transaction facts
    transform_blockchain = PythonOperator(
        task_id="transform_blockchain",
        python_callable=run_blockchain_transform,
        execution_timeout=timedelta(minutes=5),
    )

    # Task 3: Log completion and stats
    completion = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
        trigger_rule="all_done",  # run even if some transforms failed
    )

    # Task 4: Check data freshness
    freshness_check = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    transform_wikipedia = PythonOperator(
        task_id="transform_wikipedia",
        python_callable=run_wikipedia_transform,
        execution_timeout=timedelta(minutes=5),
    )

    transform_coinbase = PythonOperator(
        task_id="transform_coinbase",
        python_callable=run_coinbase_transform,
        execution_timeout=timedelta(minutes=3),
    )

    transform_nasa = PythonOperator(
        task_id="transform_nasa",
        python_callable=run_nasa_transform,
        execution_timeout=timedelta(minutes=3),
    )
    anomaly_check = PythonOperator(
        task_id="check_anomalies",
        python_callable=check_all_anomalies,
    )

    # ---- Dependencies ----
    # Health check first, then all transforms in parallel,
    # then completion gate.

    all_transforms = [
        transform_binance, transform_coingecko, transform_usgs,
        transform_blockchain, transform_wikipedia, transform_coinbase,
        transform_nasa,
    ]

    health_check >> all_transforms
    all_transforms >> completion
    completion >> freshness_check
    completion >> anomaly_check
