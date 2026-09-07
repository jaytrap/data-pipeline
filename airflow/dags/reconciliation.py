"""
Reconciliation DAG: safety net for WebSocket consumers.

Runs every 4 hours. Polls REST APIs to find data that the
WebSocket consumers may have missed during connection drops.

Missing records are written to MinIO raw zone in the same
format as the consumers, so the regular etl_pipeline DAG
processes them without any special handling.

Only covers WebSocket sources (Binance, Blockchain).
REST-polling sources (CoinGecko, USGS) don't need reconciliation
because polling is already their primary mechanism.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from transforms.alerting import on_task_failure, on_task_retry, on_dag_success
sys.path.insert(0, str(Path(__file__).parent.parent / "transforms"))


def run_binance_reconciliation(**kwargs):
    from transforms.reconcile import reconcile_binance
    reconcile_binance(
        dag_id="reconciliation",
        run_id=kwargs.get("run_id", ""),
    )


def run_blockchain_reconciliation(**kwargs):
    from transforms.reconcile import reconcile_blockchain
    reconcile_blockchain(
        dag_id="reconciliation",
        run_id=kwargs.get("run_id", ""),
    )


def log_reconciliation_summary(**kwargs):
    from transforms.utils import get_db_connection
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dag_id, task_id, status, records_in, records_out,
                       details, created_at
                FROM pipeline_audit_log
                WHERE dag_id = 'reconciliation'
                  AND created_at > NOW() - INTERVAL '5 hours'
                ORDER BY created_at DESC
            """)
            rows = cur.fetchall()
            print(f"\nReconciliation history (last 5 hours):")
            for dag, task, status, r_in, r_out, details, ts in rows:
                print(f"  [{ts}] {task}: {status} "
                      f"(in={r_in}, out={r_out})")
    finally:
        conn.close()


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
}

with DAG(
    dag_id="reconciliation",
    default_args=default_args,
    description="Poll REST APIs to catch data missed by WebSocket consumers",
    schedule_interval=timedelta(hours=4),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["reconciliation", "data-quality"],
    on_success_callback=on_dag_success,
) as dag:

    reconcile_binance = PythonOperator(
        task_id="reconcile_binance",
        python_callable=run_binance_reconciliation,
    )

    reconcile_blockchain = PythonOperator(
        task_id="reconcile_blockchain",
        python_callable=run_blockchain_reconciliation,
    )

    summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_reconciliation_summary,
        trigger_rule="all_done",
    )

    [reconcile_binance, reconcile_blockchain] >> summary