"""
Daily Report DAG.

Runs once at 23:55 UTC every day. Aggregates all transformed data
from the day and sends a structured summary to Mattermost.

Phase 1 (now):  SQL aggregations formatted as Markdown
Phase 2 (later): Pass aggregations to Ollama for a natural language
                 narrative ("Today was a volatile day for BTC...")
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).parent.parent / "transforms"))

from transforms.alerting import on_task_failure


def run_daily_report(**kwargs):
    from transforms.daily_report import generate_daily_report
    generate_daily_report(
        dag_id="daily_report",
        run_id=kwargs.get("run_id", ""),
    )


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_task_failure,
}

with DAG(
    dag_id="daily_report",
    default_args=default_args,
    description="End-of-day summary report sent to Mattermost",
    schedule_interval="55 23 * * *",  # 23:55 UTC daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["reporting", "daily"],
) as dag:

    report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=run_daily_report,
    )