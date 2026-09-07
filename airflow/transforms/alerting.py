"""
Alerting module for pipeline notifications.

Sends messages to Mattermost via incoming webhook.
Supports three alert levels:
  - info:    routine notifications (pipeline complete, reconciliation ran)
  - warning: something unusual (high latency, partial failure)
  - critical: something broke (task failed after retries, data gap detected)

Mattermost webhook format is Slack-compatible:
  POST /hooks/xxx  {"text": "message", "username": "bot-name"}
"""

import json
import logging
import os
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

WEBHOOK_URL = os.environ.get(
    "MATTERMOST_WEBHOOK_URL",
    "http://mattermost:8065/hooks/sjuxzsqsdpg35jror584mzqijo",
)
BOT_NAME = "Pipeline Bot"
CHANNEL = ""  # empty = uses the channel configured in the webhook


def send_alert(level: str, title: str, message: str, details: dict = None):
    """
    Send an alert to Mattermost.

    level: info, warning, critical
    title: short summary
    message: full description
    details: optional dict of extra context
    """
    emoji = {
        "info": ":white_check_mark:",
        "warning": ":warning:",
        "critical": ":rotating_light:",
    }.get(level, ":speech_balloon:")

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    text = f"### {emoji} {title}\n\n{message}\n\n*{timestamp}*"

    if details:
        detail_lines = "\n".join(
            f"- **{k}**: {v}" for k, v in details.items()
        )
        text += f"\n\n{detail_lines}"

    payload = {
        "username": BOT_NAME,
        "text": text,
    }

    try:
        resp = requests.post(
            WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info(f"Alert sent: [{level}] {title}")
        else:
            logger.error(
                f"Mattermost webhook returned {resp.status_code}: "
                f"{resp.text}"
            )
    except requests.RequestException as e:
        logger.error(f"Failed to send alert: {e}")


# -- Airflow callback functions --
# These get called automatically by Airflow on task events.

def on_task_failure(context):
    """Called when any task fails after all retries are exhausted."""
    task = context.get("task_instance")
    dag_id = task.dag_id
    task_id = task.task_id
    execution_date = context.get("execution_date", "")
    exception = context.get("exception", "")
    log_url = task.log_url

    send_alert(
        level="critical",
        title=f"Task failed: {dag_id}.{task_id}",
        message=f"Task `{task_id}` in DAG `{dag_id}` failed after all retries.",
        details={
            "DAG": dag_id,
            "Task": task_id,
            "Execution date": str(execution_date),
            "Error": str(exception)[:500],
            "Log URL": log_url,
        },
    )


def on_task_retry(context):
    """Called when a task fails and is about to retry."""
    task = context.get("task_instance")
    try_number = task.try_number
    max_tries = task.max_tries

    send_alert(
        level="warning",
        title=f"Task retrying: {task.dag_id}.{task.task_id}",
        message=(
            f"Task `{task.task_id}` failed, retrying "
            f"(attempt {try_number} of {max_tries})."
        ),
        details={
            "DAG": task.dag_id,
            "Task": task.task_id,
            "Error": str(context.get("exception", ""))[:300],
        },
    )


def on_dag_success(context):
    """Called when the entire DAG completes successfully."""
    dag_id = context.get("dag").dag_id

    # Only send success alerts for the main ETL pipeline,
    # not every 5-minute run. Send one per hour max.
    if dag_id == "reconciliation":
        send_alert(
            level="info",
            title="Reconciliation complete",
            message="Reconciliation DAG ran successfully.",
        )

def check_data_freshness(**kwargs):
    """
    Check if any source has fallen behind.
    Called as an Airflow task in the ETL pipeline.
    """
    from transforms.utils import get_db_connection

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_name, last_processed,
                    EXTRACT(EPOCH FROM (NOW() - last_processed)) / 60
                        AS minutes_behind
                FROM pipeline_watermark
            """)
            stale_sources = []
            for source, last_ts, minutes in cur.fetchall():
                # Alert if any source is more than 30 minutes behind
                if minutes and minutes > 30:
                    stale_sources.append({
                        "source": source,
                        "last_data": str(last_ts),
                        "minutes_behind": round(minutes),
                    })

            if stale_sources:
                details = {
                    s["source"]: f"{s['minutes_behind']} min behind"
                    for s in stale_sources
                }
                send_alert(
                    level="warning",
                    title="Stale data detected",
                    message=(
                        f"{len(stale_sources)} source(s) have not "
                        f"received fresh data in over 30 minutes."
                    ),
                    details=details,
                )
            else:
                logger.info("All sources are fresh")
    finally:
        conn.close()