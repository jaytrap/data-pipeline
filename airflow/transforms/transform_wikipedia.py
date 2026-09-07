"""
Wikipedia edit transform.

Reads raw SSE events from MinIO, extracts edit metadata,
and loads into fact_wiki_edit.

Raw Wikimedia SSE payload:
{
    "id": 1234567890,
    "type": "edit",
    "wiki": "enwiki",
    "title": "Some Article",
    "user": "EditorName",
    "bot": false,
    "minor": false,
    "length": {"old": 5000, "new": 5200},
    "timestamp": 1694100000
}
"""

import logging
from datetime import datetime, timezone

from transforms.utils import (
    get_db_connection,
    get_watermark,
    update_watermark,
    list_new_raw_files,
    read_raw_file,
    ensure_time_dimension,
    log_audit,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "wikipedia"
SOURCE_PATH = "wikipedia/"


def run(dag_id: str = "", run_id: str = "", **kwargs):
    log_audit(dag_id, "transform_wikipedia", run_id, "started")

    watermark = get_watermark(SOURCE_NAME)
    new_files = list_new_raw_files(SOURCE_PATH, since=watermark)

    if not new_files:
        logger.info("No new Wikipedia files to process")
        log_audit(dag_id, "transform_wikipedia", run_id, "success",
                  details={"message": "no new files"})
        return

    records_in = 0
    records_out = 0
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            for file_path in new_files:
                payloads = read_raw_file(file_path)

                for payload in payloads:
                    if not isinstance(payload, dict):
                        continue

                    # Skip non-edit events (log actions, categorize, etc.)
                    event_type = payload.get("type", "")
                    if event_type not in ("edit", "new"):
                        continue

                    change_id = payload.get("id")
                    if not change_id:
                        continue

                    records_in += 1

                    # Parse timestamp
                    ts = payload.get("timestamp")
                    if ts:
                        if isinstance(ts, (int, float)):
                            event_time = datetime.fromtimestamp(
                                ts, tz=timezone.utc
                            )
                        else:
                            event_time = datetime.now(timezone.utc)
                    else:
                        event_time = datetime.now(timezone.utc)

                    time_id = ensure_time_dimension(event_time)

                    # Calculate change size
                    length = payload.get("length", {})
                    old_len = length.get("old", 0) or 0
                    new_len = length.get("new", 0) or 0
                    change_size = new_len - old_len

                    cur.execute(
                        """
                        INSERT INTO fact_wiki_edit
                            (wiki_change_id, time_id, wiki, title,
                             user_name, is_bot, is_minor, change_size,
                             edit_type, ingested_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (wiki_change_id) DO NOTHING
                        """,
                        (
                            change_id, time_id,
                            payload.get("wiki", "unknown"),
                            (payload.get("title", "")[:500]),
                            payload.get("user", "anonymous"),
                            payload.get("bot", False),
                            payload.get("minor", False),
                            change_size,
                            event_type,
                        ),
                    )
                    if cur.rowcount > 0:
                        records_out += 1

        conn.commit()
    finally:
        conn.close()

    update_watermark(SOURCE_NAME, datetime.now(timezone.utc), len(new_files))

    log_audit(dag_id, "transform_wikipedia", run_id, "success",
              records_in=records_in, records_out=records_out)
    logger.info(f"Wikipedia transform: {records_in} events "
                f"-> {records_out} edits loaded")