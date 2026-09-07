"""
Blockchain.com transaction transform.

Reads raw unconfirmed Blockchain transactions from MinIO,
extract key metrics, and loads into fact_btc_transaction.


Raw payload from Blockchain.com WebSocket:
{
    "op": "utx",
    "x": {
        "hash": "abc123def...",
        "time": 1694100000,
        "inputs": [
            {"prev_out": {"value": 50000000, "addr": "1A1z..."}}
        ],
        "out": [
            {"value": 30000000, "addr": "3J98..."},
            {"value": 19950000, "addr": "bc1q..."}
        ]
    }
}

Values are in satoshis (1 BTC = 100,000,000 satoshis).
Deduplication uses the transaction hash.
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

SOURCE_NAME = "blockchain"
SOURCE_PATH = "blockchain/transactions/"
SATOSHIS_PER_BTC = 100_000_000

def run(dag_id: str = "", run_id: str = "", **kwargs):
    """Main entry point called by Airflow."""

    log_audit(dag_id, "transform_blockchain", run_id, "started")

    watermark = get_watermark(SOURCE_NAME)
    new_files = list_new_raw_files(SOURCE_PATH, since=watermark)

    if not new_files:
        logger.info("No new Blockchain files to process")
        log_audit(dag_id, "transform_blockchain", run_id, "success",
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

                    # The transaction data sits under the "x" key
                    tx = payload.get("x", payload)

                    tx_hash = tx.get("hash")
                    if not tx_hash:
                        continue

                    records_in += 1

                    # Parse timestamp
                    tx_time = tx.get("time", 0)
                    if tx_time:
                        event_time = datetime.fromtimestamp(
                            tx_time, tz=timezone.utc
                        )
                    else:
                        event_time = datetime.now(timezone.utc)

                    time_id = ensure_time_dimension(event_time)

                    # Calculate total output value in BTC
                    outputs = tx.get("out", [])
                    total_satoshis = sum(
                        o.get("value", 0) for o in outputs
                    )
                    total_btc = total_satoshis / SATOSHIS_PER_BTC

                    input_count = len(tx.get("inputs", []))
                    output_count = len(outputs)

                    cur.execute(
                        """
                        INSERT INTO fact_btc_transaction
                            (tx_hash, time_id, total_value_btc,
                             output_count, input_count, ingested_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (tx_hash) DO NOTHING
                        """,
                        (
                            tx_hash, time_id, total_btc,
                            output_count, input_count,
                        ),
                    )
                    if cur.rowcount > 0:
                        records_out += 1

        conn.commit()
    finally:
        conn.close()

    update_watermark(SOURCE_NAME, datetime.now(timezone.utc), len(new_files))

    log_audit(dag_id, "transform_blockchain", run_id, "success",
              records_in=records_in, records_out=records_out)
    logger.info(f"Blockchain transform complete: {records_in} transactions "
                f"-> {records_out} new rows loaded")