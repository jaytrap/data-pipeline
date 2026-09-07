"""
Reconciliation module.

Polls REST APIs on a schedule to catch data missed by
WebSocket consumers during connection drops.

For each source:
  1. Find the latest timestamp in the warehouse
  2. Fetch recent data from the REST API
  3. Identify records not already in the warehouse
  4. Write missing records to MinIO raw zone
  5. Regular ETL picks them up on next run

This is the safety net, not the primary ingestion path.
"""

import io
import json
import logging
import uuid
from datetime import datetime, timezone, timedelta

import requests
from minio import Minio

from transforms.utils import (
    get_db_connection,
    get_minio_client,
    log_audit,
)

logger = logging.getLogger(__name__)


def reconcile_binance(dag_id: str = "", run_id: str = "", **kwargs):
    """
    Fetch recent BTC/USDT trades from Binance REST API
    and fill any gaps in the warehouse.

    Binance REST endpoint:
      GET https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=1000

    Returns the most recent 1000 trades. We compare against
    what's in fact_trade_candle to find missing minutes.
    """
    log_audit(dag_id, "reconcile_binance", run_id, "started")

    # 1. Find the latest candle we have
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dt.full_timestamp
                FROM fact_trade_candle ftc
                JOIN dim_time dt ON ftc.time_id = dt.time_id
                ORDER BY dt.full_timestamp DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                latest = row[0]
            else:
                latest = datetime.now(timezone.utc) - timedelta(hours=4)
    finally:
        conn.close()

    logger.info(f"Binance reconciliation: latest candle at {latest}")

    # 2. Fetch recent trades from REST API
    try:
        resp = requests.get(
            "https://api.binance.com/api/v3/trades",
            params={"symbol": "BTCUSDT", "limit": 1000},
            timeout=30,
        )
        resp.raise_for_status()
        trades = resp.json()
    except requests.RequestException as e:
        logger.error(f"Binance REST API failed: {e}")
        log_audit(dag_id, "reconcile_binance", run_id, "failed",
                  details={"error": str(e)})
        return

    if not trades:
        logger.info("No trades returned from Binance API")
        log_audit(dag_id, "reconcile_binance", run_id, "success",
                  details={"message": "no trades from API"})
        return

    # 3. Filter to trades newer than our latest candle
    latest_ts = int(latest.timestamp() * 1000) if latest else 0
    missing_trades = [
        t for t in trades
        if t.get("time", 0) > latest_ts
    ]

    if not missing_trades:
        logger.info("No missing Binance trades found")
        log_audit(dag_id, "reconcile_binance", run_id, "success",
                  records_in=len(trades), records_out=0,
                  details={"message": "no gaps detected"})
        return

    # 4. Convert to the same format as WebSocket messages
    #    so the regular transform can process them
    normalized = []
    for t in missing_trades:
        normalized.append({
            "e": "trade",
            "s": "BTCUSDT",
            "p": str(t["price"]),
            "q": str(t["qty"]),
            "T": t["time"],
            "m": t.get("isBuyerMaker", False),
        })

    # 5. Write to MinIO raw zone
    written = _write_to_raw(
        source_path="binance/trades/",
        records=normalized,
        label="binance_reconciliation",
    )

    log_audit(dag_id, "reconcile_binance", run_id, "success",
              records_in=len(trades), records_out=len(normalized),
              details={
                  "latest_candle": latest.isoformat(),
                  "api_trades": len(trades),
                  "missing_trades": len(normalized),
                  "minio_path": written,
              })
    logger.info(
        f"Binance reconciliation: found {len(normalized)} missing trades, "
        f"wrote to {written}"
    )


def reconcile_blockchain(dag_id: str = "", run_id: str = "", **kwargs):
    """
    Fetch recent unconfirmed transactions from Blockchain.com REST API
    and fill any gaps in the warehouse.

    Blockchain.com REST endpoint:
      GET https://blockchain.info/unconfirmed-transactions?format=json

    Returns recent unconfirmed transactions. We compare tx hashes
    against what's in fact_btc_transaction.
    """
    log_audit(dag_id, "reconcile_blockchain", run_id, "started")

    # 1. Get existing transaction hashes from the last 4 hours
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tx_hash FROM fact_btc_transaction
                WHERE ingested_at > NOW() - INTERVAL '4 hours'
            """)
            existing_hashes = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    logger.info(
        f"Blockchain reconciliation: {len(existing_hashes)} "
        f"existing transactions in last 4 hours"
    )

    # 2. Fetch recent unconfirmed transactions
    try:
        resp = requests.get(
            "https://blockchain.info/unconfirmed-transactions",
            params={"format": "json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        api_txs = data.get("txs", [])
    except requests.RequestException as e:
        logger.error(f"Blockchain.com REST API failed: {e}")
        log_audit(dag_id, "reconcile_blockchain", run_id, "failed",
                  details={"error": str(e)})
        return

    if not api_txs:
        logger.info("No transactions returned from Blockchain API")
        log_audit(dag_id, "reconcile_blockchain", run_id, "success",
                  details={"message": "no txs from API"})
        return

    # 3. Find transactions we don't have
    missing_txs = []
    for tx in api_txs:
        tx_hash = tx.get("hash", "")
        if tx_hash and tx_hash not in existing_hashes:
            # Wrap in the same format as WebSocket messages
            missing_txs.append({
                "op": "utx",
                "x": tx,
            })

    if not missing_txs:
        logger.info("No missing Blockchain transactions found")
        log_audit(dag_id, "reconcile_blockchain", run_id, "success",
                  records_in=len(api_txs), records_out=0,
                  details={"message": "no gaps detected"})
        return

    # 4. Write to MinIO raw zone
    written = _write_to_raw(
        source_path="blockchain/transactions/",
        records=missing_txs,
        label="blockchain_reconciliation",
    )

    log_audit(dag_id, "reconcile_blockchain", run_id, "success",
              records_in=len(api_txs), records_out=len(missing_txs),
              details={
                  "api_transactions": len(api_txs),
                  "existing_hashes": len(existing_hashes),
                  "missing_transactions": len(missing_txs),
                  "minio_path": written,
              })
    logger.info(
        f"Blockchain reconciliation: found {len(missing_txs)} missing "
        f"transactions, wrote to {written}"
    )


def _write_to_raw(source_path: str, records: list[dict],
                   label: str = "reconciliation") -> str:
    """
    Write reconciliation records to MinIO raw zone as NDJSON.
    Uses the same envelope format as the consumer so the
    regular ETL transforms can process them without changes.
    """
    client = get_minio_client()
    now = datetime.now(timezone.utc)
    date_part = now.strftime("%Y-%m-%d")
    timestamp = int(now.timestamp() * 1000)
    unique_id = uuid.uuid4().hex[:8]
    object_path = (
        f"{source_path}{date_part}/"
        f"{timestamp}_{label}_{unique_id}.json"
    )

    lines = []
    for record in records:
        envelope = {
            "ingested_at": now.isoformat(),
            "source_path": source_path.rstrip("/"),
            "reconciliation": True,
            "payload": record,
        }
        lines.append(json.dumps(envelope, default=str))

    data = ("\n".join(lines) + "\n").encode("utf-8")
    stream = io.BytesIO(data)

    client.put_object(
        "raw",
        object_path,
        stream,
        length=len(data),
        content_type="application/x-ndjson",
    )

    logger.info(f"Wrote {len(records)} reconciliation records to {object_path}")
    return object_path