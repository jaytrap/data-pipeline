"""
Coinbase ticker transform.

Reads raw ticker events from MinIO and loads into fact_exchange_ticker.

Raw Coinbase ticker payload:
{
    "type": "ticker",
    "product_id": "BTC-USD",
    "price": "67234.50",
    "best_bid": "67234.00",
    "best_ask": "67235.00",
    "volume_24h": "12345.678",
    "time": "2026-09-07T22:00:00.000000Z"
}
"""

import logging
from datetime import datetime, timezone
from dateutil import parser as dateparser

from transforms.utils import (
    get_db_connection,
    get_watermark,
    update_watermark,
    list_new_raw_files,
    read_raw_file,
    ensure_time_dimension,
    ensure_coin_dimension,
    log_audit,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "coinbase"
SOURCE_PATH = "coinbase/"

PRODUCT_MAP = {
    "BTC-USD": ("btc", "Bitcoin"),
    "ETH-USD": ("eth", "Ethereum"),
}


def run(dag_id: str = "", run_id: str = "", **kwargs):
    log_audit(dag_id, "transform_coinbase", run_id, "started")

    watermark = get_watermark(SOURCE_NAME)
    new_files = list_new_raw_files(SOURCE_PATH, since=watermark)

    if not new_files:
        logger.info("No new Coinbase files to process")
        log_audit(dag_id, "transform_coinbase", run_id, "success",
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

                    if payload.get("type") != "ticker":
                        continue

                    product_id = payload.get("product_id", "")
                    if product_id not in PRODUCT_MAP:
                        continue

                    records_in += 1

                    symbol, name = PRODUCT_MAP[product_id]
                    coin_id = ensure_coin_dimension(symbol, name)

                    # Parse timestamp
                    time_str = payload.get("time", "")
                    try:
                        event_time = dateparser.isoparse(time_str)
                        if event_time.tzinfo is None:
                            event_time = event_time.replace(
                                tzinfo=timezone.utc
                            )
                    except (ValueError, TypeError):
                        event_time = datetime.now(timezone.utc)

                    time_id = ensure_time_dimension(event_time)

                    price = _safe_float(payload.get("price"))
                    best_bid = _safe_float(payload.get("best_bid"))
                    best_ask = _safe_float(payload.get("best_ask"))
                    volume_24h = _safe_float(payload.get("volume_24h"))

                    spread = None
                    if best_bid and best_ask:
                        spread = best_ask - best_bid

                    cur.execute(
                        """
                        INSERT INTO fact_exchange_ticker
                            (coin_id, time_id, exchange, price,
                             best_bid, best_ask, spread,
                             volume_24h, ingested_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            coin_id, time_id, "coinbase",
                            price, best_bid, best_ask, spread,
                            volume_24h,
                        ),
                    )
                    records_out += 1

        conn.commit()
    finally:
        conn.close()

    update_watermark(SOURCE_NAME, datetime.now(timezone.utc), len(new_files))

    log_audit(dag_id, "transform_coinbase", run_id, "success",
              records_in=records_in, records_out=records_out)
    logger.info(f"Coinbase transform: {records_in} tickers "
                f"-> {records_out} rows loaded")


def _safe_float(val) -> float:
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None