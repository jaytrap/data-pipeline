"""
CoinGecko price transform.

Reads raw price snapshots from MinIO, normalizes into per-coin rows,
and loads into fact_price_snapshot.

Raw CoinGecko payload looks like:
{
    "bitcoin": {
        "usd": 67234.50,
        "eur": 61234.20,
        "usd_market_cap": 1323456789012,
        "usd_24h_change": 2.34
    },
    "ethereum": { ... },
    "solana": { ... }
}

This transform flattens the nested structure into one row per coin
per snapshot timestamp.
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
    ensure_coin_dimension,
    log_audit,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "coingecko"
SOURCE_PATH = "coingecko/prices/"

# Map CoinGecko IDs to symbols
COIN_MAP = {
    "bitcoin": ("btc", "Bitcoin"),
    "ethereum": ("eth", "Ethereum"),
    "solana": ("sol", "Solana"),
}


def run(dag_id: str = "", run_id: str = "", **kwargs):
    """Main entry point called by Airflow."""

    log_audit(dag_id, "transform_coingecko", run_id, "started")

    watermark = get_watermark(SOURCE_NAME)
    new_files = list_new_raw_files(SOURCE_PATH, since=watermark)

    if not new_files:
        logger.info("No new CoinGecko files to process")
        log_audit(dag_id, "transform_coingecko", run_id, "success",
                  details={"message": "no new files"})
        return

    records_in = 0
    records_out = 0
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            for file_path in new_files:
                payloads = read_raw_file(file_path)

                for snapshot in payloads:
                    if not isinstance(snapshot, dict):
                        continue

                    # Use current time as snapshot timestamp
                    # (CoinGecko doesn't include timestamps in simple/price)
                    now = datetime.now(timezone.utc)
                    time_id = ensure_time_dimension(now)

                    for gecko_id, coin_data in snapshot.items():
                        if gecko_id not in COIN_MAP:
                            continue
                        if not isinstance(coin_data, dict):
                            continue

                        symbol, name = COIN_MAP[gecko_id]
                        coin_id = ensure_coin_dimension(symbol, name)

                        price_usd = coin_data.get("usd")
                        price_eur = coin_data.get("eur")
                        market_cap = coin_data.get("usd_market_cap")
                        change_24h = coin_data.get("usd_24h_change")

                        records_in += 1

                        cur.execute(
                            """
                            INSERT INTO fact_price_snapshot
                                (coin_id, time_id, price_usd, price_eur,
                                 market_cap_usd, change_24h_pct, ingested_at)
                            VALUES (%s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (coin_id, time_id) DO UPDATE SET
                                price_usd = EXCLUDED.price_usd,
                                price_eur = EXCLUDED.price_eur,
                                market_cap_usd = EXCLUDED.market_cap_usd,
                                change_24h_pct = EXCLUDED.change_24h_pct
                            """,
                            (
                                coin_id, time_id,
                                price_usd, price_eur,
                                market_cap, change_24h,
                            ),
                        )
                        records_out += 1

        conn.commit()
    finally:
        conn.close()

    update_watermark(SOURCE_NAME, datetime.now(timezone.utc), len(new_files))

    log_audit(dag_id, "transform_coingecko", run_id, "success",
              records_in=records_in, records_out=records_out)
    logger.info(f"CoinGecko transform complete: {records_in} snapshots "
                f"-> {records_out} rows loaded")
