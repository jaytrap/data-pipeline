"""
Binance trade transform.

Reads raw trade JSON from MinIO, aggregates into 1-minute OHLCV candles,
and loads into the fact_trade_candle table.

Raw Binance trade payload looks like:
{
    "e": "trade",
    "s": "BTCUSDT",
    "p": "67234.50",    # price
    "q": "0.001",       # quantity
    "T": 1694100000123, # trade time (ms)
    "m": true           # is buyer the maker (true = sell, false = buy)
}

This transform groups trades by minute and computes:
  - Open, High, Low, Close prices
  - Total volume
  - Trade count
  - Buy vs sell volume
"""

import logging
from collections import defaultdict
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

SOURCE_NAME = "binance"
SOURCE_PATH = "binance/trades/"


def run(dag_id: str = "", run_id: str = "", **kwargs):
    """Main entry point called by Airflow."""

    log_audit(dag_id, "transform_binance", run_id, "started")

    # 1. Find new raw files since last run
    watermark = get_watermark(SOURCE_NAME)
    new_files = list_new_raw_files(SOURCE_PATH, since=watermark)

    if not new_files:
        logger.info("No new Binance files to process")
        log_audit(dag_id, "transform_binance", run_id, "success",
                  details={"message": "no new files"})
        return

    # 2. Read all raw trades
    all_trades = []
    for file_path in new_files:
        payloads = read_raw_file(file_path)
        for payload in payloads:
            # Skip non-trade payloads
            if isinstance(payload, dict) and "p" in payload and "q" in payload:
                all_trades.append(payload)

    logger.info(f"Read {len(all_trades)} raw trades from {len(new_files)} files")

    if not all_trades:
        update_watermark(SOURCE_NAME, datetime.now(timezone.utc), len(new_files))
        log_audit(dag_id, "transform_binance", run_id, "success",
                  records_in=0, records_out=0)
        return

    # 3. Group trades by minute and compute OHLCV candles
    candles = _aggregate_candles(all_trades)
    logger.info(f"Aggregated into {len(candles)} 1-minute candles")

    # 4. Load candles into the warehouse
    coin_id = ensure_coin_dimension("btcusdt", "Bitcoin/USDT")
    records_out = _load_candles(candles, coin_id)

    # 5. Update watermark
    update_watermark(SOURCE_NAME, datetime.now(timezone.utc), len(new_files))

    log_audit(dag_id, "transform_binance", run_id, "success",
              records_in=len(all_trades), records_out=records_out)
    logger.info(f"Binance transform complete: {len(all_trades)} trades "
                f"-> {records_out} candles loaded")


def _aggregate_candles(trades: list[dict]) -> dict:
    """
    Group trades by truncated minute and compute OHLCV.
    Returns {minute_timestamp: candle_dict}
    """
    buckets = defaultdict(list)

    for trade in trades:
        try:
            trade_time_ms = int(trade.get("T", 0))
            if trade_time_ms == 0:
                continue
            ts = datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc)
            minute_key = ts.replace(second=0, microsecond=0)
            buckets[minute_key].append(trade)
        except (ValueError, TypeError):
            continue

    candles = {}
    for minute_ts, minute_trades in buckets.items():
        prices = []
        total_volume = 0.0
        buy_volume = 0.0
        sell_volume = 0.0

        for t in minute_trades:
            price = float(t["p"])
            qty = float(t["q"])
            prices.append(price)
            total_volume += price * qty

            # m=true means buyer is maker, so this is a sell
            if t.get("m", False):
                sell_volume += price * qty
            else:
                buy_volume += price * qty

        candles[minute_ts] = {
            "timestamp": minute_ts,
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": total_volume,
            "trade_count": len(minute_trades),
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
        }

    return candles


def _load_candles(candles: dict, coin_id: int) -> int:
    """Insert candles into fact_trade_candle. Returns count loaded."""
    conn = get_db_connection()
    loaded = 0

    try:
        with conn.cursor() as cur:
            for minute_ts, candle in candles.items():
                time_id = ensure_time_dimension(minute_ts)

                cur.execute(
                    """
                    INSERT INTO fact_trade_candle
                        (coin_id, time_id, open_price, high_price,
                         low_price, close_price, volume, trade_count,
                         buy_volume, sell_volume, ingested_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (coin_id, time_id) DO UPDATE SET
                        high_price = GREATEST(
                            fact_trade_candle.high_price,
                            EXCLUDED.high_price
                        ),
                        low_price = LEAST(
                            fact_trade_candle.low_price,
                            EXCLUDED.low_price
                        ),
                        close_price = EXCLUDED.close_price,
                        volume = fact_trade_candle.volume + EXCLUDED.volume,
                        trade_count = fact_trade_candle.trade_count
                            + EXCLUDED.trade_count,
                        buy_volume = fact_trade_candle.buy_volume
                            + EXCLUDED.buy_volume,
                        sell_volume = fact_trade_candle.sell_volume
                            + EXCLUDED.sell_volume
                    """,
                    (
                        coin_id, time_id,
                        candle["open"], candle["high"],
                        candle["low"], candle["close"],
                        candle["volume"], candle["trade_count"],
                        candle["buy_volume"], candle["sell_volume"],
                    ),
                )
                loaded += 1

        conn.commit()
    finally:
        conn.close()

    return loaded
