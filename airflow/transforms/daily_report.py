"""
End-of-day structured report.

Aggregates the day's transformed data across all sources
and sends a formatted summary to Mattermost.

Phase 1 (now):  SQL aggregations, formatted as Markdown
Phase 2 (later): Pass aggregations to Ollama for natural language narrative
"""

import logging
from datetime import datetime, timezone

from transforms.utils import get_db_connection
from transforms.alerting import send_alert

logger = logging.getLogger(__name__)


def generate_daily_report(dag_id: str = "", run_id: str = "", **kwargs):
    """Build and send the end-of-day report to Mattermost."""

    conn = get_db_connection()
    sections = []

    try:
        with conn.cursor() as cur:

            # -- Crypto Trading Summary --
            cur.execute("""
                SELECT
                    COUNT(*) AS candle_count,
                    MIN(close_price) AS low,
                    MAX(close_price) AS high,
                    (SELECT close_price FROM fact_trade_candle ftc2
                     JOIN dim_time dt2 ON ftc2.time_id = dt2.time_id
                     JOIN dim_coin dc2 ON ftc2.coin_id = dc2.coin_id
                     WHERE dc2.symbol = 'btcusdt'
                       AND dt2.date = CURRENT_DATE
                     ORDER BY dt2.full_timestamp ASC LIMIT 1) AS open_price,
                    (SELECT close_price FROM fact_trade_candle ftc3
                     JOIN dim_time dt3 ON ftc3.time_id = dt3.time_id
                     JOIN dim_coin dc3 ON ftc3.coin_id = dc3.coin_id
                     WHERE dc3.symbol = 'btcusdt'
                       AND dt3.date = CURRENT_DATE
                     ORDER BY dt3.full_timestamp DESC LIMIT 1) AS close_price,
                    SUM(volume) AS total_volume,
                    SUM(trade_count) AS total_trades,
                    ROUND((SUM(buy_volume) / NULLIF(SUM(volume), 0) * 100)::numeric, 1)
                        AS buy_pct
                FROM fact_trade_candle ftc
                JOIN dim_time dt ON ftc.time_id = dt.time_id
                JOIN dim_coin dc ON ftc.coin_id = dc.coin_id
                WHERE dc.symbol = 'btcusdt'
                  AND dt.date = CURRENT_DATE
            """)
            row = cur.fetchone()
            if row and row[0] and row[0] > 0:
                candles, low, high, open_p, close_p, vol, trades, buy_pct = row
                change_pct = 0
                if open_p and close_p and open_p > 0:
                    change_pct = ((close_p - open_p) / open_p) * 100
                direction = "up" if change_pct >= 0 else "down"

                sections.append(
                    f"#### :chart_with_upwards_trend: BTC/USDT\n"
                    f"- Open: ${open_p:,.2f} | Close: ${close_p:,.2f} "
                    f"({direction} {abs(change_pct):.2f}%)\n"
                    f"- Range: ${low:,.2f} to ${high:,.2f}\n"
                    f"- Volume: ${vol:,.0f} across {trades:,} trades\n"
                    f"- Buy pressure: {buy_pct}%"
                )

            # -- BTC Transactions Summary --
            cur.execute("""
                SELECT
                    COUNT(*) AS tx_count,
                    ROUND(SUM(total_value_btc)::numeric, 4) AS total_btc,
                    ROUND(AVG(total_value_btc)::numeric, 6) AS avg_btc,
                    ROUND(MAX(total_value_btc)::numeric, 4) AS max_btc,
                    COUNT(*) FILTER (WHERE total_value_btc > 1.0) AS whale_count
                FROM fact_btc_transaction fbt
                JOIN dim_time dt ON fbt.time_id = dt.time_id
                WHERE dt.date = CURRENT_DATE
            """)
            row = cur.fetchone()
            if row and row[0] and row[0] > 0:
                tx_count, total, avg, max_tx, whales = row
                sections.append(
                    f"#### :link: Bitcoin Transactions\n"
                    f"- {tx_count:,} transactions processed\n"
                    f"- Total: {total} BTC | Avg: {avg} BTC\n"
                    f"- Largest: {max_tx} BTC\n"
                    f"- Whale transactions (>1 BTC): {whales}"
                )

            # -- Earthquake Summary --
            cur.execute("""
                SELECT
                    COUNT(*) AS quake_count,
                    ROUND(AVG(magnitude)::numeric, 2) AS avg_mag,
                    ROUND(MAX(magnitude)::numeric, 2) AS max_mag,
                    COUNT(*) FILTER (WHERE magnitude >= 4.0) AS significant,
                    (SELECT dl.place FROM fact_earthquake fe2
                     JOIN dim_location dl ON fe2.location_id = dl.location_id
                     JOIN dim_time dt2 ON fe2.time_id = dt2.time_id
                     WHERE dt2.date = CURRENT_DATE
                     ORDER BY fe2.magnitude DESC LIMIT 1) AS strongest_place
                FROM fact_earthquake fe
                JOIN dim_time dt ON fe.time_id = dt.time_id
                WHERE dt.date = CURRENT_DATE
            """)
            row = cur.fetchone()
            if row and row[0] and row[0] > 0:
                count, avg_m, max_m, sig, place = row
                sections.append(
                    f"#### :earth_africa: Earthquakes\n"
                    f"- {count} earthquakes detected today\n"
                    f"- Average magnitude: {avg_m} | Max: {max_m}\n"
                    f"- Significant (M4.0+): {sig}\n"
                    f"- Strongest: {place or 'N/A'}"
                )

            # -- Wikipedia Activity --
            cur.execute("""
                SELECT
                    COUNT(*) AS edit_count,
                    COUNT(DISTINCT user_name) AS unique_editors,
                    COUNT(*) FILTER (WHERE is_bot) AS bot_edits,
                    ROUND(
                        (COUNT(*) FILTER (WHERE is_bot)::numeric
                         / NULLIF(COUNT(*), 0) * 100), 1
                    ) AS bot_pct,
                    COUNT(DISTINCT wiki) AS wikis_active
                FROM fact_wiki_edit fwe
                JOIN dim_time dt ON fwe.time_id = dt.time_id
                WHERE dt.date = CURRENT_DATE
            """)
            row = cur.fetchone()
            if row and row[0] and row[0] > 0:
                edits, editors, bots, bot_pct, wikis = row
                sections.append(
                    f"#### :pencil: Wikipedia\n"
                    f"- {edits:,} edits across {wikis} wiki projects\n"
                    f"- {editors:,} unique editors\n"
                    f"- Bot edits: {bots:,} ({bot_pct}%)"
                )

            # -- Coinbase Ticker Summary --
            cur.execute("""
                SELECT
                    dc.symbol,
                    COUNT(*) AS tick_count,
                    ROUND(MIN(fet.price)::numeric, 2) AS low,
                    ROUND(MAX(fet.price)::numeric, 2) AS high,
                    ROUND(AVG(fet.spread)::numeric, 4) AS avg_spread
                FROM fact_exchange_ticker fet
                JOIN dim_coin dc ON fet.coin_id = dc.coin_id
                JOIN dim_time dt ON fet.time_id = dt.time_id
                WHERE dt.date = CURRENT_DATE
                  AND fet.exchange = 'coinbase'
                GROUP BY dc.symbol
                ORDER BY tick_count DESC
            """)
            rows = cur.fetchall()
            if rows:
                lines = []
                for symbol, ticks, low, high, spread in rows:
                    lines.append(
                        f"- {symbol.upper()}: "
                        f"${low:,} to ${high:,} | "
                        f"avg spread: ${spread}"
                    )
                sections.append(
                    f"#### :bank: Coinbase Tickers\n" + "\n".join(lines)
                )

            # -- NASA NEO Summary --
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (
                        WHERE is_potentially_hazardous
                    ) AS hazardous,
                    ROUND(MIN(miss_distance_km)::numeric, 0) AS closest_km,
                    (SELECT neo_name FROM fact_neo_approach
                     WHERE ingested_at::date = CURRENT_DATE
                     ORDER BY miss_distance_km ASC NULLS LAST
                     LIMIT 1) AS closest_name
                FROM fact_neo_approach
                WHERE ingested_at::date = CURRENT_DATE
            """)
            row = cur.fetchone()
            if row and row[0] and row[0] > 0:
                total, hazardous, closest_km, closest_name = row
                sections.append(
                    f"#### :comet: Near-Earth Objects\n"
                    f"- {total} asteroid approaches tracked\n"
                    f"- Potentially hazardous: {hazardous}\n"
                    f"- Closest approach: {closest_name or 'N/A'} "
                    f"({closest_km:,.0f} km)" if closest_km else ""
                )

            # -- Pipeline Health --
            cur.execute("""
                SELECT
                    source_name,
                    files_processed,
                    ROUND(EXTRACT(EPOCH FROM (NOW() - last_processed)) / 60)
                        AS minutes_behind
                FROM pipeline_watermark
                ORDER BY source_name
            """)
            watermarks = cur.fetchall()

            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'success') AS success,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                    SUM(records_out) AS total_records
                FROM pipeline_audit_log
                WHERE created_at::date = CURRENT_DATE
            """)
            audit = cur.fetchone()

            health_lines = []
            stale = []
            for source, files, behind in watermarks:
                status = ":white_check_mark:" if behind and behind < 30 else ":warning:"
                health_lines.append(
                    f"- {status} {source}: {files} files, "
                    f"{int(behind) if behind else 0} min behind"
                )
                if behind and behind > 30:
                    stale.append(source)

            success, failed, total_records = audit if audit else (0, 0, 0)
            sections.append(
                f"#### :heartbeat: Pipeline Health\n"
                + "\n".join(health_lines)
                + f"\n- Tasks today: {success or 0} success, "
                  f"{failed or 0} failed\n"
                  f"- Total records processed: {total_records or 0:,}"
            )

    finally:
        conn.close()

    # -- Assemble and send --
    if not sections:
        logger.info("No data for daily report")
        return

    today = datetime.now(timezone.utc).strftime("%A, %B %d, %Y")
    report = (
        f"## :bar_chart: Daily Pipeline Report\n"
        f"**{today}**\n\n"
        + "\n\n---\n\n".join(sections)
    )

    _send_report(report)
    logger.info("Daily report sent to Mattermost")


def _send_report(text: str):
    """Send the report to Mattermost."""
    import json
    import os
    import requests

    webhook_url = os.environ.get(
        "MATTERMOST_WEBHOOK_URL",
        "http://mattermost:8065/hooks/sjuxzsqsdpg35jror584mzqijo",
    )

    payload = {
        "username": "Daily Report",
        "text": text,
    }

    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.error(f"Mattermost returned {resp.status_code}")
    except requests.RequestException as e:
        logger.error(f"Failed to send report: {e}")