"""
Anomaly detection module.

Checks for unusual patterns in the warehouse data and sends
alerts to Mattermost when thresholds are breached.

Anomalies detected:
  - BTC price spike or crash (> 2% move in 5 minutes)
  - Whale BTC transaction (> 10 BTC)
  - Earthquake above magnitude 5.0
  - Hazardous asteroid approaching Earth
  - Wikipedia edit velocity spike (vandalism indicator)
  - Cross-exchange price divergence (arbitrage opportunity)
  - Data gap (source stopped producing)
"""

import logging
from datetime import datetime, timezone

from transforms.utils import get_db_connection
from transforms.alerting import send_alert

logger = logging.getLogger(__name__)


def check_all_anomalies(dag_id: str = "", run_id: str = "", **kwargs):
    """Run all anomaly checks. Called by Airflow."""
    check_btc_price_movement()
    check_whale_transactions()
    check_significant_earthquakes()
    check_hazardous_asteroids()
    check_wiki_edit_spike()
    check_exchange_price_divergence()


def check_btc_price_movement():
    """Alert if BTC price moved more than 2% in the last 5 minutes."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH recent AS (
                    SELECT
                        dt.full_timestamp,
                        ftc.close_price,
                        LAG(ftc.close_price, 5) OVER (
                            ORDER BY dt.full_timestamp
                        ) AS price_5min_ago
                    FROM fact_trade_candle ftc
                    JOIN dim_time dt ON ftc.time_id = dt.time_id
                    JOIN dim_coin dc ON ftc.coin_id = dc.coin_id
                    WHERE dc.symbol = 'btcusdt'
                      AND dt.full_timestamp > NOW() - INTERVAL '15 minutes'
                    ORDER BY dt.full_timestamp DESC
                )
                SELECT
                    close_price,
                    price_5min_ago,
                    CASE
                        WHEN price_5min_ago > 0
                        THEN ((close_price - price_5min_ago)
                              / price_5min_ago * 100)
                        ELSE 0
                    END AS pct_change
                FROM recent
                WHERE price_5min_ago IS NOT NULL
                ORDER BY ABS(
                    (close_price - price_5min_ago)
                    / NULLIF(price_5min_ago, 0) * 100
                ) DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                price, prev_price, pct = row
                if abs(pct) > 2.0:
                    direction = "spike" if pct > 0 else "crash"
                    send_alert(
                        level="warning",
                        title=f"BTC price {direction}: {pct:+.2f}%",
                        message=(
                            f"BTC moved {pct:+.2f}% in the last 5 minutes."
                        ),
                        details={
                            "Current price": f"${price:,.2f}",
                            "Previous price": f"${prev_price:,.2f}",
                            "Change": f"{pct:+.2f}%",
                        },
                    )
    finally:
        conn.close()


def check_whale_transactions():
    """Alert on BTC transactions over 10 BTC in the last 10 minutes."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    fbt.tx_hash,
                    fbt.total_value_btc,
                    fbt.output_count,
                    dt.full_timestamp
                FROM fact_btc_transaction fbt
                JOIN dim_time dt ON fbt.time_id = dt.time_id
                WHERE fbt.total_value_btc > 10
                  AND fbt.ingested_at > NOW() - INTERVAL '10 minutes'
                ORDER BY fbt.total_value_btc DESC
                LIMIT 5
            """)
            rows = cur.fetchall()
            if rows:
                total_btc = sum(r[1] for r in rows)
                send_alert(
                    level="warning",
                    title=f"Whale alert: {len(rows)} large BTC transactions",
                    message=(
                        f"Detected {len(rows)} transactions over 10 BTC "
                        f"in the last 10 minutes. "
                        f"Total: {total_btc:.4f} BTC."
                    ),
                    details={
                        f"TX {r[0][:16]}...": f"{r[1]:.4f} BTC ({r[2]} outputs)"
                        for r in rows
                    },
                )
    finally:
        conn.close()


def check_significant_earthquakes():
    """Alert on earthquakes above magnitude 5.0 in the last 30 minutes."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    fe.magnitude,
                    fe.mag_type,
                    dl.place,
                    dl.region,
                    dl.depth_km,
                    dt.full_timestamp
                FROM fact_earthquake fe
                JOIN dim_time dt ON fe.time_id = dt.time_id
                JOIN dim_location dl ON fe.location_id = dl.location_id
                WHERE fe.magnitude >= 5.0
                  AND fe.ingested_at > NOW() - INTERVAL '30 minutes'
                ORDER BY fe.magnitude DESC
            """)
            rows = cur.fetchall()
            for mag, mag_type, place, region, depth, ts in rows:
                level = "critical" if mag >= 6.0 else "warning"
                send_alert(
                    level=level,
                    title=f"Earthquake M{mag:.1f} detected",
                    message=f"A magnitude {mag:.1f} earthquake occurred.",
                    details={
                        "Location": place,
                        "Region": region,
                        "Depth": f"{depth:.1f} km" if depth else "Unknown",
                        "Magnitude type": mag_type,
                        "Time": str(ts),
                    },
                )
    finally:
        conn.close()


def check_hazardous_asteroids():
    """Alert on potentially hazardous asteroids."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    neo_name,
                    miss_distance_km,
                    velocity_kph,
                    estimated_diameter_max,
                    close_approach_date
                FROM fact_neo_approach
                WHERE is_potentially_hazardous = TRUE
                  AND ingested_at > NOW() - INTERVAL '2 hours'
                ORDER BY miss_distance_km ASC
                LIMIT 5
            """)
            rows = cur.fetchall()
            for name, dist, vel, diam, approach_date in rows:
                send_alert(
                    level="info",
                    title=f"Hazardous asteroid: {name}",
                    message=(
                        f"Potentially hazardous asteroid approaching Earth."
                    ),
                    details={
                        "Name": name,
                        "Approach date": str(approach_date),
                        "Miss distance": (
                            f"{dist:,.0f} km" if dist else "Unknown"
                        ),
                        "Velocity": (
                            f"{vel:,.0f} km/h" if vel else "Unknown"
                        ),
                        "Est. diameter": (
                            f"{diam*1000:.0f} meters" if diam else "Unknown"
                        ),
                    },
                )
    finally:
        conn.close()


def check_wiki_edit_spike():
    """
    Alert if Wikipedia edit rate is 3x the normal rate.
    Potential indicator of coordinated editing or vandalism.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH hourly AS (
                    SELECT
                        DATE_TRUNC('hour', dt.full_timestamp) AS hour,
                        COUNT(*) AS edit_count
                    FROM fact_wiki_edit fwe
                    JOIN dim_time dt ON fwe.time_id = dt.time_id
                    WHERE dt.full_timestamp > NOW() - INTERVAL '24 hours'
                    GROUP BY 1
                )
                SELECT
                    AVG(edit_count) AS avg_edits,
                    MAX(edit_count) AS max_edits,
                    (SELECT edit_count FROM hourly
                     ORDER BY hour DESC LIMIT 1) AS current_hour
                FROM hourly
                WHERE hour < DATE_TRUNC('hour', NOW())
            """)
            row = cur.fetchone()
            if row and row[0] and row[2]:
                avg_edits, max_edits, current = row
                if avg_edits > 0 and current > avg_edits * 3:
                    send_alert(
                        level="warning",
                        title="Wikipedia edit spike detected",
                        message=(
                            f"Current hour has {current:.0f} edits, "
                            f"which is {current/avg_edits:.1f}x the "
                            f"24-hour average of {avg_edits:.0f}."
                        ),
                        details={
                            "Current hour": f"{current:.0f} edits",
                            "24h average": f"{avg_edits:.0f} edits/hour",
                            "24h maximum": f"{max_edits:.0f} edits",
                            "Multiplier": f"{current/avg_edits:.1f}x",
                        },
                    )
    finally:
        conn.close()


def check_exchange_price_divergence():
    """
    Alert if BTC price differs by more than 0.5% between
    Binance and Coinbase. Could indicate an arbitrage opportunity
    or exchange-specific issue.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH binance_price AS (
                    SELECT ftc.close_price
                    FROM fact_trade_candle ftc
                    JOIN dim_time dt ON ftc.time_id = dt.time_id
                    JOIN dim_coin dc ON ftc.coin_id = dc.coin_id
                    WHERE dc.symbol = 'btcusdt'
                    ORDER BY dt.full_timestamp DESC
                    LIMIT 1
                ),
                coinbase_price AS (
                    SELECT fet.price
                    FROM fact_exchange_ticker fet
                    JOIN dim_coin dc ON fet.coin_id = dc.coin_id
                    WHERE dc.symbol = 'btc'
                      AND fet.exchange = 'coinbase'
                    ORDER BY fet.ingested_at DESC
                    LIMIT 1
                )
                SELECT
                    b.close_price AS binance,
                    c.price AS coinbase,
                    ABS(b.close_price - c.price)
                        / NULLIF(b.close_price, 0) * 100
                        AS divergence_pct
                FROM binance_price b, coinbase_price c
            """)
            row = cur.fetchone()
            if row and row[2]:
                binance_px, coinbase_px, divergence = row
                if divergence > 0.5:
                    send_alert(
                        level="warning",
                        title=(
                            f"Exchange price divergence: "
                            f"{divergence:.2f}%"
                        ),
                        message=(
                            f"BTC price differs by {divergence:.2f}% "
                            f"between Binance and Coinbase."
                        ),
                        details={
                            "Binance": f"${binance_px:,.2f}",
                            "Coinbase": f"${coinbase_px:,.2f}",
                            "Divergence": f"{divergence:.2f}%",
                        },
                    )
    finally:
        conn.close()