-- ============================================================
-- Data Warehouse Schema (Star Schema)
--
-- Fact tables hold measurements (trades, prices, quakes).
-- Dimension tables hold descriptive attributes (coins, time, locations).
--
-- Airflow loads into these after transforming raw MinIO data.
-- Metabase queries these for dashboards.
-- ============================================================

-- ---------- DIMENSIONS ----------

CREATE TABLE IF NOT EXISTS dim_time (
    time_id         SERIAL PRIMARY KEY,
    full_timestamp  TIMESTAMPTZ NOT NULL UNIQUE,
    date            DATE NOT NULL,
    hour            SMALLINT NOT NULL,
    minute          SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,   -- 0=Mon, 6=Sun
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    week_of_year    SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_coin (
    coin_id     SERIAL PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL UNIQUE,  -- btc, eth, sol
    name        VARCHAR(100),                 -- Bitcoin, Ethereum
    category    VARCHAR(50)                   -- layer1, defi, meme
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    place       VARCHAR(255),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    depth_km    DOUBLE PRECISION,
    region      VARCHAR(100)
);


-- ---------- FACTS ----------

-- Aggregated trade candles (1-minute OHLCV)
-- Raw Binance trades are hundreds/sec; this rolls them up.
CREATE TABLE IF NOT EXISTS fact_trade_candle (
    candle_id       SERIAL PRIMARY KEY,
    coin_id         INT NOT NULL REFERENCES dim_coin(coin_id),
    time_id         INT NOT NULL REFERENCES dim_time(time_id),
    open_price      DOUBLE PRECISION NOT NULL,
    high_price      DOUBLE PRECISION NOT NULL,
    low_price       DOUBLE PRECISION NOT NULL,
    close_price     DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    trade_count     INT NOT NULL,
    buy_volume      DOUBLE PRECISION DEFAULT 0,
    sell_volume     DOUBLE PRECISION DEFAULT 0,
    ingested_at     TIMESTAMPTZ NOT NULL,

    UNIQUE (coin_id, time_id)
);

-- Snapshot prices from CoinGecko
CREATE TABLE IF NOT EXISTS fact_price_snapshot (
    snapshot_id     SERIAL PRIMARY KEY,
    coin_id         INT NOT NULL REFERENCES dim_coin(coin_id),
    time_id         INT NOT NULL REFERENCES dim_time(time_id),
    price_usd       DOUBLE PRECISION,
    price_eur       DOUBLE PRECISION,
    market_cap_usd  DOUBLE PRECISION,
    change_24h_pct  DOUBLE PRECISION,
    ingested_at     TIMESTAMPTZ NOT NULL,

    UNIQUE (coin_id, time_id)
);

-- Bitcoin transactions from Blockchain.com
CREATE TABLE IF NOT EXISTS fact_btc_transaction (
    tx_id           SERIAL PRIMARY KEY,
    tx_hash         VARCHAR(64) UNIQUE,
    time_id         INT NOT NULL REFERENCES dim_time(time_id),
    total_value_btc DOUBLE PRECISION NOT NULL,
    output_count    INT NOT NULL,
    input_count     INT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL
);


-- Earthquake events from USGS
CREATE TABLE IF NOT EXISTS fact_earthquake (
    quake_id        SERIAL PRIMARY KEY,
    usgs_event_id   VARCHAR(50) UNIQUE,
    location_id     INT NOT NULL REFERENCES dim_location(location_id),
    time_id         INT NOT NULL REFERENCES dim_time(time_id),
    magnitude       DOUBLE PRECISION NOT NULL,
    mag_type        VARCHAR(10),
    felt_reports    INT,
    tsunami_flag    BOOLEAN DEFAULT FALSE,
    alert_level     VARCHAR(20),
    ingested_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_wiki_edit (
    edit_id         SERIAL PRIMARY KEY,
    wiki_change_id  BIGINT UNIQUE,
    time_id         INT NOT NULL REFERENCES dim_time(time_id),
    wiki            VARCHAR(50),
    title           VARCHAR(500),
    user_name       VARCHAR(255),
    is_bot          BOOLEAN DEFAULT FALSE,
    is_minor        BOOLEAN DEFAULT FALSE,
    change_size     INT DEFAULT 0,
    edit_type       VARCHAR(20),
    ingested_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_exchange_ticker (
    ticker_id       SERIAL PRIMARY KEY,
    coin_id         INT NOT NULL REFERENCES dim_coin(coin_id),
    time_id         INT NOT NULL REFERENCES dim_time(time_id),
    exchange        VARCHAR(50) NOT NULL,
    price           DOUBLE PRECISION NOT NULL,
    best_bid        DOUBLE PRECISION,
    best_ask        DOUBLE PRECISION,
    spread          DOUBLE PRECISION,
    volume_24h      DOUBLE PRECISION,
    ingested_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_neo_approach (
    approach_id             SERIAL PRIMARY KEY,
    neo_reference_id        VARCHAR(20),
    neo_name                VARCHAR(200),
    time_id                 INT NOT NULL REFERENCES dim_time(time_id),
    absolute_magnitude      DOUBLE PRECISION,
    estimated_diameter_min  DOUBLE PRECISION,
    estimated_diameter_max  DOUBLE PRECISION,
    is_potentially_hazardous BOOLEAN DEFAULT FALSE,
    close_approach_date     DATE,
    velocity_kph            DOUBLE PRECISION,
    miss_distance_km        DOUBLE PRECISION,
    orbiting_body           VARCHAR(50),
    ingested_at             TIMESTAMPTZ NOT NULL,

    UNIQUE (neo_reference_id, close_approach_date)
);

-- ---------- PIPELINE BOOKKEEPING ----------

-- Tracks which raw files Airflow has already processed.
-- Prevents reprocessing the same file on the next DAG run.
CREATE TABLE IF NOT EXISTS pipeline_watermark (
    source_name     VARCHAR(100) PRIMARY KEY,
    last_processed  TIMESTAMPTZ NOT NULL,
    files_processed INT DEFAULT 0,
    last_run_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Audit log: what did Airflow do and when
CREATE TABLE IF NOT EXISTS pipeline_audit_log (
    log_id          SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100) NOT NULL,
    task_id         VARCHAR(100) NOT NULL,
    run_id          VARCHAR(200),
    status          VARCHAR(20) NOT NULL,  -- started, success, failed
    records_in      INT DEFAULT 0,
    records_out     INT DEFAULT 0,
    details         JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);


-- ---------- INDEXES for query performance ----------

CREATE INDEX IF NOT EXISTS idx_candle_time ON fact_trade_candle(time_id);
CREATE INDEX IF NOT EXISTS idx_candle_coin ON fact_trade_candle(coin_id);
CREATE INDEX IF NOT EXISTS idx_price_time ON fact_price_snapshot(time_id);
CREATE INDEX IF NOT EXISTS idx_quake_time ON fact_earthquake(time_id);
CREATE INDEX IF NOT EXISTS idx_quake_mag ON fact_earthquake(magnitude);
CREATE INDEX IF NOT EXISTS idx_time_date ON dim_time(date);
CREATE INDEX IF NOT EXISTS idx_btc_tx_time ON fact_btc_transaction(time_id);
CREATE INDEX IF NOT EXISTS idx_btc_tx_value ON fact_btc_transaction(total_value_btc);
CREATE INDEX IF NOT EXISTS idx_wiki_time ON fact_wiki_edit(time_id);
CREATE INDEX IF NOT EXISTS idx_wiki_wiki ON fact_wiki_edit(wiki);
CREATE INDEX IF NOT EXISTS idx_wiki_bot ON fact_wiki_edit(is_bot);
CREATE INDEX IF NOT EXISTS idx_ticker_time ON fact_exchange_ticker(time_id);
CREATE INDEX IF NOT EXISTS idx_ticker_exchange ON fact_exchange_ticker(exchange);
CREATE INDEX IF NOT EXISTS idx_ticker_coin ON fact_exchange_ticker(coin_id);
CREATE INDEX IF NOT EXISTS idx_neo_time ON fact_neo_approach(time_id);
CREATE INDEX IF NOT EXISTS idx_neo_hazardous ON fact_neo_approach(is_potentially_hazardous);
CREATE INDEX IF NOT EXISTS idx_neo_distance ON fact_neo_approach(miss_distance_km);