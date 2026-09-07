# Data Pipeline - Event-Driven ETL Architecture

A production-style data platform that replaces cron-based ETL (Azkaban, crontab)
with a modern, event-driven architecture using free, real-world data sources.

Built as a learning project to understand each component's role, why it exists,
and what happens when it fails.

## Architecture

```
┌─ INGESTION ────────────────────────────────────────────────┐
│  7 Consumers (1 image, config-driven)                      │
│  Binance WS | Blockchain WS | Coinbase WS | Wikipedia SSE │
│  CoinGecko REST | USGS REST | NASA REST                   │
└────────────────────────┬───────────────────────────────────┘
                         │
┌─ RESILIENCE ───────────┼───────────────────────────────────┐
│  RabbitMQ (message buffer, survives crashes)                │
│  Worker (drains queue to MinIO, batched writes)             │
└────────────────────────┼───────────────────────────────────┘
                         │
┌─ STORAGE ──────────────┼───────────────────────────────────┐
│  MinIO (raw zone, immutable, date-partitioned)              │
└────────────────────────┼───────────────────────────────────┘
                         │
┌─ ORCHESTRATION ────────┼───────────────────────────────────┐
│  Airflow                                                    │
│    etl_pipeline    (5 min)  : transform, load, anomalies    │
│    reconciliation  (4 hrs)  : fill WebSocket gaps via REST   │
│    cleanup         (1 hr)   : prune old data                 │
└────────────────────────┼───────────────────────────────────┘
                         │
┌─ WAREHOUSE ────────────┼───────────────────────────────────┐
│  PostgreSQL (star schema)                                   │
│    Facts: candles, prices, quakes, btc_tx, wiki_edits,      │
│           tickers, neo_approaches                           │
│    Dims: time, coin, location                               │
│    Audit: watermarks, pipeline logs                          │
└────────────────────────┼───────────────────────────────────┘
                         │
┌─ VISUALIZATION ────────┼───────────────────────────────────┐
│  Metabase (dashboards, SQL queries, auto-refresh)           │
└────────────────────────┼───────────────────────────────────┘
                         │
┌─ ALERTING ─────────────┼───────────────────────────────────┐
│  Mattermost                                                 │
│    Task failures, retries, stale data, anomaly detection     │
└────────────────────────────────────────────────────────────┘
```

## Components

### Consumer

A single Docker image that connects to any data source via configuration.
Supports three connection types:

| Type      | Protocol           | Used by                           |
|-----------|--------------------|-----------------------------------|
| websocket | WebSocket          | Binance, Blockchain, Coinbase     |
| rest_poll | HTTP GET on loop   | CoinGecko, USGS, NASA             |
| sse       | Server-Sent Events | Wikipedia                         |

Same image, different environment variables. No code changes per source.

### RabbitMQ (message queue)

Sits between consumers and MinIO. Consumers publish to a topic exchange,
a worker process drains the queue and writes to MinIO. If MinIO is down,
messages buffer in RabbitMQ. When MinIO recovers, the worker drains the
backlog. No data lost.

### MinIO (data lake)

S3-compatible object storage. Raw data lands in date-partitioned paths:

```
raw/
  binance/trades/2026-09-07/1694100000_abc123.json
  blockchain/transactions/2026-09-07/1694100001_def456.json
  coinbase/2026-09-07/1694100002_ghi789.json
  coingecko/prices/2026-09-07/1694100060_jkl012.json
  usgs/earthquakes/2026-09-07/1694100300_mno345.json
  wikipedia/2026-09-07/1694100000_pqr678.json
  nasa/2026-09-07/1694103600_stu901.json
```

### Airflow (orchestration)

Three DAGs manage the pipeline:

| DAG             | Schedule  | Purpose                                    |
|-----------------|-----------|--------------------------------------------|
| etl_pipeline    | 5 min     | Transform raw data, load warehouse, check anomalies |
| reconciliation  | 4 hours   | Poll REST APIs to catch WebSocket gaps     |
| cleanup         | 1 hour    | Delete data older than 3 hours             |

Each transform uses a watermark pattern: it tracks the last processed
timestamp per source, so files are never processed twice. If a run fails,
the watermark stays put and the next run picks up where it left off.

### PostgreSQL (data warehouse)

Star schema with fact and dimension tables:

| Table                | Source     | Description                          |
|----------------------|------------|--------------------------------------|
| fact_trade_candle    | Binance    | 1-minute OHLCV candles               |
| fact_price_snapshot  | CoinGecko  | Price, market cap, 24h change        |
| fact_earthquake      | USGS       | Magnitude, location, depth           |
| fact_btc_transaction | Blockchain | Transaction value, input/output count|
| fact_wiki_edit       | Wikipedia  | Edit metadata, bot flags, change size|
| fact_exchange_ticker | Coinbase   | Price, bid/ask spread, volume        |
| fact_neo_approach    | NASA       | Asteroid distance, velocity, hazard  |
| dim_time             | All        | Timestamp, date, hour, day of week   |
| dim_coin             | Crypto     | Symbol, name, category               |
| dim_location         | USGS       | Place, lat/lon, depth, region        |

### Metabase (visualization)

Four dashboards:

- **Crypto Trading**: BTC price, volume, buy/sell pressure, CoinGecko snapshots
- **Earthquake Monitor**: recent quakes, magnitude distribution, regional breakdown
- **Bitcoin Transactions**: volume over time, whale alerts, value distribution
- **Pipeline Health**: watermark freshness, audit log, throughput

### Mattermost (alerting)

Alerts fire on six anomaly conditions:

| Anomaly                    | Threshold                      | Level    |
|----------------------------|--------------------------------|----------|
| BTC price spike/crash      | > 2% move in 5 minutes         | warning  |
| Whale BTC transaction      | > 10 BTC in a single tx        | warning  |
| Significant earthquake     | Magnitude >= 5.0               | warning  |
| Hazardous asteroid         | NASA PHA flag = true           | info     |
| Wikipedia edit spike        | 3x the 24-hour hourly average  | warning  |
| Cross-exchange divergence  | > 0.5% price diff (Binance vs Coinbase) | warning |

Plus operational alerts: task failures (critical), task retries (warning),
and stale data (warning, any source > 30 min behind).

## Data sources

| Source     | Type      | Frequency    | Data                       | Free |
|------------|-----------|--------------|----------------------------|------|
| Binance    | websocket | ~100 msg/sec | BTC/USDT trades            | yes  |
| Blockchain | websocket | ~5 msg/sec   | Unconfirmed BTC transactions | yes |
| Coinbase   | websocket | ~10 msg/sec  | BTC/ETH ticker (price, spread) | yes |
| Wikipedia  | sse       | ~50 msg/sec  | Real-time edits across all wikis | yes |
| CoinGecko  | rest_poll | every 60s    | Crypto prices + market cap | yes  |
| USGS       | rest_poll | every 5 min  | Global earthquakes         | yes  |
| NASA       | rest_poll | every 1 hour | Near-Earth asteroid approaches | yes |

## Quick start

```bash
docker compose up --build -d
```

## Service endpoints

| Service          | URL                    | Credentials              |
|------------------|------------------------|--------------------------|
| MinIO Console    | http://localhost:9001  | minio / minio123         |
| Airflow UI       | http://localhost:8080  | admin / admin            |
| Metabase         | http://localhost:3000  | (set up on first visit)  |
| RabbitMQ Manager | http://localhost:15672 | pipeline / pipeline123   |
| Mattermost       | http://localhost:8065  | (set up on first visit)  |
| Warehouse DB     | localhost:5433         | warehouse_user / warehouse_pass |

## Project structure

```
data-pipeline/
├── docker-compose.yml
├── README.md
├── consumer/                        # Data ingestion (1 image, N instances)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── config.py                # Env vars to dataclasses
│       ├── consumer.py              # Main loop (batch + flush)
│       ├── sources/
│       │   ├── __init__.py          # Factory: create_source("websocket")
│       │   ├── base.py              # Abstract interface
│       │   ├── websocket_source.py  # Auto-reconnect WebSocket
│       │   ├── rest_poll_source.py  # HTTP poll with backoff
│       │   └── sse_source.py        # Server-Sent Events
│       └── sinks/
│           ├── __init__.py
│           ├── minio_sink.py        # Direct writes to MinIO
│           └── rabbitmq_sink.py     # Publish to RabbitMQ
├── worker/                          # Queue drain (RabbitMQ to MinIO)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       └── worker.py
├── airflow/                         # Orchestration + transforms
│   ├── Dockerfile
│   ├── dags/
│   │   ├── etl_pipeline.py          # Main ETL (every 5 min)
│   │   ├── reconciliation.py        # Gap filling (every 4 hrs)
│   │   └── cleanup.py               # Data retention (every 1 hr)
│   ├── transforms/
│   │   ├── utils.py                 # Shared: MinIO, watermarks, dims
│   │   ├── transform_binance.py     # Trades to OHLCV candles
│   │   ├── transform_coingecko.py   # Nested JSON to price rows
│   │   ├── transform_usgs.py        # GeoJSON to earthquake facts
│   │   ├── transform_blockchain.py  # BTC transactions
│   │   ├── transform_wikipedia.py   # Wiki edits
│   │   ├── transform_coinbase.py    # Exchange tickers
│   │   ├── transform_nasa.py        # Near-Earth objects
│   │   ├── reconcile.py             # REST API gap filling
│   │   ├── anomalies.py             # Threshold-based anomaly detection
│   │   ├── alerting.py              # Mattermost webhook integration
│   │   └── cleanup.py               # Data retention enforcement
│   └── plugins/
└── warehouse/
    └── init.sql                     # Star schema DDL
```

## Adding a new data source

No consumer code changes needed. Four steps:

1. **Add consumer to docker-compose.yml** (env vars only):

```yaml
  consumer-mastodon:
    build: ./consumer
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      SOURCE_NAME: mastodon
      SOURCE_TYPE: websocket
      SOURCE_URL: wss://mastodon.social/api/v1/streaming/public
      SINK_TYPE: rabbitmq
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: pipeline
      RABBITMQ_PASS: pipeline123
      RABBITMQ_EXCHANGE: raw_events
      BATCH_SIZE: 50
      BATCH_TIMEOUT: 10
      MINIO_ENDPOINT: minio:9000
      MINIO_ACCESS_KEY: minio
      MINIO_SECRET_KEY: minio123
```

2. **Create warehouse table** (SQL):

```sql
CREATE TABLE IF NOT EXISTS fact_mastodon_post (...);
```

3. **Create transform module** (`airflow/transforms/transform_mastodon.py`):
   Copy any existing transform, change the payload parsing and target table.

4. **Add task to ETL DAG** (`airflow/dags/etl_pipeline.py`):
   Add callable, PythonOperator, and dependency (three lines).

## Consumer environment variables

| Variable             | Required | Default     | Description                           |
|----------------------|----------|-------------|---------------------------------------|
| SOURCE_NAME          | yes      | unknown     | Identifier for this source            |
| SOURCE_TYPE          | yes      | rest_poll   | websocket, rest_poll, or sse          |
| SOURCE_URL           | yes      |             | URL to connect to                     |
| SOURCE_POLL_INTERVAL | no       | 60          | Seconds between polls (rest_poll only)|
| SOURCE_SUBSCRIBE_MSG | no       |             | JSON to send on WS connect            |
| SOURCE_HEADERS       | no       |             | Extra HTTP headers (Key:Value,...)    |
| SINK_TYPE            | no       | minio       | minio or rabbitmq                     |
| SINK_BUCKET          | no       | raw         | MinIO bucket name                     |
| SINK_PATH            | no       | {name}/     | Path prefix inside the bucket         |
| BATCH_SIZE           | no       | 50          | Flush after N messages                |
| BATCH_TIMEOUT        | no       | 10          | Flush after N seconds                 |
| MINIO_ENDPOINT       | yes      | minio:9000  | MinIO S3 API endpoint                 |
| MINIO_ACCESS_KEY     | yes      | minio       | MinIO access key                      |
| MINIO_SECRET_KEY     | yes      | minio123    | MinIO secret key                      |
| MINIO_SECURE         | no       | false       | Use HTTPS for MinIO                   |
| RABBITMQ_HOST        | no       | rabbitmq    | RabbitMQ hostname                     |
| RABBITMQ_PORT        | no       | 5672        | RabbitMQ AMQP port                    |
| RABBITMQ_USER        | no       | pipeline    | RabbitMQ username                     |
| RABBITMQ_PASS        | no       | pipeline123 | RabbitMQ password                     |
| RABBITMQ_EXCHANGE    | no       | raw_events  | RabbitMQ exchange name                |
| LOG_LEVEL            | no       | INFO        | DEBUG, INFO, WARNING, ERROR           |

## Data retention

The cleanup DAG runs every hour and enforces:

| Data              | Retention | Location   |
|-------------------|-----------|------------|
| Raw files         | 3 hours   | MinIO      |
| Fact table rows   | 3 hours   | PostgreSQL |
| Audit logs        | 24 hours  | PostgreSQL |
| Orphaned dim rows | Immediate | PostgreSQL |

Adjust in `airflow/transforms/cleanup.py` by changing the constants
at the top of the file.

## Design patterns used

| Pattern              | Where                    | Why                                       |
|----------------------|--------------------------|-------------------------------------------|
| Factory              | Consumer source adapters | One image, multiple protocols              |
| Adapter              | Source and sink classes   | Swap implementations without changing core |
| Envelope             | MinIO JSON files         | Metadata (ingested_at, source) wraps payload |
| Watermark            | Pipeline bookkeeping     | Track progress, prevent reprocessing       |
| Dead letter / retry  | RabbitMQ nack + requeue  | Failed writes retry automatically          |
| Star schema          | PostgreSQL warehouse     | Query-optimized analytics                  |
| Reconciliation       | Airflow DAG              | REST polling catches WebSocket gaps        |
| Circuit breaker      | Exponential backoff      | Don't hammer failing APIs                  |