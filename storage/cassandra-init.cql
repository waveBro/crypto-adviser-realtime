-- =================================================================
-- Real-time Crypto Trading Advisor
-- Database: Apache Cassandra 4.1
-- Schema: Medallion Silver + Gold Layers
--
-- Design principles:
--   1. Partition by (symbol, date) → even data distribution
--   2. Cluster by window_start DESC → latest-first reads (Grafana)
--   3. Materialised Views for alternate access patterns
--   4. TTL: Silver = 30d | Gold = 90d (cost control)
-- =================================================================

-- ─────────────────────────────────────────────────────────────────
-- Keyspace (like database )
-- ─────────────────────────────────────────────────────────────────

create keyspace if not exists crypto_advisor 
with replication = {
    'class': 'SimpleStrategy', 
    'replication_factor': 1
}
AND durable_writes = true;   -- save every write to commit log (disk)

USE crypto_advisor;



-----------SILVER LAYER: OHLCV 5-Minute Aggregations------------------
create table if not exists  ohlcv_5min(
    symbol          TEXT,
    date_bucket     DATE,           -- derived from window_start (partition key)
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,

    -- OHLCV
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    sma_5min        DOUBLE,
    vwap            DOUBLE,
    total_volume    DOUBLE,
    total_notional_usd DOUBLE,

    -- Quality metrics
    trade_count     INT,
    avg_latency_ms  DOUBLE,
    inserted_at     TIMESTAMP,

    PRIMARY KEY ((symbol, date_bucket), window_start)  
) WITH CLUSTERING ORDER BY (window_start DESC)
    AND default_time_to_live = 2592000  -- 30 days
    AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 1
  }
  AND comment = 'Silver layer: 5-min OHLCV aggregations for time-series charts';
----------------------------------------------------------


---------------------------GOLD LAYER: Risk Signals & Alerts-------------------
CREATE TABLE IF NOT EXISTS trading_signals (
    symbol              TEXT,
    date_bucket         DATE,
    window_start        TIMESTAMP,

    -- Aggregated metrics
    close               DOUBLE,
    sma_15min           DOUBLE,
    volume_15min        DOUBLE,
    buy_volume          DOUBLE,
    sell_volume         DOUBLE,
    trade_count         INT,

    -- Derived indicators
    volume_spike        BOOLEAN,
    price_deviation_pct DOUBLE,
    buy_pressure        DOUBLE,         -- 0.0 (all sells) to 1.0 (all buys)

    -- Signal output
    risk_level          TEXT,           -- LOW / MEDIUM / HIGH / CRITICAL
    alert_message       TEXT,
    inserted_at         TIMESTAMP,

    PRIMARY KEY ((symbol, date_bucket), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 7776000  -- 90 days
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 7
  }
  AND comment = 'Gold layer: risk signals and alert flags';


-----------------------------------------------------------
-- GOLD LAYER: Alert History (append-only, fast lookup by severity)
-----------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_history (
    symbol          TEXT,
    risk_level      TEXT,
    alert_time      TIMESTAMP,
    window_start    TIMESTAMP,
    price           DOUBLE,
    volume          DOUBLE,
    deviation_pct   DOUBLE,
    buy_pressure    DOUBLE,
    alert_message   TEXT,
    notified        BOOLEAN,    -- true once Telegram notification sent

    PRIMARY KEY ((symbol, risk_level), alert_time)
) WITH CLUSTERING ORDER BY (alert_time DESC)
  AND default_time_to_live = 7776000
  AND comment = 'Alert history indexed by risk level for fast alert-severity queries';


-----------------------------------------------------------------
-- MONITORING: Pipeline Health Metrics
-----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_health (
    component       TEXT,           -- 'producer' | 'spark' | 'cassandra'
    metric_time     TIMESTAMP,
    messages_per_sec DOUBLE,
    error_rate      DOUBLE,
    avg_latency_ms  DOUBLE,
    extra_json      TEXT,           -- JSON blob for component-specific metrics

    PRIMARY KEY (component, metric_time)
) WITH CLUSTERING ORDER BY (metric_time DESC)
  AND default_time_to_live = 604800  -- 7 days
  AND comment = 'Pipeline health metrics for SLA monitoring';



-- ─────────────────────────────────────────────────────────────────
-- Materialised View: Latest signal per symbol (for dashboard widgets)
-- ─────────────────────────────────────────────────────────────────
-- NOTE: Cassandra MVs require the base table PK to be present.
-- This view allows querying latest risk_level without date_bucket.

-- CREATE MATERIALIZED VIEW IF NOT EXISTS latest_signals AS
--     SELECT symbol, date_bucket, window_start,
--            risk_level, alert_message, close, sma_15min,
--            buy_pressure, price_deviation_pct, volume_spike
--     FROM trading_signals
--     WHERE symbol IS NOT NULL
--       AND date_bucket IS NOT NULL
--       AND window_start IS NOT NULL
-- PRIMARY KEY (symbol, window_start, date_bucket)
-- WITH CLUSTERING ORDER BY (window_start DESC, date_bucket DESC);


-- ─────────────────────────────────────────────────────────────────
-- Seed data for Grafana datasource validation
-- ─────────────────────────────────────────────────────────────────
INSERT INTO pipeline_health (component, metric_time, messages_per_sec, error_rate, avg_latency_ms, extra_json)
VALUES ('system', toTimestamp(now()), 0.0, 0.0, 0.0, '{"status": "initialized"}');


-- ─────────────────────────────────────────────────────────────────
-- Useful CQL queries (for manual validation / Grafana panels)
-- ─────────────────────────────────────────────────────────────────

-- Q1: Last 24h OHLCV (Bronze validation)
-- SELECT window_start, open, high, low, close, sma_5min, vwap, total_volume
-- FROM ohlcv_5min
-- WHERE symbol = 'BTCUSDT'
--   AND date_bucket = toDate(now())
-- ORDER BY window_start DESC
-- LIMIT 288;   -- 288 = 24h / 5min

-- Q2: HIGH/CRITICAL alerts today
-- SELECT window_start, risk_level, alert_message, price_deviation_pct, buy_pressure
-- FROM trading_signals
-- WHERE symbol = 'BTCUSDT'
--   AND date_bucket = toDate(now())
--   AND risk_level IN ('HIGH', 'CRITICAL')   -- secondary index needed or filter in app
-- ORDER BY window_start DESC;

-- Q3: Pipeline health last hour
-- SELECT component, metric_time, messages_per_sec, avg_latency_ms
-- FROM pipeline_health
-- WHERE component = 'producer'
--   AND metric_time > toTimestamp(now()) - 3600000
-- ORDER BY metric_time DESC;



