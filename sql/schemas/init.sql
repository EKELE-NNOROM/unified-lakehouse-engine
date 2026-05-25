-- Reference DDL for platform tables (apply per engine).

-- Vitess (commerce keyspace)
CREATE TABLE IF NOT EXISTS orders (
    event_id      VARCHAR(64) PRIMARY KEY,
    customer_id   BIGINT NOT NULL,
    amount        DECIMAL(18, 2) DEFAULT 0,
    created_at    TIMESTAMP NOT NULL,
    INDEX idx_customer (customer_id)
);

-- TiDB (HTAP serving)
CREATE TABLE IF NOT EXISTS events (
    event_id     VARCHAR(64) PRIMARY KEY,
    event_type   VARCHAR(64) NOT NULL,
    payload      JSON,
    occurred_at  TIMESTAMP NOT NULL,
    INDEX idx_type_time (event_type, occurred_at)
);

-- StarRocks (real-time OLAP)
CREATE TABLE IF NOT EXISTS event_metrics (
    event_id     VARCHAR(64),
    event_type   VARCHAR(64),
    metric_value BIGINT,
    event_date   DATE
)
DUPLICATE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 16;

-- Redshift (warehouse segments)
CREATE TABLE IF NOT EXISTS event_segments (
    event_id  VARCHAR(64),
    segment   VARCHAR(64),
    score     DOUBLE PRECISION
)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (segment);
