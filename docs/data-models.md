# Data models

Schemas for inbound events, per-engine tables, lakehouse storage, configuration, and how they relate.

## Canonical event (Kafka / pipeline)

Events on `events.raw` are JSON. The streaming pipeline normalizes them to this shape.

```mermaid
classDiagram
    class CanonicalEvent {
        +string event_id
        +string event_type
        +int customer_id
        +decimal amount
        +object payload
        +string occurred_at
    }

    class KafkaRecord {
        +string topic
        +int partition
        +int offset
        +string key
        +CanonicalEvent value
    }

    KafkaRecord --> CanonicalEvent : value after transform
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `event_id` | string | yes | From `event_id` or fallback `id` |
| `event_type` | string | yes | e.g. `order.placed`; default `unknown` |
| `customer_id` | int | no | Used for Vitess `orders` |
| `amount` | number | no | Default `0`; mapped to StarRocks `metric_value` |
| `payload` | object | no | Stored as JSON string in TiDB / Iceberg |
| `occurred_at` | ISO-8601 string | yes | UTC if omitted |

**Source example:** [`../examples/sample_events.json`](../examples/sample_events.json)

---

## Logical entity relationship (cross-system)

`event_id` is the global correlation key across stores.

```mermaid
erDiagram
    CANONICAL_EVENT ||--o| ORDERS : "event_id"
    CANONICAL_EVENT ||--|| EVENTS : "event_id PK"
    CANONICAL_EVENT ||--o| EVENT_METRICS : "event_id"
    CANONICAL_EVENT ||--o| ICEBERG_EVENTS : "event_id"
    CANONICAL_EVENT ||--o| EVENT_SEGMENTS : "event_id"

    CANONICAL_EVENT {
        string event_id PK
        string event_type
        timestamp occurred_at
    }

    ORDERS {
        string event_id PK
        bigint customer_id
        decimal amount
        timestamp created_at
    }

    EVENTS {
        string event_id PK
        string event_type
        json payload
        timestamp occurred_at
    }

    EVENT_METRICS {
        string event_id
        string event_type
        bigint metric_value
        date event_date
    }

    ICEBERG_EVENTS {
        string event_id
        string event_type
        timestamp occurred_at
        string payload
    }

    EVENT_SEGMENTS {
        string event_id
        string segment
        double score
    }
```

---

## Per-engine physical models

### Vitess — `commerce.orders` (OLTP)

```mermaid
erDiagram
    ORDERS {
        varchar event_id PK
        bigint customer_id
        decimal amount
        timestamp created_at
    }
```

Sharding: Vitess VSchema routes by shard key (see `VitessConnector.route_by_shard_key`).

---

### TiDB — `analytics.events` (HTAP)

```mermaid
erDiagram
    EVENTS {
        varchar event_id PK
        varchar event_type
        json payload
        timestamp occurred_at
    }
```

Writes use `ON DUPLICATE KEY UPDATE` (upsert) on `event_id`.

---

### StarRocks — `analytics.event_metrics` (real-time OLAP)

```mermaid
erDiagram
    EVENT_METRICS {
        varchar event_id
        varchar event_type
        bigint metric_value
        date event_date
    }
```

`DUPLICATE KEY(event_id)` — columnar aggregate-friendly layout.

---

### Iceberg — `raw.events` (lakehouse)

Namespace and table from config (`iceberg.namespace`, `iceberg.table`).

```mermaid
erDiagram
    ICEBERG_EVENTS {
        string event_id
        string event_type
        timestamp occurred_at
        string payload
    }
```

| Column | Arrow type | Notes |
|--------|------------|-------|
| `event_id` | string | Join key |
| `event_type` | string | Partition / filter candidate |
| `occurred_at` | timestamp(us) | Incremental sync filter |
| `payload` | string | JSON serialized |

Warehouse path: `s3://…` from `iceberg.warehouse` in config.

---

### Redshift — `public.event_segments` (warehouse)

```mermaid
erDiagram
    EVENT_SEGMENTS {
        varchar event_id
        varchar segment
        double score
    }
```

Loaded via `COPY … FROM s3://… FORMAT AS PARQUET` in batch sync.

---

## Federated query model (Trino)

Trino does not own the data; it exposes **catalog.schema.table** over remote systems.

```mermaid
flowchart TB
    subgraph federated [FederatedSource - config object]
        FS1[catalog.schema.table + alias + columns]
        FS2[catalog.schema.table + alias + columns]
        FSN[...]
    end

    subgraph sql [Generated SQL]
        J[JOIN ON event_id]
    end

    FS1 --> J
    FS2 --> J
    FSN --> J
```

Default three-source join (when SQL not provided):

| Alias | Fully qualified table | Selected columns |
|-------|----------------------|------------------|
| `i` | `iceberg.analytics.events` | `event_id`, `event_type` |
| `s` | `starrocks.analytics.event_metrics` | `event_id`, `metric_value` |
| `r` | `redshift.public.event_segments` | `event_id`, `segment` |

Single-source query: one `FederatedSource` → `SELECT … FROM catalog.schema.table` (no join).

---

## Platform configuration model

```mermaid
classDiagram
    class PlatformConfig {
        +KafkaConfig kafka
        +TiDBConfig tidb
        +VitessConfig vitess
        +IcebergConfig iceberg
        +TrinoConfig trino
        +RedshiftConfig redshift
        +StarRocksConfig starrocks
    }

    class KafkaConfig {
        +string bootstrap_servers
        +string consumer_group
        +string security_protocol
    }

    class TiDBConfig {
        +string host
        +int port
        +string database
    }

    class VitessConfig {
        +string vtgate_host
        +int vtgate_port
        +string keyspace
    }

    class IcebergConfig {
        +string catalog_name
        +string warehouse
        +string namespace
        +string table
    }

    class TrinoConfig {
        +string host
        +int port
        +string catalog
        +string trino_schema
    }

    PlatformConfig --> KafkaConfig
    PlatformConfig --> TiDBConfig
    PlatformConfig --> VitessConfig
    PlatformConfig --> IcebergConfig
    PlatformConfig --> TrinoConfig
    PlatformConfig --> RedshiftConfig
    PlatformConfig --> StarRocksConfig
```

Loaded from `platform.yaml` via `load_config()` → `LakehouseEngine.from_yaml()`.

---

## Query result model (API)

All SQL connectors return the same structure to callers.

```mermaid
classDiagram
    class QueryResult {
        +list~string~ columns
        +list~tuple~ rows
        +int row_count
    }
```

Used by `QueryRouter`, `lakehouse query` JSON output, and library callers.

---

## Workload type → store mapping

```mermaid
flowchart LR
    OLTP[WorkloadType.OLTP] --> Vitess
    HTAP[WorkloadType.HTAP] --> TiDB
    OLAP_R[OLAP_REALTIME] --> StarRocks
    OLAP_W[OLAP_WAREHOUSE] --> Redshift
    LAKE[LAKEHOUSE] --> Trino
    FED[FEDERATED] --> Trino
```

---

## DDL reference

Physical `CREATE TABLE` statements: [`../sql/schemas/init.sql`](../sql/schemas/init.sql)

Trino catalog wiring: [`../sql/trino/catalogs.properties`](../sql/trino/catalogs.properties)
