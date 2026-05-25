# Data flow diagrams

How data moves through the platform: ingress, real-time fan-out, batch lakehouse sync, and query paths.

## Platform overview

```mermaid
flowchart TB
    subgraph sources [Event sources]
        APP[Applications / APIs]
        FILE[JSON files via CLI publish]
    end

    subgraph ingress [Streaming ingress]
        K[Kafka topic events.raw]
    end

    subgraph realtime [Real-time path - StreamingPipeline]
        V[Vitess orders]
        T[TiDB events]
        SR1[StarRocks event_metrics]
    end

    subgraph lake [Lakehouse path]
        IC[Iceberg raw.events]
        TR[Trino SQL engine]
    end

    subgraph batch [Batch path - LakehouseSyncPipeline]
        SR2[StarRocks aggregates]
        RS[Redshift event_segments via S3 COPY]
    end

    subgraph consumers [Query consumers]
        CLI[lakehouse CLI / Python API]
        BI[BI tools via Trino JDBC]
    end

    APP --> K
    FILE --> K
    K --> V
    K --> T
    K --> SR1
    K -.->|compact_iceberg / ETL| IC
    IC --> TR
    TR --> SR2
    TR --> RS
    TR --> BI
    V --> CLI
    T --> CLI
    SR1 --> CLI
    SR2 --> CLI
    RS --> CLI
```

## Real-time streaming flow

Triggered by `lakehouse stream` or `StreamingPipeline.run()`.

```mermaid
flowchart LR
    A[Kafka poll] --> B[Transform JSON]
    B --> C{Targets configured?}
    C -->|Vitess| D[INSERT orders]
    C -->|TiDB| E[UPSERT events]
    C -->|StarRocks| F[Buffer event_metrics rows]
    F --> G[stream_load batch at end]
```

| Step | Component | Output |
|------|-----------|--------|
| 1 | `KafkaConnector.consume` | Parsed event dict |
| 2 | `_default_transform` | Canonical `event_id`, `event_type`, `occurred_at`, … |
| 3 | Vitess | Row in `orders` |
| 4 | TiDB | Row in `events` (upsert on `event_id`) |
| 5 | StarRocks | Rows in `event_metrics` (batched insert) |

## Lakehouse batch sync flow

Triggered by `lakehouse sync --since <timestamp>` or `LakehouseSyncPipeline.run_incremental()`.

```mermaid
flowchart TD
    A[Iceberg scan row_filter since timestamp] --> B{Any rows?}
    B -->|No| Z[Return stats iceberg_rows=0]
    B -->|Yes| C[Trino GROUP BY event_type]
    C --> D{StarRocks enabled?}
    D -->|Yes| E[stream_load aggregates]
    D -->|No| F
    E --> F{Redshift enabled?}
    F -->|Yes| G[COPY from S3 Parquet]
    F -->|No| H[Return stats JSON]
    G --> H
```

## Query / workload routing

```mermaid
flowchart TD
    Q[SQL + WorkloadType] --> R[QueryRouter.route]
    R --> W1[oltp → Vitess]
    R --> W2[htap → TiDB]
    R --> W3[olap_realtime → StarRocks]
    R --> W4[olap_warehouse → Redshift]
    R --> W5[lakehouse → Trino execute user SQL]
    R --> W6{federated}
    W6 -->|SQL provided| W5
    W6 -->|SQL empty| W7[build_federated_sql from FederatedSource list]
    W7 --> W5
```

| Workload | Data accessed | Typical use |
|----------|---------------|-------------|
| `oltp` | Vitess `commerce.orders` | Point writes / transactional reads |
| `htap` | TiDB `analytics.events` | Serving layer, mixed workloads |
| `olap_realtime` | StarRocks `event_metrics` | Dashboards, low latency |
| `olap_warehouse` | Redshift `event_segments` | Large historical analytics |
| `lakehouse` | Trino → Iceberg (single catalog) | Lake SQL, audits |
| `federated` | Trino → 1..N catalogs | Cross-engine joins |

## Federated query data flow

```mermaid
flowchart LR
    subgraph trino [Trino coordinator]
        P[Parse & plan]
    end

    subgraph catalogs [Trino catalogs - in place]
        ICE[iceberg.analytics.events]
        STR[starrocks.analytics.event_metrics]
        RED[redshift.public.event_segments]
    end

    P --> ICE
    P --> STR
    P --> RED
    ICE --> R[Result rows]
    STR --> R
    RED --> R
```

Trino reads remote catalogs without copying all data into one store first. Join keys (default `event_id`) align rows across systems.

## CLI command → data touchpoints

```mermaid
flowchart LR
    subgraph cmds [lakehouse commands]
        H[health]
        P[publish]
        S[stream]
        Y[sync]
        Q[query]
    end

    subgraph systems [Systems touched]
        K2[Kafka]
        V2[Vitess]
        T2[TiDB]
        I2[Iceberg]
        TR2[Trino]
        SR3[StarRocks]
        RS2[Redshift]
    end

    H --> K2 & V2 & T2 & I2 & TR2 & SR3 & RS2
    P --> K2
    S --> K2 & V2 & T2 & SR3
    Y --> I2 & TR2 & SR3 & RS2
    Q --> V2 & T2 & TR2 & SR3 & RS2
```

`publish` only connects to Kafka. `health` probes all connectors. `stream` / `sync` / `query` touch subsets per workload.
