# Sequence diagrams

Interaction order between operators, the Python engine, and external systems.

## CLI startup (shared context)

Every subcommand runs the Click group first, then the subcommand.

```mermaid
sequenceDiagram
    actor User
    participant CLI as lakehouse CLI (Click)
    participant Main as main()
    participant Engine as LakehouseEngine
    participant YAML as platform.yaml

    User->>CLI: lakehouse -c platform.yaml <subcommand>
    CLI->>Main: invoke group
    alt config path provided
        Main->>YAML: read
        Main->>Engine: from_yaml(path)
    else no config
        Main->>Engine: default PlatformConfig
    end
    Main->>Main: ctx.obj["engine"] = Engine
    CLI->>CLI: invoke subcommand (health / stream / …)
```

## Health check

```mermaid
sequenceDiagram
    actor User
    participant CLI as health command
    participant Engine as LakehouseEngine
    participant Conn as Connectors (×7)

    User->>CLI: lakehouse health
    CLI->>Engine: connect_all()
    loop each connector
        Engine->>Conn: connect()
        Engine->>Conn: health()
        Conn-->>Engine: ok / fail
    end
    Engine-->>CLI: dict name → bool
    CLI->>User: print status lines
    CLI->>Engine: close_all()
```

## Publish events to Kafka

```mermaid
sequenceDiagram
    actor User
    participant CLI as publish command
    participant Engine as LakehouseEngine
    participant Kafka as KafkaConnector
    participant Broker as Kafka cluster

    User->>CLI: publish events.raw -f sample_events.json
    CLI->>Kafka: connect()
    loop each event in JSON
        CLI->>Engine: publish_event(topic, event)
        Engine->>Kafka: publish(key=event_id, value=JSON)
        Kafka->>Broker: produce
        Broker-->>Kafka: ack
    end
    CLI->>Kafka: close()
    CLI->>User: Published N events
```

## Real-time streaming pipeline

```mermaid
sequenceDiagram
    actor User
    participant CLI as stream command
    participant Engine as LakehouseEngine
    participant Pipe as StreamingPipeline
    participant Kafka as KafkaConnector
    participant Vitess as VitessConnector
    participant TiDB as TiDBConnector
    participant SR as StarRocksConnector

    User->>CLI: lakehouse stream --topic events.raw
    CLI->>Engine: connect_all()
    CLI->>Engine: streaming_pipeline(topic)
    Engine-->>Pipe: configured pipeline
    CLI->>Pipe: run(batch_size)

    Pipe->>Kafka: subscribe([topic])
    loop up to batch_size messages
        Pipe->>Kafka: consume() / yield record
        Kafka-->>Pipe: record
        Pipe->>Pipe: transform(value)
        opt Vitess configured
            Pipe->>Vitess: execute INSERT orders
        end
        opt TiDB configured
            Pipe->>TiDB: upsert_event(events)
        end
        Pipe->>Pipe: append to olap_batch
    end
    opt StarRocks configured
        Pipe->>SR: stream_load(event_metrics, olap_batch)
    end
    Pipe-->>CLI: processed count
    CLI->>User: Processed N events
    CLI->>Engine: close_all()
```

## Lakehouse incremental sync

```mermaid
sequenceDiagram
    actor User
    participant CLI as sync command
    participant Engine as LakehouseEngine
    participant Pipe as LakehouseSyncPipeline
    participant Iceberg as IcebergConnector
    participant Trino as TrinoConnector
    participant SR as StarRocksConnector
    participant RS as RedshiftConnector
    participant S3 as S3 staging

    User->>CLI: lakehouse sync --since 2026-05-25T00:00:00Z
    CLI->>Engine: connect_all()
    CLI->>Engine: lakehouse_pipeline()
    CLI->>Pipe: run_incremental(since)

    Pipe->>Iceberg: scan_sql_filter(occurred_at >= since)
    Iceberg-->>Pipe: records[]

    alt no rows
        Pipe-->>CLI: stats iceberg_rows=0
    else has rows
        Pipe->>Trino: execute GROUP BY event_type
        Trino-->>Pipe: aggregates
        opt StarRocks
            Pipe->>SR: stream_load(aggregate rows)
        end
        opt Redshift
            Pipe->>RS: copy_from_s3(table, s3_path)
            RS->>S3: COPY PARQUET
        end
        Pipe-->>CLI: stats JSON
    end
    CLI->>User: print stats
    CLI->>Engine: close_all()
```

## Query routing (single workload)

Example: `lakehouse query "SELECT COUNT(*) FROM events" --workload lakehouse`

```mermaid
sequenceDiagram
    actor User
    participant CLI as query command
    participant Engine as LakehouseEngine
    participant Router as QueryRouter
    participant Trino as TrinoConnector
    participant Coord as Trino coordinator

    User->>CLI: query SQL + --workload lakehouse
    CLI->>Engine: connect_all()
    CLI->>Engine: query(sql, LAKEHOUSE)
    Engine->>Router: route(sql, workload)
    Router->>Trino: execute(sql)
    Trino->>Coord: JDBC query
    Coord-->>Trino: rows
    Trino-->>Router: QueryResult
    Router-->>Engine: QueryResult
    Engine-->>CLI: QueryResult
    CLI->>User: JSON columns + rows
    CLI->>Engine: close_all()
```

## Federated query (multi-catalog)

When `--workload federated` is used with **empty** SQL, the engine builds SQL from `FederatedSource` list.

```mermaid
sequenceDiagram
    actor User
    participant CLI as query command
    participant Router as QueryRouter
    participant Build as build_federated_sql
    participant Trino as TrinoConnector
    participant Coord as Trino coordinator
    participant ICE as iceberg catalog
    participant STR as starrocks catalog
    participant RED as redshift catalog

    User->>CLI: query "" --workload federated
    CLI->>Router: route("", FEDERATED)
    Router->>Build: DEFAULT_FEDERATED_SOURCES
    Build-->>Router: JOIN SQL
    Router->>Trino: federated_query / execute
    Trino->>Coord: execute join SQL
    Coord->>ICE: scan events
    Coord->>STR: scan event_metrics
    Coord->>RED: scan event_segments
    ICE-->>Coord: rows
    STR-->>Coord: rows
    RED-->>Coord: rows
    Coord-->>Trino: joined result
    Trino-->>CLI: QueryResult JSON
```

When the user **provides SQL**, `route` skips `build_federated_sql` and sends SQL directly to Trino.

## Kafka consume generator (library detail)

```mermaid
sequenceDiagram
    participant Caller as StreamingPipeline
    participant Consume as KafkaConnector.consume
    participant Consumer as confluent Consumer
    participant Broker as Kafka

    Caller->>Consume: for record in consume(max=500)
    loop until max_messages or poll timeout
        Consume->>Consumer: poll(timeout)
        Consumer->>Broker: fetch
        Broker-->>Consumer: message / null
        alt message ok
            Consume->>Consume: parse JSON → record dict
            Consume-->>Caller: yield record
            Note over Caller: pipeline processes one event
        end
    end
```

`yield` pauses the generator until the caller asks for the next record.

## Application API (without CLI)

```mermaid
sequenceDiagram
    participant App as Your Python app
    participant Engine as LakehouseEngine
    participant Pipe as StreamingPipeline

    App->>Engine: from_yaml("platform.yaml")
    App->>Engine: kafka.connect()
    App->>Engine: streaming_pipeline().run()
    Engine->>Pipe: run()
    Note over Pipe: same sequence as stream CLI
    App->>Engine: query(sql, WorkloadType.HTAP)
    App->>Engine: close_all()
```
