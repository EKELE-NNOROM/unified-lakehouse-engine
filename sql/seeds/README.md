# Seed data

Reference rows aligned with [`../../examples/sample_events.json`](../../examples/sample_events.json) (25 events, shared `event_id` keys).

## Automated (Kafka + pipelines)

From the repo root, with services running and DDL applied (`sql/schemas/init.sql`):

```bash
# Publish to Kafka and run streaming pipeline (Vitess, TiDB, StarRocks)
lakehouse -c platform.yaml seed

# Kafka only (same as publish)
lakehouse -c platform.yaml seed --no-stream
```

## Manual (direct SQL)

Use when you want rows without going through Kafka. Apply DDL first, then run the file for each engine you have:

| File | Engine | Database / keyspace |
|------|--------|---------------------|
| `vitess.sql` | Vitess | `commerce` |
| `tidb.sql` | TiDB | `analytics` |
| `starrocks.sql` | StarRocks | `analytics` |
| `redshift.sql` | Redshift | `public` |

Iceberg is not seeded via SQL here; use the streaming/sync pipelines or `LakehouseSyncPipeline.compact_iceberg()` in code.

## Federated demo query

After seeding and sync (if Iceberg/Trino are up):

```bash
lakehouse -c platform.yaml sync --since 2026-05-25T00:00:00Z
lakehouse -c platform.yaml query "" --workload federated
```
