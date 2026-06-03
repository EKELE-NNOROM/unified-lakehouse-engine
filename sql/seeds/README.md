# Seed data

Reference rows aligned with [`../../examples/sample_events.json`](../../examples/sample_events.json) (25 events, shared `event_id` keys).

## Seed data overview

| Asset | Description | Purpose | Why it matters |
|-------|-------------|---------|----------------|
| `examples/sample_events.json` | 25 synthetic JSON events (`evt-001`–`evt-025`) with types such as `order.placed`, `click`, and `page.view`; shared fields include `event_id`, `customer_id`, `amount`, and `occurred_at` | Canonical **source-of-truth** for demos and pipeline testing; ingested via Kafka | Gives every engine the **same logical events** so streaming, correlation, and federated joins behave predictably without live production traffic |
| `lakehouse seed` (CLI) | Publishes `sample_events.json` to Kafka (`events.raw`), then optionally runs the streaming pipeline (`--stream` / `--no-stream`) | **One-command** bootstrap: Kafka → Vitess, TiDB, StarRocks in a single flow | Speeds onboarding and end-to-end validation; operators do not need custom scripts for each datastore |
| `sql/seeds/vitess.sql` | 17 `INSERT`s into `commerce.orders` (order lifecycle events only) | Manual OLTP seed when skipping Kafka | Lets you demo **sharded OLTP** writes in isolation or recover Vitess without re-consuming the topic |
| `sql/seeds/tidb.sql` | 25 `INSERT`s into `analytics.events` with JSON `payload` | Manual HTAP / serving-layer seed | Supports **full event history** queries on TiDB and HTAP workload demos without a running consumer |
| `sql/seeds/starrocks.sql` | 25 `INSERT`s into `analytics.event_metrics` (`metric_value`, `event_date`) | Manual real-time OLAP seed | Populates **columnar metrics** for dashboards and `olap_realtime` queries without running `stream` |
| `sql/seeds/redshift.sql` | 25 `INSERT`s into `public.event_segments` (`segment`, `score`) | Warehouse enrichment for federated joins | Adds **analytical dimensions** not present in raw events so Trino cross-catalog joins return meaningful rows |
| Iceberg (via pipelines) | No static SQL file; rows land through `stream` / `sync` / `compact_iceberg()` | Lakehouse layer on object storage | Reflects how **open-table** systems are usually loaded (append/snapshot), not ad hoc `INSERT` |

**Design principles:** small enough to commit in git; **no PII**; **`event_id` correlation** across all targets; supports both **automated** (Kafka path) and **manual** (per-engine SQL) workflows.

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
