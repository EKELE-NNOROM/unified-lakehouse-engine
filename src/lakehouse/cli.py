"""Command-line interface for the Unified Lakehouse Engine.

Purpose
-------
``cli.py`` is the **operator-facing shell** for this project. It does not
implement analytics logic itself; it wires user input (flags, arguments,
config path) to :class:`lakehouse.engine.LakehouseEngine` and prints results.

Think of it as a thin adapter:

    Terminal  →  cli.py (Click)  →  LakehouseEngine  →  connectors / pipelines

Why it exists
-------------
- **Discoverability**: After ``pip install -e .``, the ``lakehouse`` command
  is on your PATH (registered in ``pyproject.toml`` under ``[project.scripts]``).
- **Operations**: Health checks, one-off publishes, pipeline runs, and ad-hoc
  SQL without writing a Python script.
- **Separation**: Library code (``engine.py``, connectors, pipelines) stays
  importable; the CLI is optional for programmatic use.

These commands are **project-specific**, not industry-standard tools. Equivalent
work elsewhere might use ``kafka-console-producer``, the ``trino`` CLI, Airflow,
etc.

Subcommands
-----------
- ``health``   — ping every configured datastore
- ``publish``  — load JSON events into Kafka
- ``stream``   — run the real-time Kafka → OLTP/OLAP pipeline
- ``sync``     — incremental Iceberg → Trino → warehouse sync
- ``query``    — run SQL routed by workload type (Vitess, TiDB, Trino, …)

For embedded use, prefer the Python API in ``engine.py`` instead of subprocess
calls to this CLI.
"""

from __future__ import annotations

import json
from pathlib import Path

import click

from lakehouse.engine import LakehouseEngine
from lakehouse.query.federation import WorkloadType


@click.group()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    default=None,
    help="Path to platform.yaml (see config/platform.example.yaml).",
)
@click.pass_context
def main(ctx: click.Context, config: str | None) -> None:
    """Unified Lakehouse Engine — entry point for all subcommands.

    Loads :class:`~lakehouse.engine.LakehouseEngine` once per invocation and
    stores it on the Click context so subcommands share the same config.
    """
    ctx.ensure_object(dict)
    ctx.obj["engine"] = (
        LakehouseEngine.from_yaml(config) if config else LakehouseEngine()
    )


@main.command()
@click.pass_context
def health(ctx: click.Context) -> None:
    """Check connectivity to every configured datastore.

    Connects to Kafka, TiDB, Vitess, Iceberg, Trino, Redshift, and StarRocks,
    then prints ``ok`` or ``unavailable`` for each. Exits with code 1 if any
    backend fails.
    """
    engine: LakehouseEngine = ctx.obj["engine"]
    status = engine.connect_all()
    engine.close_all()
    for name, ok in status.items():
        click.echo(f"  {name}: {'ok' if ok else 'unavailable'}")
    if not all(status.values()):
        raise SystemExit(1)


@main.command("stream")
@click.option("--topic", default="events.raw", help="Kafka topic to consume.")
@click.option("--batch-size", default=500, type=int, help="Max messages per run.")
@click.pass_context
def stream(ctx: click.Context, topic: str, batch_size: int) -> None:
    """Run the real-time streaming pipeline (Kafka → Vitess / TiDB / StarRocks).

    Delegates to :meth:`lakehouse.engine.LakehouseEngine.streaming_pipeline`.
    Requires running Kafka and target databases.
    """
    engine: LakehouseEngine = ctx.obj["engine"]
    engine.connect_all()
    try:
        count = engine.streaming_pipeline(topic).run(batch_size=batch_size)
        click.echo(f"Processed {count} events")
    finally:
        engine.close_all()


@main.command("sync")
@click.option(
    "--since",
    required=True,
    help="ISO-8601 timestamp; only rows at or after this time are synced.",
)
@click.pass_context
def sync(ctx: click.Context, since: str) -> None:
    """Run incremental lakehouse sync (Iceberg → Trino → StarRocks / Redshift).

    Delegates to :meth:`lakehouse.engine.LakehouseEngine.lakehouse_pipeline`.
    Prints JSON stats (row counts, load results).
    """
    engine: LakehouseEngine = ctx.obj["engine"]
    engine.connect_all()
    try:
        stats = engine.lakehouse_pipeline().run_incremental(since)
        click.echo(json.dumps(stats, indent=2))
    finally:
        engine.close_all()


@main.command()
@click.argument("sql")
@click.option(
    "--workload",
    type=click.Choice([w.value for w in WorkloadType]),
    default=WorkloadType.LAKEHOUSE.value,
    help="Which engine runs the SQL (lakehouse=Trino+Iceberg; federated=multi-catalog).",
)
@click.pass_context
def query(ctx: click.Context, sql: str, workload: str) -> None:
    """Execute SQL and print results as JSON.

    The ``--workload`` flag selects the target engine (e.g. ``lakehouse`` sends
    SQL to Trino over Iceberg; ``oltp`` sends it to Vitess).
    """
    engine: LakehouseEngine = ctx.obj["engine"]
    engine.connect_all()
    try:
        result = engine.query(sql, WorkloadType(workload))
        click.echo(
            json.dumps({"columns": result.columns, "rows": result.rows}, default=str)
        )
    finally:
        engine.close_all()


@main.command()
@click.argument("topic")
@click.option(
    "--file",
    "-f",
    type=click.Path(exists=True),
    required=True,
    help="JSON file: one event object or an array of events.",
)
@click.pass_context
def publish(ctx: click.Context, topic: str, file: str) -> None:
    """Publish events from a JSON file to a Kafka topic.

    Useful for seeding ``events.raw`` before running ``lakehouse stream``.
    Only connects to Kafka (not the full engine stack).
    """
    engine: LakehouseEngine = ctx.obj["engine"]
    engine.kafka.connect()
    try:
        events = json.loads(Path(file).read_text())
        if isinstance(events, dict):
            events = [events]
        for event in events:
            engine.publish_event(topic, event)
        click.echo(f"Published {len(events)} events to {topic}")
    finally:
        engine.kafka.close()
