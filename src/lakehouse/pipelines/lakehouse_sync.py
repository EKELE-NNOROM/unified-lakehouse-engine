"""Batch pipeline: Iceberg lakehouse → Trino → OLAP warehouses.

Implements the **lakehouse** pattern in this project:

1. Read incremental rows from an Iceberg table (open format on object storage).
2. Compute aggregates in Trino (SQL over the Iceberg catalog).
3. Push results to StarRocks (low latency) and/or Redshift (bulk warehouse).

Invoked via :meth:`lakehouse.engine.LakehouseEngine.lakehouse_pipeline` or
``lakehouse sync --since <timestamp>``.
"""

from __future__ import annotations

import json
from typing import Any

from lakehouse.connectors.iceberg import IcebergConnector
from lakehouse.connectors.redshift import RedshiftConnector
from lakehouse.connectors.starrocks import StarRocksConnector
from lakehouse.connectors.trino import TrinoConnector


class LakehouseSyncPipeline:
    """Incremental sync from lake tables into serving/warehouse engines."""

    def __init__(
        self,
        iceberg: IcebergConnector,
        trino: TrinoConnector,
        starrocks: StarRocksConnector | None = None,
        redshift: RedshiftConnector | None = None,
        *,
        iceberg_table: str = "events",
        starrocks_table: str = "event_metrics",
        redshift_table: str = "event_segments",
        s3_staging: str = "s3://lakehouse/staging/events/",
    ) -> None:
        """Configure table names and optional downstream targets.

        Args:
            iceberg: Source lake table connector.
            trino: Engine for aggregate SQL over Iceberg.
            starrocks: Optional real-time OLAP load target.
            redshift: Optional warehouse load target (S3 COPY).
            iceberg_table: Table name in the Iceberg catalog.
            starrocks_table: Destination aggregate table in StarRocks.
            redshift_table: Destination table in Redshift.
            s3_staging: S3 prefix for Redshift COPY (Parquet).
        """
        self._iceberg = iceberg
        self._trino = trino
        self._starrocks = starrocks
        self._redshift = redshift
        self._iceberg_table = iceberg_table
        self._starrocks_table = starrocks_table
        self._redshift_table = redshift_table
        self._s3_staging = s3_staging

    def run_incremental(self, since_timestamp: str) -> dict[str, int]:
        """Sync rows with ``occurred_at >= since_timestamp``.

        Args:
            since_timestamp: ISO-8601 string (e.g. ``2026-05-25T00:00:00Z``).

        Returns:
            Stats dict: ``iceberg_rows``, ``aggregate_groups``, optional
            ``starrocks_loaded`` and ``redshift_copy``.
        """
        predicate = f"occurred_at >= '{since_timestamp}'"
        records = self._iceberg.scan_sql_filter(predicate)
        stats: dict[str, int] = {"iceberg_rows": len(records)}

        if not records:
            return stats

        aggregates = self._trino.execute(
            f"""
            SELECT event_type,
                   COUNT(*) AS event_count,
                   COUNT(DISTINCT event_id) AS unique_events
            FROM iceberg.analytics.{self._iceberg_table}
            WHERE occurred_at >= TIMESTAMP '{since_timestamp}'
            GROUP BY event_type
            """
        )
        stats["aggregate_groups"] = aggregates.row_count

        if self._starrocks:
            rows = [
                {
                    "event_type": r[0],
                    "event_count": r[1],
                    "unique_events": r[2],
                }
                for r in aggregates.rows
            ]
            stats["starrocks_loaded"] = self._starrocks.stream_load(
                self._starrocks_table, rows
            )

        if self._redshift:
            self._redshift.copy_from_s3(self._redshift_table, self._s3_staging)
            stats["redshift_copy"] = 1

        return stats

    def compact_iceberg(self, records: list[dict[str, Any]]) -> int:
        """Append a micro-batch of events into the Iceberg table.

        Args:
            records: Raw event dicts from streaming or external sources.

        Returns:
            Number of rows appended.
        """
        normalized = [
            {
                "event_id": r["event_id"],
                "event_type": r.get("event_type", "unknown"),
                "occurred_at": r.get("occurred_at"),
                "payload": json.dumps(r.get("payload", {})),
            }
            for r in records
        ]
        return self._iceberg.append_batch(normalized)
