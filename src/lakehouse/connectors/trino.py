"""Trino connector (distributed federated SQL).

Trino queries data **in place** across catalogs (Iceberg, MySQL, Redshift,
etc.) without moving it into a single warehouse first.

For a **single** catalog, use :meth:`execute` (or ``--workload lakehouse``).
For **multiple** catalogs, use :meth:`federated_query` with a list of
:class:`~lakehouse.query.federation.FederatedSource`, or pass full SQL via
``--workload federated``.
"""

from __future__ import annotations

from typing import Any, Sequence

import trino

from lakehouse.config import TrinoConfig
from lakehouse.connectors.base import Connector, QueryResult
from lakehouse.query.federation import (
    DEFAULT_FEDERATED_SOURCES,
    FederatedSource,
    build_federated_sql,
)


class TrinoConnector(Connector):
    """Run SQL and configurable multi-catalog queries on a Trino coordinator."""

    name = "trino"

    def __init__(self, config: TrinoConfig) -> None:
        self._config = config
        self._conn: trino.dbapi.Connection | None = None

    def connect(self) -> None:
        """Open a DB-API connection to the Trino coordinator."""
        self._conn = trino.dbapi.connect(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            catalog=self._config.catalog,
            schema=self._config.trino_schema,
        )

    def close(self) -> None:
        if self._conn:
            self._conn.close()
        self._conn = None

    def health(self) -> bool:
        try:
            self.execute("SELECT 1")
            return True
        except Exception:
            return False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> QueryResult:
        """Execute SQL and fetch all rows.

        Use this for **one** Trino catalog when you already have the SQL
        (e.g. ``SELECT COUNT(*) FROM iceberg.analytics.events``).

        Args:
            sql: Trino SQL (ANSI-ish).
            params: Optional query parameters for the driver.

        Returns:
            :class:`QueryResult` with column names and tuples.
        """
        if not self._conn:
            raise RuntimeError("Trino not connected")
        cur = self._conn.cursor()
        cur.execute(sql, params)
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        return QueryResult.from_cursor(columns, rows)

    def federated_query(
        self,
        sources: Sequence[FederatedSource] | None = None,
        *,
        join_key: str = "event_id",
        join_type: str = "LEFT",
        limit: int | None = 1000,
    ) -> QueryResult:
        """Query one or more Trino catalogs (built dynamically, not hard-coded).

        Args:
            sources: Tables to read/join. One entry = single-catalog scan; two or
                more = chained joins on ``join_key``. Defaults to
                :data:`~lakehouse.query.federation.DEFAULT_FEDERATED_SOURCES`.
            join_key: Equi-join column when ``len(sources) > 1``.
            join_type: Join keyword (``LEFT``, ``INNER``, …).
            limit: Row limit, or ``None`` for no limit.

        Returns:
            Query result from generated or implied SQL.

        Example — Iceberg only::

            federated_query([
                FederatedSource("iceberg", "analytics", "events", "i", ("event_id",)),
            ])

        Example — custom two-way join::

            federated_query([
                FederatedSource("iceberg", "analytics", "events", "i", ("event_id", "event_type")),
                FederatedSource("starrocks", "analytics", "event_metrics", "s", ("event_id", "metric_value")),
            ])
        """
        tables = sources if sources is not None else DEFAULT_FEDERATED_SOURCES
        sql = build_federated_sql(
            tables, join_key=join_key, join_type=join_type, limit=limit
        )
        return self.execute(sql)
