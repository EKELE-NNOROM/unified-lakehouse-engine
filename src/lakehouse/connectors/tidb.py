"""TiDB connector (HTAP — hybrid transactional/analytical processing).

TiDB speaks the MySQL protocol. This connector is used for upserting into
serving tables (e.g. ``events``) where you want both OLTP writes and light
analytics on the same cluster.
"""

from __future__ import annotations

from typing import Any

import pymysql
from pymysql.cursors import DictCursor

from lakehouse.config import TiDBConfig
from lakehouse.connectors.base import Connector, QueryResult


class TiDBConnector(Connector):
    """Execute SQL and upsert events against a TiDB cluster."""

    name = "tidb"

    def __init__(self, config: TiDBConfig) -> None:
        self._config = config
        self._conn: pymysql.Connection | None = None

    def connect(self) -> None:
        """Open a PyMySQL connection with dict rows and autocommit."""
        self._conn = pymysql.connect(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            database=self._config.database,
            cursorclass=DictCursor,
            autocommit=True,
        )

    def close(self) -> None:
        if self._conn:
            self._conn.close()
        self._conn = None

    def health(self) -> bool:
        """Run ``SELECT 1``; return False on any error."""
        try:
            self.execute("SELECT 1")
            return True
        except Exception:
            return False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> QueryResult:
        """Run a parameterized query and return rows as :class:`QueryResult`.

        Args:
            sql: SQL with ``%s`` placeholders when ``params`` is set.
            params: Bound values for placeholders.

        Returns:
            Empty result for statements without a result set (DDL/DML).
        """
        if not self._conn:
            raise RuntimeError("TiDB not connected")
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return QueryResult(columns=[], rows=[], row_count=0)
            columns = [d[0] for d in cur.description]
            rows = [tuple(r.values()) for r in cur.fetchall()]
            return QueryResult.from_cursor(columns, rows)

    def upsert_event(self, table: str, event: dict[str, Any]) -> None:
        """Insert or update a row keyed by ``event_id``.

        Uses MySQL ``ON DUPLICATE KEY UPDATE`` (requires ``event_id`` as PK).

        Args:
            table: Target table name.
            event: Column name → value mapping.
        """
        cols = ", ".join(event.keys())
        placeholders = ", ".join(["%s"] * len(event))
        updates = ", ".join(f"{k}=VALUES({k})" for k in event if k != "event_id")
        sql = (
            f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )
        self.execute(sql, tuple(event.values()))
