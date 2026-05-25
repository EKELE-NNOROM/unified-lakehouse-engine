"""StarRocks connector (real-time OLAP / columnar MPP).

StarRocks exposes a MySQL-compatible FE port. Used for low-latency aggregates
fed by the streaming pipeline or lakehouse sync.
"""

from __future__ import annotations

from typing import Any

import pymysql
from pymysql.cursors import DictCursor

from lakehouse.config import StarRocksConfig
from lakehouse.connectors.base import Connector, QueryResult


class StarRocksConnector(Connector):
    """Execute SQL and batch-insert metric rows into StarRocks."""

    name = "starrocks"

    def __init__(self, config: StarRocksConfig) -> None:
        self._config = config
        self._conn: pymysql.Connection | None = None

    def connect(self) -> None:
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
        try:
            self.execute("SELECT 1")
            return True
        except Exception:
            return False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> QueryResult:
        """Run parameterized SQL; see :class:`TiDBConnector.execute`."""
        if not self._conn:
            raise RuntimeError("StarRocks not connected")
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return QueryResult(columns=[], rows=[], row_count=0)
            columns = [d[0] for d in cur.description]
            rows = [tuple(r.values()) for r in cur.fetchall()]
            return QueryResult.from_cursor(columns, rows)

    def stream_load(self, table: str, rows: list[dict[str, Any]]) -> int:
        """Insert many rows (one INSERT per row; suitable for moderate batches).

        Args:
            table: Destination table.
            rows: Homogeneous dicts (same keys in each row).

        Returns:
            Number of rows inserted.
        """
        if not rows:
            return 0
        cols = ", ".join(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(rows[0]))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        for row in rows:
            self.execute(sql, tuple(row.values()))
        return len(rows)
