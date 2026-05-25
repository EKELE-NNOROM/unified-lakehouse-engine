"""Vitess connector (horizontally sharded OLTP via VTGate).

Applications talk to **VTGate**, which routes queries to the correct shard
using the keyspace VSchema. This connector uses the MySQL protocol on the
VTGate port.
"""

from __future__ import annotations

from typing import Any

import pymysql
from pymysql.cursors import DictCursor

from lakehouse.config import VitessConfig
from lakehouse.connectors.base import Connector, QueryResult


class VitessConnector(Connector):
    """Execute SQL against a Vitess keyspace through VTGate."""

    name = "vitess"

    def __init__(self, config: VitessConfig) -> None:
        self._config = config
        self._conn: pymysql.Connection | None = None

    def connect(self) -> None:
        """Connect to VTGate using the configured keyspace as the database."""
        self._conn = pymysql.connect(
            host=self._config.vtgate_host,
            port=self._config.vtgate_port,
            user=self._config.user,
            password=self._config.password,
            database=self._config.keyspace,
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
        """Run SQL on the keyspace; see :class:`TiDBConnector.execute`."""
        if not self._conn:
            raise RuntimeError("Vitess not connected")
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return QueryResult(columns=[], rows=[], row_count=0)
            columns = [d[0] for d in cur.description]
            rows = [tuple(r.values()) for r in cur.fetchall()]
            return QueryResult.from_cursor(columns, rows)

    def route_by_shard_key(
        self, table: str, shard_key: str, payload: dict[str, Any]
    ) -> None:
        """Insert a row including an explicit shard key for VSchema routing.

        Vitess uses the VSchema to pick a shard; the shard key column must
        match your schema definition.

        Args:
            table: Table name in the keyspace.
            shard_key: Value used for sharding (added to payload as ``shard_key``).
            payload: Remaining column values.
        """
        payload = {**payload, "shard_key": shard_key}
        cols = ", ".join(payload.keys())
        placeholders = ", ".join(["%s"] * len(payload))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        self.execute(sql, tuple(payload.values()))
