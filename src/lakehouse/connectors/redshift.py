"""Amazon Redshift connector (cloud data warehouse).

Redshift uses the PostgreSQL wire protocol. This connector supports interactive
SQL via SQLAlchemy and bulk **COPY** loads from S3 (typical warehouse pattern).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from lakehouse.config import RedshiftConfig
from lakehouse.connectors.base import Connector, QueryResult


class RedshiftConnector(Connector):
    """Query and bulk-load data in Amazon Redshift."""

    name = "redshift"

    def __init__(self, config: RedshiftConfig) -> None:
        self._config = config
        self._engine: Engine | None = None

    def connect(self) -> None:
        """Create a SQLAlchemy engine with connection health checks."""
        url = (
            f"postgresql+psycopg2://{self._config.user}:{self._config.password}"
            f"@{self._config.host}:{self._config.port}/{self._config.database}"
        )
        self._engine = create_engine(url, pool_pre_ping=True)

    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
        self._engine = None

    def health(self) -> bool:
        try:
            self.execute("SELECT 1")
            return True
        except Exception:
            return False

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> QueryResult:
        """Run SQL with optional named parameters (SQLAlchemy ``text``).

        Args:
            sql: SQL string.
            params: Bind map for ``:name`` style placeholders.

        Returns:
            Fetched rows as :class:`QueryResult`.
        """
        if not self._engine:
            raise RuntimeError("Redshift not connected")
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            columns = list(result.keys())
            rows = [tuple(r) for r in result.fetchall()]
            return QueryResult.from_cursor(columns, rows)

    def copy_from_s3(self, table: str, s3_path: str) -> None:
        """Bulk load Parquet files from S3 using Redshift COPY.

        Args:
            table: Target table name (within ``db_schema``).
            s3_path: S3 URI prefix for staged files.

        Note:
            ``iam_role`` in config is required for COPY from S3 in production.
        """
        role = f"iam_role '{self._config.iam_role}'" if self._config.iam_role else ""
        sql = f"""
        COPY {self._config.db_schema}.{table}
        FROM '{s3_path}'
        {role}
        FORMAT AS PARQUET
        """
        self.execute(sql)
