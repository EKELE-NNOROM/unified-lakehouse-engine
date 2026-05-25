"""Apache Iceberg connector (open lakehouse table format).

Iceberg tables live on object storage (S3, etc.) with ACID commits, schema
evolution, and time travel. This connector uses a REST catalog (e.g. Tabular,
Nessie, or a REST shim) via PyIceberg.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from lakehouse.config import IcebergConfig
from lakehouse.connectors.base import Connector


class IcebergConnector(Connector):
    """Load or create an Iceberg table and read/write snapshots."""

    name = "iceberg"

    def __init__(self, config: IcebergConfig) -> None:
        self._config = config
        self._catalog = None
        self._table: Table | None = None

    def connect(self) -> None:
        """Attach to REST catalog and load (or create) the configured table."""
        self._catalog = load_catalog(
            self._config.catalog_name,
            **{
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": self._config.warehouse,
            },
        )
        identifier = (self._config.namespace, self._config.table)
        if self._catalog.table_exists(identifier):
            self._table = self._catalog.load_table(identifier)
        else:
            schema = pa.schema(
                [
                    ("event_id", pa.string()),
                    ("event_type", pa.string()),
                    ("occurred_at", pa.timestamp("us")),
                    ("payload", pa.string()),
                ]
            )
            self._table = self._catalog.create_table(identifier, schema=schema)

    def close(self) -> None:
        self._catalog = None
        self._table = None

    def health(self) -> bool:
        """True when a table handle is loaded."""
        return self._table is not None

    def append_batch(self, records: list[dict[str, Any]]) -> int:
        """Append rows as a PyArrow batch (new Iceberg snapshot).

        Args:
            records: List of dicts matching the table schema.

        Returns:
            Number of records written.
        """
        if not self._table:
            raise RuntimeError("Iceberg table not loaded")
        arrow_table = pa.Table.from_pylist(records)
        self._table.append(arrow_table)
        return len(records)

    def scan_sql_filter(self, predicate: str) -> list[dict[str, Any]]:
        """Scan rows matching an Iceberg row filter expression.

        Args:
            predicate: Iceberg expression string (e.g. ``occurred_at >= '...'``).

        Returns:
            Rows as Python dicts.
        """
        if not self._table:
            raise RuntimeError("Iceberg table not loaded")
        scan = self._table.scan(row_filter=predicate)
        df = scan.to_arrow()
        return df.to_pylist()
