"""Shared abstractions for all datastore connectors.

Every connector implements :class:`Connector` (connect, close, health) and
returns SQL results as :class:`QueryResult`. This keeps pipelines and the
engine agnostic of driver-specific APIs.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class QueryResult:
    """Normalized tabular result from any SQL-capable connector.

    Attributes:
        columns: Column names in result order.
        rows: Tuple of cell values per row.
        row_count: Number of rows (convenience, equals ``len(rows)``).
    """

    columns: list[str]
    rows: list[tuple[Any, ...]]
    row_count: int

    @classmethod
    def from_cursor(
        cls, columns: list[str], rows: list[tuple[Any, ...]]
    ) -> QueryResult:
        """Build a result from driver column names and fetched rows."""
        return cls(columns=columns, rows=rows, row_count=len(rows))


class Connector(ABC):
    """Minimal lifecycle contract for Kafka, SQL engines, and Iceberg.

    Subclasses set ``name`` (e.g. ``"kafka"``) for health reporting.
    Supports ``with Connector() as c:`` via :meth:`__enter__` / :meth:`__exit__`.
    """

    name: str

    @abstractmethod
    def connect(self) -> None:
        """Open client connections (idempotent where possible)."""

    @abstractmethod
    def close(self) -> None:
        """Release connections and other resources."""

    @abstractmethod
    def health(self) -> bool:
        """Return True if the backend is reachable and usable."""

    def __enter__(self) -> Connector:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
