"""Workload-aware SQL routing across heterogeneous engines.

Not every query belongs on the same database: point lookups go to Vitess,
interactive analytics to StarRocks, lake scans to Trino/Iceberg, etc.
:class:`QueryRouter` encodes those rules so callers pass a :class:`WorkloadType`.

Federated Trino queries use :class:`FederatedSource` so you can join *N*
catalogs or query a *single* catalog without hard-coded SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Sequence

from lakehouse.connectors.base import QueryResult

if TYPE_CHECKING:
    from lakehouse.engine import LakehouseEngine


class WorkloadType(str, Enum):
    """Label that selects which backend runs a query.

    Values match the ``--workload`` flag on ``lakehouse query``.

    - ``LAKEHOUSE`` — one Trino catalog (usually Iceberg); pass your SQL.
    - ``FEDERATED`` — Trino across catalogs; pass SQL *or* rely on
      :func:`build_federated_sql` with a source list.
    """

    OLTP = "oltp"
    HTAP = "htap"
    OLAP_REALTIME = "olap_realtime"
    OLAP_WAREHOUSE = "olap_warehouse"
    LAKEHOUSE = "lakehouse"
    FEDERATED = "federated"


@dataclass(frozen=True)
class FederatedSource:
    """One table exposed through a Trino catalog.

    Example::

        FederatedSource("iceberg", "analytics", "events", "i", ("event_id",))

    Attributes:
        catalog: Trino catalog name (e.g. ``iceberg``, ``starrocks``).
        schema: Schema inside that catalog.
        table: Table name.
        alias: SQL alias for joins (e.g. ``i``).
        columns: Columns to project; empty tuple means ``alias.*`` (single-source
            queries only — prefer explicit columns when joining).
    """

    catalog: str
    schema: str
    table: str
    alias: str
    columns: tuple[str, ...] = ()


# Default three-way join used when ``FEDERATED`` runs without custom SQL/sources.
DEFAULT_FEDERATED_SOURCES: tuple[FederatedSource, ...] = (
    FederatedSource("iceberg", "analytics", "events", "i", ("event_id", "event_type")),
    FederatedSource(
        "starrocks", "analytics", "event_metrics", "s", ("event_id", "metric_value")
    ),
    FederatedSource("redshift", "public", "event_segments", "r", ("event_id", "segment")),
)


def build_federated_sql(
    sources: Sequence[FederatedSource],
    *,
    join_key: str = "event_id",
    join_type: str = "LEFT",
    limit: int | None = 1000,
) -> str:
    """Build Trino SQL for one or more catalog sources.

    - **One source** — simple ``SELECT … FROM catalog.schema.table`` (no join).
    - **Multiple sources** — chain ``JOIN``s on ``join_key`` to the first alias.

    Args:
        sources: Tables to query, in join order (first = driving table).
        join_key: Column name equi-joined across sources (default ``event_id``).
        join_type: ``INNER``, ``LEFT``, etc. (applied to each join after the first).
        limit: Optional ``LIMIT`` clause; ``None`` omits it.

    Returns:
        SQL string ready for :meth:`~lakehouse.connectors.trino.TrinoConnector.execute`.

    Raises:
        ValueError: Empty ``sources`` or missing ``join_key`` in columns when joining.
    """
    if not sources:
        raise ValueError("At least one FederatedSource is required")

    if len(sources) == 1:
        return _build_single_source_sql(sources[0], limit)

    return _build_multi_source_sql(sources, join_key=join_key, join_type=join_type, limit=limit)


def _qualified(source: FederatedSource) -> str:
    return f"{source.catalog}.{source.schema}.{source.table} {source.alias}"


def _select_list(sources: Sequence[FederatedSource]) -> str:
    parts: list[str] = []
    for s in sources:
        if s.columns:
            parts.extend(f"{s.alias}.{c}" for c in s.columns)
        else:
            parts.append(f"{s.alias}.*")
    return ", ".join(parts)


def _build_single_source_sql(source: FederatedSource, limit: int | None) -> str:
    select = _select_list((source,))
    sql = f"SELECT {select} FROM {_qualified(source)}"
    if limit is not None:
        sql += f"\nLIMIT {limit}"
    return sql


def _build_multi_source_sql(
    sources: Sequence[FederatedSource],
    *,
    join_key: str,
    join_type: str,
    limit: int | None,
) -> str:
    base = sources[0]
    select = _select_list(sources)
    sql = f"SELECT {select}\nFROM {_qualified(base)}"
    for s in sources[1:]:
        sql += (
            f"\n{join_type} JOIN {_qualified(s)}"
            f" ON {base.alias}.{join_key} = {s.alias}.{join_key}"
        )
    if limit is not None:
        sql += f"\nLIMIT {limit}"
    return sql


class QueryRouter:
    """Maps a workload label to the correct connector on a :class:`LakehouseEngine`."""

    def __init__(self, engine: LakehouseEngine) -> None:
        """Attach to an engine that already holds configured connectors."""
        self._engine = engine

    def route(
        self,
        sql: str,
        workload: WorkloadType,
        *,
        federated_sources: Sequence[FederatedSource] | None = None,
    ) -> QueryResult:
        """Execute SQL on the chosen engine.

        Args:
            sql: SQL for direct-connector workloads and for ``LAKEHOUSE`` /
                ``FEDERATED`` when you already know the statement.
            workload: Target system; see :class:`WorkloadType`.
            federated_sources: For ``FEDERATED`` only — tables to join. When
                ``sql`` is non-empty, SQL wins and sources are ignored. When
                ``sql`` is empty, builds SQL from ``federated_sources`` or
                :data:`DEFAULT_FEDERATED_SOURCES`.

        Returns:
            Normalized :class:`~lakehouse.connectors.base.QueryResult`.
        """
        match workload:
            case WorkloadType.OLTP:
                return self._engine.vitess.execute(sql)
            case WorkloadType.HTAP:
                return self._engine.tidb.execute(sql)
            case WorkloadType.OLAP_REALTIME:
                return self._engine.starrocks.execute(sql)
            case WorkloadType.OLAP_WAREHOUSE:
                return self._engine.redshift.execute(sql)
            case WorkloadType.LAKEHOUSE:
                return self._engine.trino.execute(sql)
            case WorkloadType.FEDERATED:
                return self._route_federated(sql, federated_sources)
            case _:
                raise ValueError(f"Unknown workload: {workload}")

    def _route_federated(
        self,
        sql: str,
        federated_sources: Sequence[FederatedSource] | None,
    ) -> QueryResult:
        """Run user SQL, or build SQL from one-or-more :class:`FederatedSource`."""
        if sql.strip():
            return self._engine.trino.execute(sql)
        sources = federated_sources if federated_sources is not None else DEFAULT_FEDERATED_SOURCES
        return self._engine.trino.federated_query(sources=sources)

    def explain_routing(self, workload: WorkloadType) -> str:
        """Human-readable description of where this workload runs (for docs/tests)."""
        routes = {
            WorkloadType.OLTP: "Vitess (sharded MySQL via VTGate)",
            WorkloadType.HTAP: "TiDB (distributed HTAP)",
            WorkloadType.OLAP_REALTIME: "StarRocks (columnar MPP)",
            WorkloadType.OLAP_WAREHOUSE: "Redshift (cloud warehouse)",
            WorkloadType.LAKEHOUSE: "Trino → Iceberg catalog (pass SQL)",
            WorkloadType.FEDERATED: "Trino — your SQL or dynamic multi-catalog join",
        }
        return routes[workload]
