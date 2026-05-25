"""Central orchestration engine for the analytics platform.

``LakehouseEngine`` is the main programmatic entry point. It owns one connector
per datastore (Kafka, TiDB, Vitess, Iceberg, Trino, Redshift, StarRocks),
exposes factory methods for pipelines, and routes SQL via :class:`~lakehouse.query.federation.QueryRouter`.

Typical usage::

    engine = LakehouseEngine.from_yaml("platform.yaml")
    engine.connect_all()
    engine.streaming_pipeline().run()
    result = engine.query("SELECT COUNT(*) FROM events", WorkloadType.LAKEHOUSE)
    engine.close_all()

The CLI (``lakehouse``) is a thin wrapper around this class; prefer importing
``LakehouseEngine`` directly when building services or notebooks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lakehouse.config import PlatformConfig, load_config
from lakehouse.connectors.iceberg import IcebergConnector
from lakehouse.connectors.kafka import KafkaConnector
from lakehouse.connectors.redshift import RedshiftConnector
from lakehouse.connectors.starrocks import StarRocksConnector
from lakehouse.connectors.tidb import TiDBConnector
from lakehouse.connectors.trino import TrinoConnector
from lakehouse.connectors.vitess import VitessConnector
from lakehouse.pipelines.lakehouse_sync import LakehouseSyncPipeline
from lakehouse.pipelines.streaming import StreamingPipeline
from lakehouse.query.federation import QueryRouter, WorkloadType


class LakehouseEngine:
    """Unified coordinator for connectors, pipelines, and query routing.

    Workload placement (which engine handles which job):

    - **Kafka** — real-time event ingress
    - **Vitess** — horizontally sharded OLTP (MySQL via VTGate)
    - **TiDB** — HTAP: transactional + light analytical serving
    - **Iceberg** — open table format on object storage (lakehouse layer)
    - **Trino** — federated SQL across catalogs
    - **StarRocks** — low-latency OLAP
    - **Redshift** — large-scale warehouse analytics
    """

    def __init__(self, config: PlatformConfig | None = None) -> None:
        """Create an engine with the given config, or sensible defaults.

        Args:
            config: Parsed :class:`~lakehouse.config.PlatformConfig`. When
                omitted, localhost defaults are used (see ``config.py``).
        """
        self.config = config or PlatformConfig()
        self.kafka = KafkaConnector(self.config.kafka)
        self.tidb = TiDBConnector(self.config.tidb)
        self.vitess = VitessConnector(self.config.vitess)
        self.iceberg = IcebergConnector(self.config.iceberg)
        self.trino = TrinoConnector(self.config.trino)
        self.redshift = RedshiftConnector(self.config.redshift)
        self.starrocks = StarRocksConnector(self.config.starrocks)
        self._router: QueryRouter | None = None

    @classmethod
    def from_yaml(cls, path: str | Path) -> LakehouseEngine:
        """Build an engine from a ``platform.yaml`` file.

        Args:
            path: Filesystem path to YAML (see ``config/platform.example.yaml``).

        Returns:
            Engine with all connector settings populated from the file.
        """
        return cls(load_config(path))

    def connect_all(self) -> dict[str, bool]:
        """Open connections to every connector and report health.

        Returns:
            Mapping of connector name (e.g. ``"kafka"``) to whether
            :meth:`~lakehouse.connectors.base.Connector.health` succeeded.
            Failed connections are recorded as ``False`` without raising.
        """
        connectors = [
            self.kafka,
            self.tidb,
            self.vitess,
            self.iceberg,
            self.trino,
            self.redshift,
            self.starrocks,
        ]
        health: dict[str, bool] = {}
        for conn in connectors:
            try:
                conn.connect()
                health[conn.name] = conn.health()
            except Exception:
                health[conn.name] = False
        return health

    def close_all(self) -> None:
        """Release all open connections. Safe to call multiple times."""
        for conn in (
            self.kafka,
            self.tidb,
            self.vitess,
            self.iceberg,
            self.trino,
            self.redshift,
            self.starrocks,
        ):
            conn.close()

    @property
    def router(self) -> QueryRouter:
        """Lazy-initialized SQL router for workload-aware queries."""
        if self._router is None:
            self._router = QueryRouter(self)
        return self._router

    def streaming_pipeline(self, topic: str = "events.raw") -> StreamingPipeline:
        """Return a configured real-time ingestion pipeline.

        Args:
            topic: Kafka topic name to consume (default ``events.raw``).

        Returns:
            Pipeline that fans out to Vitess, TiDB, and StarRocks when those
            connectors are available.
        """
        return StreamingPipeline(
            self.kafka,
            vitess=self.vitess,
            tidb=self.tidb,
            starrocks=self.starrocks,
            source_topic=topic,
        )

    def lakehouse_pipeline(self) -> LakehouseSyncPipeline:
        """Return a configured batch sync pipeline (Iceberg → warehouses).

        Reads new Iceberg snapshots, aggregates via Trino, loads OLAP targets.
        """
        return LakehouseSyncPipeline(
            self.iceberg,
            self.trino,
            starrocks=self.starrocks,
            redshift=self.redshift,
        )

    def query(
        self,
        sql: str,
        workload: WorkloadType = WorkloadType.LAKEHOUSE,
        **route_kwargs: Any,
    ) -> Any:
        """Run SQL on the engine best suited for the workload type.

        Args:
            sql: SQL string. Required for ``LAKEHOUSE`` and direct connectors.
                For ``FEDERATED``, if empty, a join is built from
                ``federated_sources`` (see :meth:`~lakehouse.query.federation.QueryRouter.route`).
            workload: Target system; see :class:`~lakehouse.query.federation.WorkloadType`.
            **route_kwargs: Forwarded to :meth:`~lakehouse.query.federation.QueryRouter.route`
                (e.g. ``federated_sources=[...]``).

        Returns:
            :class:`~lakehouse.connectors.base.QueryResult` with columns and rows.
        """
        return self.router.route(sql, workload, **route_kwargs)

    def publish_event(self, topic: str, event: dict[str, Any]) -> None:
        """Send one JSON event to Kafka (uses ``event_id`` as the message key).

        Args:
            topic: Destination Kafka topic.
            event: Serializable dict (must be JSON-encodable).
        """
        self.kafka.publish(topic, event.get("event_id"), event)
