"""Real-time pipeline: Kafka → OLTP / HTAP / OLAP.

Consumes JSON events from a Kafka topic, normalizes them, and writes to:

- **Vitess** — transactional ``orders`` rows (sharded OLTP)
- **TiDB** — upserted ``events`` table (HTAP serving)
- **StarRocks** — batched ``event_metrics`` (real-time aggregates)

Invoked via :meth:`lakehouse.engine.LakehouseEngine.streaming_pipeline` or
``lakehouse stream``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from lakehouse.connectors.kafka import KafkaConnector
from lakehouse.connectors.starrocks import StarRocksConnector
from lakehouse.connectors.tidb import TiDBConnector
from lakehouse.connectors.vitess import VitessConnector


class StreamingPipeline:
    """Fan-out consumer that lands each event on multiple serving layers."""

    def __init__(
        self,
        kafka: KafkaConnector,
        vitess: VitessConnector | None = None,
        tidb: TiDBConnector | None = None,
        starrocks: StarRocksConnector | None = None,
        *,
        source_topic: str = "events.raw",
        transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        """Wire connectors; omitted backends are skipped at runtime.

        Args:
            kafka: Connected Kafka consumer/producer.
            vitess: Optional OLTP target.
            tidb: Optional HTAP target.
            starrocks: Optional OLAP target (batch load at end of run).
            source_topic: Kafka topic to subscribe to.
            transform: Callable to normalize raw event JSON; defaults to
                :meth:`_default_transform`.
        """
        self._kafka = kafka
        self._vitess = vitess
        self._tidb = tidb
        self._starrocks = starrocks
        self._topic = source_topic
        self._transform = transform or self._default_transform

    def run(self, batch_size: int = 500) -> int:
        """Consume up to ``batch_size`` messages and write to configured stores.

        Returns:
            Number of events successfully processed.
        """
        self._kafka.subscribe([self._topic])
        processed = 0
        olap_batch: list[dict[str, Any]] = []

        for record in self._kafka.consume(max_messages=batch_size):
            event = self._transform(record["value"])
            event_id = event["event_id"]

            if self._vitess:
                self._vitess.execute(
                    "INSERT INTO orders (event_id, customer_id, amount, created_at) "
                    "VALUES (%s, %s, %s, %s)",
                    (
                        event_id,
                        event.get("customer_id"),
                        event.get("amount", 0),
                        event["occurred_at"],
                    ),
                )

            if self._tidb:
                self._tidb.upsert_event(
                    "events",
                    {
                        "event_id": event_id,
                        "event_type": event["event_type"],
                        "payload": json.dumps(event.get("payload", {})),
                        "occurred_at": event["occurred_at"],
                    },
                )

            olap_batch.append(
                {
                    "event_id": event_id,
                    "event_type": event["event_type"],
                    "metric_value": event.get("amount", 1),
                    "event_date": event["occurred_at"][:10],
                }
            )
            processed += 1

        if self._starrocks and olap_batch:
            self._starrocks.stream_load("event_metrics", olap_batch)

        return processed

    @staticmethod
    def _default_transform(raw: dict[str, Any]) -> dict[str, Any]:
        """Map heterogeneous inbound JSON to the canonical event shape.

        Fills missing ``event_id`` from ``id``, default ``event_type``, and
        timestamps in UTC ISO format.
        """
        now = datetime.now(timezone.utc).isoformat()
        return {
            "event_id": raw.get("event_id", raw.get("id")),
            "event_type": raw.get("event_type", "unknown"),
            "customer_id": raw.get("customer_id"),
            "amount": raw.get("amount", 0),
            "payload": raw.get("payload", raw),
            "occurred_at": raw.get("occurred_at", now),
        }
