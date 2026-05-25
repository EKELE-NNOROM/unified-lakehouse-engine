"""Apache Kafka connector for event streaming.

Wraps ``confluent-kafka`` producer, consumer, and admin clients. Used as the
ingress path for :class:`~lakehouse.pipelines.streaming.StreamingPipeline` and
for seeding topics via ``lakehouse publish``.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Iterator

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from lakehouse.config import KafkaConfig
from lakehouse.connectors.base import Connector


class KafkaConnector(Connector):
    """Produce and consume JSON events on Kafka topics.

    Attributes:
        name: Always ``"kafka"`` (used in health output).
    """

    name = "kafka"

    def __init__(self, config: KafkaConfig) -> None:
        """Store config; clients are created in :meth:`connect`."""
        self._config = config
        self._producer: Producer | None = None
        self._consumer: Consumer | None = None
        self._admin: AdminClient | None = None

    def connect(self) -> None:
        """Initialize admin, producer (acks=all), and consumer (earliest offset)."""
        common = {
            "bootstrap.servers": self._config.bootstrap_servers,
            "security.protocol": self._config.security_protocol,
        }
        self._admin = AdminClient(common)
        self._producer = Producer({**common, "acks": "all"})
        self._consumer = Consumer(
            {
                **common,
                "group.id": self._config.consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

    def close(self) -> None:
        """Close consumer and drop producer/admin references."""
        if self._consumer:
            self._consumer.close()
        self._producer = None
        self._consumer = None
        self._admin = None

    def health(self) -> bool:
        """List topics via admin API; False if not connected."""
        if not self._admin:
            return False
        return len(self._admin.list_topics(timeout=5).topics) >= 0

    def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        """Serialize ``value`` as JSON and produce one message.

        Args:
            topic: Target topic name.
            key: Optional partition key (UTF-8 encoded).
            value: Dict serialized with ``json.dumps``.

        Raises:
            RuntimeError: If producer is not connected or delivery fails.
        """
        if not self._producer:
            raise RuntimeError("Kafka producer not connected")
        payload = json.dumps(value).encode("utf-8")
        self._producer.produce(
            topic,
            key=key.encode("utf-8") if key else None,
            value=payload,
            callback=self._delivery_callback,
        )
        self._producer.flush(timeout=10)

    def subscribe(self, topics: list[str]) -> None:
        """Assign the consumer to one or more topics."""
        if not self._consumer:
            raise RuntimeError("Kafka consumer not connected")
        self._consumer.subscribe(topics)

    def consume(
        self,
        max_messages: int = 100,
        timeout: float = 1.0,
        handler: Callable[[dict[str, Any]], None] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Poll messages up to ``max_messages`` and yield parsed records.

        Yields dicts with keys: ``topic``, ``partition``, ``offset``, ``key``,
        ``value`` (parsed JSON). Skips errored messages. Stops on poll timeout.

        Args:
            max_messages: Upper bound on messages returned this call.
            timeout: Poll timeout in seconds per message.
            handler: Optional side-effect callback per record.
        """
        if not self._consumer:
            raise RuntimeError("Kafka consumer not connected")
        count = 0
        while count < max_messages:
            msg = self._consumer.poll(timeout)
            if msg is None:
                break
            if msg.error():
                continue
            record = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": json.loads(msg.value().decode("utf-8")),
            }
            if handler:
                handler(record)
            yield record
            count += 1

    @staticmethod
    def _delivery_callback(err: object, msg: object) -> None:
        """Producer delivery report; raises on failure."""
        if err:
            raise RuntimeError(f"Kafka delivery failed: {err}")
