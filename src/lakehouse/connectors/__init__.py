"""Datastore connectors — one class per integrated system.

Each connector wraps a vendor client (Kafka, PyMySQL, Trino, PyIceberg, etc.)
behind the shared :class:`~lakehouse.connectors.base.Connector` interface.
"""

from lakehouse.connectors.base import Connector, QueryResult
from lakehouse.connectors.iceberg import IcebergConnector
from lakehouse.connectors.kafka import KafkaConnector
from lakehouse.connectors.redshift import RedshiftConnector
from lakehouse.connectors.starrocks import StarRocksConnector
from lakehouse.connectors.tidb import TiDBConnector
from lakehouse.connectors.trino import TrinoConnector
from lakehouse.connectors.vitess import VitessConnector

__all__ = [
    "Connector",
    "QueryResult",
    "KafkaConnector",
    "TiDBConnector",
    "VitessConnector",
    "IcebergConnector",
    "TrinoConnector",
    "RedshiftConnector",
    "StarRocksConnector",
]
