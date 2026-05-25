"""Platform configuration: YAML files and typed settings models.

Configuration flow:

1. Copy ``config/platform.example.yaml`` to ``platform.yaml`` at the repo root.
2. Point each section at your real cluster hostnames/ports.
3. Pass ``-c platform.yaml`` to the CLI, or call ``LakehouseEngine.from_yaml(...)``.

Optional environment overrides use the ``LAKEHOUSE_`` prefix (see :class:`Settings`).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseModel):
    """Connection settings for Apache Kafka (streaming ingress)."""

    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str | None = None
    consumer_group: str = "lakehouse-engine"
    security_protocol: str = "PLAINTEXT"


class TiDBConfig(BaseModel):
    """Connection settings for TiDB (HTAP, MySQL-compatible protocol)."""

    host: str = "localhost"
    port: int = 4000
    user: str = "root"
    password: str = ""
    database: str = "analytics"


class VitessConfig(BaseModel):
    """Connection settings for Vitess VTGate (sharded OLTP)."""

    vtgate_host: str = "localhost"
    vtgate_port: int = 15306
    user: str = "root"
    password: str = ""
    keyspace: str = "commerce"


class IcebergConfig(BaseModel):
    """Iceberg REST catalog and default table location (lakehouse storage)."""

    catalog_name: str = "lakehouse"
    warehouse: str = "s3://lakehouse/warehouse"
    namespace: str = "raw"
    table: str = "events"


class TrinoConfig(BaseModel):
    """Trino coordinator settings (federated SQL engine)."""

    host: str = "localhost"
    port: int = 8080
    user: str = "lakehouse"
    catalog: str = "iceberg"
    trino_schema: str = "analytics"


class RedshiftConfig(BaseModel):
    """Amazon Redshift warehouse connection (PostgreSQL wire protocol)."""

    host: str = "localhost"
    port: int = 5439
    user: str = "admin"
    password: str = ""
    database: str = "analytics"
    db_schema: str = "public"
    iam_role: str | None = None


class StarRocksConfig(BaseModel):
    """StarRocks FE connection (real-time OLAP, MySQL protocol)."""

    host: str = "localhost"
    port: int = 9030
    user: str = "root"
    password: str = ""
    database: str = "analytics"


class PlatformConfig(BaseModel):
    """Root config object: one nested model per integrated system."""

    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    tidb: TiDBConfig = Field(default_factory=TiDBConfig)
    vitess: VitessConfig = Field(default_factory=VitessConfig)
    iceberg: IcebergConfig = Field(default_factory=IcebergConfig)
    trino: TrinoConfig = Field(default_factory=TrinoConfig)
    redshift: RedshiftConfig = Field(default_factory=RedshiftConfig)
    starrocks: StarRocksConfig = Field(default_factory=StarRocksConfig)


class Settings(BaseSettings):
    """Optional environment-variable overrides (prefix ``LAKEHOUSE_``).

    Example: ``LAKEHOUSE_KAFKA__BOOTSTRAP_SERVERS=kafka:9092``
    """

    model_config = SettingsConfigDict(
        env_prefix="LAKEHOUSE_",
        env_nested_delimiter="__",
        extra="ignore",
    )
    config_path: Path | None = None


def load_config(path: Path | str | None = None) -> PlatformConfig:
    """Parse a YAML config file into a :class:`PlatformConfig`.

    Args:
        path: Path to YAML. If ``None``, returns defaults (localhost).

    Returns:
        Validated config ready for :class:`~lakehouse.engine.LakehouseEngine`.
    """
    if path is None:
        return PlatformConfig()
    data: dict[str, Any] = yaml.safe_load(Path(path).read_text()) or {}
    return PlatformConfig.model_validate(data)
