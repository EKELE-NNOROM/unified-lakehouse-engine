"""Tests for federated SQL builder (no live Trino required)."""

from lakehouse.query.federation import (
    DEFAULT_FEDERATED_SOURCES,
    FederatedSource,
    build_federated_sql,
)


def test_build_sql_single_source() -> None:
    sql = build_federated_sql(
        [FederatedSource("iceberg", "analytics", "events", "i", ("event_id", "event_type"))],
        limit=100,
    )
    assert "FROM iceberg.analytics.events i" in sql
    assert "JOIN" not in sql
    assert "LIMIT 100" in sql


def test_build_sql_two_sources() -> None:
    sql = build_federated_sql(
        [
            FederatedSource("iceberg", "analytics", "events", "i", ("event_id",)),
            FederatedSource(
                "starrocks", "analytics", "event_metrics", "s", ("event_id", "metric_value")
            ),
        ],
        join_key="event_id",
    )
    assert "LEFT JOIN starrocks.analytics.event_metrics s" in sql
    assert "ON i.event_id = s.event_id" in sql


def test_default_sources_has_three_entries() -> None:
    assert len(DEFAULT_FEDERATED_SOURCES) == 3
