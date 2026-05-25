"""Unit tests (no live cluster required)."""

from lakehouse.config import PlatformConfig
from lakehouse.engine import LakehouseEngine
from lakehouse.query.federation import QueryRouter, WorkloadType


def test_platform_config_defaults() -> None:
    cfg = PlatformConfig()
    assert cfg.kafka.bootstrap_servers == "localhost:9092"
    assert cfg.iceberg.catalog_name == "lakehouse"


def test_engine_wiring() -> None:
    engine = LakehouseEngine()
    assert engine.kafka.name == "kafka"
    assert engine.trino.name == "trino"
    pipeline = engine.streaming_pipeline("test.topic")
    assert pipeline._topic == "test.topic"


def test_query_router_explain() -> None:
    engine = LakehouseEngine()
    router = QueryRouter(engine)
    assert "Vitess" in router.explain_routing(WorkloadType.OLTP)
    assert "Trino" in router.explain_routing(WorkloadType.LAKEHOUSE)
    assert "multi-catalog" in router.explain_routing(WorkloadType.FEDERATED)


def test_streaming_transform() -> None:
    from lakehouse.pipelines.streaming import StreamingPipeline

    raw = {"id": "x-1", "event_type": "click", "amount": 5}
    out = StreamingPipeline._default_transform(raw)
    assert out["event_id"] == "x-1"
    assert out["event_type"] == "click"
