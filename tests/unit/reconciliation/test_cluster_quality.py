"""Tests for cluster quality metrics."""

import pytest

from bq_entity_resolution.config.schema import ClusterQualityConfig
from bq_entity_resolution.reconciliation.engine import ReconciliationEngine
from bq_entity_resolution.sql.generator import SQLGenerator


def test_cluster_quality_config_defaults():
    """Default cluster quality config is disabled."""
    cq = ClusterQualityConfig()
    assert not cq.enabled
    assert not cq.persist_to_table
    assert cq.alert_max_cluster_size == 100
    assert cq.alert_singleton_ratio == 0.95


def test_cluster_quality_config_enabled():
    """Cluster quality config can be enabled with custom thresholds."""
    cq = ClusterQualityConfig(
        enabled=True,
        alert_max_cluster_size=50,
        alert_singleton_ratio=0.9,
    )
    assert cq.enabled
    assert cq.alert_max_cluster_size == 50


def test_cluster_quality_sql_generates(sample_config):
    """Cluster quality SQL renders successfully."""
    sql_gen = SQLGenerator()
    engine = ReconciliationEngine(sample_config, sql_gen)

    sql = engine.generate_quality_metrics_sql()
    assert "cluster_count" in sql
    assert "singleton_count" in sql
    assert "singleton_ratio" in sql
    assert "max_cluster_size" in sql
    assert "avg_cluster_size" in sql
    assert "median_cluster_size" in sql
    assert "avg_match_confidence" in sql


def test_cluster_quality_references_tables(sample_config):
    """Cluster quality SQL references cluster and matches tables."""
    sql_gen = SQLGenerator()
    engine = ReconciliationEngine(sample_config, sql_gen)

    sql = engine.generate_quality_metrics_sql()
    assert "entity_clusters" in sql
    assert "all_matched_pairs" in sql


def test_cluster_quality_includes_source_diversity(sample_config):
    """Cluster quality SQL includes source diversity metric."""
    sql_gen = SQLGenerator()
    engine = ReconciliationEngine(sample_config, sql_gen)

    sql = engine.generate_quality_metrics_sql()
    assert "source_count" in sql
    assert "avg_source_diversity" in sql


def test_monitoring_config_includes_cluster_quality(sample_config):
    """MonitoringConfig has cluster_quality field."""
    assert hasattr(sample_config.monitoring, "cluster_quality")
    assert not sample_config.monitoring.cluster_quality.enabled


def test_context_has_cluster_quality_field():
    """PipelineContext has cluster_quality attribute."""
    from datetime import datetime, timezone
    from bq_entity_resolution.pipeline.context import PipelineContext

    ctx = PipelineContext(
        run_id="test",
        started_at=datetime.now(timezone.utc),
        config=None,  # type: ignore
    )
    assert ctx.cluster_quality is None
