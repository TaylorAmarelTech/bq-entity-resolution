"""Tests for blocking evaluation metrics."""

import pytest

from bq_entity_resolution.blocking.engine import BlockingEngine
from bq_entity_resolution.config.schema import BlockingMetricsConfig
from bq_entity_resolution.sql.generator import SQLGenerator


def test_blocking_metrics_config_defaults():
    """Default blocking metrics config is disabled."""
    bm = BlockingMetricsConfig()
    assert not bm.enabled
    assert not bm.persist_to_table


def test_blocking_metrics_sql_generates(sample_config):
    """Blocking metrics SQL renders successfully."""
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    tier = sample_config.matching_tiers[0]

    sql = engine.generate_metrics_sql(tier)
    assert "candidate_pairs" in sql
    assert "matched_pairs" in sql
    assert "precision" in sql
    assert "reduction_ratio" in sql
    assert tier.name in sql


def test_blocking_metrics_references_correct_tables(sample_config):
    """Blocking metrics SQL references candidates and matches tables."""
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    tier = sample_config.matching_tiers[1]  # fuzzy

    sql = engine.generate_metrics_sql(tier)
    assert f"candidates_{tier.name}" in sql
    assert f"matches_{tier.name}" in sql


def test_blocking_metrics_includes_tier_name(sample_config):
    """Metrics output includes the tier name."""
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    tier = sample_config.matching_tiers[0]

    sql = engine.generate_metrics_sql(tier)
    assert f"'{tier.name}'" in sql


def test_blocking_metrics_includes_timestamp(sample_config):
    """Metrics output includes a computed_at timestamp."""
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    tier = sample_config.matching_tiers[0]

    sql = engine.generate_metrics_sql(tier)
    assert "computed_at" in sql


def test_monitoring_config_includes_blocking_metrics(sample_config):
    """MonitoringConfig has blocking_metrics field."""
    assert hasattr(sample_config.monitoring, "blocking_metrics")
    assert not sample_config.monitoring.blocking_metrics.enabled
