"""Tests for link type support in blocking."""

import pytest

from bq_entity_resolution.blocking.engine import BlockingEngine
from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.sql.generator import SQLGenerator


def test_default_link_type(sample_config):
    """Default link_type is 'link_and_dedupe'."""
    assert sample_config.link_type == "link_and_dedupe"


def test_link_and_dedupe_no_source_filter(sample_config):
    """link_and_dedupe generates no source_name filter."""
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    tier = sample_config.matching_tiers[0]

    sql = engine.generate_candidates_sql(tier, tier_index=0)
    assert "l.source_name = r.source_name" not in sql
    assert "l.source_name != r.source_name" not in sql


def test_dedupe_only_same_source_filter(sample_config):
    """dedupe_only adds AND l.source_name = r.source_name."""
    sample_config.link_type = "dedupe_only"
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    tier = sample_config.matching_tiers[0]

    sql = engine.generate_candidates_sql(tier, tier_index=0)
    assert "l.source_name = r.source_name" in sql
    assert "l.source_name != r.source_name" not in sql


def test_link_only_different_source_filter(sample_config):
    """link_only adds AND l.source_name != r.source_name."""
    sample_config.link_type = "link_only"
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    tier = sample_config.matching_tiers[0]

    sql = engine.generate_candidates_sql(tier, tier_index=0)
    assert "l.source_name != r.source_name" in sql
    assert "l.source_name = r.source_name" not in sql


def test_link_type_applied_to_all_paths(sample_config):
    """Link type filter applies to every blocking path."""
    sample_config.link_type = "dedupe_only"
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    # fuzzy tier has one blocking path
    tier = sample_config.matching_tiers[1]

    sql = engine.generate_candidates_sql(tier, tier_index=1)
    assert "l.source_name = r.source_name" in sql


def test_link_type_applied_to_cross_batch(sample_config):
    """Link type filter present in cross-batch section when enabled."""
    sample_config.link_type = "link_only"
    sample_config.matching_tiers[0].blocking.cross_batch = True
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    tier = sample_config.matching_tiers[0]

    sql = engine.generate_candidates_sql(tier, tier_index=0)
    # cross_path section should exist and contain source filter
    assert "cross_path_0" in sql
    assert "l.source_name != r.source_name" in sql


def test_link_type_invalid_value_rejected():
    """Invalid link_type values are rejected by schema validation."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        PipelineConfig(
            version="1.0",
            project={"name": "t", "bq_project": "p", "bq_dataset_bronze": "b",
                      "bq_dataset_silver": "s", "bq_dataset_gold": "g",
                      "watermark_dataset": "w"},
            sources=[],
            matching_tiers=[],
            link_type="invalid_type",
        )


def test_link_type_passes_through_engine(sample_config):
    """BlockingEngine passes link_type from config to template render."""
    sample_config.link_type = "dedupe_only"
    sql_gen = SQLGenerator()
    engine = BlockingEngine(sample_config, sql_gen)
    tier = sample_config.matching_tiers[0]

    # The generated SQL should reflect the link_type
    sql = engine.generate_candidates_sql(tier, tier_index=0)
    # Confirm intra-batch has the filter
    assert "intra_path_0" in sql
    assert "l.source_name = r.source_name" in sql
