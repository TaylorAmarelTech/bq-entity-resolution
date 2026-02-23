"""Tests for term frequency adjustments in matching."""

import pytest

from bq_entity_resolution.config.schema import (
    ComparisonDef,
    ComparisonLevelDef,
    TermFrequencyConfig,
)
from bq_entity_resolution.features.engine import FeatureEngine
from bq_entity_resolution.matching.engine import MatchingEngine
from bq_entity_resolution.naming import term_frequency_table
from bq_entity_resolution.sql.generator import SQLGenerator


# ---------------------------------------------------------------
# FeatureEngine: TF SQL generation
# ---------------------------------------------------------------


def test_generate_tf_sql_returns_none_when_disabled(sample_config):
    """No TF SQL generated when no comparison has TF enabled."""
    engine = FeatureEngine(sample_config)
    assert engine.generate_term_frequency_sql() is None


def test_generate_tf_sql_returns_sql_when_enabled(sample_config):
    """TF SQL generated when a comparison has TF enabled."""
    tier = sample_config.matching_tiers[1]  # fuzzy tier
    tier.comparisons[0].tf_adjustment = TermFrequencyConfig(enabled=True)

    engine = FeatureEngine(sample_config)
    sql = engine.generate_term_frequency_sql()
    assert sql is not None
    assert "CREATE OR REPLACE TABLE" in sql
    assert "tf_frequency" in sql
    assert "term_frequencies" in sql


def test_tf_sql_includes_correct_column(sample_config):
    """TF SQL computes frequencies for the correct column."""
    tier = sample_config.matching_tiers[1]
    tier.comparisons[0].tf_adjustment = TermFrequencyConfig(
        enabled=True,
        tf_adjustment_column="last_name_clean",
    )

    engine = FeatureEngine(sample_config)
    sql = engine.generate_term_frequency_sql()
    assert "last_name_clean" in sql


def test_tf_sql_deduplicates_columns(sample_config):
    """Same column referenced by multiple comparisons appears only once."""
    tier = sample_config.matching_tiers[1]
    # Both comparisons use TF on same default column (left)
    tier.comparisons[0].tf_adjustment = TermFrequencyConfig(enabled=True)
    # Add another comparison with TF on same column
    tier.comparisons.append(ComparisonDef(
        left="first_name_clean",
        right="first_name_clean",
        method="exact",
        weight=1.0,
        tf_adjustment=TermFrequencyConfig(enabled=True),
    ))

    engine = FeatureEngine(sample_config)
    columns = engine._collect_tf_columns()
    col_names = [c["column_name"] for c in columns]
    assert len(col_names) == len(set(col_names)), "Columns should be deduplicated"


# ---------------------------------------------------------------
# MatchingEngine: TF in sum-based scoring
# ---------------------------------------------------------------


def test_sum_scoring_includes_tf_join(sample_config):
    """Sum-based scoring includes TF table join when TF enabled."""
    tier = sample_config.matching_tiers[1]
    tier.comparisons[0].tf_adjustment = TermFrequencyConfig(enabled=True)

    engine = MatchingEngine(sample_config)
    sql = engine.generate_tier_sql(tier, tier_index=1)
    assert "term_frequencies" in sql
    assert "tf_frequency" in sql
    assert "LEFT JOIN" in sql


def test_sum_scoring_no_tf_join_when_disabled(sample_config):
    """Sum-based scoring has no TF join when TF is disabled."""
    tier = sample_config.matching_tiers[1]

    engine = MatchingEngine(sample_config)
    sql = engine.generate_tier_sql(tier, tier_index=1)
    assert "term_frequencies" not in sql


def test_sum_tf_uses_minimum_u(sample_config):
    """TF adjustment uses the minimum_u floor value."""
    tier = sample_config.matching_tiers[1]
    tier.comparisons[0].tf_adjustment = TermFrequencyConfig(
        enabled=True,
        tf_minimum_u_value=0.005,
    )

    engine = MatchingEngine(sample_config)
    sql = engine.generate_tier_sql(tier, tier_index=1)
    assert "0.005" in sql


# ---------------------------------------------------------------
# MatchingEngine: TF in Fellegi-Sunter scoring
# ---------------------------------------------------------------


def test_fs_scoring_includes_tf_join(sample_config):
    """F-S scoring includes TF join when TF is enabled on a comparison."""
    tier = sample_config.matching_tiers[1]
    tier.threshold.method = "fellegi_sunter"
    tier.threshold.match_threshold = 5.0
    # Add levels to first comparison
    tier.comparisons[0].levels = [
        ComparisonLevelDef(label="exact", method="exact", m=0.9, u=0.1),
        ComparisonLevelDef(label="else", m=0.1, u=0.9),
    ]
    tier.comparisons[0].tf_adjustment = TermFrequencyConfig(enabled=True)

    engine = MatchingEngine(sample_config)
    sql = engine.generate_tier_sql(tier, tier_index=1)
    assert "term_frequencies" in sql
    assert "LEFT JOIN" in sql


def test_fs_tf_adjusted_uses_dynamic_weight(sample_config):
    """TF-adjusted F-S levels compute log-weight dynamically."""
    tier = sample_config.matching_tiers[1]
    tier.threshold.method = "fellegi_sunter"
    tier.threshold.match_threshold = 5.0
    tier.comparisons[0].levels = [
        ComparisonLevelDef(label="exact", method="exact", m=0.9, u=0.1),
        ComparisonLevelDef(label="else", m=0.1, u=0.9),
    ]
    tier.comparisons[0].tf_adjustment = TermFrequencyConfig(enabled=True)

    engine = MatchingEngine(sample_config)
    sql = engine.generate_tier_sql(tier, tier_index=1)
    # Should have LOG-based dynamic computation
    assert "LOG(" in sql
    assert "GREATEST" in sql


def test_fs_non_tf_levels_use_static_weight(sample_config):
    """Non-TF levels in F-S still use pre-computed static log-weight."""
    tier = sample_config.matching_tiers[1]
    tier.threshold.method = "fellegi_sunter"
    tier.threshold.match_threshold = 5.0
    # Comparison without TF
    tier.comparisons[1].levels = [
        ComparisonLevelDef(label="exact", method="exact", m=0.9, u=0.1),
        ComparisonLevelDef(label="else", m=0.1, u=0.9),
    ]

    engine = MatchingEngine(sample_config)
    comparisons = engine._build_level_comparisons(tier)
    # Second comparison has no TF
    for level in comparisons[1]["levels"]:
        assert not level.get("tf_adjusted", False)


# ---------------------------------------------------------------
# Naming: term_frequency_table
# ---------------------------------------------------------------


def test_term_frequency_table_name(sample_config):
    """term_frequency_table returns correct fully-qualified name."""
    result = term_frequency_table(sample_config)
    assert result == "test-project.test_silver.term_frequencies"
