"""Tests for Fellegi-Sunter SQL generation."""

import math

from bq_entity_resolution.matching.engine import MatchingEngine
from bq_entity_resolution.sql.generator import SQLGenerator


def test_fellegi_sunter_template_renders(sample_config):
    """F-S template renders without errors."""
    sql_gen = SQLGenerator()
    engine = MatchingEngine(sample_config, sql_gen)

    tier = sample_config.matching_tiers[0]
    # Override threshold to fellegi_sunter
    tier.threshold.method = "fellegi_sunter"
    tier.threshold.match_threshold = 4.0

    # Set manual params
    from bq_entity_resolution.matching.parameters import (
        ComparisonParameters,
        TierParameters,
    )

    params = TierParameters(
        tier_name=tier.name,
        comparisons=[
            ComparisonParameters(
                comparison_name=f"{comp.left}__{comp.method}".replace(".", "_"),
                levels=[
                    {"label": "match", "m": 0.9, "u": 0.1},
                    {"label": "else", "m": 0.1, "u": 0.9},
                ],
            )
            for comp in tier.comparisons
        ],
        prior_match_prob=0.1,
    )
    engine.set_tier_parameters(tier.name, params)

    sql = engine.generate_tier_sql(tier, tier_index=0)
    assert "CREATE OR REPLACE TABLE" in sql
    assert "POW(2.0, total_score)" in sql
    assert "match_confidence" in sql
    assert "log_weight_" in sql
    assert "4.0" in sql  # match_threshold
    # COALESCE guards against NULL from missing ELSE
    assert "COALESCE(CASE" in sql
    # Overflow protection: clamped match_confidence
    assert "WHEN total_score > 50 THEN 1.0" in sql


def test_fellegi_sunter_log_weights_in_sql(sample_config):
    """F-S SQL contains computed log weights."""
    sql_gen = SQLGenerator()
    engine = MatchingEngine(sample_config, sql_gen)

    tier = sample_config.matching_tiers[0]
    tier.threshold.method = "fellegi_sunter"
    tier.threshold.match_threshold = 3.0

    from bq_entity_resolution.matching.parameters import (
        ComparisonParameters,
        TierParameters,
    )

    params = TierParameters(
        tier_name=tier.name,
        comparisons=[
            ComparisonParameters(
                comparison_name=f"{comp.left}__{comp.method}".replace(".", "_"),
                levels=[
                    {"label": "match", "m": 0.95, "u": 0.05},
                    {"label": "else", "m": 0.05, "u": 0.95},
                ],
            )
            for comp in tier.comparisons
        ],
        prior_match_prob=0.1,
    )
    engine.set_tier_parameters(tier.name, params)

    sql = engine.generate_tier_sql(tier, tier_index=0)

    # log2(0.95/0.05) ≈ 4.247928
    expected_match_weight = round(math.log2(0.95 / 0.05), 6)
    assert str(expected_match_weight) in sql

    # log2(0.05/0.95) ≈ -4.247928
    expected_else_weight = round(math.log2(0.05 / 0.95), 6)
    assert str(expected_else_weight) in sql


def test_fellegi_sunter_with_explicit_levels(sample_config):
    """F-S with multi-level comparisons produces correct CASE/WHEN structure."""
    sql_gen = SQLGenerator()
    engine = MatchingEngine(sample_config, sql_gen)

    from bq_entity_resolution.config.schema import ComparisonLevelDef

    tier = sample_config.matching_tiers[0]
    tier.threshold.method = "fellegi_sunter"
    tier.threshold.match_threshold = 5.0

    # Add levels to first comparison
    tier.comparisons[0].levels = [
        ComparisonLevelDef(label="exact", method="exact", m=0.95, u=0.01),
        ComparisonLevelDef(
            label="fuzzy",
            method="levenshtein",
            params={"max_distance": 2},
            m=0.70,
            u=0.10,
        ),
        ComparisonLevelDef(label="else", m=0.05, u=0.89),
    ]

    sql = engine.generate_tier_sql(tier, tier_index=0)

    # Should have WHEN clauses for each level
    assert "WHEN" in sql
    assert "ELSE" in sql
    assert "EDIT_DISTANCE" in sql  # levenshtein in fuzzy level


def test_sum_scoring_unchanged(sample_config):
    """Sum-based scoring still works exactly as before."""
    sql_gen = SQLGenerator()
    engine = MatchingEngine(sample_config, sql_gen)

    tier = sample_config.matching_tiers[0]
    assert tier.threshold.method == "sum"  # default

    sql = engine.generate_tier_sql(tier, tier_index=0)
    assert "CASE WHEN" in sql
    assert "total_score" in sql
    assert "tier_comparisons" not in sql or "CREATE OR REPLACE TABLE" in sql
    # Should NOT have POW(2.0, ...) — that's F-S only
    assert "POW(2.0" not in sql


def test_auto_binary_levels(sample_config):
    """Comparisons without explicit levels get auto binary levels."""
    sql_gen = SQLGenerator()
    engine = MatchingEngine(sample_config, sql_gen)

    tier = sample_config.matching_tiers[0]

    levels = engine._build_level_comparisons(tier)
    for comp in levels:
        assert len(comp["levels"]) == 2
        assert comp["levels"][0]["label"] == "match"
        assert comp["levels"][1]["label"] == "else"
        assert comp["levels"][0]["sql_expr"] is not None
        assert comp["levels"][1]["sql_expr"] is None


def test_fellegi_sunter_coalesce_null_guard(sample_config):
    """COALESCE wraps CASE to prevent NULL propagation from missing ELSE."""
    sql_gen = SQLGenerator()
    engine = MatchingEngine(sample_config, sql_gen)

    from bq_entity_resolution.config.schema import ComparisonLevelDef
    from bq_entity_resolution.matching.parameters import (
        ComparisonParameters,
        TierParameters,
    )

    tier = sample_config.matching_tiers[0]
    tier.threshold.method = "fellegi_sunter"
    tier.threshold.match_threshold = 3.0

    # Explicit levels WITHOUT an else (no catch-all) — should still not produce NULL
    tier.comparisons[0].levels = [
        ComparisonLevelDef(label="exact", method="exact", m=0.95, u=0.01),
        ComparisonLevelDef(
            label="fuzzy", method="levenshtein",
            params={"max_distance": 2}, m=0.70, u=0.10,
        ),
    ]

    params = TierParameters(
        tier_name=tier.name,
        comparisons=[
            ComparisonParameters(
                comparison_name=f"{comp.left}__{comp.method}".replace(".", "_"),
                levels=[
                    {"label": "match", "m": 0.9, "u": 0.1},
                    {"label": "else", "m": 0.1, "u": 0.9},
                ],
            )
            for comp in tier.comparisons
        ],
        prior_match_prob=0.1,
    )
    engine.set_tier_parameters(tier.name, params)

    sql = engine.generate_tier_sql(tier, tier_index=0)
    # Every CASE is wrapped in COALESCE(..., 0.0)
    assert sql.count("COALESCE(CASE") >= 2  # at least per-column + total


def test_fellegi_sunter_overflow_clamp(sample_config):
    """Match confidence uses clamping to avoid POW(2.0, x) overflow."""
    sql_gen = SQLGenerator()
    engine = MatchingEngine(sample_config, sql_gen)

    from bq_entity_resolution.matching.parameters import (
        ComparisonParameters,
        TierParameters,
    )

    tier = sample_config.matching_tiers[0]
    tier.threshold.method = "fellegi_sunter"
    tier.threshold.match_threshold = 3.0

    params = TierParameters(
        tier_name=tier.name,
        comparisons=[
            ComparisonParameters(
                comparison_name=f"{comp.left}__{comp.method}".replace(".", "_"),
                levels=[
                    {"label": "match", "m": 0.9, "u": 0.1},
                    {"label": "else", "m": 0.1, "u": 0.9},
                ],
            )
            for comp in tier.comparisons
        ],
        prior_match_prob=0.1,
    )
    engine.set_tier_parameters(tier.name, params)

    sql = engine.generate_tier_sql(tier, tier_index=0)
    # Clamp: high scores → 1.0, low scores → 0.0
    assert "WHEN total_score > 50 THEN 1.0" in sql
    assert "WHEN total_score < -50 THEN 0.0" in sql
    # Still uses POW for mid-range scores
    assert "SAFE_DIVIDE(POW(2.0, total_score)" in sql


def test_needs_jaro_winkler_in_levels(sample_config):
    """Jaro-Winkler detection works for comparison levels too."""
    from bq_entity_resolution.config.schema import ComparisonLevelDef

    sql_gen = SQLGenerator()
    engine = MatchingEngine(sample_config, sql_gen)

    # Initially no JW
    assert not engine._needs_jaro_winkler()

    # Add JW in a level
    tier = sample_config.matching_tiers[0]
    tier.comparisons[0].levels = [
        ComparisonLevelDef(label="exact", method="exact", m=0.9, u=0.1),
        ComparisonLevelDef(label="fuzzy", method="jaro_winkler", m=0.7, u=0.2),
        ComparisonLevelDef(label="else", m=0.1, u=0.9),
    ]
    assert engine._needs_jaro_winkler()
