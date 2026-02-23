"""Tests for per-match audit trail."""

import pytest

from bq_entity_resolution.config.schema import (
    AuditTrailConfig,
    ComparisonLevelDef,
)
from bq_entity_resolution.matching.engine import MatchingEngine
from bq_entity_resolution.reconciliation.engine import ReconciliationEngine
from bq_entity_resolution.sql.generator import SQLGenerator


# ---------------------------------------------------------------
# Config model tests
# ---------------------------------------------------------------


def test_audit_trail_config_defaults():
    """Default audit trail config is disabled."""
    at = AuditTrailConfig()
    assert not at.enabled
    assert at.include_individual_scores


def test_audit_trail_config_enabled():
    """Audit trail config can be enabled."""
    at = AuditTrailConfig(enabled=True)
    assert at.enabled


# ---------------------------------------------------------------
# Sum-based scoring: audit trail
# ---------------------------------------------------------------


def test_sum_audit_trail_includes_match_detail(sample_config):
    """Sum-based scoring includes TO_JSON_STRING match_detail when audit enabled."""
    sample_config.reconciliation.output.audit_trail.enabled = True
    tier = sample_config.matching_tiers[1]  # fuzzy

    engine = MatchingEngine(sample_config)
    sql = engine.generate_tier_sql(tier, tier_index=1)
    assert "TO_JSON_STRING" in sql
    assert "match_detail" in sql


def test_sum_no_audit_trail_by_default(sample_config):
    """Sum-based scoring does NOT include match_detail when audit disabled."""
    tier = sample_config.matching_tiers[1]

    engine = MatchingEngine(sample_config)
    sql = engine.generate_tier_sql(tier, tier_index=1)
    assert "match_detail" not in sql


# ---------------------------------------------------------------
# F-S scoring: audit trail
# ---------------------------------------------------------------


def test_fs_audit_trail_includes_match_detail(sample_config):
    """F-S scoring includes TO_JSON_STRING match_detail when audit enabled."""
    sample_config.reconciliation.output.audit_trail.enabled = True
    tier = sample_config.matching_tiers[1]
    tier.threshold.method = "fellegi_sunter"
    tier.threshold.match_threshold = 5.0
    tier.comparisons[0].levels = [
        ComparisonLevelDef(label="exact", method="exact", m=0.9, u=0.1),
        ComparisonLevelDef(label="else", m=0.1, u=0.9),
    ]

    engine = MatchingEngine(sample_config)
    sql = engine.generate_tier_sql(tier, tier_index=1)
    assert "TO_JSON_STRING" in sql
    assert "match_detail" in sql


# ---------------------------------------------------------------
# Reconciliation: matches table DDL
# ---------------------------------------------------------------


def test_matches_table_has_audit_column(sample_config):
    """Accumulated matches table includes match_detail when audit enabled."""
    sample_config.reconciliation.output.audit_trail.enabled = True

    engine = ReconciliationEngine(sample_config)
    sql = engine.generate_create_matches_table_sql()
    assert "match_detail STRING" in sql


def test_matches_table_no_audit_column_by_default(sample_config):
    """Accumulated matches table does NOT include match_detail by default."""
    engine = ReconciliationEngine(sample_config)
    sql = engine.generate_create_matches_table_sql()
    assert "match_detail" not in sql


# ---------------------------------------------------------------
# Accumulate matches: audit trail column
# ---------------------------------------------------------------


def test_accumulate_matches_includes_audit_column(sample_config):
    """Accumulate matches SQL includes match_detail when audit enabled."""
    sample_config.reconciliation.output.audit_trail.enabled = True
    tier = sample_config.matching_tiers[0]

    engine = MatchingEngine(sample_config)
    sql = engine.generate_accumulate_matches_sql(
        tier, "proj.silver.all_matched_pairs"
    )
    assert "match_detail" in sql
