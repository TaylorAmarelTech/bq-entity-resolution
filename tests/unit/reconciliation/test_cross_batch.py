"""Tests for cross-batch cluster merging (Issue 3)."""

from bq_entity_resolution.reconciliation.engine import ReconciliationEngine
from bq_entity_resolution.sql.generator import SQLGenerator

# ---------------------------------------------------------------
# Canonical index creation
# ---------------------------------------------------------------


def test_create_canonical_index_sql_has_full_schema(sample_config):
    """DDL mirrors featured schema + cluster_id."""
    engine = ReconciliationEngine(sample_config, SQLGenerator())
    sql = engine.generate_create_canonical_index_sql()

    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "canonical_index" in sql
    assert "CLUSTER BY entity_uid" in sql
    assert "CAST(NULL AS STRING) AS cluster_id" in sql
    assert "WHERE FALSE" in sql
    assert "featured" in sql


# ---------------------------------------------------------------
# Canonical index population
# ---------------------------------------------------------------


def test_populate_canonical_index_sql(sample_config):
    """Populate SQL updates prior cluster_ids and inserts new entities."""
    engine = ReconciliationEngine(sample_config, SQLGenerator())
    sql = engine.generate_populate_canonical_index_sql()

    # Should UPDATE prior entities' cluster_ids
    assert "UPDATE" in sql
    assert "cluster_id" in sql

    # Should INSERT new entities
    assert "INSERT INTO" in sql
    assert "NOT IN" in sql
    assert "canonical_index" in sql


# ---------------------------------------------------------------
# Incremental clustering
# ---------------------------------------------------------------


def test_incremental_cluster_sql_uses_canonical_table(sample_config):
    """Incremental clustering initializes from canonical_index + new singletons."""
    engine = ReconciliationEngine(sample_config, SQLGenerator())
    sql = engine.generate_cluster_sql(cross_batch=True)

    # Should reference canonical_index for initialization
    assert "canonical_index" in sql
    # Should UNION ALL with featured (new singletons)
    assert "UNION ALL" in sql
    # Should NOT IN to exclude prior entities
    assert "NOT IN" in sql
    # Standard cluster propagation
    assert "WHILE" in sql
    assert "_edge_clusters" in sql


def test_standard_cluster_sql_unchanged(sample_config):
    """Standard (non-cross-batch) clustering is unchanged."""
    engine = ReconciliationEngine(sample_config, SQLGenerator())
    sql = engine.generate_cluster_sql(cross_batch=False)

    # Should NOT reference canonical_index
    assert "canonical_index" not in sql
    # Standard initialization from featured only
    assert "entity_uid AS cluster_id" in sql
    assert "WHILE" in sql


# ---------------------------------------------------------------
# Gold output cross-batch support
# ---------------------------------------------------------------


def test_gold_output_with_canonical_index(sample_config):
    """Gold output reads from canonical_index when use_canonical=True."""
    engine = ReconciliationEngine(sample_config, SQLGenerator())
    sql = engine.generate_gold_output_sql(use_canonical=True)

    assert "canonical_index" in sql
    # Should NOT join source_table + cluster_table when using canonical
    # (canonical_index already has cluster_id)
    assert "resolved_entities" in sql


def test_gold_output_standard(sample_config):
    """Standard gold output reads from featured + cluster_table."""
    engine = ReconciliationEngine(sample_config, SQLGenerator())
    sql = engine.generate_gold_output_sql(use_canonical=False)

    # Should NOT reference canonical_index
    assert "canonical_index" not in sql
    # Standard join
    assert "featured" in sql
    assert "entity_clusters" in sql


# ---------------------------------------------------------------
# Orchestrator integration (unit-level)
# ---------------------------------------------------------------


def test_init_matches_creates_canonical_index_with_full_schema(sample_config):
    """When cross_batch enabled, _init_matches_table creates proper canonical_index."""
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    from bq_entity_resolution.pipeline.context import PipelineContext
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator

    # Enable cross_batch
    sample_config.matching_tiers[0].blocking.cross_batch = True

    orch = object.__new__(PipelineOrchestrator)
    orch.config = sample_config
    orch.reconciliation_engine = ReconciliationEngine(sample_config, SQLGenerator())
    orch.runner = MagicMock()

    ctx = PipelineContext(
        run_id="test_run",
        started_at=datetime.now(UTC),
        config=sample_config,
    )

    orch._init_matches_table(ctx)

    # Should have been called twice: matches table + canonical index
    assert orch.runner.execute.call_count == 2
    canon_sql = orch.runner.execute.call_args_list[1][0][0]
    assert "CREATE TABLE IF NOT EXISTS" in canon_sql
    assert "CLUSTER BY entity_uid" in canon_sql
    assert "cluster_id" in canon_sql
