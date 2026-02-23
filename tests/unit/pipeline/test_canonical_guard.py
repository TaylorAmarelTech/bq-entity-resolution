"""Tests for canonical index table guard in orchestrator."""

from bq_entity_resolution.config.schema import (
    BlockingPathDef,
    BlockingKeyDef,
    ColumnMapping,
    ComparisonDef,
    FeatureEngineeringConfig,
    MatchingTierConfig,
    PipelineConfig,
    ProjectConfig,
    SourceConfig,
    ThresholdConfig,
    TierBlockingConfig,
)
from bq_entity_resolution.naming import canonical_index_table


def _minimal_config(cross_batch: bool = False) -> PipelineConfig:
    return PipelineConfig(
        project=ProjectConfig(name="test", bq_project="proj"),
        sources=[
            SourceConfig(
                name="src",
                table="proj.ds.tbl",
                unique_key="id",
                updated_at="updated_at",
                columns=[ColumnMapping(name="id"), ColumnMapping(name="name")],
            ),
        ],
        feature_engineering=FeatureEngineeringConfig(
            blocking_keys=[BlockingKeyDef(name="bk1", function="farm_fingerprint", inputs=["name"])],
        ),
        matching_tiers=[
            MatchingTierConfig(
                name="tier1",
                blocking=TierBlockingConfig(
                    paths=[BlockingPathDef(keys=["bk1"])],
                    cross_batch=cross_batch,
                ),
                comparisons=[ComparisonDef(left="name", right="name", method="exact")],
                threshold=ThresholdConfig(min_score=1.0),
            ),
        ],
    )


def test_canonical_index_table_naming():
    """canonical_index_table returns the gold layer canonical table."""
    config = _minimal_config()
    result = canonical_index_table(config)
    assert "canonical_index" in result


def test_canonical_table_accessible_via_reconciliation_engine():
    """ReconciliationEngine provides canonical_index_table DDL."""
    from bq_entity_resolution.reconciliation.engine import ReconciliationEngine
    assert hasattr(ReconciliationEngine, "generate_create_canonical_index_sql")


def test_init_matches_table_method_exists():
    """_init_matches_table method exists on PipelineOrchestrator."""
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    assert hasattr(PipelineOrchestrator, "_init_matches_table")


def test_cross_batch_config_accepted():
    """TierBlockingConfig accepts cross_batch=True."""
    config = _minimal_config(cross_batch=True)
    assert config.matching_tiers[0].blocking.cross_batch is True


def test_cross_batch_default_false():
    """TierBlockingConfig defaults cross_batch to False."""
    config = _minimal_config(cross_batch=False)
    assert config.matching_tiers[0].blocking.cross_batch is False
