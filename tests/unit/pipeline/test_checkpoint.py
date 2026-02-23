"""Tests for pipeline checkpoint/resume functionality."""

from datetime import UTC
from unittest.mock import MagicMock

from bq_entity_resolution.config.schema import (
    BlockingKeyDef,
    BlockingPathDef,
    ColumnMapping,
    ComparisonDef,
    FeatureEngineeringConfig,
    MatchingTierConfig,
    PipelineConfig,
    ProjectConfig,
    ScaleConfig,
    SourceConfig,
    ThresholdConfig,
    TierBlockingConfig,
)
from bq_entity_resolution.pipeline import orchestrator as orch_module
from bq_entity_resolution.pipeline.context import PipelineContext


def _minimal_config(checkpoint_enabled: bool = False) -> PipelineConfig:
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
            blocking_keys=[
                BlockingKeyDef(name="bk1", function="farm_fingerprint", inputs=["name"]),
            ],
        ),
        matching_tiers=[
            MatchingTierConfig(
                name="tier1",
                blocking=TierBlockingConfig(paths=[BlockingPathDef(keys=["bk1"])]),
                comparisons=[ComparisonDef(left="name", right="name", method="exact")],
                threshold=ThresholdConfig(min_score=1.0),
            ),
        ],
        scale=ScaleConfig(checkpoint_enabled=checkpoint_enabled),
    )


def _mock_context(config: PipelineConfig, completed: set | None = None) -> PipelineContext:
    from datetime import datetime
    ctx = PipelineContext(
        run_id="test_run",
        started_at=datetime.now(UTC),
        config=config,
    )
    if completed:
        ctx.completed_stages = completed
    return ctx


def test_checkpoint_stage_constants_defined():
    """All stage constants exist as module-level attributes."""
    expected = [
        "STAGE_WATERMARKS", "STAGE_STAGING", "STAGE_FEATURES",
        "STAGE_TERM_FREQ", "STAGE_EMBEDDINGS", "STAGE_UDFS",
        "STAGE_PARAMS", "STAGE_MATCHES_INIT", "STAGE_TIERS",
        "STAGE_RECONCILE", "STAGE_REVIEW", "STAGE_WATERMARK_ADV",
    ]
    for name in expected:
        assert hasattr(orch_module, name), f"Missing stage constant: {name}"
        assert isinstance(getattr(orch_module, name), str)


def test_should_skip_stage_disabled():
    """When checkpoint is disabled, _should_skip_stage always returns False."""
    config = _minimal_config(checkpoint_enabled=False)
    # Access the method via a mock orchestrator
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    ctx = _mock_context(config, completed={"init_watermarks"})
    assert orch._should_skip_stage(ctx, "init_watermarks") is False


def test_should_skip_stage_enabled_not_in_set():
    """Returns False for stages not yet completed."""
    config = _minimal_config(checkpoint_enabled=True)
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    ctx = _mock_context(config)
    assert orch._should_skip_stage(ctx, "init_watermarks") is False


def test_should_skip_stage_enabled_in_set():
    """Returns True for stages already in completed_stages."""
    config = _minimal_config(checkpoint_enabled=True)
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    ctx = _mock_context(config, completed={"init_watermarks"})
    assert orch._should_skip_stage(ctx, "init_watermarks") is True


def test_mark_stage_complete_adds_to_set():
    """_mark_stage_complete adds the stage to completed_stages."""
    config = _minimal_config()
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    orch.checkpoint_mgr = None
    ctx = _mock_context(config)
    assert "feature_engineering" not in ctx.completed_stages
    orch._mark_stage_complete(ctx, "feature_engineering")
    assert "feature_engineering" in ctx.completed_stages


def test_checkpoint_disabled_runs_all_stages():
    """With checkpoint disabled (default), all stages execute normally."""
    config = _minimal_config(checkpoint_enabled=False)
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    ctx = _mock_context(config)
    # All stages should NOT be skipped
    for stage in [
        orch_module.STAGE_WATERMARKS, orch_module.STAGE_STAGING,
        orch_module.STAGE_FEATURES, orch_module.STAGE_TIERS,
    ]:
        assert orch._should_skip_stage(ctx, stage) is False


def test_checkpoint_enabled_marks_stages():
    """With checkpoint enabled, _mark_stage_complete populates completed_stages."""
    config = _minimal_config(checkpoint_enabled=True)
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    orch.checkpoint_mgr = MagicMock()
    ctx = _mock_context(config)
    orch._mark_stage_complete(ctx, orch_module.STAGE_WATERMARKS)
    orch._mark_stage_complete(ctx, orch_module.STAGE_STAGING)
    assert orch_module.STAGE_WATERMARKS in ctx.completed_stages
    assert orch_module.STAGE_STAGING in ctx.completed_stages
    assert orch_module.STAGE_FEATURES not in ctx.completed_stages


def test_checkpoint_skips_completed_stages():
    """Pre-populated completed_stages cause those stages to be skipped."""
    config = _minimal_config(checkpoint_enabled=True)
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    ctx = _mock_context(
        config,
        completed={
            orch_module.STAGE_WATERMARKS,
            orch_module.STAGE_STAGING,
            orch_module.STAGE_FEATURES,
        },
    )
    assert orch._should_skip_stage(ctx, orch_module.STAGE_WATERMARKS) is True
    assert orch._should_skip_stage(ctx, orch_module.STAGE_STAGING) is True
    assert orch._should_skip_stage(ctx, orch_module.STAGE_FEATURES) is True
    assert orch._should_skip_stage(ctx, orch_module.STAGE_TIERS) is False


def test_mark_stage_complete_persists_to_checkpoint_mgr():
    """When checkpoint_mgr is set, _mark_stage_complete persists to BQ."""
    config = _minimal_config(checkpoint_enabled=True)
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    orch.checkpoint_mgr = MagicMock()
    ctx = _mock_context(config)
    orch._mark_stage_complete(ctx, orch_module.STAGE_STAGING)
    assert orch_module.STAGE_STAGING in ctx.completed_stages
    orch.checkpoint_mgr.mark_stage_complete.assert_called_once_with(
        ctx.run_id, orch_module.STAGE_STAGING,
    )


def test_mark_stage_complete_no_checkpoint_mgr():
    """When checkpoint_mgr is None, stage is still added to in-memory set."""
    config = _minimal_config(checkpoint_enabled=False)
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    orch.checkpoint_mgr = None
    ctx = _mock_context(config)
    orch._mark_stage_complete(ctx, orch_module.STAGE_STAGING)
    assert orch_module.STAGE_STAGING in ctx.completed_stages


def test_mark_stage_complete_checkpoint_failure_does_not_raise():
    """Checkpoint persistence failure is logged but doesn't abort the pipeline."""
    config = _minimal_config(checkpoint_enabled=True)
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator
    orch = object.__new__(PipelineOrchestrator)
    orch.config = config
    orch.checkpoint_mgr = MagicMock()
    orch.checkpoint_mgr.mark_stage_complete.side_effect = RuntimeError("BQ down")
    ctx = _mock_context(config)
    # Should not raise
    orch._mark_stage_complete(ctx, orch_module.STAGE_STAGING)
    assert orch_module.STAGE_STAGING in ctx.completed_stages
