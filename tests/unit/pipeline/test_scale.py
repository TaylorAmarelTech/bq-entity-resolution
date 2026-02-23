"""Tests for scale optimizations."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from bq_entity_resolution.clients.bigquery import BigQueryClient
from bq_entity_resolution.config.schema import ScaleConfig
from bq_entity_resolution.pipeline.context import PipelineContext


# ---------------------------------------------------------------
# Config model tests
# ---------------------------------------------------------------


def test_scale_config_defaults():
    """Default scale config has no limits."""
    sc = ScaleConfig()
    assert sc.max_bytes_billed is None
    assert sc.featured_table_clustering == []
    assert sc.candidates_clustering == ["l_entity_uid"]
    assert not sc.checkpoint_enabled


def test_scale_config_custom():
    """Scale config accepts custom values."""
    sc = ScaleConfig(
        max_bytes_billed=10_000_000_000,
        featured_table_clustering=["source_name", "entity_uid"],
        checkpoint_enabled=True,
    )
    assert sc.max_bytes_billed == 10_000_000_000
    assert "source_name" in sc.featured_table_clustering
    assert sc.checkpoint_enabled


def test_pipeline_config_has_scale(sample_config):
    """PipelineConfig includes scale field."""
    assert hasattr(sample_config, "scale")
    assert sample_config.scale.max_bytes_billed is None


# ---------------------------------------------------------------
# BigQueryClient: max_bytes_billed
# ---------------------------------------------------------------


def test_bq_client_accepts_max_bytes_billed():
    """BigQueryClient constructor accepts max_bytes_billed parameter."""
    # Cannot instantiate real client without credentials, but check the signature
    import inspect
    sig = inspect.signature(BigQueryClient.__init__)
    assert "max_bytes_billed" in sig.parameters


def test_bq_client_default_no_max_bytes():
    """BigQueryClient defaults to no max_bytes_billed."""
    import inspect
    sig = inspect.signature(BigQueryClient.__init__)
    param = sig.parameters["max_bytes_billed"]
    assert param.default is None


# ---------------------------------------------------------------
# Checkpoint support
# ---------------------------------------------------------------


def test_context_has_completed_stages():
    """PipelineContext includes completed_stages set."""
    ctx = PipelineContext(
        run_id="test",
        started_at=datetime.now(timezone.utc),
        config=None,  # type: ignore
    )
    assert isinstance(ctx.completed_stages, set)
    assert len(ctx.completed_stages) == 0


def test_context_completed_stages_tracking():
    """completed_stages tracks which stages are done."""
    ctx = PipelineContext(
        run_id="test",
        started_at=datetime.now(timezone.utc),
        config=None,  # type: ignore
    )
    ctx.completed_stages.add("staging")
    ctx.completed_stages.add("features")
    assert "staging" in ctx.completed_stages
    assert "features" in ctx.completed_stages
    assert "matching" not in ctx.completed_stages


def test_should_skip_stage_disabled(sample_config):
    """_should_skip_stage returns False when checkpoints disabled."""
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator

    # Mock the BQ client to avoid credentials
    orch = object.__new__(PipelineOrchestrator)
    orch.config = sample_config

    ctx = PipelineContext(
        run_id="test",
        started_at=datetime.now(timezone.utc),
        config=sample_config,
    )
    ctx.completed_stages.add("staging")

    assert not orch._should_skip_stage(ctx, "staging")


def test_should_skip_stage_enabled(sample_config):
    """_should_skip_stage returns True when stage already completed."""
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator

    sample_config.scale.checkpoint_enabled = True
    orch = object.__new__(PipelineOrchestrator)
    orch.config = sample_config

    ctx = PipelineContext(
        run_id="test",
        started_at=datetime.now(timezone.utc),
        config=sample_config,
    )
    ctx.completed_stages.add("staging")

    assert orch._should_skip_stage(ctx, "staging")
    assert not orch._should_skip_stage(ctx, "features")
