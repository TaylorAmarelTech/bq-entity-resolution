"""High-level Pipeline API: the recommended entry point.

Combines DAG construction, validation, planning, and execution
into a single fluent interface. This replaces the old orchestrator
as the primary way to build and run entity resolution pipelines.

Usage:
    from bq_entity_resolution.pipeline.pipeline import Pipeline

    # Build from config
    pipeline = Pipeline(config)

    # Validate before running
    violations = pipeline.validate()
    if violations:
        for v in violations:
            print(f"  {v.stage_name}: {v.message}")

    # Generate execution plan
    plan = pipeline.plan(full_refresh=True)
    print(plan.preview())

    # Execute against a backend
    result = pipeline.execute(plan, backend=my_backend)

    # Or use quick_config for minimal setup
    from bq_entity_resolution.config.presets import quick_config
    config = quick_config(
        bq_project="my-project",
        source_table="my-project.raw.customers",
        columns=["first_name", "last_name", "email"],
    )
    pipeline = Pipeline(config)
    result = pipeline.run(backend=my_backend, full_refresh=True)
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from bq_entity_resolution.backends.protocol import Backend
from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.monitoring.metrics import MetricsCollector
from bq_entity_resolution.pipeline.dag import StageDAG, build_pipeline_dag
from bq_entity_resolution.pipeline.executor import (
    CheckpointManagerProtocol,
    PipelineExecutor,
    PipelineResult,
    ProgressCallback,
)
from bq_entity_resolution.pipeline.gates import DataQualityGate, default_gates
from bq_entity_resolution.pipeline.plan import PipelinePlan, create_plan
from bq_entity_resolution.pipeline.validator import (
    ContractViolation,
    validate_dag_contracts,
    validate_stage_configs,
)

logger = logging.getLogger(__name__)


class Pipeline:
    """High-level API for entity resolution pipelines.

    This is the recommended entry point. It provides:
    1. Compile-time validation (before any SQL runs)
    2. Plan preview (see all SQL before executing)
    3. Backend-agnostic execution (BigQuery or DuckDB)
    4. Quality gates (runtime assertions after each stage)
    5. Checkpoint/resume for crash recovery
    6. Progress callbacks for UI integration
    """

    def __init__(
        self,
        config: PipelineConfig,
        quality_gates: list[DataQualityGate] | None = None,
    ):
        self._config = config
        self._dag = build_pipeline_dag(config)
        self._gates = (
            quality_gates
            if quality_gates is not None
            else default_gates(config)
        )

    @property
    def config(self) -> PipelineConfig:
        return self._config

    @property
    def dag(self) -> StageDAG:
        """The pipeline's stage DAG."""
        return self._dag

    @property
    def stage_names(self) -> list[str]:
        """Stage names in execution order."""
        return self._dag.stage_names

    def validate(self) -> list[ContractViolation]:
        """Run compile-time validation.

        Checks:
        1. All stage inputs are produced by upstream stages
        2. Stage-specific config validation (missing keys, etc.)

        Returns a list of violations. Empty = valid.
        """
        violations = validate_dag_contracts(
            self._dag,
            external_tables=self._external_tables(),
        )
        violations.extend(validate_stage_configs(self._dag))
        return violations

    def plan(self, **kwargs: Any) -> PipelinePlan:
        """Generate an immutable execution plan.

        The plan contains all SQL to execute, in order, without
        actually running anything. Use plan.preview() to inspect.

        Common kwargs:
            full_refresh: bool — ignore watermarks, reprocess everything
            watermark: dict — per-source watermark values
        """
        return create_plan(self._dag, **kwargs)

    def execute(
        self,
        plan: PipelinePlan,
        backend: Backend,
        run_id: str | None = None,
        skip_stages: set[str] | None = None,
        checkpoint_manager: CheckpointManagerProtocol | None = None,
        resume: bool = False,
        on_progress: ProgressCallback | None = None,
    ) -> PipelineResult:
        """Execute a pre-generated plan against a backend.

        Args:
            plan: The immutable plan from self.plan().
            backend: Execution backend (BigQuery or DuckDB).
            run_id: Optional run identifier.
            skip_stages: Stage names to skip (checkpoint/resume).
            checkpoint_manager: Optional checkpoint manager for crash recovery.
            resume: If True, auto-detect resumable run from checkpoint.
            on_progress: Optional callback for progress reporting.
        """
        executor = PipelineExecutor(
            backend=backend,
            quality_gates=self._gates,
            checkpoint_manager=checkpoint_manager,
            on_progress=on_progress,
        )
        return executor.execute(
            plan,
            run_id=run_id,
            skip_stages=skip_stages,
            resume=resume,
        )

    def run(
        self,
        backend: Backend,
        full_refresh: bool = False,
        run_id: str | None = None,
        skip_stages: set[str] | None = None,
        checkpoint_manager: CheckpointManagerProtocol | None = None,
        resume: bool = False,
        on_progress: ProgressCallback | None = None,
        **plan_kwargs: Any,
    ) -> PipelineResult:
        """Convenience: validate, plan, and execute in one call.

        Args:
            backend: Execution backend.
            full_refresh: Ignore watermarks.
            run_id: Optional run identifier.
            skip_stages: Stages to skip.
            checkpoint_manager: Optional checkpoint persistence.
            resume: Auto-resume from last incomplete run.
            on_progress: Optional progress callback.
            **plan_kwargs: Additional kwargs for plan generation.
        """
        # Validate first
        violations = self.validate()
        errors = [v for v in violations if v.severity == "error"]
        if errors:
            error_msgs = "; ".join(
                f"{v.stage_name}: {v.message}" for v in errors
            )
            raise ValueError(
                f"Pipeline validation failed: {error_msgs}"
            )

        warnings = [v for v in violations if v.severity == "warning"]
        for w in warnings:
            logger.warning(
                "Validation warning for '%s': %s",
                w.stage_name,
                w.message,
            )

        # Plan
        plan = self.plan(full_refresh=full_refresh, **plan_kwargs)
        logger.info(
            "Pipeline plan: %d stages, %d SQL statements",
            len(plan.stages),
            plan.total_sql_count,
        )

        # Execute
        result = self.execute(
            plan,
            backend=backend,
            run_id=run_id,
            skip_stages=skip_stages,
            checkpoint_manager=checkpoint_manager,
            resume=resume,
            on_progress=on_progress,
        )

        # Record metrics (safe: configs may not have monitoring section)
        try:
            monitoring = getattr(self._config, "monitoring", None)
            metrics_cfg = getattr(monitoring, "metrics", None) if monitoring else None
            if metrics_cfg and getattr(metrics_cfg, "enabled", False):
                collector = MetricsCollector(self._config)
                collector.set_backend(backend)
                collector.record_run(result)
        except Exception:
            logger.warning("Failed to record metrics", exc_info=True)

        return result

    @classmethod
    def from_table(
        cls,
        table: str,
        backend: Backend,
        bq_project: str | None = None,
        unique_key: str = "id",
        updated_at: str = "updated_at",
        column_roles: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> "Pipeline":
        """Create a Pipeline directly from a BigQuery table reference.

        The ultimate minimal setup: auto-discovers columns, detects roles,
        generates features, blocking keys, comparisons, and tiers.

        Args:
            table: Fully-qualified BigQuery table (project.dataset.table).
            backend: Backend for schema introspection.
            bq_project: GCP project (auto-detected from table if not set).
            unique_key: Primary key column.
            updated_at: Timestamp column.
            column_roles: Explicit role overrides {column_name: role}.
            **kwargs: Additional Pipeline constructor kwargs.

        Example:
            from bq_entity_resolution import Pipeline
            from bq_entity_resolution.backends.bigquery import BigQueryBackend

            backend = BigQueryBackend(project="my-project")
            pipeline = Pipeline.from_table(
                "my-project.raw.customers",
                backend=backend,
            )
            result = pipeline.run(backend=backend)
        """
        from bq_entity_resolution.config.presets import quick_config
        from bq_entity_resolution.config.roles import detect_role
        from bq_entity_resolution.config.schema import SourceConfig

        # Discover columns from live table
        source = SourceConfig.from_table(
            table=table,
            backend=backend,
            unique_key=unique_key,
            updated_at=updated_at,
        )

        # Build role map from discovered columns
        role_map: dict[str, str] = {}
        if column_roles:
            role_map.update(column_roles)
        for col in source.columns:
            if col.name not in role_map and col.role:
                role_map[col.name] = col.role

        # Derive project from table
        if not bq_project:
            parts = table.split(".")
            bq_project = parts[0] if len(parts) >= 3 else "default-project"

        config = quick_config(
            bq_project=bq_project,
            source_table=table,
            unique_key=unique_key,
            updated_at=updated_at,
            column_roles=role_map if role_map else None,
            columns=[c.name for c in source.columns] if not role_map else None,
        )

        return cls(config, **kwargs)

    def _external_tables(self) -> set[str]:
        """Collect external table names from config sources."""
        return {source.table for source in self._config.sources}
