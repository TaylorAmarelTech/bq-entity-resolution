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
from collections.abc import Callable
from typing import Any

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
from bq_entity_resolution.stages.base import Stage

logger = logging.getLogger(__name__)

DagBuilder = Callable[[PipelineConfig], StageDAG]


class Pipeline:
    """High-level API for entity resolution pipelines.

    This is the recommended entry point. It provides:
    1. Compile-time validation (before any SQL runs)
    2. Plan preview (see all SQL before executing)
    3. Backend-agnostic execution (BigQuery or DuckDB)
    4. Quality gates (runtime assertions after each stage)
    5. Checkpoint/resume for crash recovery
    6. Progress callbacks for UI integration

    Extensibility:
    - **stage_overrides**: Replace built-in stages by name with custom
      implementations. E.g., ``{"clustering": MyClusteringStage(config)}``.
    - **exclude_stages**: Remove built-in stages by name.
      E.g., ``{"cluster_quality"}``.
    - **dag_builder**: Replace the entire DAG construction function.
      Receives a PipelineConfig and must return a StageDAG.
    - **Pipeline.from_stages()**: Build a pipeline from a custom list
      of Stage objects or a pre-built StageDAG.
    """

    def __init__(
        self,
        config: PipelineConfig,
        quality_gates: list[DataQualityGate] | None = None,
        dag_builder: DagBuilder | None = None,
        stage_overrides: dict[str, Stage] | None = None,
        exclude_stages: set[str] | None = None,
    ):
        self._config = config
        if dag_builder:
            self._dag = dag_builder(config)
        else:
            self._dag = build_pipeline_dag(
                config,
                stage_overrides=stage_overrides,
                exclude_stages=exclude_stages,
            )
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

    @classmethod
    def from_stages(
        cls,
        config: PipelineConfig,
        stages: list[Stage] | None = None,
        dag: StageDAG | None = None,
        explicit_edges: dict[str, list[str]] | None = None,
        quality_gates: list[DataQualityGate] | None = None,
    ) -> Pipeline:
        """Create a Pipeline from custom stages or a pre-built DAG.

        Use this when you need full control over the stage graph —
        injecting custom stages, reordering, or replacing the entire
        pipeline structure.

        Provide EITHER ``stages`` (will be assembled into a DAG) or
        ``dag`` (used as-is). If both are given, ``dag`` takes precedence.

        Args:
            config: Pipeline configuration.
            stages: List of Stage objects to assemble into a DAG.
            dag: Pre-built StageDAG (takes precedence over ``stages``).
            explicit_edges: Extra dependency edges when using ``stages``.
                Maps stage name to list of dependency stage names.
            quality_gates: Optional custom quality gates.

        Example::

            from bq_entity_resolution import (
                Pipeline, Stage, StageDAG, build_pipeline_dag,
            )

            # Option A: Modify the default DAG
            default_dag = build_pipeline_dag(config)
            stages = list(default_dag.stages)
            stages.append(MyCustomEnrichmentStage(config))
            pipeline = Pipeline.from_stages(
                config,
                stages=stages,
                explicit_edges={"my_enrichment": ["feature_engineering"]},
            )

            # Option B: Provide a fully custom DAG
            dag = StageDAG.from_stages(my_stages, my_edges)
            pipeline = Pipeline.from_stages(config, dag=dag)
        """
        inst = object.__new__(cls)
        inst._config = config
        if dag is not None:
            inst._dag = dag
        elif stages is not None:
            inst._dag = StageDAG.from_stages(stages, explicit_edges)
        else:
            raise ValueError("Provide either 'stages' or 'dag'")
        inst._gates = (
            quality_gates
            if quality_gates is not None
            else default_gates(config)
        )
        return inst

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
        drain: bool = False,
        run_id: str | None = None,
        skip_stages: set[str] | None = None,
        checkpoint_manager: CheckpointManagerProtocol | None = None,
        resume: bool = False,
        on_progress: ProgressCallback | None = None,
        watermark_manager: Any | None = None,
        **plan_kwargs: Any,
    ) -> PipelineResult:
        """Convenience: validate, plan, and execute in one call.

        Args:
            backend: Execution backend.
            full_refresh: Ignore watermarks.
            drain: If True, auto-loop through batches until all
                unprocessed records are consumed. Each iteration
                advances the watermark and processes the next batch.
            run_id: Optional run identifier.
            skip_stages: Stages to skip.
            checkpoint_manager: Optional checkpoint persistence.
            resume: Auto-resume from last incomplete run.
            on_progress: Optional progress callback.
            watermark_manager: Optional WatermarkManager instance for
                reading/advancing watermarks. When not provided,
                watermark kwargs are passed through to plan().
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

        # Check drain mode from config if not explicitly passed
        inc = getattr(self._config, "incremental", None)
        if not drain and inc and getattr(inc, "drain_mode", False):
            drain = True
        max_iterations = getattr(inc, "drain_max_iterations", 100) if inc else 100

        # Read initial watermark if manager is provided
        watermark = plan_kwargs.pop("watermark", None)
        if watermark_manager and not full_refresh and not watermark:
            for source in self._config.sources:
                wm = watermark_manager.read(source.name)
                if wm:
                    watermark = wm
                    break

        iteration = 0
        result: PipelineResult | None = None

        while True:
            # Plan
            plan = self.plan(
                full_refresh=full_refresh,
                watermark=watermark,
                **plan_kwargs,
            )
            logger.info(
                "Pipeline plan (iteration %d): %d stages, %d SQL statements",
                iteration,
                len(plan.stages),
                plan.total_sql_count,
            )

            # Execute
            result = self.execute(
                plan,
                backend=backend,
                run_id=f"{run_id}_iter{iteration}" if run_id and drain else run_id,
                skip_stages=skip_stages,
                checkpoint_manager=checkpoint_manager,
                resume=resume if iteration == 0 else False,
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

            # Advance watermark after successful execution
            if watermark_manager and not full_refresh and result.success:
                cursor_columns = getattr(inc, "cursor_columns", ["updated_at"]) if inc else ["updated_at"]
                try:
                    from bq_entity_resolution.naming import staged_table
                    staged = staged_table(self._config, self._config.sources[0].name)
                    new_wm = watermark_manager.compute_new_watermark_from_staged(
                        staged_table=staged,
                        cursor_columns=cursor_columns,
                        column_mapping={"updated_at": "source_updated_at"},
                    )
                    if new_wm:
                        watermark_manager.write(
                            self._config.sources[0].name,
                            new_wm,
                            run_id=run_id or "",
                        )
                        watermark = new_wm
                        logger.info(
                            "Watermark advanced: %s", new_wm
                        )
                except Exception:
                    logger.warning("Failed to advance watermark", exc_info=True)

            # Drain mode: continue if there might be more records
            if not drain:
                break

            iteration += 1
            if iteration >= max_iterations:
                logger.warning(
                    "Drain mode: reached max iterations (%d), stopping",
                    max_iterations,
                )
                break

            # Check if there are more records to process
            if watermark_manager and watermark:
                cursor_columns = getattr(inc, "cursor_columns", ["updated_at"]) if inc else ["updated_at"]
                try:
                    has_more = watermark_manager.has_unprocessed_records(
                        source_table=self._config.sources[0].table,
                        cursor_columns=cursor_columns,
                        current_watermark=watermark,
                    )
                    if not has_more:
                        logger.info(
                            "Drain mode: no more unprocessed records after "
                            "%d iterations", iteration
                        )
                        break
                except Exception:
                    logger.warning(
                        "Failed to check for unprocessed records, stopping drain",
                        exc_info=True,
                    )
                    break
            else:
                # No watermark manager — can't determine if more records exist
                logger.info(
                    "Drain mode: no watermark manager, running single iteration"
                )
                break

        assert result is not None
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
    ) -> Pipeline:
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
