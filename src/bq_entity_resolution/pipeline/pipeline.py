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
from typing import Any

from bq_entity_resolution.backends.protocol import Backend
from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.pipeline.dag import StageDAG, build_pipeline_dag
from bq_entity_resolution.pipeline.executor import PipelineExecutor, PipelineResult
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
    ) -> PipelineResult:
        """Execute a pre-generated plan against a backend.

        Args:
            plan: The immutable plan from self.plan().
            backend: Execution backend (BigQuery or DuckDB).
            run_id: Optional run identifier.
            skip_stages: Stage names to skip (checkpoint/resume).
        """
        executor = PipelineExecutor(
            backend=backend,
            quality_gates=self._gates,
        )
        return executor.execute(
            plan,
            run_id=run_id,
            skip_stages=skip_stages,
        )

    def run(
        self,
        backend: Backend,
        full_refresh: bool = False,
        run_id: str | None = None,
        skip_stages: set[str] | None = None,
        **plan_kwargs: Any,
    ) -> PipelineResult:
        """Convenience: validate, plan, and execute in one call.

        Args:
            backend: Execution backend.
            full_refresh: Ignore watermarks.
            run_id: Optional run identifier.
            skip_stages: Stages to skip.
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
        return self.execute(
            plan,
            backend=backend,
            run_id=run_id,
            skip_stages=skip_stages,
        )

    def _external_tables(self) -> set[str]:
        """Collect external table names from config sources.

        Also includes pipeline-managed tables that are initialized
        and accumulated during execution but not produced by a
        single stage (e.g., the all_matches_table).
        """
        from bq_entity_resolution.naming import all_matches_table

        tables = {source.table for source in self._config.sources}
        # all_matches_table is accumulated across tiers, not by one stage
        tables.add(all_matches_table(self._config))
        return tables
