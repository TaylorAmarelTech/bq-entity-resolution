"""Pipeline executor: runs a PipelinePlan against a Backend.

Separates execution from planning. Handles:
1. Running SQL in dependency order
2. Quality gate checks after each stage
3. Checkpoint/resume (skip_stages)
4. Metrics collection
5. Error handling with diagnostics
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from bq_entity_resolution.backends.protocol import Backend, QueryResult
from bq_entity_resolution.pipeline.plan import PipelinePlan, StagePlan

logger = logging.getLogger(__name__)


@dataclass
class StageExecutionResult:
    """Result of executing a single stage."""

    stage_name: str
    success: bool = True
    error: Optional[str] = None
    sql_count: int = 0
    duration_seconds: float = 0.0
    rows_affected: int = 0
    skipped: bool = False


@dataclass
class PipelineResult:
    """Result of executing a complete pipeline."""

    run_id: str
    started_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    finished_at: Optional[datetime] = None
    status: str = "running"
    error: Optional[str] = None
    stage_results: list[StageExecutionResult] = field(default_factory=list)
    sql_log: list[dict] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.status == "success"

    @property
    def duration_seconds(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0

    @property
    def completed_stages(self) -> list[str]:
        return [
            r.stage_name
            for r in self.stage_results
            if r.success and not r.skipped
        ]


class PipelineExecutor:
    """Executes a PipelinePlan against a Backend.

    Handles the plan/execute split: all SQL is pre-generated,
    and the executor runs it in order with error handling,
    quality gates, and metrics collection.
    """

    def __init__(
        self,
        backend: Backend,
        quality_gates: list[Any] | None = None,
    ):
        self.backend = backend
        self.quality_gates = quality_gates or []

    def execute(
        self,
        plan: PipelinePlan,
        run_id: str | None = None,
        skip_stages: set[str] | None = None,
    ) -> PipelineResult:
        """Execute a pipeline plan.

        Args:
            plan: The immutable pipeline plan to execute.
            run_id: Optional run identifier. Auto-generated if not provided.
            skip_stages: Stage names to skip (for checkpoint/resume).
        """
        skip_stages = skip_stages or set()
        run_id = run_id or self._generate_run_id()

        result = PipelineResult(run_id=run_id)
        logger.info("Pipeline execution started: %s", run_id)

        try:
            for stage_plan in plan.stages:
                if stage_plan.stage_name in skip_stages:
                    result.stage_results.append(StageExecutionResult(
                        stage_name=stage_plan.stage_name,
                        skipped=True,
                    ))
                    logger.info(
                        "Skipping stage '%s' (checkpoint resume)",
                        stage_plan.stage_name,
                    )
                    continue

                stage_result = self._execute_stage(stage_plan, result)
                result.stage_results.append(stage_result)

                if not stage_result.success:
                    raise RuntimeError(
                        f"Stage '{stage_plan.stage_name}' failed: "
                        f"{stage_result.error}"
                    )

                # Run quality gates
                self._check_gates(stage_plan, result)

            result.status = "success"
            logger.info(
                "Pipeline completed: %s (%.1fs)",
                run_id,
                result.duration_seconds,
            )

        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            logger.exception("Pipeline failed: %s", run_id)
            raise

        finally:
            result.finished_at = datetime.now(timezone.utc)

        return result

    def _execute_stage(
        self,
        stage_plan: StagePlan,
        pipeline_result: PipelineResult,
    ) -> StageExecutionResult:
        """Execute a single stage's SQL statements."""
        stage_result = StageExecutionResult(
            stage_name=stage_plan.stage_name,
            sql_count=stage_plan.sql_count,
        )

        if stage_plan.sql_count == 0:
            logger.info(
                "Stage '%s': no SQL to execute", stage_plan.stage_name
            )
            return stage_result

        start = time.monotonic()
        logger.info(
            "Executing stage '%s' (%d SQL statement(s))",
            stage_plan.stage_name,
            stage_plan.sql_count,
        )

        try:
            for expr in stage_plan.sql_expressions:
                sql = expr.render()

                pipeline_result.sql_log.append({
                    "stage": stage_plan.stage_name,
                    "sql": sql,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

                # Use execute_script for BQ scripting blocks
                if "DECLARE" in sql and "WHILE" in sql:
                    query_result = self.backend.execute_script(
                        sql, label=stage_plan.stage_name
                    )
                else:
                    query_result = self.backend.execute(
                        sql, label=stage_plan.stage_name
                    )

                stage_result.rows_affected += query_result.rows_affected

        except Exception as e:
            stage_result.success = False
            stage_result.error = str(e)
            logger.error(
                "Stage '%s' failed: %s", stage_plan.stage_name, e
            )

        finally:
            stage_result.duration_seconds = time.monotonic() - start

        return stage_result

    def _check_gates(
        self,
        stage_plan: StagePlan,
        pipeline_result: PipelineResult,
    ) -> None:
        """Run quality gates for a completed stage."""
        for gate in self.quality_gates:
            if gate.applies_to(stage_plan.stage_name):
                gate_result = gate.check(
                    stage_name=stage_plan.stage_name,
                    backend=self.backend,
                    outputs=stage_plan.outputs,
                )
                if not gate_result.passed:
                    if gate_result.severity == "error":
                        raise RuntimeError(
                            f"Quality gate failed for "
                            f"'{stage_plan.stage_name}': "
                            f"{gate_result.message}"
                        )
                    else:
                        logger.warning(
                            "Quality gate warning for '%s': %s",
                            stage_plan.stage_name,
                            gate_result.message,
                        )

    @staticmethod
    def _generate_run_id() -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"er_run_{ts}"
