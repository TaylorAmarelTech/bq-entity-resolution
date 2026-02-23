"""Pipeline executor: runs a PipelinePlan against a Backend.

Separates execution from planning. Handles:
1. Running SQL in dependency order
2. Quality gate checks after each stage
3. Checkpoint/resume via CheckpointManager
4. Metrics collection
5. Error handling with diagnostics
6. Progress callbacks
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Protocol

from bq_entity_resolution.backends.protocol import Backend, QueryResult
from bq_entity_resolution.pipeline.plan import PipelinePlan, StagePlan

logger = logging.getLogger(__name__)


# BQ scripting markers — any of these indicate a script block
_SCRIPT_MARKERS = frozenset({"DECLARE ", "BEGIN TRANSACTION", "BEGIN\n", "WHILE "})


def _is_script_block(sql: str) -> bool:
    """Detect whether SQL is a BigQuery scripting block.

    Checks for common scripting patterns: DECLARE, WHILE,
    BEGIN TRANSACTION, etc.
    """
    upper = sql.lstrip().upper()
    return any(marker in upper for marker in _SCRIPT_MARKERS)


class ProgressCallback(Protocol):
    """Protocol for pipeline progress reporting."""

    def __call__(
        self,
        stage_name: str,
        stage_index: int,
        total_stages: int,
        status: str,
    ) -> None: ...


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


class CheckpointManagerProtocol(Protocol):
    """Protocol for checkpoint persistence (avoids circular import)."""

    def ensure_table_exists(self) -> None: ...
    def load_completed_stages(self, run_id: str) -> set[str]: ...
    def find_resumable_run(self) -> str | None: ...
    def mark_stage_complete(self, run_id: str, stage_name: str) -> None: ...
    def mark_run_complete(self, run_id: str) -> None: ...


class PipelineExecutor:
    """Executes a PipelinePlan against a Backend.

    Handles the plan/execute split: all SQL is pre-generated,
    and the executor runs it in order with error handling,
    quality gates, checkpoint persistence, and metrics collection.

    Retry strategy: The executor does NOT implement its own retry logic
    for transient errors (503 ServiceUnavailable, 429 TooManyRequests,
    500 InternalServerError). Retries are the responsibility of the
    backend implementation. The BigQueryClient in clients/bigquery.py
    provides exponential-backoff retries (3 attempts, 5s/10s/20s delay)
    for these transient errors. DuckDB operations are local and do not
    need network retries.
    """

    def __init__(
        self,
        backend: Backend,
        quality_gates: list[Any] | None = None,
        checkpoint_manager: CheckpointManagerProtocol | None = None,
        on_progress: ProgressCallback | None = None,
    ):
        self.backend = backend
        self.quality_gates = quality_gates or []
        self._checkpoint = checkpoint_manager
        self._on_progress = on_progress

    def execute(
        self,
        plan: PipelinePlan,
        run_id: str | None = None,
        skip_stages: set[str] | None = None,
        resume: bool = False,
    ) -> PipelineResult:
        """Execute a pipeline plan.

        Args:
            plan: The immutable pipeline plan to execute.
            run_id: Optional run identifier. Auto-generated if not provided.
            skip_stages: Stage names to skip (for checkpoint/resume).
            resume: If True and checkpoint_manager is set, auto-detect
                resumable run and skip completed stages.
        """
        skip_stages = set(skip_stages) if skip_stages else set()

        # Auto-resume from checkpoint if requested
        if resume and self._checkpoint:
            try:
                self._checkpoint.ensure_table_exists()
                resumable_run_id = self._checkpoint.find_resumable_run()
                if resumable_run_id:
                    completed = self._checkpoint.load_completed_stages(
                        resumable_run_id
                    )
                    skip_stages |= completed
                    run_id = resumable_run_id
                    logger.info(
                        "Resuming run '%s' — skipping %d completed stages: %s",
                        run_id, len(completed), sorted(completed),
                    )
            except Exception:
                logger.warning(
                    "Failed to load checkpoint state; starting fresh",
                    exc_info=True,
                )

        run_id = run_id or self._generate_run_id()
        result = PipelineResult(run_id=run_id)
        total_stages = len(plan.stages)
        logger.info("Pipeline execution started: %s (%d stages)", run_id, total_stages)

        try:
            for idx, stage_plan in enumerate(plan.stages):
                if stage_plan.stage_name in skip_stages:
                    result.stage_results.append(StageExecutionResult(
                        stage_name=stage_plan.stage_name,
                        skipped=True,
                    ))
                    logger.info(
                        "Skipping stage '%s' (checkpoint resume)",
                        stage_plan.stage_name,
                    )
                    self._notify_progress(
                        stage_plan.stage_name, idx, total_stages, "skipped"
                    )
                    continue

                self._notify_progress(
                    stage_plan.stage_name, idx, total_stages, "running"
                )
                stage_result = self._execute_stage(stage_plan, result)
                result.stage_results.append(stage_result)

                if not stage_result.success:
                    self._notify_progress(
                        stage_plan.stage_name, idx, total_stages, "failed"
                    )
                    raise RuntimeError(
                        f"Stage '{stage_plan.stage_name}' failed: "
                        f"{stage_result.error}"
                    )

                # Run quality gates BEFORE checkpoint (so failed gates
                # are not persisted as completed on resume)
                self._check_gates(stage_plan, result)

                # Persist checkpoint after successful stage + gates
                if self._checkpoint:
                    try:
                        self._checkpoint.mark_stage_complete(
                            run_id, stage_plan.stage_name
                        )
                    except Exception:
                        logger.error(
                            "Failed to persist checkpoint for stage '%s' in run '%s'. "
                            "Stage completed successfully but checkpoint was NOT saved — "
                            "this stage may re-execute on resume.",
                            stage_plan.stage_name,
                            run_id,
                            exc_info=True,
                        )

                self._notify_progress(
                    stage_plan.stage_name, idx, total_stages, "completed"
                )

            result.status = "success"

            # Mark entire run as complete in checkpoint
            if self._checkpoint:
                try:
                    self._checkpoint.mark_run_complete(run_id)
                except Exception:
                    logger.error(
                        "Failed to mark run '%s' as complete in checkpoint. "
                        "Pipeline finished successfully but may appear incomplete on resume.",
                        run_id,
                        exc_info=True,
                    )

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
                if _is_script_block(sql):
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

    def _notify_progress(
        self,
        stage_name: str,
        stage_index: int,
        total_stages: int,
        status: str,
    ) -> None:
        """Send progress notification if a callback is registered."""
        if self._on_progress:
            try:
                self._on_progress(stage_name, stage_index, total_stages, status)
            except Exception:
                pass  # Never let progress callback crash the pipeline

    @staticmethod
    def _generate_run_id() -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"er_run_{ts}"
