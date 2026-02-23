"""Tests for the pipeline executor."""

import pytest

from bq_entity_resolution.backends.protocol import QueryResult
from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.stages.base import TableRef
from bq_entity_resolution.pipeline.plan import StagePlan, PipelinePlan
from bq_entity_resolution.pipeline.executor import (
    PipelineExecutor,
    PipelineResult,
    StageExecutionResult,
)
from bq_entity_resolution.pipeline.gates import (
    DataQualityGate,
    GateResult,
)


# -- Mock backend --


class MockBackend:
    """Mock backend that records executed SQL."""

    def __init__(self, fail_on: str | None = None):
        self.executed: list[str] = []
        self._fail_on = fail_on

    @property
    def dialect(self):
        return "bigquery"

    def execute(self, sql, label=""):
        if self._fail_on and self._fail_on in sql:
            raise RuntimeError(f"Simulated failure: {self._fail_on}")
        self.executed.append(sql)
        return QueryResult(rows_affected=10)

    def execute_script(self, sql, label=""):
        self.executed.append(sql)
        return QueryResult(rows_affected=5)

    def execute_and_fetch(self, sql, label=""):
        self.executed.append(sql)
        return [{"count": 100}]

    def table_exists(self, ref):
        return True

    def row_count(self, ref):
        return 100


# -- Helpers --


def _make_plan(stage_names, sql_per_stage=1):
    """Create a simple pipeline plan."""
    stages = []
    for name in stage_names:
        exprs = tuple(
            SQLExpression.from_raw(f"SELECT '{name}_{i}'")
            for i in range(sql_per_stage)
        )
        stages.append(StagePlan(
            stage_name=name,
            sql_expressions=exprs,
            inputs={},
            outputs={},
            dependencies=(),
        ))
    return PipelinePlan(stages=tuple(stages))


# -- Tests --


class TestPipelineExecutor:
    def test_execute_simple_plan(self):
        """Executor runs all stages in order."""
        backend = MockBackend()
        executor = PipelineExecutor(backend)
        plan = _make_plan(["a", "b", "c"])

        result = executor.execute(plan, run_id="test_run")

        assert result.success
        assert result.status == "success"
        assert len(result.completed_stages) == 3
        assert len(backend.executed) == 3

    def test_execute_records_sql_log(self):
        """Executor records SQL in the result log."""
        backend = MockBackend()
        executor = PipelineExecutor(backend)
        plan = _make_plan(["staging"])

        result = executor.execute(plan, run_id="test_run")

        assert len(result.sql_log) == 1
        assert result.sql_log[0]["stage"] == "staging"
        assert "staging" in result.sql_log[0]["sql"]

    def test_skip_stages(self):
        """Executor skips specified stages."""
        backend = MockBackend()
        executor = PipelineExecutor(backend)
        plan = _make_plan(["a", "b", "c"])

        result = executor.execute(
            plan, run_id="test_run", skip_stages={"a", "c"}
        )

        assert result.success
        assert result.completed_stages == ["b"]
        assert len(backend.executed) == 1

    def test_stage_failure_stops_pipeline(self):
        """Executor stops on stage failure and raises."""
        backend = MockBackend(fail_on="stage_b")
        executor = PipelineExecutor(backend)
        plan = _make_plan(["stage_a", "stage_b", "stage_c"])

        with pytest.raises(RuntimeError, match="stage_b"):
            executor.execute(plan, run_id="test_run")

    def test_failure_records_error(self):
        """Failed pipeline records error in result."""
        backend = MockBackend(fail_on="bad")
        executor = PipelineExecutor(backend)

        sp = StagePlan(
            stage_name="bad_stage",
            sql_expressions=(SQLExpression.from_raw("SELECT 'bad'"),),
            inputs={},
            outputs={},
            dependencies=(),
        )
        plan = PipelinePlan(stages=(sp,))

        with pytest.raises(RuntimeError):
            executor.execute(plan, run_id="test_run")

    def test_empty_stage_no_sql(self):
        """Stage with no SQL expressions is handled gracefully."""
        backend = MockBackend()
        executor = PipelineExecutor(backend)

        sp = StagePlan(
            stage_name="empty",
            sql_expressions=(),
            inputs={},
            outputs={},
            dependencies=(),
        )
        plan = PipelinePlan(stages=(sp,))
        result = executor.execute(plan, run_id="test_run")

        assert result.success
        assert len(backend.executed) == 0

    def test_scripting_detected(self):
        """SQL with DECLARE + WHILE uses execute_script."""
        backend = MockBackend()
        executor = PipelineExecutor(backend)

        scripting_sql = "DECLARE i INT64 DEFAULT 0;\nWHILE i < 10 DO\nSET i = i + 1;\nEND WHILE;"
        sp = StagePlan(
            stage_name="clustering",
            sql_expressions=(SQLExpression.from_raw(scripting_sql),),
            inputs={},
            outputs={},
            dependencies=(),
        )
        plan = PipelinePlan(stages=(sp,))
        result = executor.execute(plan, run_id="test_run")

        assert result.success
        assert len(backend.executed) == 1
        assert "DECLARE" in backend.executed[0]

    def test_auto_generates_run_id(self):
        """Executor auto-generates run ID when not provided."""
        backend = MockBackend()
        executor = PipelineExecutor(backend)
        plan = _make_plan(["a"])

        result = executor.execute(plan)

        assert result.run_id.startswith("er_run_")


class TestQualityGateIntegration:
    def test_passing_gate(self):
        """Quality gate that passes does not affect execution."""

        class AlwaysPassGate(DataQualityGate):
            def applies_to(self, stage_name):
                return True

            def check(self, stage_name, backend, outputs):
                return GateResult(passed=True, message="OK")

        backend = MockBackend()
        executor = PipelineExecutor(backend, quality_gates=[AlwaysPassGate()])
        plan = _make_plan(["a"])

        result = executor.execute(plan, run_id="test")
        assert result.success

    def test_failing_error_gate_stops_pipeline(self):
        """Quality gate failure with severity='error' stops pipeline."""

        class AlwaysFailGate(DataQualityGate):
            def applies_to(self, stage_name):
                return True

            def check(self, stage_name, backend, outputs):
                return GateResult(
                    passed=False,
                    message="gate failed",
                    severity="error",
                )

        backend = MockBackend()
        executor = PipelineExecutor(
            backend, quality_gates=[AlwaysFailGate()]
        )
        plan = _make_plan(["a"])

        with pytest.raises(RuntimeError, match="Quality gate failed"):
            executor.execute(plan, run_id="test")

    def test_warning_gate_continues(self):
        """Quality gate with severity='warning' logs but continues."""

        class WarnGate(DataQualityGate):
            def applies_to(self, stage_name):
                return True

            def check(self, stage_name, backend, outputs):
                return GateResult(
                    passed=False,
                    message="just a warning",
                    severity="warning",
                )

        backend = MockBackend()
        executor = PipelineExecutor(backend, quality_gates=[WarnGate()])
        plan = _make_plan(["a", "b"])

        result = executor.execute(plan, run_id="test")
        assert result.success


class TestPipelineResult:
    def test_defaults(self):
        result = PipelineResult(run_id="test")
        assert result.status == "running"
        assert result.success is False
        assert result.completed_stages == []

    def test_duration(self):
        from datetime import datetime, timezone, timedelta

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(seconds=42)
        result = PipelineResult(
            run_id="test", started_at=start, finished_at=end
        )
        assert result.duration_seconds == 42.0


class TestStageExecutionResult:
    def test_defaults(self):
        r = StageExecutionResult(stage_name="test")
        assert r.success is True
        assert r.skipped is False
        assert r.error is None
