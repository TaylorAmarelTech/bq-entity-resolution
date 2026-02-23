"""Pipeline plan: immutable snapshot of planned SQL execution.

The plan phase generates all SQL without executing it. This enables:
1. Preview of all SQL before execution
2. Validation of stage contracts
3. Testing SQL generation independently from execution
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.stages.base import TableRef

if TYPE_CHECKING:
    from bq_entity_resolution.pipeline.dag import StageDAG


@dataclass(frozen=True)
class StagePlan:
    """Plan for a single stage."""

    stage_name: str
    sql_expressions: tuple[SQLExpression, ...]
    inputs: dict[str, TableRef]
    outputs: dict[str, TableRef]
    dependencies: tuple[str, ...]

    @property
    def sql_count(self) -> int:
        return len(self.sql_expressions)

    def render_sql(self, dialect: str = "bigquery") -> list[str]:
        """Render all SQL expressions for this stage."""
        return [expr.render(dialect) for expr in self.sql_expressions]


@dataclass(frozen=True)
class PipelinePlan:
    """Immutable plan for a complete pipeline execution.

    Created by planning all stages in DAG order. Can be previewed,
    validated, and then executed by PipelineExecutor.
    """

    stages: tuple[StagePlan, ...]

    @property
    def stage_names(self) -> list[str]:
        return [s.stage_name for s in self.stages]

    @property
    def total_sql_count(self) -> int:
        return sum(s.sql_count for s in self.stages)

    def get_stage(self, name: str) -> StagePlan:
        """Get a stage plan by name."""
        for s in self.stages:
            if s.stage_name == name:
                return s
        raise KeyError(f"Stage not found: {name}")

    def all_sql(self, dialect: str = "bigquery") -> list[str]:
        """Render all SQL statements in execution order."""
        result = []
        for stage in self.stages:
            result.extend(stage.render_sql(dialect))
        return result

    def preview(self) -> str:
        """Human-readable summary of the plan."""
        lines = [
            f"Pipeline Plan: {len(self.stages)} stages, "
            f"{self.total_sql_count} SQL statements",
            "",
        ]
        for i, stage in enumerate(self.stages, 1):
            deps = (
                ", ".join(stage.dependencies)
                if stage.dependencies
                else "(none)"
            )
            inputs = (
                ", ".join(stage.inputs.keys())
                if stage.inputs
                else "(none)"
            )
            outputs = (
                ", ".join(stage.outputs.keys())
                if stage.outputs
                else "(none)"
            )
            lines.append(f"  {i}. {stage.stage_name}")
            lines.append(f"     deps: {deps}")
            lines.append(f"     inputs: {inputs}")
            lines.append(f"     outputs: {outputs}")
            lines.append(f"     sql: {stage.sql_count} statement(s)")
        return "\n".join(lines)


def create_plan(dag: StageDAG, **plan_kwargs: Any) -> PipelinePlan:
    """Generate a PipelinePlan from a StageDAG.

    plan_kwargs are passed to each stage's plan() method.
    Common kwargs:
      - watermark: dict[str, Any] | None
      - full_refresh: bool
    """
    stage_plans = []
    for stage in dag.stages:
        exprs = stage.plan(**plan_kwargs)

        stage_plans.append(StagePlan(
            stage_name=stage.name,
            sql_expressions=tuple(exprs),
            inputs=dict(stage.inputs),
            outputs=dict(stage.outputs),
            dependencies=tuple(dag.get_dependencies(stage.name)),
        ))

    return PipelinePlan(stages=tuple(stage_plans))
