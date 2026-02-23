"""Pipeline stages: composable, testable units of pipeline work.

Each stage declares its inputs, outputs, and plan() method.
The plan() method returns SQL expressions without side effects.
The executor handles running the SQL and managing state.
"""

from bq_entity_resolution.stages.base import Stage, TableRef, StageResult

__all__ = ["Stage", "TableRef", "StageResult"]
