"""
Pipeline run context: carries state through pipeline execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.naming import all_matches_table as _all_matches_table


@dataclass
class PipelineContext:
    """Mutable context object passed through all pipeline stages."""

    run_id: str
    started_at: datetime
    config: PipelineConfig
    full_refresh: bool = False
    finished_at: Optional[datetime] = None
    status: str = "running"
    error: Optional[str] = None
    watermarks: dict[str, Any] = field(default_factory=dict)
    staged_sources: list[str] = field(default_factory=list)
    tier_results: dict[str, dict] = field(default_factory=dict)
    sql_log: list[dict] = field(default_factory=list)
    cluster_quality: Optional[dict] = None
    completed_stages: set = field(default_factory=set)

    @property
    def all_matches_table(self) -> str:
        """Fully-qualified table for accumulated matched pairs."""
        return _all_matches_table(self.config)

    def log_sql(self, stage: str, step: str, sql: str) -> None:
        """Record executed SQL for debugging and audit."""
        self.sql_log.append({
            "stage": stage,
            "step": step,
            "sql": sql,
            "timestamp": datetime.now(tz=None).isoformat(),
        })

    @property
    def duration_seconds(self) -> float:
        """Total pipeline duration in seconds."""
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return (datetime.now(tz=None) - self.started_at).total_seconds()
