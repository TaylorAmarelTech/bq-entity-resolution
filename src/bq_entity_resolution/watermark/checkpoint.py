"""
Checkpoint manager for crash-resilient pipeline execution.

Persists completed stage state to a BigQuery metadata table so that
a pipeline crash mid-run can resume from the last completed stage
instead of re-running everything from scratch.
"""

from __future__ import annotations

import logging

from bq_entity_resolution.sql.generator import SQLGenerator

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages pipeline checkpoint state in a BigQuery metadata table."""

    def __init__(
        self,
        bq_client: object,
        checkpoint_table: str,
        sql_gen: SQLGenerator | None = None,
    ):
        self._client = bq_client
        self._table = checkpoint_table
        self._sql_gen = sql_gen or SQLGenerator()

    def ensure_table_exists(self) -> None:
        """Create the checkpoint table if it doesn't exist."""
        sql = self._sql_gen.render(
            "watermark/create_checkpoint_table.sql.j2",
            table=self._table,
        )
        self._client.execute(sql, job_label="ensure_checkpoint_table")

    def load_completed_stages(self, run_id: str) -> set[str]:
        """Load completed stages for a given run_id."""
        safe_run_id = run_id.replace("'", "''")
        sql = (
            f"SELECT stage_name FROM `{self._table}` "
            f"WHERE run_id = '{safe_run_id}' AND status = 'completed'"
        )
        rows = self._client.execute_and_fetch(sql)
        return {row["stage_name"] for row in rows}

    def find_resumable_run(self) -> str | None:
        """Find the most recent incomplete run_id that can be resumed.

        Returns None if no resumable run exists.
        """
        sql = (
            f"SELECT run_id, MAX(completed_at) AS last_checkpoint "
            f"FROM `{self._table}` "
            f"WHERE status = 'completed' AND stage_name != '__run_complete__' "
            f"GROUP BY run_id "
            f"HAVING run_id NOT IN ("
            f"  SELECT run_id FROM `{self._table}` "
            f"  WHERE stage_name = '__run_complete__'"
            f") "
            f"ORDER BY last_checkpoint DESC LIMIT 1"
        )
        rows = self._client.execute_and_fetch(sql)
        if rows:
            return rows[0]["run_id"]
        return None

    def mark_stage_complete(self, run_id: str, stage_name: str) -> None:
        """Record that a stage has completed for a run."""
        safe_run_id = run_id.replace("'", "''")
        safe_stage = stage_name.replace("'", "''")
        sql = (
            f"INSERT INTO `{self._table}` "
            f"(run_id, stage_name, completed_at, status) "
            f"VALUES ('{safe_run_id}', '{safe_stage}', "
            f"CURRENT_TIMESTAMP(), 'completed')"
        )
        self._client.execute(sql, job_label=f"checkpoint_{stage_name}")

    def mark_run_complete(self, run_id: str) -> None:
        """Mark an entire run as complete (prevents future resume)."""
        self.mark_stage_complete(run_id, "__run_complete__")
