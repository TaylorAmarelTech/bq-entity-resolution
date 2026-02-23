"""
Watermark manager for incremental processing.

Runtime watermark tracking backed by a BigQuery metadata table.
Only advances watermarks after successful pipeline completion.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from bq_entity_resolution.exceptions import WatermarkError
from bq_entity_resolution.sql.generator import SQLGenerator

logger = logging.getLogger(__name__)


class WatermarkManager:
    """
    Manages watermark state in a BigQuery metadata table.

    Advantages over dbt compile-time watermarks:
    - Read at runtime, not compile time
    - Advance only on success (transactional)
    - Compound cursor support (multiple columns)
    - Grace period for late-arriving data
    - Full audit trail
    """

    def __init__(
        self,
        bq_client: Any,  # BigQueryClient
        watermark_table: str,
        sql_gen: SQLGenerator | None = None,
    ):
        self.bq_client = bq_client
        self.watermark_table = watermark_table
        self.sql_gen = sql_gen or SQLGenerator()

    def ensure_table_exists(self) -> None:
        """Create the watermark table if it doesn't exist."""
        sql = self.sql_gen.render(
            "watermark/create_watermark_table.sql.j2",
            table=self.watermark_table,
        )
        self.bq_client.execute(sql, job_label="ensure_watermark_table")
        logger.info("Watermark table ensured: %s", self.watermark_table)

    def read(self, source_name: str) -> dict[str, Any] | None:
        """
        Read current watermark for a source.

        Returns dict of {cursor_column: cursor_value} or None if no watermark.
        """
        sql = self.sql_gen.render(
            "watermark/read_watermark.sql.j2",
            table=self.watermark_table,
            source_name=source_name,
        )
        try:
            rows = self.bq_client.execute_and_fetch(sql)
        except Exception as exc:
            raise WatermarkError(
                f"Failed to read watermark for '{source_name}': {exc}"
            ) from exc

        if not rows:
            logger.info("No watermark found for source '%s'", source_name)
            return None

        result = {}
        for row in rows:
            value = _deserialize(row["cursor_value"], row["cursor_type"])
            result[row["cursor_column"]] = value

        logger.info("Read watermark for '%s': %s", source_name, result)
        return result

    def write(
        self,
        source_name: str,
        cursors: dict[str, Any],
        run_id: str = "",
    ) -> None:
        """
        Write new watermark values.

        Marks previous values as not current and inserts new values.
        """
        if not cursors:
            logger.warning("No cursor values to write for '%s'", source_name)
            return

        cursor_rows = [
            {
                "column": col,
                "value": _serialize(val),
                "type": _type_name(val),
            }
            for col, val in cursors.items()
        ]

        sql = self.sql_gen.render(
            "watermark/update_watermark.sql.j2",
            table=self.watermark_table,
            source_name=source_name,
            cursors=cursor_rows,
            run_id=run_id,
            now=datetime.now(timezone.utc).isoformat(),
        )

        try:
            self.bq_client.execute(sql, job_label=f"watermark_update_{source_name}")
        except Exception as exc:
            raise WatermarkError(
                f"Failed to write watermark for '{source_name}': {exc}"
            ) from exc

        logger.info("Watermark updated for '%s': %s", source_name, cursors)

    def compute_new_watermark(
        self,
        source_table: str,
        cursor_columns: list[str],
    ) -> dict[str, Any]:
        """Query the source table for the maximum cursor values."""
        max_exprs = ", ".join(
            f"MAX({col}) AS max_{col}" for col in cursor_columns
        )
        sql = f"SELECT {max_exprs} FROM `{source_table}`"

        try:
            rows = self.bq_client.execute_and_fetch(sql)
        except Exception as exc:
            raise WatermarkError(
                f"Failed to compute watermark from '{source_table}': {exc}"
            ) from exc

        if not rows:
            return {}

        row = rows[0]
        return {
            col: row[f"max_{col}"]
            for col in cursor_columns
            if row.get(f"max_{col}") is not None
        }


def _serialize(value: Any) -> str:
    """Serialize a watermark value to string for storage."""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _deserialize(value: str, type_name: str) -> Any:
    """Deserialize a stored watermark value."""
    if type_name == "TIMESTAMP":
        return datetime.fromisoformat(value)
    if type_name == "INT64":
        return int(value)
    if type_name == "FLOAT64":
        return float(value)
    return value


def _type_name(value: Any) -> str:
    """Determine the type name for a watermark value."""
    if isinstance(value, datetime):
        return "TIMESTAMP"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    return "STRING"
