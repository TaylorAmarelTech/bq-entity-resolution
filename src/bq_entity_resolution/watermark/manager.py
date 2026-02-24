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
from bq_entity_resolution.sql.builders.watermark import (
    build_create_watermark_table_sql,
    build_read_watermark_sql,
    build_update_watermark_sql,
)

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
    ):
        self.bq_client = bq_client
        self.watermark_table = watermark_table

    def ensure_table_exists(self) -> None:
        """Create the watermark table if it doesn't exist."""
        expr = build_create_watermark_table_sql(self.watermark_table)
        self.bq_client.execute(expr.render(), job_label="ensure_watermark_table")
        logger.info("Watermark table ensured: %s", self.watermark_table)

    def read(self, source_name: str) -> dict[str, Any] | None:
        """
        Read current watermark for a source.

        Returns dict of {cursor_column: cursor_value} or None if no watermark.
        """
        expr = build_read_watermark_sql(self.watermark_table, source_name)
        try:
            rows = self.bq_client.execute_and_fetch(expr.render())
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

        expr = build_update_watermark_sql(
            table=self.watermark_table,
            source_name=source_name,
            cursors=cursor_rows,
            run_id=run_id,
            now=datetime.now(timezone.utc).isoformat(),
        )

        try:
            self.bq_client.execute(
                expr.render(), job_label=f"watermark_update_{source_name}"
            )
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

    def compute_new_watermark_from_staged(
        self,
        staged_table: str,
        cursor_columns: list[str],
        column_mapping: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Compute new watermark from the staged (bronze) table.

        This is the correct approach for ordered cursors: the watermark
        advances to the MAX values of what was actually staged in this
        batch, not what exists in the raw source (which may be far ahead).

        This prevents watermark drift that could cause the pipeline to
        skip unprocessed records between the batch boundary and the
        source maximum.

        Args:
            staged_table: The bronze staging table for the current batch.
            cursor_columns: Original source cursor column names.
            column_mapping: Optional mapping from source column names to
                staged column names (e.g. {"updated_at": "source_updated_at"}).
        """
        mapping = column_mapping or {}
        max_exprs = ", ".join(
            f"MAX({mapping.get(col, col)}) AS max_{col}"
            for col in cursor_columns
        )
        sql = f"SELECT {max_exprs} FROM `{staged_table}`"

        try:
            rows = self.bq_client.execute_and_fetch(sql)
        except Exception as exc:
            raise WatermarkError(
                f"Failed to compute watermark from staged '{staged_table}': {exc}"
            ) from exc

        if not rows:
            return {}

        row = rows[0]
        return {
            col: row[f"max_{col}"]
            for col in cursor_columns
            if row.get(f"max_{col}") is not None
        }

    def has_unprocessed_records(
        self,
        source_table: str,
        cursor_columns: list[str],
        current_watermark: dict[str, Any],
    ) -> bool:
        """Check if there are records beyond the current watermark.

        Used by drain mode to determine when to stop iterating.
        """
        if not current_watermark:
            return True

        conditions: list[str] = []
        for col in cursor_columns:
            val = current_watermark.get(col)
            if val is not None:
                from bq_entity_resolution.sql.builders.staging import (
                    _format_watermark_value,
                )
                formatted = _format_watermark_value(val)
                conditions.append(f"{col} > {formatted}")

        if not conditions:
            return True

        where_clause = " OR ".join(conditions)
        sql = (
            f"SELECT COUNT(*) AS cnt FROM `{source_table}` "
            f"WHERE {where_clause} LIMIT 1"
        )

        try:
            rows = self.bq_client.execute_and_fetch(sql)
        except Exception as exc:
            logger.warning(
                "Failed to check unprocessed records: %s", exc
            )
            return False

        return bool(rows and rows[0].get("cnt", 0) > 0)


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
