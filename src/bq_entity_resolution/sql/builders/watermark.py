"""SQL builder for watermark and checkpoint tables.

Replaces:
- watermark/create_watermark_table.sql.j2
- watermark/read_watermark.sql.j2
- watermark/update_watermark.sql.j2
- watermark/create_checkpoint_table.sql.j2
"""

from __future__ import annotations

from bq_entity_resolution.columns import (
    CHECKPOINT_COMPLETED_AT,
    CHECKPOINT_RUN_ID,
    CHECKPOINT_STAGE_NAME,
    CHECKPOINT_STATUS,
    WATERMARK_CURSOR_COLUMN,
    WATERMARK_CURSOR_TYPE,
    WATERMARK_CURSOR_VALUE,
    WATERMARK_IS_CURRENT,
    WATERMARK_RUN_ID,
    WATERMARK_SOURCE_NAME,
    WATERMARK_UPDATED_AT,
)
from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.sql.utils import sql_escape


def build_create_watermark_table_sql(table: str) -> SQLExpression:
    """Build DDL to create the watermark tracking table."""
    sql = (
        f"CREATE TABLE IF NOT EXISTS `{table}` (\n"
        f"  {WATERMARK_SOURCE_NAME} STRING NOT NULL,\n"
        f"  {WATERMARK_CURSOR_COLUMN} STRING NOT NULL,\n"
        f"  {WATERMARK_CURSOR_VALUE} STRING NOT NULL,\n"
        f"  {WATERMARK_CURSOR_TYPE} STRING NOT NULL,\n"
        f"  {WATERMARK_UPDATED_AT} TIMESTAMP NOT NULL,\n"
        f"  {WATERMARK_RUN_ID} STRING,\n"
        f"  {WATERMARK_IS_CURRENT} BOOL NOT NULL DEFAULT TRUE\n"
        f")\n"
        f"PARTITION BY DATE({WATERMARK_UPDATED_AT})\n"
        f"CLUSTER BY {WATERMARK_SOURCE_NAME}"
    )
    return SQLExpression.from_raw(sql)


def build_read_watermark_sql(table: str, source_name: str) -> SQLExpression:
    """Build SQL to read current watermark values for a source."""
    escaped = sql_escape(source_name)
    sql = (
        f"SELECT\n"
        f"  {WATERMARK_CURSOR_COLUMN},\n"
        f"  {WATERMARK_CURSOR_VALUE},\n"
        f"  {WATERMARK_CURSOR_TYPE},\n"
        f"  {WATERMARK_UPDATED_AT}\n"
        f"FROM `{table}`\n"
        f"WHERE {WATERMARK_SOURCE_NAME} = '{escaped}'\n"
        f"  AND {WATERMARK_IS_CURRENT} = TRUE\n"
        f"ORDER BY {WATERMARK_CURSOR_COLUMN}"
    )
    return SQLExpression.from_raw(sql)


def build_update_watermark_sql(
    table: str,
    source_name: str,
    cursors: list[dict],
    run_id: str,
    now: str,
) -> SQLExpression:
    """Build SQL to update watermark values atomically.

    Uses BEGIN/COMMIT TRANSACTION for atomicity.

    Args:
        table: Fully-qualified watermark table name.
        source_name: Source identifier.
        cursors: List of {"column": str, "value": str, "type": str}.
        run_id: Pipeline run identifier.
        now: ISO timestamp string.
    """
    escaped_source = sql_escape(source_name)
    escaped_run_id = sql_escape(run_id)
    escaped_now = sql_escape(now)

    lines: list[str] = []
    lines.append("BEGIN TRANSACTION;")
    lines.append("")
    lines.append("-- Mark previous watermarks as not current")
    lines.append(f"UPDATE `{table}`")
    lines.append(f"SET {WATERMARK_IS_CURRENT} = FALSE")
    lines.append(f"WHERE {WATERMARK_SOURCE_NAME} = '{escaped_source}'")
    lines.append(f"  AND {WATERMARK_IS_CURRENT} = TRUE;")
    lines.append("")
    lines.append("-- Insert new watermark values")
    lines.append(f"INSERT INTO `{table}`")
    lines.append(
        f"  ({WATERMARK_SOURCE_NAME}, {WATERMARK_CURSOR_COLUMN}, {WATERMARK_CURSOR_VALUE}, "
        f"{WATERMARK_CURSOR_TYPE}, {WATERMARK_UPDATED_AT}, {WATERMARK_RUN_ID}, {WATERMARK_IS_CURRENT})"
    )
    lines.append("VALUES")

    value_rows: list[str] = []
    for cursor in cursors:
        escaped_col = sql_escape(cursor["column"])
        escaped_val = sql_escape(cursor["value"])
        escaped_type = sql_escape(cursor["type"])
        value_rows.append(
            f"  ('{escaped_source}', '{escaped_col}', '{escaped_val}', "
            f"'{escaped_type}', TIMESTAMP('{escaped_now}'), "
            f"'{escaped_run_id}', TRUE)"
        )

    lines.append(",\n".join(value_rows) + ";")
    lines.append("")
    lines.append("COMMIT TRANSACTION;")

    return SQLExpression.from_raw("\n".join(lines))


def build_create_checkpoint_table_sql(table: str) -> SQLExpression:
    """Build DDL to create the pipeline checkpoint table."""
    sql = (
        f"CREATE TABLE IF NOT EXISTS `{table}` (\n"
        f"  {CHECKPOINT_RUN_ID} STRING NOT NULL,\n"
        f"  {CHECKPOINT_STAGE_NAME} STRING NOT NULL,\n"
        f"  {CHECKPOINT_COMPLETED_AT} TIMESTAMP NOT NULL,\n"
        f"  {CHECKPOINT_STATUS} STRING NOT NULL\n"
        f")\n"
        f"PARTITION BY DATE({CHECKPOINT_COMPLETED_AT})\n"
        f"CLUSTER BY {CHECKPOINT_RUN_ID}"
    )
    return SQLExpression.from_raw(sql)
