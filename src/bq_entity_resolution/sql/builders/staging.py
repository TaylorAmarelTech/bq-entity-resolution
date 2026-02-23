"""SQL builder for incremental staging (replaces incremental_load.sql.j2).

Generates SQL to load source data into bronze staging area with:
- Deterministic entity UID generation via FARM_FINGERPRINT
- Watermark-based incremental filtering with grace period
- Source-level filters and supplemental joins
- Batch size limits
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bq_entity_resolution.columns import ENTITY_UID, SOURCE_NAME, SOURCE_UPDATED_AT, PIPELINE_LOADED_AT
from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.sql.utils import sql_escape


@dataclass(frozen=True)
class JoinDef:
    """A supplemental join for staging."""
    table: str
    on: str
    type: str = "LEFT"
    alias: str = ""


@dataclass(frozen=True)
class StagingParams:
    """Parameters for staging SQL generation."""
    target_table: str
    source_name: str
    source_table: str
    unique_key: str
    updated_at: str
    columns: list[str] = field(default_factory=list)
    passthrough_columns: list[str] = field(default_factory=list)
    joins: list[JoinDef] = field(default_factory=list)
    filter: str | None = None
    watermark: dict[str, Any] | None = None
    grace_period_hours: int = 0
    full_refresh: bool = False
    partition_column: str | None = None
    batch_size: int | None = None
    cluster_by: list[str] = field(default_factory=list)


def _format_watermark_value(val: Any) -> str:
    """Format a watermark value for SQL embedding."""
    if val is None:
        return "NULL"
    s = str(val)
    # Timestamps
    if "T" in s or ("-" in s and ":" in s):
        safe = s.replace("'", "''")
        return f"TIMESTAMP('{safe}')"
    # Numeric
    try:
        float(s)
        return s
    except (ValueError, TypeError):
        pass
    safe = s.replace("'", "''")
    return f"'{safe}'"


def build_staging_sql(params: StagingParams) -> SQLExpression:
    """Build staging/incremental load SQL.

    Returns an SQLExpression wrapping the generated SQL string.
    """
    parts: list[str] = []

    # CREATE OR REPLACE TABLE
    parts.append(f"CREATE OR REPLACE TABLE `{params.target_table}`")
    if params.cluster_by:
        parts.append(f"CLUSTER BY {', '.join(params.cluster_by)}")
    parts.append("AS")
    parts.append("")
    parts.append("SELECT")

    # Entity UID
    escaped_name = sql_escape(params.source_name)
    parts.append(f"  FARM_FINGERPRINT(")
    parts.append(f"    CONCAT(")
    parts.append(f"      '{escaped_name}', '||',")
    parts.append(f"      CAST({params.unique_key} AS STRING)")
    parts.append(f"    )")
    parts.append(f"  ) AS {ENTITY_UID},")
    parts.append(f"")
    parts.append(f"  '{escaped_name}' AS {SOURCE_NAME},")
    parts.append(f"")

    # Source columns
    for col in params.columns:
        parts.append(f"  {col},")

    # Passthrough columns
    for col in params.passthrough_columns:
        parts.append(f"  {col},")

    # Metadata columns
    parts.append(f"  {params.updated_at} AS {SOURCE_UPDATED_AT},")
    parts.append(f"  CURRENT_TIMESTAMP() AS {PIPELINE_LOADED_AT}")
    parts.append(f"")
    parts.append(f"FROM `{params.source_table}` AS src")

    # Supplemental joins
    for i, join in enumerate(params.joins):
        alias = join.alias or f"j_{i}"
        parts.append(f"{join.type} JOIN `{join.table}` AS {alias}")
        parts.append(f"  ON {join.on}")

    parts.append(f"WHERE 1=1")

    # Source-level filter
    if params.filter:
        parts.append(f"AND ({params.filter})")

    # Incremental filter with grace period
    if params.watermark and not params.full_refresh:
        conditions: list[str] = []
        for col, val in params.watermark.items():
            formatted_val = _format_watermark_value(val)
            if params.grace_period_hours and params.grace_period_hours > 0:
                conditions.append(
                    f"{col} > TIMESTAMP_SUB({formatted_val}, "
                    f"INTERVAL {params.grace_period_hours} HOUR)"
                )
            else:
                conditions.append(f"{col} > {formatted_val}")
        parts.append(f"AND (")
        parts.append(f"  {' OR '.join(conditions)}")
        parts.append(f")")

    # Exclude incomplete current day
    if params.partition_column:
        parts.append(f"AND {params.partition_column} < CURRENT_DATE()")

    # Batch size limit with deterministic ordering
    if params.batch_size:
        parts.append(f"ORDER BY {params.updated_at}, {ENTITY_UID}")
        parts.append(f"LIMIT {params.batch_size}")

    return SQLExpression.from_raw("\n".join(parts))
