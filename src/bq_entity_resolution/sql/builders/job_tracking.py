"""SQL builder for BigQuery job tracking tables.

Persists per-query metadata (job_id, bytes_billed, slot_milliseconds,
duration, rows_affected) for cost monitoring and performance analysis.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from bq_entity_resolution.columns import (
    JOB_TRACKING_BYTES_BILLED,
    JOB_TRACKING_DURATION_SECONDS,
    JOB_TRACKING_JOB_ID,
    JOB_TRACKING_QUERY_INDEX,
    JOB_TRACKING_ROWS_AFFECTED,
    JOB_TRACKING_RUN_ID,
    JOB_TRACKING_SLOT_MILLISECONDS,
    JOB_TRACKING_SQL_HASH,
    JOB_TRACKING_STAGE_NAME,
    JOB_TRACKING_STARTED_AT,
    JOB_TRACKING_TOTAL_BYTES_PROCESSED,
)
from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.sql.utils import sql_escape, validate_table_ref


@dataclass(frozen=True)
class JobDetail:
    """Per-query job metadata collected during pipeline execution."""

    stage_name: str
    query_index: int
    job_id: str
    bytes_billed: int
    total_bytes_processed: int
    slot_milliseconds: int
    duration_seconds: float
    rows_affected: int
    started_at: str
    sql_hash: str


def compute_sql_hash(sql: str) -> str:
    """Compute a truncated SHA-256 hash for cross-run SQL comparison.

    Returns the first 16 hex characters of the SHA-256 digest.
    """
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]


def build_create_job_tracking_table_sql(table: str) -> SQLExpression:
    """Build DDL to create the job tracking table."""
    validate_table_ref(table)
    sql = (
        f"CREATE TABLE IF NOT EXISTS `{table}` (\n"
        f"  {JOB_TRACKING_RUN_ID} STRING NOT NULL,\n"
        f"  {JOB_TRACKING_STAGE_NAME} STRING NOT NULL,\n"
        f"  {JOB_TRACKING_QUERY_INDEX} INT64 NOT NULL,\n"
        f"  {JOB_TRACKING_JOB_ID} STRING,\n"
        f"  {JOB_TRACKING_BYTES_BILLED} INT64,\n"
        f"  {JOB_TRACKING_TOTAL_BYTES_PROCESSED} INT64,\n"
        f"  {JOB_TRACKING_SLOT_MILLISECONDS} INT64,\n"
        f"  {JOB_TRACKING_DURATION_SECONDS} FLOAT64,\n"
        f"  {JOB_TRACKING_ROWS_AFFECTED} INT64,\n"
        f"  {JOB_TRACKING_STARTED_AT} TIMESTAMP,\n"
        f"  {JOB_TRACKING_SQL_HASH} STRING\n"
        f")\n"
        f"PARTITION BY DATE({JOB_TRACKING_STARTED_AT})\n"
        f"CLUSTER BY {JOB_TRACKING_RUN_ID}, {JOB_TRACKING_STAGE_NAME}"
    )
    return SQLExpression.from_raw(sql)


def build_insert_job_details_sql(
    table: str,
    run_id: str,
    details: list[JobDetail],
) -> SQLExpression:
    """Build SQL to insert job tracking details.

    Args:
        table: Fully-qualified job tracking table name.
        run_id: Pipeline run identifier.
        details: List of JobDetail records to insert.
    """
    validate_table_ref(table)
    if not details:
        raise ValueError("No job details to insert")

    escaped_run_id = sql_escape(run_id)

    lines: list[str] = []
    lines.append(f"INSERT INTO `{table}`")
    lines.append(
        f"  ({JOB_TRACKING_RUN_ID}, {JOB_TRACKING_STAGE_NAME}, "
        f"{JOB_TRACKING_QUERY_INDEX}, {JOB_TRACKING_JOB_ID}, "
        f"{JOB_TRACKING_BYTES_BILLED}, {JOB_TRACKING_TOTAL_BYTES_PROCESSED}, "
        f"{JOB_TRACKING_SLOT_MILLISECONDS}, {JOB_TRACKING_DURATION_SECONDS}, "
        f"{JOB_TRACKING_ROWS_AFFECTED}, {JOB_TRACKING_STARTED_AT}, "
        f"{JOB_TRACKING_SQL_HASH})"
    )
    lines.append("VALUES")

    value_rows: list[str] = []
    for d in details:
        escaped_stage = sql_escape(d.stage_name)
        escaped_job_id = sql_escape(d.job_id) if d.job_id else ""
        escaped_hash = sql_escape(d.sql_hash) if d.sql_hash else ""
        escaped_started = sql_escape(d.started_at) if d.started_at else ""
        job_id_val = f"'{escaped_job_id}'" if d.job_id else "NULL"
        started_val = (
            f"TIMESTAMP('{escaped_started}')" if d.started_at else "NULL"
        )
        hash_val = f"'{escaped_hash}'" if d.sql_hash else "NULL"
        value_rows.append(
            f"  ('{escaped_run_id}', '{escaped_stage}', "
            f"{d.query_index}, {job_id_val}, "
            f"{d.bytes_billed}, {d.total_bytes_processed}, "
            f"{d.slot_milliseconds}, {d.duration_seconds}, "
            f"{d.rows_affected}, {started_val}, {hash_val})"
        )

    lines.append(",\n".join(value_rows))
    return SQLExpression.from_raw("\n".join(lines))
