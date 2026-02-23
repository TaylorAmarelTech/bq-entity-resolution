"""BigQuery backend: production execution with retries and cost tracking.

Wraps the existing BigQueryClient to conform to the Backend protocol.
"""

from __future__ import annotations

import logging

from bq_entity_resolution.backends.protocol import (
    Backend,
    ColumnDef,
    QueryResult,
    TableSchema,
)
from bq_entity_resolution.clients.bigquery import BigQueryClient
from bq_entity_resolution.clients.bigquery import QueryResult as BQQueryResult

logger = logging.getLogger(__name__)

# BigQuery type mapping for INFORMATION_SCHEMA
_BQ_TYPE_MAP = {
    "STRING": "STRING",
    "INT64": "INT64",
    "INTEGER": "INT64",
    "FLOAT64": "FLOAT64",
    "FLOAT": "FLOAT64",
    "BOOL": "BOOL",
    "BOOLEAN": "BOOL",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "BYTES": "BYTES",
    "NUMERIC": "NUMERIC",
    "BIGNUMERIC": "BIGNUMERIC",
    "GEOGRAPHY": "GEOGRAPHY",
    "JSON": "JSON",
}


class BigQueryBackend:
    """BigQuery production backend.

    Delegates to the existing BigQueryClient which handles retries,
    job labeling, dry-run support, and structured logging.
    """

    def __init__(
        self,
        project: str,
        location: str = "US",
        dry_run: bool = False,
        max_bytes_billed: int | None = None,
    ):
        self._client = BigQueryClient(
            project=project,
            location=location,
            dry_run=dry_run,
            max_bytes_billed=max_bytes_billed,
        )
        self._project = project

    @property
    def dialect(self) -> str:
        return "bigquery"

    def execute(self, sql: str, label: str = "") -> QueryResult:
        bq_result = self._client.execute(sql, job_label=label)
        return _convert_result(bq_result)

    def execute_and_fetch(self, sql: str, label: str = "") -> list[dict]:
        return self._client.execute_and_fetch(sql, job_label=label)

    def execute_script(self, sql: str, label: str = "") -> QueryResult:
        # BQ handles scripts transparently via execute
        bq_result = self._client.execute(sql, job_label=label)
        return _convert_result(bq_result)

    def execute_script_and_fetch(self, sql: str, label: str = "") -> list[dict]:
        return self._client.execute_script_and_fetch(sql, job_label=label)

    def table_exists(self, table_ref: str) -> bool:
        return self._client.table_exists(table_ref)

    def get_table_schema(self, table_ref: str) -> TableSchema:
        """Read table schema from BigQuery INFORMATION_SCHEMA."""
        parts = table_ref.replace("`", "").split(".")
        if len(parts) != 3:
            raise ValueError(f"Expected project.dataset.table, got: {table_ref}")
        project, dataset, table = parts
        sql = (
            f"SELECT column_name, data_type, is_nullable "
            f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
            f"WHERE table_name = '{table}' "
            f"ORDER BY ordinal_position"
        )
        rows = self._client.execute_and_fetch(sql, job_label="schema_introspection")
        columns = tuple(
            ColumnDef(
                name=row["column_name"],
                type=_BQ_TYPE_MAP.get(row["data_type"], row["data_type"]),
                nullable=row["is_nullable"] == "YES",
            )
            for row in rows
        )
        return TableSchema(columns=columns)

    def row_count(self, table_ref: str) -> int:
        rows = self._client.execute_and_fetch(
            f"SELECT COUNT(*) AS cnt FROM `{table_ref}`",
            job_label="row_count",
        )
        return rows[0]["cnt"] if rows else 0


def _convert_result(bq_result: BQQueryResult) -> QueryResult:
    """Convert BQ client result to backend protocol result."""
    return QueryResult(
        job_id=bq_result.job_id,
        rows_affected=bq_result.rows_affected,
        bytes_billed=bq_result.bytes_billed,
        duration_seconds=bq_result.duration_seconds,
        total_bytes_processed=bq_result.total_bytes_processed,
        slot_milliseconds=bq_result.slot_milliseconds,
    )
