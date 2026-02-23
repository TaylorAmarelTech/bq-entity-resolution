"""DuckDB backend: core connection management, execute, fetch, table operations.

NOT a production backend. Exists exclusively for:
1. Running integration tests without BigQuery
2. Local development and debugging
3. Validating SQL correctness before deploying to BQ
"""

from __future__ import annotations

import logging
import time

import duckdb

from bq_entity_resolution.backends.protocol import (
    ColumnDef,
    QueryResult,
    TableSchema,
)
from bq_entity_resolution.backends.duckdb.scripting import (
    execute_bq_scripting,
    is_bq_scripting,
    split_statements,
)
from bq_entity_resolution.backends.duckdb.shims import register_bq_shims
from bq_entity_resolution.backends.duckdb.sql_adapter import adapt_sql

logger = logging.getLogger(__name__)

# DuckDB type mapping
_DUCKDB_TYPE_MAP = {
    "VARCHAR": "STRING",
    "BIGINT": "INT64",
    "INTEGER": "INT64",
    "DOUBLE": "FLOAT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    "DATE": "DATE",
    "BLOB": "BYTES",
    "HUGEINT": "BIGNUMERIC",
}


class DuckDBBackend:
    """DuckDB local backend for development and testing.

    Provides BQ-compatible function shims so that SQL generated
    for the BigQuery dialect can execute locally.
    """

    def __init__(self, database: str = ":memory:"):
        self._conn = duckdb.connect(database)
        self._has_spatial = register_bq_shims(self._conn)

    @property
    def dialect(self) -> str:
        return "duckdb"

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Direct access to the DuckDB connection for test setup."""
        return self._conn

    @property
    def has_spatial(self) -> bool:
        """Whether the spatial extension is loaded."""
        return self._has_spatial

    @staticmethod
    def _adapt_sql(sql: str) -> str:
        """Adapt BigQuery SQL to DuckDB-compatible SQL.

        Delegates to the standalone ``adapt_sql()`` function in
        :mod:`bq_entity_resolution.backends.duckdb.sql_adapter`.
        Kept as a method for backward compatibility with tests.
        """
        return adapt_sql(sql)

    @staticmethod
    def _split_statements(sql: str) -> list[str]:
        """Split SQL into statements. Delegates to scripting module."""
        return split_statements(sql)

    @staticmethod
    def _rewrite_unnest(sql: str) -> str:
        """Rewrite UNNEST patterns. Delegates to sql_adapter module."""
        from bq_entity_resolution.backends.duckdb.sql_adapter import rewrite_unnest
        return rewrite_unnest(sql)

    @staticmethod
    def _rewrite_ml_distance(sql: str) -> str:
        """Rewrite ML.DISTANCE. Delegates to sql_adapter module."""
        from bq_entity_resolution.backends.duckdb.sql_adapter import rewrite_ml_distance
        return rewrite_ml_distance(sql)

    @staticmethod
    def _substitute_vars(sql: str, variables: dict[str, object]) -> str:
        """Substitute variables in SQL. Delegates to scripting module."""
        from bq_entity_resolution.backends.duckdb.scripting import substitute_vars
        return substitute_vars(sql, variables)

    def execute(self, sql: str, label: str = "") -> QueryResult:
        sql = adapt_sql(sql)
        start = time.monotonic()
        try:
            result = self._conn.execute(sql)
            duration = time.monotonic() - start
            row_count = 0
            if result and result.description:
                try:
                    rows = result.fetchall()
                    row_count = len(rows)
                except duckdb.InvalidInputException:
                    pass  # DDL statements don't return rows
            return QueryResult(
                job_id=f"duckdb_{label}",
                rows_affected=row_count,
                duration_seconds=duration,
            )
        except Exception as e:
            logger.error("DuckDB execution error (label=%s): %s", label, e)
            logger.error("SQL:\n%s", sql[:500])
            raise

    def execute_and_fetch(self, sql: str, label: str = "") -> list[dict]:
        sql = adapt_sql(sql)
        result = self._conn.execute(sql)
        if result.description is None:
            return []
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def execute_script(self, sql: str, label: str = "") -> QueryResult:
        """Execute a multi-statement script.

        Detects BQ scripting (DECLARE/WHILE/LOOP/SET) and interprets
        it in Python. Otherwise splits on semicolons and executes
        each statement sequentially.
        """
        sql = adapt_sql(sql)
        start = time.monotonic()

        if is_bq_scripting(sql):
            total_rows = execute_bq_scripting(self._conn, sql, split_statements)
        else:
            statements = split_statements(sql)
            total_rows = 0
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue
                result = self._conn.execute(stmt)
                if result and result.description:
                    try:
                        total_rows += len(result.fetchall())
                    except duckdb.InvalidInputException:
                        pass

        return QueryResult(
            job_id=f"duckdb_script_{label}",
            rows_affected=total_rows,
            duration_seconds=time.monotonic() - start,
        )

    def execute_script_and_fetch(self, sql: str, label: str = "") -> list[dict]:
        sql = adapt_sql(sql)
        statements = split_statements(sql)
        last_result: list[dict] = []
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            result = self._conn.execute(stmt)
            if result and result.description:
                columns = [desc[0] for desc in result.description]
                last_result = [dict(zip(columns, row)) for row in result.fetchall()]
        return last_result

    def table_exists(self, table_ref: str) -> bool:
        table_name = _local_table_name(table_ref)
        try:
            self._conn.execute(f"SELECT 1 FROM {table_name} LIMIT 0")
            return True
        except duckdb.CatalogException:
            return False

    def get_table_schema(self, table_ref: str) -> TableSchema:
        table_name = _local_table_name(table_ref)
        result = self._conn.execute(f"DESCRIBE {table_name}")
        columns = []
        for row in result.fetchall():
            col_name = row[0]
            col_type = row[1]
            nullable = row[3] != "NO"  # null column
            mapped_type = _DUCKDB_TYPE_MAP.get(col_type.upper(), col_type.upper())
            columns.append(ColumnDef(name=col_name, type=mapped_type, nullable=nullable))
        return TableSchema(columns=tuple(columns))

    def row_count(self, table_ref: str) -> int:
        table_name = _local_table_name(table_ref)
        result = self._conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        return result.fetchone()[0]

    def load_csv(self, table_name: str, csv_path: str) -> None:
        """Load a CSV file into a table for test setup."""
        self._conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{csv_path}')"
        )

    def create_table_from_data(self, table_name: str, data: list[dict]) -> None:
        """Create a table from a list of dicts for test setup."""
        if not data:
            return
        columns = list(data[0].keys())
        col_defs = ", ".join(f"{c} VARCHAR" for c in columns)
        self._conn.execute(f"CREATE OR REPLACE TABLE {table_name} ({col_defs})")
        for row in data:
            values = ", ".join(
                f"'{v}'" if v is not None else "NULL" for v in row.values()
            )
            self._conn.execute(f"INSERT INTO {table_name} VALUES ({values})")


def _local_table_name(fq_name: str) -> str:
    """Convert a fully-qualified BQ table name to a local DuckDB name."""
    # "project.dataset.table" -> "table"
    parts = fq_name.replace("`", "").split(".")
    return parts[-1]
