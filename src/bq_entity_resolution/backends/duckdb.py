"""DuckDB backend: local development and testing with BQ function shims.

NOT a production backend. Exists exclusively for:
1. Running integration tests without BigQuery
2. Local development and debugging
3. Validating SQL correctness before deploying to BQ

Registers macros that approximate BigQuery-specific functions so that
SQL generated for the BQ dialect can run locally with minor adaptation.
"""

from __future__ import annotations

import logging
import re
import time

import duckdb

from bq_entity_resolution.backends.protocol import (
    ColumnDef,
    QueryResult,
    TableSchema,
)

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
        self._register_bq_shims()

    @property
    def dialect(self) -> str:
        return "duckdb"

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Direct access to the DuckDB connection for test setup."""
        return self._conn

    def execute(self, sql: str, label: str = "") -> QueryResult:
        sql = self._adapt_sql(sql)
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
        sql = self._adapt_sql(sql)
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
        sql = self._adapt_sql(sql)
        start = time.monotonic()

        if self._is_bq_scripting(sql):
            total_rows = self._execute_bq_scripting(sql)
        else:
            statements = self._split_statements(sql)
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
        sql = self._adapt_sql(sql)
        statements = self._split_statements(sql)
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
        table_name = self._local_table_name(table_ref)
        try:
            self._conn.execute(f"SELECT 1 FROM {table_name} LIMIT 0")
            return True
        except duckdb.CatalogException:
            return False

    def get_table_schema(self, table_ref: str) -> TableSchema:
        table_name = self._local_table_name(table_ref)
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
        table_name = self._local_table_name(table_ref)
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

    # ------------------------------------------------------------------
    # BQ function shims
    # ------------------------------------------------------------------

    def _register_bq_shims(self) -> None:
        """Register DuckDB macros that approximate BQ-specific functions."""
        shims = [
            # FARM_FINGERPRINT → deterministic hash
            "CREATE OR REPLACE MACRO FARM_FINGERPRINT(x) AS hash(CAST(x AS VARCHAR))",
            # SAFE_DIVIDE → NULL-safe division
            "CREATE OR REPLACE MACRO SAFE_DIVIDE(a, b) AS "
            "CASE WHEN b = 0 OR b IS NULL THEN NULL ELSE CAST(a AS DOUBLE) / CAST(b AS DOUBLE) END",
            # FORMAT_DATE → strftime
            "CREATE OR REPLACE MACRO FORMAT_DATE(fmt, d) AS strftime(d, fmt)",
            # EDIT_DISTANCE → native levenshtein
            "CREATE OR REPLACE MACRO EDIT_DISTANCE(a, b) AS "
            "levenshtein(CAST(a AS VARCHAR), CAST(b AS VARCHAR))",
            # jaro_winkler UDF → native jaro_winkler_similarity
            "CREATE OR REPLACE MACRO jaro_winkler(a, b) AS "
            "jaro_winkler_similarity(CAST(a AS VARCHAR), CAST(b AS VARCHAR))",
        ]
        for shim in shims:
            try:
                self._conn.execute(shim)
            except Exception as e:
                logger.debug("Failed to register shim: %s (%s)", shim[:80], e)

        # Register Python UDFs for phonetic functions
        self._register_phonetic_udfs()
        # Try to load spatial extension for geo functions
        self._register_geo_shims()

    def _register_phonetic_udfs(self) -> None:
        """Register Python UDFs for SOUNDEX and double_metaphone."""
        # Real SOUNDEX implementation
        def _soundex(s: str) -> str | None:
            if not s or not s.strip():
                return None
            s = s.strip().upper()
            # Keep first letter
            first = ""
            for ch in s:
                if ch.isalpha():
                    first = ch
                    break
            if not first:
                return None
            coding = {
                "B": "1", "F": "1", "P": "1", "V": "1",
                "C": "2", "G": "2", "J": "2", "K": "2", "Q": "2",
                "S": "2", "X": "2", "Z": "2",
                "D": "3", "T": "3",
                "L": "4",
                "M": "5", "N": "5",
                "R": "6",
            }
            result = first
            prev_code = coding.get(first, "0")
            started = False
            for ch in s:
                if not started:
                    if ch == first:
                        started = True
                    continue
                code = coding.get(ch, "0")
                if code != "0" and code != prev_code:
                    result += code
                prev_code = code if code != "0" else prev_code
                if len(result) == 4:
                    break
            return result.ljust(4, "0")

        try:
            self._conn.create_function(
                "SOUNDEX", _soundex, ["VARCHAR"], "VARCHAR",
                null_handling="special",
            )
        except Exception as e:
            logger.debug("Failed to register SOUNDEX UDF: %s", e)

        # metaphone — delegates to SOUNDEX unless metaphone library available
        try:
            from metaphone import doublemetaphone

            def _metaphone(s: str) -> str | None:
                if not s or not s.strip():
                    return None
                result = doublemetaphone(s.strip())
                return result[0] if result[0] else result[1]

            def _dm_primary(s: str) -> str | None:
                if not s or not s.strip():
                    return None
                return doublemetaphone(s.strip())[0] or None

            def _dm_alternate(s: str) -> str | None:
                if not s or not s.strip():
                    return None
                return doublemetaphone(s.strip())[1] or None

            self._conn.create_function(
                "metaphone", _metaphone, ["VARCHAR"], "VARCHAR",
                null_handling="special",
            )
            self._conn.create_function(
                "double_metaphone_primary", _dm_primary,
                ["VARCHAR"], "VARCHAR",
                null_handling="special",
            )
            self._conn.create_function(
                "double_metaphone_alternate", _dm_alternate,
                ["VARCHAR"], "VARCHAR",
                null_handling="special",
            )
        except ImportError:
            logger.debug("metaphone library not available; using SOUNDEX fallback")
            try:
                self._conn.execute(
                    "CREATE OR REPLACE MACRO metaphone(x) AS SOUNDEX(x)"
                )
            except Exception as e:
                logger.debug("Failed to register metaphone fallback: %s", e)
        except Exception as e:
            logger.debug("Failed to register metaphone UDFs: %s", e)

    def _register_geo_shims(self) -> None:
        """Try to load DuckDB spatial extension for BQ geo function compatibility."""
        try:
            self._conn.execute("INSTALL spatial")
            self._conn.execute("LOAD spatial")
            # ST_GEOGPOINT(lon, lat) → ST_Point(lon, lat)
            self._conn.execute(
                "CREATE OR REPLACE MACRO ST_GEOGPOINT(lon, lat) AS "
                "ST_Point(CAST(lon AS DOUBLE), CAST(lat AS DOUBLE))"
            )
            self._has_spatial = True
            logger.debug("Spatial extension loaded — geo functions available")
        except Exception:
            self._has_spatial = False
            logger.debug("Spatial extension not available — geo functions disabled")

    @property
    def has_spatial(self) -> bool:
        """Whether the spatial extension is loaded."""
        return getattr(self, "_has_spatial", False)

    # ------------------------------------------------------------------
    # BQ scripting interpreter
    # ------------------------------------------------------------------

    def _is_bq_scripting(self, sql: str) -> bool:
        """Detect if SQL contains BQ scripting constructs."""
        upper = sql.upper().lstrip()
        # Any DECLARE, SET, WHILE, or LOOP statement indicates BQ scripting
        return bool(
            re.search(r'\bDECLARE\b', upper)
            or (re.search(r'\bSET\b', upper) and re.search(r'\bSET\s+\w+\s*=', sql, re.IGNORECASE))
        )

    def _execute_bq_scripting(self, sql: str) -> int:
        """Interpret BQ scripting (DECLARE/WHILE/SET/LOOP) in Python.

        Returns number of rows affected across all statements.
        """
        total_rows = 0
        variables: dict[str, object] = {}

        # Parse DECLARE statements
        for m in re.finditer(
            r'DECLARE\s+(\w+)\s+(\w+)(?:\s+DEFAULT\s+(.+?))?;',
            sql, re.IGNORECASE,
        ):
            var_name = m.group(1)
            default_val = m.group(3)
            if default_val is not None:
                default_val = default_val.strip()
                # Try to evaluate as SQL
                try:
                    rows = self._conn.execute(f"SELECT {default_val}").fetchall()
                    variables[var_name] = rows[0][0]
                except Exception:
                    # Try as literal
                    try:
                        variables[var_name] = int(default_val)
                    except ValueError:
                        variables[var_name] = default_val
            else:
                variables[var_name] = None

        # Extract loop body
        loop_match = re.search(
            r'(?:WHILE\s+(.+?)\s+DO|LOOP)\s*(.*?)\s*END\s+(?:WHILE|LOOP)',
            sql, re.IGNORECASE | re.DOTALL,
        )

        if not loop_match:
            # No loop — just execute non-DECLARE statements
            for stmt in self._split_statements(sql):
                stmt = stmt.strip()
                if not stmt or re.match(r'DECLARE\b', stmt, re.IGNORECASE):
                    continue
                # Handle SET var = expr
                set_match = re.match(
                    r'SET\s+(\w+)\s*=\s*(.+)', stmt, re.IGNORECASE
                )
                if set_match:
                    var_name = set_match.group(1)
                    expr = self._substitute_vars(set_match.group(2), variables)
                    try:
                        r = self._conn.execute(f"SELECT {expr}")
                        variables[var_name] = r.fetchone()[0]
                    except Exception:
                        pass
                    continue
                stmt = self._substitute_vars(stmt, variables)
                result = self._conn.execute(stmt)
                if result and result.description:
                    try:
                        total_rows += len(result.fetchall())
                    except duckdb.InvalidInputException:
                        pass
            return total_rows

        condition = loop_match.group(1)  # None for LOOP (unconditional)
        loop_body = loop_match.group(2)

        # Extract pre-loop and post-loop statements
        pre_loop = sql[:loop_match.start()]
        post_loop = sql[loop_match.end():]

        # Execute pre-loop (non-DECLARE) statements
        for stmt in self._split_statements(pre_loop):
            stmt = stmt.strip()
            if not stmt or re.match(r'DECLARE\b', stmt, re.IGNORECASE):
                continue
            stmt = self._substitute_vars(stmt, variables)
            result = self._conn.execute(stmt)
            if result and result.description:
                try:
                    total_rows += len(result.fetchall())
                except duckdb.InvalidInputException:
                    pass

        # Execute loop
        max_iterations = 100
        for _ in range(max_iterations):
            # Check condition
            if condition:
                cond_sql = self._substitute_vars(condition, variables)
                try:
                    result = self._conn.execute(f"SELECT {cond_sql}")
                    if not result.fetchone()[0]:
                        break
                except Exception:
                    break

            # Execute loop body
            should_leave = False
            for stmt in self._split_statements(loop_body):
                stmt = stmt.strip()
                if not stmt:
                    continue

                # Handle LEAVE
                if re.match(r'LEAVE\b', stmt, re.IGNORECASE):
                    should_leave = True
                    break

                # Handle SET var = (expr)
                set_match = re.match(
                    r'SET\s+(\w+)\s*=\s*(.+)', stmt, re.IGNORECASE
                )
                if set_match:
                    var_name = set_match.group(1)
                    expr = self._substitute_vars(set_match.group(2), variables)
                    try:
                        result = self._conn.execute(f"SELECT {expr}")
                        variables[var_name] = result.fetchone()[0]
                    except Exception:
                        pass
                    continue

                # Regular statement
                stmt = self._substitute_vars(stmt, variables)
                try:
                    result = self._conn.execute(stmt)
                    if result and result.description:
                        try:
                            total_rows += len(result.fetchall())
                        except duckdb.InvalidInputException:
                            pass
                except Exception as e:
                    logger.debug("BQ scripting loop error: %s", e)

            if should_leave:
                break

        # Execute post-loop statements
        for stmt in self._split_statements(post_loop):
            stmt = stmt.strip()
            if not stmt:
                continue
            stmt = self._substitute_vars(stmt, variables)
            try:
                result = self._conn.execute(stmt)
                if result and result.description:
                    try:
                        total_rows += len(result.fetchall())
                    except duckdb.InvalidInputException:
                        pass
            except Exception as e:
                logger.debug("BQ scripting post-loop error: %s", e)

        return total_rows

    @staticmethod
    def _substitute_vars(sql: str, variables: dict[str, object]) -> str:
        """Substitute variable references in SQL."""
        for var_name, value in variables.items():
            if value is None:
                replacement = "NULL"
            elif isinstance(value, str):
                replacement = f"'{value}'"
            else:
                replacement = str(value)
            sql = re.sub(rf'\b{var_name}\b', replacement, sql)
        return sql

    # ------------------------------------------------------------------
    # SQL adaptation
    # ------------------------------------------------------------------

    def _adapt_sql(self, sql: str) -> str:
        """Adapt BigQuery SQL to DuckDB-compatible SQL."""
        # Replace fully-qualified backtick-quoted table names with simple names
        # e.g. `proj.dataset.table` → table  (MUST run before backtick stripping)
        sql = re.sub(
            r'`([^`\s]+)\.([^`\s]+)\.([^`\s]+)`',
            lambda m: m.group(3),
            sql,
        )
        # Remove remaining backtick quoting (simple identifiers)
        sql = sql.replace("`", "")
        # CURRENT_TIMESTAMP() → current_timestamp (DuckDB form)
        sql = re.sub(r'CURRENT_TIMESTAMP\(\)', 'current_timestamp', sql)
        # ARRAY_AGG(col ORDER BY ... LIMIT n)[OFFSET(0)] → DuckDB equivalent
        # BQ: ARRAY_AGG(expr ORDER BY ... LIMIT 1)[OFFSET(0)]
        # DuckDB: (SELECT expr ORDER BY ... LIMIT 1) via FIRST/MIN or subquery
        sql = re.sub(
            r'ARRAY_AGG\((\w+)\s+ORDER\s+BY\s+([^)]+?)\s+LIMIT\s+1\)\[OFFSET\(0\)\]',
            r'(SELECT \1 FROM canonical_scores cs2 WHERE cs2.cluster_id = canonical_scores.cluster_id ORDER BY \2 LIMIT 1)',
            sql,
        )
        # APPROX_QUANTILES(col, n)[OFFSET(m)] → PERCENTILE_CONT approximation
        sql = re.sub(
            r'APPROX_QUANTILES\(([^,]+),\s*2\)\[OFFSET\(1\)\]',
            r'MEDIAN(\1)',
            sql,
        )
        # UNIX_MICROS(ts) → EPOCH_US(ts)
        sql = re.sub(r'UNIX_MICROS\(', 'EPOCH_US(', sql)
        # Strip BQ UDF dataset prefixes: {udf_dataset}.func(...) → func(...)
        sql = re.sub(r'\{udf_dataset\}\.', '', sql)
        # Strip BQ raw string prefix: r'pattern' → 'pattern'
        sql = re.sub(r"\br('(?:[^'\\]|\\.)*')", r'\1', sql)
        # Also strip resolved UDF dataset refs: `proj.dataset.func`(...) → func(...)
        # (already handled by backtick stripping above for 3-part names)
        # SPLIT → string_split (BQ SPLIT returns ARRAY<STRING>)
        sql = re.sub(r'\bSPLIT\(', 'string_split(', sql)
        # ARRAY_LENGTH → len
        sql = re.sub(r'\bARRAY_LENGTH\(', 'len(', sql)
        # REGEXP_CONTAINS → regexp_matches
        sql = re.sub(r'\bREGEXP_CONTAINS\(', 'regexp_matches(', sql)
        # Strip CLUSTER BY clause (DuckDB doesn't support it)
        sql = re.sub(r'\bCLUSTER\s+BY\s+[^\n]+', '', sql)
        # BQ type names → DuckDB equivalents
        sql = re.sub(r'\bAS\s+FLOAT64\b', 'AS DOUBLE', sql)
        sql = re.sub(r'\bAS\s+INT64\b', 'AS BIGINT', sql)
        sql = re.sub(r'\bAS\s+STRING\b', 'AS VARCHAR', sql)
        # [OFFSET(n)] → [n+1] (BQ 0-indexed → DuckDB 1-indexed)
        # Must run AFTER APPROX_QUANTILES and ARRAY_AGG[OFFSET] rewrites
        sql = re.sub(
            r'\[OFFSET\((\d+)\)\]',
            lambda m: f'[{int(m.group(1)) + 1}]',
            sql,
        )
        # [ORDINAL(n)] → [n] (both 1-indexed, just strip the wrapper)
        sql = re.sub(r'\[ORDINAL\((\d+)\)\]', r'[\1]', sql)
        # DATE_DIFF(d1, d2, UNIT) → date_diff('unit', d2, d1) (arg reorder + lowercase unit)
        sql = re.sub(
            r'\bDATE_DIFF\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*(\w+)\s*\)',
            lambda m: f"date_diff('{m.group(3).lower()}', {m.group(2).strip()}, {m.group(1).strip()})",
            sql,
        )
        # * EXCEPT(col) → * EXCLUDE(col) (DuckDB column exclusion syntax)
        sql = re.sub(r'\*\s+EXCEPT\s*\(', '* EXCLUDE(', sql)
        # SAFE_CAST(expr AS type) → TRY_CAST(expr AS type)
        sql = re.sub(r'\bSAFE_CAST\(', 'TRY_CAST(', sql)
        # ML.DISTANCE(a, b, 'COSINE') → (1.0 - list_cosine_similarity(a, b))
        sql = self._rewrite_ml_distance(sql)
        # BQ UNNEST patterns → DuckDB subquery wrappers
        sql = self._rewrite_unnest(sql)
        # QUALIFY is natively supported in DuckDB ≥0.8 — no rewrite needed
        return sql

    @staticmethod
    def _rewrite_unnest(sql: str) -> str:
        """Rewrite BQ UNNEST patterns to DuckDB-compatible syntax.

        BQ: FROM UNNEST(expr) AS alias [WITH OFFSET AS pos]
        DuckDB: FROM (SELECT UNNEST(expr) AS alias) _unnest_sub

        BQ: IN UNNEST(expr)
        DuckDB: = ANY(expr)
        """
        def find_matching_paren(s: str, start: int) -> int:
            depth = 0
            for i in range(start, len(s)):
                if s[i] == "(":
                    depth += 1
                elif s[i] == ")":
                    depth -= 1
                    if depth == 0:
                        return i
            return -1

        # Iterative: process one UNNEST at a time, re-scanning after each change
        max_iter = 20
        for _ in range(max_iter):
            m = re.search(r'\bUNNEST\(', sql, re.IGNORECASE)
            if not m:
                break

            unnest_start = m.start()
            paren_start = m.end() - 1
            paren_end = find_matching_paren(sql, paren_start)
            if paren_end == -1:
                break

            inner_expr = sql[paren_start + 1:paren_end]
            before = sql[:unnest_start]
            after = sql[paren_end + 1:]

            # Skip if preceded by SELECT (already rewritten)
            if re.search(r'\bSELECT\s+$', before, re.IGNORECASE):
                # Mark as processed by moving past it
                # Replace UNNEST with a temporary marker to skip it
                sql = before + "_UNNEST_DONE_(" + inner_expr + ")" + after
                continue

            # IN UNNEST(expr) → = ANY(expr)
            if re.search(r'\bIN\s*$', before, re.IGNORECASE):
                sql = (
                    re.sub(r'\bIN\s*$', '= ANY(', before, flags=re.IGNORECASE)
                    + inner_expr + ")"
                    + after
                )
                continue

            # FROM UNNEST(expr) [AS] alias [WITH OFFSET [AS] pos]
            alias_match = re.match(
                r'\s+(?:AS\s+)?(\w+)(\s+WITH\s+OFFSET\s+(?:AS\s+)?(\w+))?',
                after, re.IGNORECASE,
            )
            if alias_match:
                alias = alias_match.group(1)
                has_offset = alias_match.group(2) is not None
                remaining = after[alias_match.end():]

                from_match = re.search(r'\bFROM\s*$', before, re.IGNORECASE)
                if from_match:
                    sql = (
                        before[:from_match.start()]
                        + f"FROM (SELECT _UNNEST_DONE_({inner_expr}) AS {alias}) _usub"
                        + remaining
                    )
                else:
                    sql = (
                        before
                        + f"(SELECT _UNNEST_DONE_({inner_expr}) AS {alias}) _usub"
                        + remaining
                    )

                if has_offset:
                    offset_alias = alias_match.group(3) or "pos"
                    sql = re.sub(
                        rf'\s+ORDER\s+BY\s+{offset_alias}\b', '', sql
                    )
            else:
                # No alias — skip to avoid infinite loop
                sql = before + "_UNNEST_DONE_(" + inner_expr + ")" + after

        # Restore UNNEST from markers
        sql = sql.replace("_UNNEST_DONE_(", "UNNEST(")

        return sql

    @staticmethod
    def _rewrite_ml_distance(sql: str) -> str:
        """Rewrite ML.DISTANCE(a, b, 'COSINE') to DuckDB equivalent.

        Handles nested brackets/parens in arguments (e.g. array literals).
        """
        result = []
        i = 0
        marker = "ML.DISTANCE("
        while i < len(sql):
            pos = sql.upper().find(marker.upper(), i)
            if pos == -1:
                result.append(sql[i:])
                break
            result.append(sql[i:pos])
            # Parse args with bracket/paren awareness
            start = pos + len(marker)
            args = []
            depth = 0
            current: list[str] = []
            j = start
            while j < len(sql):
                ch = sql[j]
                if ch in ("(", "["):
                    depth += 1
                    current.append(ch)
                elif ch in (")", "]"):
                    if depth == 0:
                        # End of ML.DISTANCE(...)
                        arg = "".join(current).strip()
                        if arg:
                            args.append(arg)
                        j += 1
                        break
                    depth -= 1
                    current.append(ch)
                elif ch == "," and depth == 0:
                    args.append("".join(current).strip())
                    current = []
                else:
                    current.append(ch)
                j += 1

            if len(args) >= 3 and "COSINE" in args[2].upper():
                result.append(f"(1.0 - list_cosine_similarity({args[0]}, {args[1]}))")
            else:
                # Not a COSINE distance — keep original
                result.append(sql[pos:j])
            i = j
        return "".join(result)

    @staticmethod
    def _split_statements(sql: str) -> list[str]:
        """Split SQL script into individual statements."""
        # Simple split on semicolons not inside strings
        statements = []
        current = []
        in_string = False
        escape_next = False
        for char in sql:
            if escape_next:
                current.append(char)
                escape_next = False
                continue
            if char == "\\":
                escape_next = True
                current.append(char)
                continue
            if char == "'":
                in_string = not in_string
            if char == ";" and not in_string:
                stmt = "".join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(char)
        # Last statement (no trailing semicolon)
        stmt = "".join(current).strip()
        if stmt:
            statements.append(stmt)
        return statements

    @staticmethod
    def _local_table_name(fq_name: str) -> str:
        """Convert a fully-qualified BQ table name to a local DuckDB name."""
        # "project.dataset.table" → "table"
        parts = fq_name.replace("`", "").split(".")
        return parts[-1]
