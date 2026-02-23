"""
BigQuery client wrapper with retry logic, structured logging, and dry-run support.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from google.api_core.exceptions import BadRequest, InternalServerError, ServiceUnavailable, TooManyRequests
from google.cloud import bigquery

from bq_entity_resolution.exceptions import SQLExecutionError

logger = logging.getLogger(__name__)

RETRYABLE_ERRORS = (ServiceUnavailable, InternalServerError, TooManyRequests)
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5


@dataclass
class QueryResult:
    """Result metadata from a BigQuery query execution."""

    job_id: str = ""
    total_bytes_processed: int = 0
    rows_affected: int = 0
    bytes_billed: int = 0
    duration_seconds: float = 0.0
    slot_milliseconds: int = 0
    rows: list[dict] = field(default_factory=list)


class BigQueryClient:
    """
    Wrapper around google.cloud.bigquery.Client.

    Features:
    - Automatic retries for transient errors
    - Job labeling for cost tracking
    - Structured logging of query metrics
    - Dry-run support for cost estimation
    """

    def __init__(
        self,
        project: str,
        location: str = "US",
        dry_run: bool = False,
        max_bytes_billed: int | None = None,
    ):
        self.project = project
        self.location = location
        self.dry_run = dry_run
        self.max_bytes_billed = max_bytes_billed
        self._client = bigquery.Client(project=project, location=location)

    def execute(
        self,
        sql: str,
        job_label: str = "",
        timeout: int = 600,
    ) -> QueryResult:
        """Execute a SQL statement and return result metadata."""
        job_config = bigquery.QueryJobConfig(
            labels={"pipeline_step": job_label[:63]} if job_label else {},
            dry_run=self.dry_run,
        )
        if self.max_bytes_billed is not None:
            job_config.maximum_bytes_billed = self.max_bytes_billed

        start = time.monotonic()
        attempt = 0

        while attempt <= MAX_RETRIES:
            try:
                logger.debug("Executing SQL (label=%s): %.200s...", job_label, sql)
                job = self._client.query(sql, job_config=job_config)

                if self.dry_run:
                    return QueryResult(
                        job_id="dry_run",
                        total_bytes_processed=job.total_bytes_processed or 0,
                    )

                result = job.result(timeout=timeout)
                duration = time.monotonic() - start

                qr = QueryResult(
                    job_id=job.job_id,
                    total_bytes_processed=job.total_bytes_processed or 0,
                    rows_affected=result.total_rows if result else 0,
                    bytes_billed=job.total_bytes_billed or 0,
                    duration_seconds=duration,
                    slot_milliseconds=getattr(job, "slot_millis", 0) or 0,
                )

                logger.info(
                    "Query complete: job=%s duration=%.1fs bytes_billed=%d label=%s",
                    job.job_id,
                    duration,
                    qr.bytes_billed,
                    job_label,
                )
                return qr

            except RETRYABLE_ERRORS as e:
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise SQLExecutionError(
                        f"Query failed after {MAX_RETRIES} retries: {e}",
                        sql=sql,
                    ) from e
                wait = RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Retryable error (attempt %d/%d), waiting %ds: %s",
                    attempt,
                    MAX_RETRIES,
                    wait,
                    e,
                )
                time.sleep(wait)

            except BadRequest as e:
                logger.error("SQL error in %s: %s", job_label, e)
                logger.error("SQL:\n%s", sql)
                raise SQLExecutionError(
                    f"SQL syntax/semantic error: {e}",
                    sql=sql,
                ) from e

        # Should not reach here, but just in case
        raise SQLExecutionError("Query failed: exceeded retry logic", sql=sql)

    def execute_and_fetch(
        self,
        sql: str,
        job_label: str = "",
        timeout: int = 600,
    ) -> list[dict]:
        """Execute SQL and return rows as list of dicts.

        Includes retry logic for transient errors matching ``execute()``.
        """
        job_config = bigquery.QueryJobConfig(
            labels={"pipeline_step": job_label[:63]} if job_label else {},
        )

        attempt = 0
        while attempt <= MAX_RETRIES:
            try:
                job = self._client.query(sql, job_config=job_config)
                return [dict(row) for row in job.result(timeout=timeout)]

            except RETRYABLE_ERRORS as e:
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise SQLExecutionError(
                        f"Fetch query failed after {MAX_RETRIES} retries: {e}",
                        sql=sql,
                    ) from e
                wait = RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Retryable error in fetch (attempt %d/%d), waiting %ds: %s",
                    attempt,
                    MAX_RETRIES,
                    wait,
                    e,
                )
                time.sleep(wait)

            except BadRequest as e:
                logger.error("SQL error in fetch %s: %s", job_label, e)
                raise SQLExecutionError(
                    f"SQL syntax/semantic error: {e}",
                    sql=sql,
                ) from e

        raise SQLExecutionError("Fetch query failed: exceeded retry logic", sql=sql)

    def execute_script_and_fetch(
        self,
        sql: str,
        job_label: str = "",
        timeout: int = 600,
    ) -> list[dict]:
        """Execute a multi-statement SQL script and return the final result set as list of dicts.

        BQ Python client handles scripting transparently — ``job.result()``
        returns the result of the last SELECT in the script.  Includes the
        same retry logic as ``execute()``.
        """
        job_config = bigquery.QueryJobConfig(
            labels={"pipeline_step": job_label[:63]} if job_label else {},
        )

        attempt = 0
        while attempt <= MAX_RETRIES:
            try:
                logger.debug(
                    "Executing script+fetch (label=%s): %.200s...", job_label, sql
                )
                job = self._client.query(sql, job_config=job_config)
                rows = [dict(row) for row in job.result(timeout=timeout)]
                logger.info(
                    "Script+fetch complete: job=%s rows=%d label=%s",
                    job.job_id,
                    len(rows),
                    job_label,
                )
                return rows

            except RETRYABLE_ERRORS as e:
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise SQLExecutionError(
                        f"Script query failed after {MAX_RETRIES} retries: {e}",
                        sql=sql,
                    ) from e
                wait = RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Retryable error (attempt %d/%d), waiting %ds: %s",
                    attempt,
                    MAX_RETRIES,
                    wait,
                    e,
                )
                time.sleep(wait)

            except BadRequest as e:
                logger.error("Script SQL error in %s: %s", job_label, e)
                raise SQLExecutionError(
                    f"Script SQL error: {e}",
                    sql=sql,
                ) from e

        raise SQLExecutionError("Script query failed: exceeded retry logic", sql=sql)

    def table_exists(self, table_ref: str) -> bool:
        """Check if a table exists."""
        try:
            self._client.get_table(table_ref)
            return True
        except Exception:
            return False
