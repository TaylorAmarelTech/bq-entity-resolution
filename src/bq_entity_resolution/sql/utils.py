"""Shared SQL utility functions.

Common SQL escaping and identifier formatting used across builders.
"""

from __future__ import annotations

from bq_entity_resolution.constants import BQ_RESERVED_WORDS


def sql_escape(value: object) -> str:
    """Escape single quotes for SQL string literals."""
    return str(value).replace("'", "''")


def bq_escape(identifier: str) -> str:
    """Backtick-escape a BigQuery identifier if reserved or contains special chars."""
    if identifier.upper() in BQ_RESERVED_WORDS or "." in identifier or "-" in identifier:
        return f"`{identifier}`"
    return identifier
