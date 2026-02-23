"""Shared SQL utility functions.

Common SQL escaping and identifier formatting used across builders.
"""

from __future__ import annotations

import re

from bq_entity_resolution.constants import BQ_RESERVED_WORDS

# Valid BQ identifier: letters, digits, underscores only
_VALID_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# Valid table reference: project.dataset.table with optional hyphens in project
_VALID_TABLE_REF = re.compile(r'^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$')


def sql_escape(value: object) -> str:
    """Escape single quotes for SQL string literals."""
    return str(value).replace("'", "''")


def bq_escape(identifier: str) -> str:
    """Backtick-escape a BigQuery identifier if reserved or contains special chars."""
    if identifier.upper() in BQ_RESERVED_WORDS or "." in identifier or "-" in identifier:
        return f"`{identifier}`"
    return identifier


def validate_identifier(name: str, context: str = "identifier") -> str:
    """Validate that a string is a safe SQL identifier.

    Rejects names containing SQL injection characters (quotes, semicolons,
    comments, etc.). Returns the name unchanged if valid.

    Raises ValueError if the name contains unsafe characters.
    """
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(
            f"Invalid SQL {context}: {name!r}. "
            f"Identifiers must contain only letters, digits, and underscores."
        )
    return name


def validate_table_ref(name: str) -> str:
    """Validate that a string is a safe fully-qualified table reference.

    Expected format: project.dataset.table or `project.dataset.table`.

    Raises ValueError if the format is invalid.
    """
    # Strip backtick quoting
    clean = name.strip('`')
    if not _VALID_TABLE_REF.match(clean):
        raise ValueError(
            f"Invalid table reference: {name!r}. "
            f"Expected format: project.dataset.table"
        )
    return name
