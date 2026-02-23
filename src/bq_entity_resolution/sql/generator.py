"""
Central SQL generator using Jinja2 templates.

Renders BigQuery SQL from templates with config-derived parameters.
Templates handle SQL structure; Python handles all logic and data.
"""

from __future__ import annotations

import logging
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from bq_entity_resolution.constants import BQ_RESERVED_WORDS
from bq_entity_resolution.exceptions import SQLGenerationError

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"

# Required parameters per template — validated before render for clear errors
TEMPLATE_REQUIRED_PARAMS: dict[str, list[str]] = {
    "staging/incremental_load.sql.j2": ["target_table", "source"],
    "features/all_features.sql.j2": [
        "target_table", "source_tables", "feature_expressions",
    ],
    "blocking/multi_path_candidates.sql.j2": [
        "target_table", "source_table", "blocking_paths",
    ],
    "matching/tier_comparisons.sql.j2": [
        "matches_table", "candidates_table", "comparisons",
    ],
    "matching/tier_fellegi_sunter.sql.j2": [
        "matches_table", "candidates_table", "comparisons",
    ],
    "watermark/read_watermark.sql.j2": ["table", "source_name"],
    "watermark/update_watermark.sql.j2": [
        "table", "source_name", "cursors", "run_id", "now",
    ],
}


class SQLGenerator:
    """Renders Jinja2 SQL templates with pipeline parameters."""

    def __init__(self, templates_dir: Path | None = None):
        tdir = templates_dir or TEMPLATES_DIR
        self.env = Environment(
            loader=FileSystemLoader(str(tdir)),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )
        # Custom filters
        self.env.filters["bq_escape"] = bq_escape
        self.env.filters["farm_fp"] = farm_fingerprint_expr
        self.env.filters["coalesce_cast"] = coalesce_cast_expr
        self.env.filters["format_watermark_value"] = format_watermark_value
        self.env.filters["sql_escape"] = sql_escape

    def render(self, template_name: str, **kwargs: object) -> str:
        """Render a SQL template. Raises SQLGenerationError on failure."""
        # Validate required parameters before rendering
        required = TEMPLATE_REQUIRED_PARAMS.get(template_name, [])
        missing = [p for p in required if p not in kwargs]
        if missing:
            raise SQLGenerationError(
                f"Template '{template_name}' missing required parameters: {missing}"
            )

        try:
            template = self.env.get_template(template_name)
            sql = template.render(**kwargs)
            logger.debug("Rendered %s (%d chars)", template_name, len(sql))
            return sql
        except Exception as exc:
            raise SQLGenerationError(
                f"Failed to render template '{template_name}': {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Jinja2 filter functions
# ---------------------------------------------------------------------------

def bq_escape(identifier: str) -> str:
    """Backtick-escape a BigQuery identifier if it conflicts with reserved words."""
    if identifier.upper() in BQ_RESERVED_WORDS or "." in identifier or "-" in identifier:
        return f"`{identifier}`"
    return identifier


def farm_fingerprint_expr(columns: list[str] | str) -> str:
    """Generate a FARM_FINGERPRINT expression for one or more columns."""
    if isinstance(columns, str):
        columns = [columns]
    if len(columns) == 1:
        return f"FARM_FINGERPRINT(CAST({columns[0]} AS STRING))"
    parts = ", '||', ".join(
        f"COALESCE(CAST({c} AS STRING), '')" for c in columns
    )
    return f"FARM_FINGERPRINT(CONCAT({parts}))"


def coalesce_cast_expr(column: str) -> str:
    """COALESCE + CAST for safe string concatenation."""
    return f"COALESCE(CAST({column} AS STRING), '')"


def format_watermark_value(value: object) -> str:
    """Format a watermark value for SQL injection into a WHERE clause.

    Handles TIMESTAMP strings, numeric types, and plain strings.
    """
    if value is None:
        return "NULL"

    from datetime import datetime

    if isinstance(value, datetime):
        return f"TIMESTAMP('{value.isoformat()}')"

    s = str(value)

    # ISO timestamp patterns (contains T or space between date and time)
    if _looks_like_timestamp(s):
        return f"TIMESTAMP('{s}')"

    # Numeric (int or float)
    try:
        float(s)
        return s
    except ValueError:
        pass

    # Default: string literal
    # Escape single quotes to prevent SQL injection
    escaped = s.replace("'", "''")
    return f"'{escaped}'"


def sql_escape(value: object) -> str:
    """Escape a value for safe inclusion in a SQL string literal.

    Doubles single quotes per standard SQL quoting rules (BigQuery).
    """
    return str(value).replace("'", "''")


def _looks_like_timestamp(s: str) -> bool:
    """Heuristic check if a string looks like a timestamp."""
    import re
    return bool(re.match(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", s))
