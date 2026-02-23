"""
Date comparison functions.

Comparisons for date and temporal values.
"""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.matching.comparisons import register


# ---------------------------------------------------------------------------
# Date comparisons
# ---------------------------------------------------------------------------
# PERF: Date columns are stored as INT32 internally in BigQuery.
# DATE_DIFF is a single arithmetic operation — very fast.


@register("date_within_days")
def date_within_days(left: str, right: str, days: int = 0, **_: Any) -> str:
    """Date values within N days."""
    return (
        f"(ABS(DATE_DIFF(l.{left}, r.{right}, DAY)) <= {days} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )
