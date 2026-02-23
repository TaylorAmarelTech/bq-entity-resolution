"""
Numeric comparison functions.

Comparisons for numeric/continuous values (absolute difference, tolerance).
"""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.matching.comparisons import register


# ---------------------------------------------------------------------------
# Numeric comparisons
# ---------------------------------------------------------------------------
# PERF: These are the cheapest comparisons after exact-match on INT64.
# Numeric columns are stored as fixed-width INT64/FLOAT64. ABS(a - b)
# is a single arithmetic operation — no string allocation or byte scanning.


@register("numeric_within")
def numeric_within(left: str, right: str, tolerance: float = 0, **_: Any) -> str:
    """Numeric values within tolerance.

    COST: 1 -- arithmetic on fixed-width numeric types. Very fast.
    """
    return (
        f"(ABS(CAST(l.{left} AS FLOAT64) - CAST(r.{right} AS FLOAT64)) <= {tolerance} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )
