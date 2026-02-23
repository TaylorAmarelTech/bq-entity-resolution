"""
Composite and embedding comparison functions.

Vector/embedding similarity and multi-field composite comparisons.
"""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.matching.comparisons import register


# ---------------------------------------------------------------------------
# Embedding / vector similarity
# ---------------------------------------------------------------------------
# PERF: ML.DISTANCE is the most expensive comparison. It performs dot product
# and norm calculations on FLOAT64 arrays (typically 768-1536 dimensions).
# Use ONLY when embedding-based matching is required, and ONLY after blocking
# has reduced candidates to a manageable count (< 1M pairs recommended).
# For large-scale embedding matching, prefer LSH bucket blocking (INT64
# bucket keys via FARM_FINGERPRINT) to avoid computing distance on all pairs.


@register("cosine_similarity")
def cosine_similarity(
    left: str, right: str, min_similarity: float = 0.85, **_: Any
) -> str:
    """Cosine similarity >= threshold using ML.DISTANCE.

    COST: 50 -- most expensive comparison. Use LSH blocking to pre-filter.
    """
    # ML.DISTANCE returns distance (1-similarity for cosine)
    max_distance = 1.0 - min_similarity
    return (
        f"(ML.DISTANCE(l.{left}, r.{right}, 'COSINE') <= {max_distance} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("cosine_similarity_score")
def cosine_similarity_score(left: str, right: str, **_: Any) -> str:
    """Cosine similarity score (1 - distance)."""
    return (
        f"CASE WHEN l.{left} IS NOT NULL AND r.{right} IS NOT NULL "
        f"THEN 1.0 - ML.DISTANCE(l.{left}, r.{right}, 'COSINE') "
        f"ELSE 0.0 END"
    )
