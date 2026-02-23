"""
Comparison function registry.

Maps comparison method names to BigQuery SQL expression generators.
Each function generates a boolean SQL expression comparing left and right columns.

Usage in YAML:
  comparisons:
    - left: "first_name_clean"
      right: "first_name_clean"
      method: "levenshtein"
      params: {max_distance: 2}
"""

from __future__ import annotations

from typing import Any, Callable

ComparisonFunction = Callable[..., str]

COMPARISON_FUNCTIONS: dict[str, ComparisonFunction] = {}

# Relative cost of each comparison method (lower = cheaper).
# Used to sort comparisons so cheap ones execute first in SQL.
COMPARISON_COSTS: dict[str, int] = {
    # Tier 1: O(1) integer/hash/null comparisons
    "exact": 1,
    "exact_or_null": 1,
    "different": 1,
    "null_either": 1,
    "numeric_within": 1,
    "date_within_days": 1,
    "exact_case_insensitive": 2,
    "length_mismatch": 2,
    # Tier 2: simple string ops
    "soundex_match": 3,
    "starts_with": 5,
    "contains": 5,
    "abbreviation_match": 5,
    # Tier 3: O(n) edit distance
    "levenshtein": 10,
    "levenshtein_normalized": 12,
    "levenshtein_score": 12,
    # Tier 4: UDF calls (JS execution overhead)
    "metaphone_match": 15,
    "double_metaphone_match": 15,
    "initials_match": 15,
    "jaro_winkler": 20,
    "jaro_winkler_score": 20,
    # Tier 5: complex subqueries / ML / geo
    "geo_within_km": 25,
    "geo_distance_score": 25,
    "token_set_match": 30,
    "token_set_score": 30,
    "cosine_similarity": 50,
    "cosine_similarity_score": 50,
}


def register(name: str) -> Callable[[ComparisonFunction], ComparisonFunction]:
    """Decorator to register a comparison function."""

    def decorator(func: ComparisonFunction) -> ComparisonFunction:
        COMPARISON_FUNCTIONS[name] = func
        return func

    return decorator


# ---------------------------------------------------------------------------
# Exact matching
# ---------------------------------------------------------------------------


@register("exact")
def exact(left: str, right: str, **_: Any) -> str:
    """Exact equality (both non-null)."""
    return f"(l.{left} = r.{right} AND l.{left} IS NOT NULL)"


@register("exact_case_insensitive")
def exact_ci(left: str, right: str, **_: Any) -> str:
    """Case-insensitive exact equality."""
    return f"(UPPER(l.{left}) = UPPER(r.{right}) AND l.{left} IS NOT NULL)"


@register("exact_or_null")
def exact_or_null(left: str, right: str, **_: Any) -> str:
    """Match if equal or if either is null (permissive)."""
    return f"(l.{left} = r.{right} OR l.{left} IS NULL OR r.{right} IS NULL)"


# ---------------------------------------------------------------------------
# Edit distance
# ---------------------------------------------------------------------------


@register("levenshtein")
def levenshtein(left: str, right: str, max_distance: int = 2, **_: Any) -> str:
    """Edit distance within threshold (BigQuery EDIT_DISTANCE)."""
    return (
        f"(EDIT_DISTANCE(l.{left}, r.{right}) <= {max_distance} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("levenshtein_normalized")
def levenshtein_normalized(
    left: str, right: str, threshold: float = 0.8, **_: Any
) -> str:
    """Normalized edit distance similarity >= threshold."""
    return (
        f"(1.0 - SAFE_DIVIDE("
        f"CAST(EDIT_DISTANCE(l.{left}, r.{right}) AS FLOAT64), "
        f"GREATEST(CHAR_LENGTH(l.{left}), CHAR_LENGTH(r.{right}))"
        f") >= {threshold} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("levenshtein_score")
def levenshtein_score(left: str, right: str, **_: Any) -> str:
    """Returns normalized edit distance similarity as a score (not boolean)."""
    return (
        f"CASE WHEN l.{left} IS NOT NULL AND r.{right} IS NOT NULL "
        f"THEN 1.0 - SAFE_DIVIDE("
        f"CAST(EDIT_DISTANCE(l.{left}, r.{right}) AS FLOAT64), "
        f"GREATEST(CHAR_LENGTH(l.{left}), CHAR_LENGTH(r.{right}))) "
        f"ELSE 0.0 END"
    )


# ---------------------------------------------------------------------------
# Jaro-Winkler
# ---------------------------------------------------------------------------


# UDF dataset placeholder — replaced at SQL generation time by the matching engine
# when it resolves the {udf_dataset} variable from config
_UDF_DATASET_PLACEHOLDER = "{udf_dataset}"


@register("jaro_winkler")
def jaro_winkler(
    left: str, right: str, threshold: float = 0.85, udf_dataset: str = "", **_: Any
) -> str:
    """Jaro-Winkler similarity >= threshold (BigQuery JS UDF)."""
    ds = udf_dataset or _UDF_DATASET_PLACEHOLDER
    return (
        f"(`{ds}.jaro_winkler`(l.{left}, r.{right}) >= {threshold} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("jaro_winkler_score")
def jaro_winkler_score(
    left: str, right: str, udf_dataset: str = "", **_: Any
) -> str:
    """Jaro-Winkler similarity score (BigQuery JS UDF)."""
    ds = udf_dataset or _UDF_DATASET_PLACEHOLDER
    return (
        f"CASE WHEN l.{left} IS NOT NULL AND r.{right} IS NOT NULL "
        f"THEN `{ds}.jaro_winkler`(l.{left}, r.{right}) "
        f"ELSE 0.0 END"
    )


# ---------------------------------------------------------------------------
# Phonetic matching
# ---------------------------------------------------------------------------


@register("soundex_match")
def soundex_match(left: str, right: str, **_: Any) -> str:
    """Soundex codes match."""
    return (
        f"(SOUNDEX(l.{left}) = SOUNDEX(r.{right}) "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


# ---------------------------------------------------------------------------
# Embedding / vector similarity
# ---------------------------------------------------------------------------


@register("cosine_similarity")
def cosine_similarity(
    left: str, right: str, min_similarity: float = 0.85, **_: Any
) -> str:
    """Cosine similarity >= threshold using ML.DISTANCE."""
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


# ---------------------------------------------------------------------------
# Numeric / date comparisons
# ---------------------------------------------------------------------------


@register("numeric_within")
def numeric_within(left: str, right: str, tolerance: float = 0, **_: Any) -> str:
    """Numeric values within tolerance."""
    return (
        f"(ABS(CAST(l.{left} AS FLOAT64) - CAST(r.{right} AS FLOAT64)) <= {tolerance} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("date_within_days")
def date_within_days(left: str, right: str, days: int = 0, **_: Any) -> str:
    """Date values within N days."""
    return (
        f"(ABS(DATE_DIFF(l.{left}, r.{right}, DAY)) <= {days} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


# ---------------------------------------------------------------------------
# String containment
# ---------------------------------------------------------------------------


@register("contains")
def contains(left: str, right: str, **_: Any) -> str:
    """Left value contains right value (or vice versa)."""
    return (
        f"((STRPOS(l.{left}, r.{right}) > 0 OR STRPOS(r.{right}, l.{left}) > 0) "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("starts_with")
def starts_with(left: str, right: str, **_: Any) -> str:
    """Left value starts with right value."""
    return (
        f"(STARTS_WITH(l.{left}, r.{right}) "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


# ---------------------------------------------------------------------------
# Hard negative helpers
# ---------------------------------------------------------------------------


@register("different")
def different(left: str, right: str, **_: Any) -> str:
    """Returns TRUE when values differ (both non-null). For hard negatives."""
    return (
        f"(l.{left} != r.{right} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("null_either")
def null_either(left: str, right: str, **_: Any) -> str:
    """Returns TRUE when either value is null."""
    return f"(l.{left} IS NULL OR r.{right} IS NULL)"


@register("length_mismatch")
def length_mismatch(
    left: str, right: str, max_diff: int = 5, **_: Any
) -> str:
    """Returns TRUE when string lengths differ by more than threshold."""
    return (
        f"(ABS(CHAR_LENGTH(l.{left}) - CHAR_LENGTH(r.{right})) > {max_diff} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


# ---------------------------------------------------------------------------
# Token-based matching (handles transpositions at the word level)
# ---------------------------------------------------------------------------


@register("token_set_match")
def token_set_match(left: str, right: str, min_overlap: float = 0.5, **_: Any) -> str:
    """Token overlap ratio >= threshold. Handles name word transpositions.

    Computes |intersection| / |union| of word tokens (Jaccard similarity).
    Uses inclusion-exclusion (|A| + |B| - |A∩B|) to avoid redundant SPLIT calls.
    """
    return (
        f"(SAFE_DIVIDE("
        f"  (SELECT COUNTIF(w IN UNNEST(SPLIT(UPPER(r.{right}), ' '))) "
        f"   FROM UNNEST(SPLIT(UPPER(l.{left}), ' ')) AS w),"
        f"  ARRAY_LENGTH(SPLIT(UPPER(l.{left}), ' ')) + "
        f"  ARRAY_LENGTH(SPLIT(UPPER(r.{right}), ' ')) - "
        f"  (SELECT COUNTIF(w IN UNNEST(SPLIT(UPPER(r.{right}), ' '))) "
        f"   FROM UNNEST(SPLIT(UPPER(l.{left}), ' ')) AS w)"
        f") >= {min_overlap} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("token_set_score")
def token_set_score(left: str, right: str, **_: Any) -> str:
    """Token overlap ratio as a score (Jaccard similarity of word tokens).

    Uses inclusion-exclusion (|A| + |B| - |A∩B|) to avoid redundant SPLIT calls.
    """
    return (
        f"CASE WHEN l.{left} IS NOT NULL AND r.{right} IS NOT NULL THEN "
        f"SAFE_DIVIDE("
        f"  (SELECT COUNTIF(w IN UNNEST(SPLIT(UPPER(r.{right}), ' '))) "
        f"   FROM UNNEST(SPLIT(UPPER(l.{left}), ' ')) AS w),"
        f"  ARRAY_LENGTH(SPLIT(UPPER(l.{left}), ' ')) + "
        f"  ARRAY_LENGTH(SPLIT(UPPER(r.{right}), ' ')) - "
        f"  (SELECT COUNTIF(w IN UNNEST(SPLIT(UPPER(r.{right}), ' '))) "
        f"   FROM UNNEST(SPLIT(UPPER(l.{left}), ' ')) AS w)"
        f") ELSE 0.0 END"
    )


@register("initials_match")
def initials_match(left: str, right: str, **_: Any) -> str:
    """Check if initials of two names match (e.g., 'J.S.' matches 'John Smith')."""
    return (
        f"((SELECT STRING_AGG(LEFT(w, 1), '' ORDER BY pos) "
        f"  FROM UNNEST(SPLIT(UPPER(l.{left}), ' ')) w WITH OFFSET pos) = "
        f" (SELECT STRING_AGG(LEFT(w, 1), '' ORDER BY pos) "
        f"  FROM UNNEST(SPLIT(UPPER(r.{right}), ' ')) w WITH OFFSET pos) "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("abbreviation_match")
def abbreviation_match(left: str, right: str, **_: Any) -> str:
    """Match where one value is an abbreviation of another.

    E.g., 'J' matches 'JAMES', 'ROBT' matches 'ROBERT'.
    """
    return (
        f"((STARTS_WITH(UPPER(l.{left}), UPPER(r.{right})) "
        f"  OR STARTS_WITH(UPPER(r.{right}), UPPER(l.{left}))) "
        f"AND CHAR_LENGTH(LEAST(l.{left}, r.{right})) >= 1 "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


# ---------------------------------------------------------------------------
# Geo-spatial comparisons
# ---------------------------------------------------------------------------


@register("geo_within_km")
def geo_within_km(
    left: str,
    right: str,
    max_km: float = 10.0,
    left_lon: str = "",
    right_lon: str = "",
    **_: Any,
) -> str:
    """Boolean: two lat/lon points are within max_km kilometers.

    left/right are latitude columns; left_lon/right_lon are longitude columns.
    Uses BigQuery ST_DISTANCE for geodesic accuracy.
    """
    return (
        f"(ST_DISTANCE("
        f"ST_GEOGPOINT(l.{left_lon}, l.{left}), "
        f"ST_GEOGPOINT(r.{right_lon}, r.{right})"
        f") / 1000.0 <= {max_km} "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL "
        f"AND l.{left_lon} IS NOT NULL AND r.{right_lon} IS NOT NULL)"
    )


@register("geo_distance_score")
def geo_distance_score(
    left: str,
    right: str,
    max_km: float = 50.0,
    left_lon: str = "",
    right_lon: str = "",
    **_: Any,
) -> str:
    """Proximity score 0.0–1.0 based on distance between two lat/lon points.

    Score = 1 - (distance_km / max_km), clamped to [0, 1].
    left/right are latitude columns; left_lon/right_lon are longitude columns.
    """
    return (
        f"CASE WHEN l.{left} IS NOT NULL AND r.{right} IS NOT NULL "
        f"AND l.{left_lon} IS NOT NULL AND r.{right_lon} IS NOT NULL "
        f"THEN GREATEST(0.0, 1.0 - ST_DISTANCE("
        f"ST_GEOGPOINT(l.{left_lon}, l.{left}), "
        f"ST_GEOGPOINT(r.{right_lon}, r.{right})"
        f") / 1000.0 / {max_km}) "
        f"ELSE 0.0 END"
    )


# ---------------------------------------------------------------------------
# Phonetic comparisons (UDF-based)
# ---------------------------------------------------------------------------


@register("metaphone_match")
def metaphone_match(
    left: str, right: str, udf_dataset: str = "", **_: Any
) -> str:
    """Metaphone codes match (BigQuery JS UDF).

    Requires a ``metaphone(STRING) -> STRING`` UDF in udf_dataset.
    """
    ds = udf_dataset or _UDF_DATASET_PLACEHOLDER
    return (
        f"(`{ds}.metaphone`(l.{left}) = `{ds}.metaphone`(r.{right}) "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )


@register("double_metaphone_match")
def double_metaphone_match(
    left: str, right: str, udf_dataset: str = "", **_: Any
) -> str:
    """Double Metaphone match: primary or alternate codes overlap.

    Requires ``double_metaphone_primary(STRING) -> STRING`` and
    ``double_metaphone_alternate(STRING) -> STRING`` UDFs in udf_dataset.
    """
    ds = udf_dataset or _UDF_DATASET_PLACEHOLDER
    return (
        f"((`{ds}.double_metaphone_primary`(l.{left}) = "
        f"`{ds}.double_metaphone_primary`(r.{right}) "
        f"OR `{ds}.double_metaphone_primary`(l.{left}) = "
        f"`{ds}.double_metaphone_alternate`(r.{right}) "
        f"OR `{ds}.double_metaphone_alternate`(l.{left}) = "
        f"`{ds}.double_metaphone_primary`(r.{right})) "
        f"AND l.{left} IS NOT NULL AND r.{right} IS NOT NULL)"
    )
