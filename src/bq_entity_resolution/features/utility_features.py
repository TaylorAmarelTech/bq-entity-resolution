"""Generic / utility feature functions."""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.features.registry import register


@register("upper_trim")
def upper_trim(inputs: list[str], **_: Any) -> str:
    """Uppercase and trim whitespace."""
    return f"TRIM(UPPER({inputs[0]}))"


@register("lower_trim")
def lower_trim(inputs: list[str], **_: Any) -> str:
    """Lowercase and trim whitespace."""
    return f"TRIM(LOWER({inputs[0]}))"


@register("left")
def left_func(inputs: list[str], length: int = 5, **_: Any) -> str:
    """Extract leftmost N characters."""
    return f"LEFT({inputs[0]}, {length})"


@register("right")
def right_func(inputs: list[str], length: int = 4, **_: Any) -> str:
    """Extract rightmost N characters."""
    return f"RIGHT({inputs[0]}, {length})"


@register("coalesce")
def coalesce_func(inputs: list[str], **_: Any) -> str:
    """COALESCE multiple columns."""
    return f"COALESCE({', '.join(inputs)})"


@register("concat")
def concat_func(inputs: list[str], separator: str = " ", **_: Any) -> str:
    """Concatenate columns with separator."""
    parts = f", '{separator}', ".join(
        f"COALESCE(CAST({c} AS STRING), '')" for c in inputs
    )
    return f"CONCAT({parts})"


@register("nullif_empty")
def nullif_empty(inputs: list[str], **_: Any) -> str:
    """Convert empty strings to NULL."""
    return f"NULLIF(TRIM({inputs[0]}), '')"


@register("is_not_null")
def is_not_null(inputs: list[str], **_: Any) -> str:
    """Returns 1 if column is not null, 0 otherwise.

    OUTPUT TYPE: INT64 (0 or 1) — ideal for match flags and filtering.
    Use to track whether an enrichment join found a match (e.g., whether
    the Census geocoder returned a result for this address).
    """
    return f"CASE WHEN {inputs[0]} IS NOT NULL THEN 1 ELSE 0 END"


@register("char_length")
def char_length(inputs: list[str], **_: Any) -> str:
    """String length.

    OUTPUT TYPE: INT64 — cheap to compute and compare.
    """
    return f"CHAR_LENGTH({inputs[0]})"


@register("soundex")
def soundex(inputs: list[str], **_: Any) -> str:
    """Soundex phonetic encoding.

    OUTPUT TYPE: STRING (4 chars, e.g. 'S530')
    PERF: Returns STRING — for blocking, consider wrapping in FARM_FINGERPRINT
    to get INT64: FARM_FINGERPRINT(SOUNDEX(col)) for ~3-5x faster equi-joins.
    As a comparison feature this is fine as-is.
    """
    return f"SOUNDEX({inputs[0]})"
