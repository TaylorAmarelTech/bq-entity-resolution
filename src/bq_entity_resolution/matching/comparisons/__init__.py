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

BigQuery Comparison Performance Notes
======================================
Comparison functions are evaluated PER CANDIDATE PAIR in the scoring stage.
If blocking produces 10M candidate pairs, each comparison runs 10M times.
The cost difference between comparison methods is therefore amplified by
the number of candidates:

    Cost tier 1  (INT64 equality):          ~0.001 slot-seconds per 1M pairs
    Cost tier 2  (short STRING equality):   ~0.003 slot-seconds per 1M pairs
    Cost tier 3  (EDIT_DISTANCE):           ~0.05  slot-seconds per 1M pairs
    Cost tier 4  (JS UDF jaro_winkler):     ~0.5   slot-seconds per 1M pairs
    Cost tier 5  (ML.DISTANCE cosine):      ~1.0   slot-seconds per 1M pairs

Key optimization: When columns being compared are INT64 (e.g., pre-computed
FARM_FINGERPRINT features), "exact" comparisons use INT64 = INT64 which is
a single 8-byte comparison. When columns are STRING, BQ must compare
byte-by-byte. For high-volume matching, pre-compute fingerprints of
cleaned values and do INT64 exact-match before expensive fuzzy comparisons.

The COMPARISON_COSTS dict below ranks methods by relative cost. SQL builders
should order comparisons cheapest-first so BigQuery's short-circuit
evaluation can skip expensive comparisons when cheap ones already fail.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)

ComparisonFunction = Callable[..., str]

COMPARISON_FUNCTIONS: dict[str, ComparisonFunction] = {}

_plugins_loaded = False

# Relative cost of each comparison method (lower = cheaper).
# SQL builders should sort comparisons by this cost so that BigQuery
# evaluates cheap comparisons first. In CASE WHEN chains, BQ can
# short-circuit: if an early cheap check fails, expensive checks
# are never evaluated for that pair.
#
# IMPORTANT: The actual cost depends on the column TYPE being compared:
#   - "exact" on INT64 columns (e.g. fp_ fingerprints):  cost ~1
#   - "exact" on short STRING columns (e.g. state):       cost ~2
#   - "exact" on long STRING columns (e.g. full address): cost ~5
# The costs below assume typical column types. Pre-computing
# FARM_FINGERPRINT features effectively moves STRING comparisons
# from cost tier 2 down to cost tier 1.
COMPARISON_COSTS: dict[str, int] = {
    # Tier 1: O(1) integer/hash/null comparisons — 8-byte fixed-width
    # These are essentially free. INT64 = INT64 is a single CPU instruction.
    "exact": 1,
    "exact_or_null": 1,
    "different": 1,
    "null_either": 1,
    "numeric_within": 1,       # ABS(INT64 - INT64) — fast arithmetic
    "date_within_days": 1,     # DATE_DIFF — fast on DATE type (INT32 internal)
    "exact_case_insensitive": 2,  # UPPER() per row adds STRING allocation
    "length_mismatch": 2,     # CHAR_LENGTH → INT64 comparison
    # Tier 2: simple string ops — O(n) where n = string length
    "soundex_match": 3,        # SOUNDEX computes 4-char code, then STRING =
    "starts_with": 5,          # STARTS_WITH — prefix scan, early exit
    "contains": 5,             # STRPOS — substring scan both directions
    "abbreviation_match": 5,   # STARTS_WITH + CHAR_LENGTH
    # Tier 3: O(n*m) edit distance — quadratic in string length
    # For a 20-char name, this is ~400 character comparisons per pair.
    "levenshtein": 10,
    "levenshtein_normalized": 12,  # edit_distance + division
    "levenshtein_score": 12,
    # Tier 4: UDF calls (JS execution overhead ~10-50x native)
    # Each call serializes data to V8 JS engine and back.
    "metaphone_match": 15,     # JS UDF called twice (left + right)
    "double_metaphone_match": 15,  # JS UDF called 4x (primary + alternate)
    "initials_match": 15,      # UNNEST + STRING_AGG subquery per side
    "jaro_winkler": 20,        # JS UDF — most expensive string comparison
    "jaro_winkler_score": 20,
    # Tier 5: complex subqueries / ML / geo — avoid at scale if possible
    "geo_within_km": 25,       # ST_DISTANCE — geodesic math per pair
    "geo_distance_score": 25,
    "token_set_match": 30,     # UNNEST + IN + COUNTIF subquery per pair
    "token_set_score": 30,
    "cosine_similarity": 50,   # ML.DISTANCE — vector math on FLOAT64 arrays
    "cosine_similarity_score": 50,
}


def register(name: str) -> Callable[[ComparisonFunction], ComparisonFunction]:
    """Decorator to register a comparison function.

    Can be used by external packages to add custom comparison methods::

        from bq_entity_resolution import register_comparison

        @register_comparison("my_similarity")
        def my_similarity(left: str, right: str, **_: Any) -> str:
            return f"(l.{left} = r.{right})"

    The function is then available in YAML config::

        comparisons:
          - left: col_a
            right: col_a
            method: my_similarity
            weight: 3.0

    External packages can also auto-register via entry_points::

        [project.entry-points."bq_er.comparisons"]
        my_pkg = "my_pkg.comparisons"
    """

    def decorator(func: ComparisonFunction) -> ComparisonFunction:
        COMPARISON_FUNCTIONS[name] = func
        return func

    return decorator


def load_comparison_plugins() -> None:
    """Discover and load comparison function plugins from entry_points.

    Called automatically on first registry miss during config validation.
    Safe to call multiple times — only loads once.
    """
    global _plugins_loaded
    if _plugins_loaded:
        return
    _plugins_loaded = True

    try:
        from importlib.metadata import entry_points
    except ImportError:
        return

    try:
        eps = entry_points(group="bq_er.comparisons")
    except TypeError:
        all_eps = entry_points()
        eps = all_eps.get("bq_er.comparisons", [])

    for ep in eps:
        try:
            ep.load()
            logger.debug("Loaded comparison plugin '%s'", ep.name)
        except Exception as exc:
            logger.warning("Failed to load comparison plugin '%s': %s", ep.name, exc)


# UDF dataset placeholder — replaced at SQL generation time by the matching engine
# when it resolves the {udf_dataset} variable from config
_UDF_DATASET_PLACEHOLDER = "{udf_dataset}"


# Import sub-modules to trigger @register decorators
import bq_entity_resolution.matching.comparisons.composite_comparisons  # noqa: E402, F401
import bq_entity_resolution.matching.comparisons.date_comparisons  # noqa: E402, F401
import bq_entity_resolution.matching.comparisons.geo_comparisons  # noqa: E402, F401
import bq_entity_resolution.matching.comparisons.null_comparisons  # noqa: E402, F401
import bq_entity_resolution.matching.comparisons.numeric_comparisons  # noqa: E402, F401
import bq_entity_resolution.matching.comparisons.string_comparisons  # noqa: E402, F401
