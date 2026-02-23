"""
Feature function registry.

Maps config function names to BigQuery SQL expression generators.
Each registered function takes a list of input column names and optional
keyword params, and returns a BigQuery SQL expression string.

Usage in YAML:
  features:
    - name: "first_name_clean"
      function: "name_clean"
      input: "raw_first_name"

This translates to:  FEATURE_FUNCTIONS["name_clean"](["raw_first_name"]) -> SQL expr

BigQuery Performance Notes — Column Types and Blocking Efficiency
=================================================================
BigQuery stores and compares column types with very different performance
characteristics. When choosing features for **blocking keys**, prefer
functions that produce INT64 output over STRING output:

    INT64 equality    ~3-5x faster than STRING equality in JOINs/WHEREs
    INT64 GROUP BY    uses hash aggregation natively (8-byte fixed width)
    INT64 CLUSTER BY  produces tighter physical sort blocks in BQ storage

The cost hierarchy for column types in BigQuery equi-joins:

    INT64 / DATE (fixed-width)  >  short STRING (< 32 bytes)
    >  long STRING              >  REGEXP/LIKE
    >  EDIT_DISTANCE             >  JS UDF (jaro_winkler)

Functions that return INT64 (prefer these for blocking keys):
    farm_fingerprint, farm_fingerprint_concat, name_fingerprint,
    sorted_name_fingerprint, nickname_match_key, dob_year, year_of_date,
    char_length, age_from_dob

Functions that return STRING (fine for feature comparison, slower for blocking):
    soundex, name_clean, email_domain, phone_last_four, zip3, zip5,
    metaphone, initials, address_standardize, etc.

To convert any STRING blocking key to INT64, wrap it in FARM_FINGERPRINT:
    FARM_FINGERPRINT(SOUNDEX(col))  — phonetic blocking as INT64
    FARM_FINGERPRINT(LEFT(col, 3))  — prefix blocking as INT64

FARM_FINGERPRINT produces a deterministic INT64 hash. Collisions are
astronomically rare (~1 in 2^63) and acceptable for blocking because
blocking is a recall-oriented filter — false positives are eliminated
in the comparison stage.
"""

from __future__ import annotations

from typing import Any, Callable

FeatureFunction = Callable[..., str]

FEATURE_FUNCTIONS: dict[str, FeatureFunction] = {}


def register(name: str) -> Callable[[FeatureFunction], FeatureFunction]:
    """Decorator to register a feature function."""

    def decorator(func: FeatureFunction) -> FeatureFunction:
        FEATURE_FUNCTIONS[name] = func
        return func

    return decorator


# Import sub-modules to trigger @register decorators
import bq_entity_resolution.features.name_features  # noqa: E402, F401
import bq_entity_resolution.features.address_features  # noqa: E402, F401
import bq_entity_resolution.features.contact_features  # noqa: E402, F401
import bq_entity_resolution.features.date_identity_features  # noqa: E402, F401
import bq_entity_resolution.features.geo_features  # noqa: E402, F401
import bq_entity_resolution.features.blocking_keys  # noqa: E402, F401
import bq_entity_resolution.features.utility_features  # noqa: E402, F401
import bq_entity_resolution.features.phonetic_features  # noqa: E402, F401
import bq_entity_resolution.features.zip_features  # noqa: E402, F401
