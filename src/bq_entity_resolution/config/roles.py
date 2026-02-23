"""Column role mappings: semantic roles drive auto-feature generation.

Roles are semantic labels like 'first_name', 'last_name', 'date_of_birth'.
Given a role, the system knows which features, blocking keys, and
comparisons to generate automatically.

This enables progressive disclosure: users assign roles to columns and
the system generates the full configuration.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Role → Feature mapping
# ---------------------------------------------------------------------------

# Each role maps to a list of feature definitions.
# Format: (feature_name_suffix, function, input_override?)
# The actual feature name is constructed as: {column}_{suffix}

ROLE_FEATURES: dict[str, list[tuple[str, str]]] = {
    "first_name": [
        ("clean", "name_clean"),
        ("soundex", "soundex"),
        ("metaphone", "metaphone"),
    ],
    "last_name": [
        ("clean", "name_clean"),
        ("soundex", "soundex"),
        ("metaphone", "metaphone"),
    ],
    "full_name": [
        ("clean", "name_clean"),
        ("soundex", "soundex"),
    ],
    "date_of_birth": [
        ("year", "dob_year"),
    ],
    "email": [
        ("domain", "email_domain"),
        ("local", "email_local_part"),
        ("clean", "lower_trim"),
    ],
    "phone": [
        ("std", "phone_standardize"),
        ("last4", "phone_last_four"),
    ],
    "address_line_1": [
        ("std", "address_standardize"),
        ("street_number", "extract_street_number"),
    ],
    "city": [
        ("clean", "name_clean"),
        ("soundex", "soundex"),
    ],
    "state": [],  # No features needed — used directly
    "zip_code": [
        ("prefix3", "zip3"),
    ],
    "ssn": [
        ("last4", "ssn_last_four"),
    ],
    "company_name": [
        ("clean", "name_clean"),
        ("no_suffix", "strip_business_suffix"),
    ],
    "ein": [
        ("clean", "lower_trim"),
    ],
}

# ---------------------------------------------------------------------------
# Role → Blocking key mapping
# ---------------------------------------------------------------------------

# Each role maps to blocking keys to generate.
# Format: (key_name_suffix, function)

ROLE_BLOCKING_KEYS: dict[str, list[tuple[str, str]]] = {
    "first_name": [
        ("first_soundex", "soundex"),
    ],
    "last_name": [
        ("last_soundex", "soundex"),
    ],
    "full_name": [
        ("name_soundex", "soundex"),
    ],
    "date_of_birth": [
        ("dob_year", "dob_year"),
    ],
    "email": [
        ("email_domain", "email_domain"),
    ],
    "phone": [
        ("phone_last4", "phone_last_four"),
    ],
    "zip_code": [
        ("zip3", "zip3"),
    ],
    "ssn": [
        ("ssn_last4", "ssn_last_four"),
    ],
    "company_name": [
        ("company_soundex", "soundex"),
    ],
}

# ---------------------------------------------------------------------------
# Role → Comparison mapping
# ---------------------------------------------------------------------------

# Each role maps to comparisons to generate.
# Format: (comparison_name, method, left_suffix, right_suffix, weight, params)

@dataclass(frozen=True)
class ComparisonSpec:
    """Specification for an auto-generated comparison."""
    name_suffix: str
    method: str
    feature_suffix: str  # Applied to both left and right columns
    weight: float = 1.0
    params: dict = field(default_factory=dict)


ROLE_COMPARISONS: dict[str, list[ComparisonSpec]] = {
    "first_name": [
        ComparisonSpec("jw", "jaro_winkler", "clean", weight=2.0),
        ComparisonSpec("exact", "exact", "clean", weight=3.0),
    ],
    "last_name": [
        ComparisonSpec("jw", "jaro_winkler", "clean", weight=2.0),
        ComparisonSpec("exact", "exact", "clean", weight=3.0),
    ],
    "full_name": [
        ComparisonSpec("jw", "jaro_winkler", "clean", weight=2.5),
    ],
    "date_of_birth": [
        ComparisonSpec("exact", "exact", "", weight=4.0),
    ],
    "email": [
        ComparisonSpec("exact", "exact", "clean", weight=5.0),  # clean = lower_trim
        ComparisonSpec("domain_match", "exact", "domain", weight=0.5),
    ],
    "phone": [
        ComparisonSpec("exact", "exact", "std", weight=4.0),
    ],
    "address_line_1": [
        ComparisonSpec("lev", "levenshtein", "std", weight=1.5),
    ],
    "city": [
        ComparisonSpec("exact", "exact", "clean", weight=1.0),
    ],
    "state": [
        ComparisonSpec("exact", "exact", "", weight=0.5),
    ],
    "zip_code": [
        ComparisonSpec("exact", "exact", "", weight=1.5),
    ],
    "ssn": [
        ComparisonSpec("exact", "exact", "last4", weight=5.0),
    ],
    "company_name": [
        ComparisonSpec("jw", "jaro_winkler", "clean", weight=2.5),
        ComparisonSpec("exact", "exact", "clean", weight=4.0),
    ],
    "ein": [
        ComparisonSpec("exact", "exact", "clean", weight=5.0),  # clean = lower_trim
    ],
}


# ---------------------------------------------------------------------------
# Role detection from column names
# ---------------------------------------------------------------------------

# Common column name patterns that suggest a role.
# Keys are substrings to match (case-insensitive), values are roles.

_NAME_PATTERNS: dict[str, str] = {
    "first_name": "first_name",
    "fname": "first_name",
    "given_name": "first_name",
    "last_name": "last_name",
    "lname": "last_name",
    "surname": "last_name",
    "family_name": "last_name",
    "full_name": "full_name",
    "name": "full_name",
    "dob": "date_of_birth",
    "date_of_birth": "date_of_birth",
    "birth_date": "date_of_birth",
    "birthday": "date_of_birth",
    "email": "email",
    "email_address": "email",
    "phone": "phone",
    "phone_number": "phone",
    "mobile": "phone",
    "cell_phone": "phone",
    "address": "address_line_1",
    "address_line_1": "address_line_1",
    "street": "address_line_1",
    "city": "city",
    "state": "state",
    "zip": "zip_code",
    "zip_code": "zip_code",
    "postal_code": "zip_code",
    "ssn": "ssn",
    "social_security": "ssn",
    "company": "company_name",
    "company_name": "company_name",
    "business_name": "company_name",
    "org_name": "company_name",
    "ein": "ein",
    "tax_id": "ein",
}


def detect_role(column_name: str) -> str | None:
    """Detect a semantic role from a column name.

    Uses pattern matching on common naming conventions.
    Returns None if no role can be determined.
    """
    lower = column_name.lower().strip()

    # Exact match first
    if lower in _NAME_PATTERNS:
        return _NAME_PATTERNS[lower]

    # Substring match (longest first for specificity)
    for pattern in sorted(_NAME_PATTERNS.keys(), key=len, reverse=True):
        if pattern in lower:
            return _NAME_PATTERNS[pattern]

    return None


def features_for_role(
    column_name: str, role: str
) -> list[dict]:
    """Generate feature definitions for a column with a given role.

    Returns a list of dicts suitable for FeatureDef construction:
    [{"name": "first_name_clean", "function": "name_clean", "inputs": ["first_name"]}, ...]
    """
    specs = ROLE_FEATURES.get(role, [])
    features = []
    for suffix, function in specs:
        feat_name = f"{column_name}_{suffix}" if suffix else column_name
        features.append({
            "name": feat_name,
            "function": function,
            "inputs": [column_name],
        })
    return features


def blocking_keys_for_role(
    column_name: str, role: str
) -> list[dict]:
    """Generate blocking key definitions for a column with a given role.

    Returns a list of dicts suitable for BlockingKeyDef construction.
    """
    specs = ROLE_BLOCKING_KEYS.get(role, [])
    keys = []
    for suffix, function in specs:
        keys.append({
            "name": f"bk_{suffix}",
            "function": function,
            "inputs": [column_name],
        })
    return keys


def comparisons_for_role(
    column_name: str, role: str
) -> list[dict]:
    """Generate comparison definitions for a column with a given role.

    Returns a list of dicts suitable for ComparisonDef construction.
    The left and right columns are the feature-transformed version.
    """
    specs = ROLE_COMPARISONS.get(role, [])
    comparisons = []
    for spec in specs:
        feature_col = (
            f"{column_name}_{spec.feature_suffix}"
            if spec.feature_suffix
            else column_name
        )
        comparisons.append({
            "name": f"{column_name}_{spec.name_suffix}",
            "left": feature_col,
            "right": feature_col,
            "method": spec.method,
            "weight": spec.weight,
            "params": dict(spec.params),
        })
    return comparisons


# ---------------------------------------------------------------------------
# Available roles
# ---------------------------------------------------------------------------

PERSON_ROLES = {
    "first_name", "last_name", "full_name",
    "date_of_birth", "email", "phone",
    "address_line_1", "city", "state", "zip_code", "ssn",
}

BUSINESS_ROLES = {
    "company_name", "ein",
    "address_line_1", "city", "state", "zip_code",
    "phone", "email",
}
