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


# ---------------------------------------------------------------------------
# Name features
# ---------------------------------------------------------------------------


@register("name_clean")
def name_clean(inputs: list[str], **_: Any) -> str:
    """Uppercase, remove non-alpha (keep spaces/hyphens), collapse whitespace."""
    col = inputs[0]
    return (
        f"TRIM(UPPER(REGEXP_REPLACE("
        f"REGEXP_REPLACE({col}, r'[^a-zA-Z\\\\s\\\\-]', ''), "
        f"r'\\\\s+', ' ')))"
    )


@register("name_clean_strict")
def name_clean_strict(inputs: list[str], **_: Any) -> str:
    """Uppercase, remove everything except letters, collapse whitespace."""
    col = inputs[0]
    return (
        f"TRIM(UPPER(REGEXP_REPLACE("
        f"REGEXP_REPLACE({col}, r'[^a-zA-Z\\\\s]', ''), "
        f"r'\\\\s+', ' ')))"
    )


@register("first_letter")
def first_letter(inputs: list[str], **_: Any) -> str:
    """Extract first character."""
    return f"LEFT({inputs[0]}, 1)"


@register("first_n_chars")
def first_n_chars(inputs: list[str], length: int = 3, **_: Any) -> str:
    """Extract first N characters."""
    return f"LEFT({inputs[0]}, {length})"


@register("char_length")
def char_length(inputs: list[str], **_: Any) -> str:
    """String length."""
    return f"CHAR_LENGTH({inputs[0]})"


@register("soundex")
def soundex(inputs: list[str], **_: Any) -> str:
    """Soundex phonetic encoding."""
    return f"SOUNDEX({inputs[0]})"


@register("extract_salutation")
def extract_salutation(inputs: list[str], **_: Any) -> str:
    """Extract salutation (MR, MRS, MS, DR, PROF) from name string."""
    col = inputs[0]
    return (
        f"CASE "
        f"WHEN REGEXP_CONTAINS(UPPER({col}), r'^(MR|MRS|MS|MISS|DR|PROF|REV|HON)[\\\\.\\\\s]') "
        f"THEN REGEXP_EXTRACT(UPPER({col}), r'^(MR|MRS|MS|MISS|DR|PROF|REV|HON)') "
        f"ELSE NULL END"
    )


@register("strip_salutation")
def strip_salutation(inputs: list[str], **_: Any) -> str:
    """Remove salutation prefix from name string."""
    col = inputs[0]
    return (
        f"TRIM(REGEXP_REPLACE(UPPER({col}), "
        f"r'^(MR|MRS|MS|MISS|DR|PROF|REV|HON)[\\\\.\\\\s]+', ''))"
    )


@register("extract_suffix")
def extract_suffix(inputs: list[str], **_: Any) -> str:
    """Extract name suffix (JR, SR, II, III, IV, etc.)."""
    col = inputs[0]
    return (
        f"REGEXP_EXTRACT(UPPER({col}), "
        f"r'\\\\b(JR|SR|II|III|IV|V|VI|VII|VIII|IX|X|ESQ|PHD|MD)\\\\b')"
    )


@register("strip_suffix")
def strip_suffix(inputs: list[str], **_: Any) -> str:
    """Remove name suffix from string."""
    col = inputs[0]
    return (
        f"TRIM(REGEXP_REPLACE(UPPER({col}), "
        f"r'\\\\b(JR|SR|II|III|IV|V|VI|VII|VIII|IX|X|ESQ|PHD|MD)\\\\.?\\\\s*$', ''))"
    )


@register("word_count")
def word_count(inputs: list[str], **_: Any) -> str:
    """Count number of words in a string."""
    col = inputs[0]
    return f"ARRAY_LENGTH(SPLIT(TRIM({col}), ' '))"


@register("first_word")
def first_word(inputs: list[str], **_: Any) -> str:
    """Extract first word from a string."""
    col = inputs[0]
    return f"SPLIT(TRIM({col}), ' ')[OFFSET(0)]"


@register("last_word")
def last_word(inputs: list[str], **_: Any) -> str:
    """Extract last word from a string."""
    col = inputs[0]
    return f"ARRAY_REVERSE(SPLIT(TRIM({col}), ' '))[OFFSET(0)]"


@register("initials")
def initials(inputs: list[str], **_: Any) -> str:
    """Extract initials from each word (e.g., 'John Adam Smith' -> 'JAS')."""
    col = inputs[0]
    return (
        f"(SELECT STRING_AGG(LEFT(word, 1), '' ORDER BY pos) "
        f"FROM UNNEST(SPLIT(TRIM({col}), ' ')) AS word WITH OFFSET AS pos)"
    )


@register("strip_business_suffix")
def strip_business_suffix(inputs: list[str], **_: Any) -> str:
    """Remove common business suffixes (LLC, INC, CORP, LTD, etc.)."""
    col = inputs[0]
    return (
        f"TRIM(REGEXP_REPLACE(UPPER({col}), "
        f"r'\\\\b(LLC|INC|CORP|CORPORATION|LTD|LIMITED|LP|LLP|"
        f"PLLC|PC|PA|COMPANY|CO|GROUP|HOLDINGS|ENTERPRISES?|"
        f"ASSOCIATES?|PARTNERS?|SERVICES?|SOLUTIONS?|"
        f"INTERNATIONAL|INTL|NATIONAL|NATL)\\\\.?\\\\s*$', ''))"
    )


@register("name_fingerprint")
def name_fingerprint(inputs: list[str], **_: Any) -> str:
    """FARM_FINGERPRINT of alpha-only characters (removes spaces, punctuation)."""
    col = inputs[0]
    return f"FARM_FINGERPRINT(REGEXP_REPLACE(UPPER({col}), r'[^A-Z]', ''))"


# ---------------------------------------------------------------------------
# Address features
# ---------------------------------------------------------------------------


@register("address_standardize")
def address_standardize(inputs: list[str], **_: Any) -> str:
    """Standardize address: uppercase, abbreviate common street types, collapse whitespace.

    Covers 30+ common street type abbreviations per USPS Publication 28.
    """
    col = inputs[0]
    # Build nested REGEXP_REPLACE chain for street type standardization
    replacements = [
        ("STREET", "ST"), ("AVENUE", "AVE"), ("BOULEVARD", "BLVD"),
        ("DRIVE", "DR"), ("LANE", "LN"), ("ROAD", "RD"),
        ("HIGHWAY", "HWY"), ("PARKWAY", "PKWY"), ("PLACE", "PL"),
        ("CIRCLE", "CIR"), ("COURT", "CT"), ("TERRACE", "TER"),
        ("TRAIL", "TRL"), ("WAY", "WAY"), ("SQUARE", "SQ"),
        ("EXPRESSWAY", "EXPY"), ("FREEWAY", "FWY"), ("TURNPIKE", "TPKE"),
        ("PIKE", "PIKE"), ("ALLEY", "ALY"), ("CROSSING", "XING"),
        ("CRESCENT", "CRES"), ("HEIGHTS", "HTS"), ("JUNCTION", "JCT"),
        ("LOOP", "LOOP"), ("RIDGE", "RDG"), ("VALLEY", "VLY"),
        ("POINT", "PT"), ("GROVE", "GRV"), ("GARDENS", "GDNS"),
        # Common direction abbreviations
        ("NORTH", "N"), ("SOUTH", "S"), ("EAST", "E"), ("WEST", "W"),
        ("NORTHEAST", "NE"), ("NORTHWEST", "NW"),
        ("SOUTHEAST", "SE"), ("SOUTHWEST", "SW"),
        # Unit designators
        ("APARTMENT", "APT"), ("SUITE", "STE"), ("BUILDING", "BLDG"),
        ("FLOOR", "FL"), ("DEPARTMENT", "DEPT"),
    ]
    # Start with UPPER
    expr = f"UPPER({col})"
    for full, abbr in replacements:
        expr = f"REGEXP_REPLACE({expr}, r'\\\\b{full}\\\\b', '{abbr}')"
    # Collapse whitespace and trim
    return f"TRIM(REGEXP_REPLACE({expr}, r'\\\\s+', ' '))"


@register("extract_street_number")
def extract_street_number(inputs: list[str], **_: Any) -> str:
    """Extract leading street number from address."""
    col = inputs[0]
    return f"REGEXP_EXTRACT({col}, r'^(\\\\d+)')"


@register("extract_street_name")
def extract_street_name(inputs: list[str], **_: Any) -> str:
    """Extract street name (after number, before type suffix)."""
    col = inputs[0]
    return f"TRIM(REGEXP_EXTRACT(UPPER({col}), r'^\\\\d+\\\\s+(.+?)\\\\s*$'))"


@register("extract_unit_number")
def extract_unit_number(inputs: list[str], **_: Any) -> str:
    """Extract apartment/suite/unit number."""
    col = inputs[0]
    return (
        f"REGEXP_EXTRACT(UPPER({col}), "
        f"r'(?:APT|SUITE|STE|UNIT|#|NO)\\\\.?\\\\s*(\\\\w+)')"
    )


# ---------------------------------------------------------------------------
# Contact features
# ---------------------------------------------------------------------------


@register("phone_standardize")
def phone_standardize(inputs: list[str], **_: Any) -> str:
    """Normalize phone: strip non-digits, handle country codes.

    Strips leading '1' (US) or '0' (UK/EU) for consistent comparison.
    Falls back to last 10 digits for US-style numbers.
    """
    col = inputs[0]
    return (
        f"CASE "
        f"WHEN CHAR_LENGTH(REGEXP_REPLACE({col}, r'[^0-9]', '')) > 10 "
        f"AND STARTS_WITH(REGEXP_REPLACE({col}, r'[^0-9]', ''), '1') "
        f"THEN RIGHT(REGEXP_REPLACE({col}, r'[^0-9]', ''), "
        f"CHAR_LENGTH(REGEXP_REPLACE({col}, r'[^0-9]', '')) - 1) "
        f"WHEN CHAR_LENGTH(REGEXP_REPLACE({col}, r'[^0-9]', '')) > 10 "
        f"AND STARTS_WITH(REGEXP_REPLACE({col}, r'[^0-9]', ''), '0') "
        f"THEN RIGHT(REGEXP_REPLACE({col}, r'[^0-9]', ''), "
        f"CHAR_LENGTH(REGEXP_REPLACE({col}, r'[^0-9]', '')) - 1) "
        f"ELSE RIGHT(REGEXP_REPLACE({col}, r'[^0-9]', ''), 10) "
        f"END"
    )


@register("phone_area_code")
def phone_area_code(inputs: list[str], **_: Any) -> str:
    """Extract area code (first 3 digits of 10-digit normalized phone)."""
    col = inputs[0]
    return f"LEFT(RIGHT(REGEXP_REPLACE({col}, r'[^0-9]', ''), 10), 3)"


@register("phone_last_four")
def phone_last_four(inputs: list[str], **_: Any) -> str:
    """Extract last 4 digits of phone number (subscriber number)."""
    col = inputs[0]
    return f"RIGHT(REGEXP_REPLACE({col}, r'[^0-9]', ''), 4)"


@register("email_domain")
def email_domain(inputs: list[str], **_: Any) -> str:
    """Extract email domain (after @)."""
    return f"LOWER(REGEXP_EXTRACT({inputs[0]}, r'@(.+)$'))"


@register("email_local_part")
def email_local_part(inputs: list[str], **_: Any) -> str:
    """Extract email local part (before @)."""
    return f"LOWER(REGEXP_EXTRACT({inputs[0]}, r'^([^@]+)'))"


@register("email_domain_type")
def email_domain_type(inputs: list[str], **_: Any) -> str:
    """Classify email domain as FREE, BUSINESS, or UNKNOWN."""
    col = inputs[0]
    return (
        f"CASE "
        f"WHEN LOWER(REGEXP_EXTRACT({col}, r'@(.+)$')) IN "
        f"('gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com',"
        f"'icloud.com','mail.com','protonmail.com','live.com','msn.com') "
        f"THEN 'FREE' "
        f"WHEN REGEXP_EXTRACT({col}, r'@(.+)$') IS NOT NULL THEN 'BUSINESS' "
        f"ELSE NULL END"
    )


# ---------------------------------------------------------------------------
# Generic / utility features
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Blocking key functions
# ---------------------------------------------------------------------------


@register("farm_fingerprint")
def farm_fingerprint(inputs: list[str], **_: Any) -> str:
    """FARM_FINGERPRINT of a single column."""
    return f"FARM_FINGERPRINT({inputs[0]})"


@register("farm_fingerprint_concat")
def farm_fingerprint_concat(inputs: list[str], **_: Any) -> str:
    """FARM_FINGERPRINT of concatenated columns."""
    parts = ", '||', ".join(
        f"COALESCE(CAST({c} AS STRING), '')" for c in inputs
    )
    return f"FARM_FINGERPRINT(CONCAT({parts}))"


@register("identity")
def identity_func(inputs: list[str], **_: Any) -> str:
    """Pass through column unchanged."""
    return inputs[0]


# ---------------------------------------------------------------------------
# Nickname / alias resolution
# ---------------------------------------------------------------------------


# Common English name nickname mappings (canonical -> variants)
_NICKNAME_PAIRS = [
    ("ROBERT", "BOB"), ("ROBERT", "ROB"), ("ROBERT", "BOBBY"), ("ROBERT", "ROBBIE"),
    ("WILLIAM", "BILL"), ("WILLIAM", "WILL"), ("WILLIAM", "BILLY"), ("WILLIAM", "WILLY"),
    ("RICHARD", "RICK"), ("RICHARD", "DICK"), ("RICHARD", "RICH"), ("RICHARD", "RICKY"),
    ("JAMES", "JIM"), ("JAMES", "JIMMY"), ("JAMES", "JAMIE"),
    ("JOHN", "JACK"), ("JOHN", "JOHNNY"), ("JOHN", "JON"),
    ("JOSEPH", "JOE"), ("JOSEPH", "JOEY"),
    ("THOMAS", "TOM"), ("THOMAS", "TOMMY"),
    ("MICHAEL", "MIKE"), ("MICHAEL", "MIKEY"),
    ("CHARLES", "CHUCK"), ("CHARLES", "CHARLIE"),
    ("EDWARD", "ED"), ("EDWARD", "TED"), ("EDWARD", "EDDIE"), ("EDWARD", "TEDDY"),
    ("DAVID", "DAVE"), ("DAVID", "DAVY"),
    ("DANIEL", "DAN"), ("DANIEL", "DANNY"),
    ("MATTHEW", "MATT"), ("MATTHEW", "MATTY"),
    ("ANTHONY", "TONY"),
    ("CHRISTOPHER", "CHRIS"),
    ("NICHOLAS", "NICK"), ("NICHOLAS", "NICKY"),
    ("BENJAMIN", "BEN"), ("BENJAMIN", "BENNY"),
    ("SAMUEL", "SAM"), ("SAMUEL", "SAMMY"),
    ("JONATHAN", "JON"), ("JONATHAN", "NATHAN"),
    ("STEPHEN", "STEVE"), ("STEVEN", "STEVE"),
    ("TIMOTHY", "TIM"), ("TIMOTHY", "TIMMY"),
    ("ANDREW", "ANDY"), ("ANDREW", "DREW"),
    ("PATRICK", "PAT"), ("PATRICK", "PADDY"),
    ("ALEXANDER", "ALEX"),
    ("GREGORY", "GREG"),
    ("LAWRENCE", "LARRY"),
    ("RAYMOND", "RAY"),
    ("CATHERINE", "CATHY"), ("CATHERINE", "KATE"), ("CATHERINE", "KATHY"),
    ("ELIZABETH", "LIZ"), ("ELIZABETH", "BETH"), ("ELIZABETH", "ELIZA"),
    ("ELIZABETH", "BETTY"), ("ELIZABETH", "LIZZY"),
    ("JENNIFER", "JEN"), ("JENNIFER", "JENNY"),
    ("MARGARET", "MAGGIE"), ("MARGARET", "MEG"), ("MARGARET", "PEGGY"),
    ("PATRICIA", "PAT"), ("PATRICIA", "PATTY"),
    ("JESSICA", "JESS"), ("JESSICA", "JESSIE"),
    ("REBECCA", "BECKY"), ("REBECCA", "BECCA"),
    ("SUSAN", "SUE"), ("SUSAN", "SUZY"),
    ("DOROTHY", "DOT"), ("DOROTHY", "DOTTY"),
    ("VIRGINIA", "GINNY"), ("VIRGINIA", "GINGER"),
    ("BARBARA", "BARB"), ("BARBARA", "BARBIE"),
    ("DEBORAH", "DEB"), ("DEBORAH", "DEBBIE"),
    ("CHRISTINE", "CHRIS"), ("CHRISTINE", "TINA"),
    ("CHRISTINA", "CHRIS"), ("CHRISTINA", "TINA"),
    ("VICTORIA", "VICKY"), ("VICTORIA", "TORI"),
    ("ALEXANDER", "SANDY"), ("ALEXANDRA", "ALEX"), ("ALEXANDRA", "SANDY"),
]


@register("nickname_canonical")
def nickname_canonical(inputs: list[str], **_: Any) -> str:
    """Map common nicknames to their canonical form.

    E.g., 'BOB' -> 'ROBERT', 'BILL' -> 'WILLIAM'.
    If no nickname match, returns the uppercased input unchanged.
    """
    col = inputs[0]
    # Build a CASE expression that maps nicknames -> canonical names
    cases = []
    # Build reverse mapping: variant -> canonical
    seen: dict[str, str] = {}
    for canonical, variant in _NICKNAME_PAIRS:
        if variant not in seen:
            seen[variant] = canonical
    for variant, canonical in sorted(seen.items()):
        cases.append(f"WHEN '{variant}' THEN '{canonical}'")
    case_expr = " ".join(cases)
    return f"CASE UPPER(TRIM({col})) {case_expr} ELSE UPPER(TRIM({col})) END"


@register("nickname_match_key")
def nickname_match_key(inputs: list[str], **_: Any) -> str:
    """Generate a blocking key that groups nicknames together.

    Returns FARM_FINGERPRINT of the canonical name form.
    """
    col = inputs[0]
    # Reuse nickname_canonical logic
    canonical_expr = nickname_canonical(inputs)
    return f"FARM_FINGERPRINT({canonical_expr})"


# ---------------------------------------------------------------------------
# Transposition-aware features
# ---------------------------------------------------------------------------


@register("sorted_name_tokens")
def sorted_name_tokens(inputs: list[str], **_: Any) -> str:
    """Sort words in a name alphabetically to handle transpositions.

    'Smith John' and 'John Smith' both become 'JOHN SMITH'.
    """
    col = inputs[0]
    return (
        f"(SELECT STRING_AGG(word, ' ' ORDER BY word) "
        f"FROM UNNEST(SPLIT(TRIM(UPPER({col})), ' ')) AS word "
        f"WHERE word != '')"
    )


@register("sorted_name_fingerprint")
def sorted_name_fingerprint(inputs: list[str], **_: Any) -> str:
    """FARM_FINGERPRINT of sorted name tokens — catches transpositions."""
    col = inputs[0]
    sorted_expr = sorted_name_tokens(inputs)
    return f"FARM_FINGERPRINT({sorted_expr})"


# ---------------------------------------------------------------------------
# Zip / postal code features
# ---------------------------------------------------------------------------


@register("zip5")
def zip5(inputs: list[str], **_: Any) -> str:
    """Extract first 5 digits of a zip/postal code."""
    col = inputs[0]
    return f"LEFT(REGEXP_REPLACE({col}, r'[^0-9]', ''), 5)"


@register("zip3")
def zip3(inputs: list[str], **_: Any) -> str:
    """Extract first 3 digits of a zip code (SCF area)."""
    col = inputs[0]
    return f"LEFT(REGEXP_REPLACE({col}, r'[^0-9]', ''), 3)"


# ---------------------------------------------------------------------------
# Date features
# ---------------------------------------------------------------------------


@register("year_of_date")
def year_of_date(inputs: list[str], **_: Any) -> str:
    """Extract year from a date/timestamp column."""
    return f"EXTRACT(YEAR FROM {inputs[0]})"


@register("date_to_string")
def date_to_string(inputs: list[str], fmt: str = "%Y%m%d", **_: Any) -> str:
    """Format a date as a string for blocking/comparison."""
    return f"FORMAT_DATE('{fmt}', {inputs[0]})"


# ---------------------------------------------------------------------------
# DOB / SSN / Phone identity features
# ---------------------------------------------------------------------------


@register("dob_year")
def dob_year(inputs: list[str], **_: Any) -> str:
    """Extract year of birth from a DATE column for blocking."""
    col = inputs[0]
    return f"EXTRACT(YEAR FROM {col})"


@register("age_from_dob")
def age_from_dob(inputs: list[str], **_: Any) -> str:
    """Compute current age in years from a DATE column."""
    col = inputs[0]
    return f"DATE_DIFF(CURRENT_DATE(), {col}, YEAR)"


@register("ssn_last_four")
def ssn_last_four(inputs: list[str], **_: Any) -> str:
    """Extract last 4 digits of an SSN (strips dashes/spaces first)."""
    col = inputs[0]
    return f"RIGHT(REGEXP_REPLACE({col}, r'[^0-9]', ''), 4)"


@register("ssn_clean")
def ssn_clean(inputs: list[str], **_: Any) -> str:
    """Strip non-digit characters from an SSN (e.g. '123-45-6789' → '123456789')."""
    col = inputs[0]
    return f"REGEXP_REPLACE({col}, r'[^0-9]', '')"


@register("dob_mmdd")
def dob_mmdd(inputs: list[str], **_: Any) -> str:
    """Extract month+day from a DATE column as MMDD string for blocking."""
    col = inputs[0]
    return f"FORMAT_DATE('%m%d', {col})"


@register("phone_area_code")
def phone_area_code(inputs: list[str], **_: Any) -> str:
    """Extract the 3-digit area code from a phone number."""
    col = inputs[0]
    return f"LEFT(REGEXP_REPLACE({col}, r'[^0-9]', ''), 3)"


# ---------------------------------------------------------------------------
# Geo-spatial features
# ---------------------------------------------------------------------------


@register("geo_hash")
def geo_hash(inputs: list[str], precision: int = 6, **_: Any) -> str:
    """Geohash from latitude and longitude columns.

    Uses BigQuery ST_GEOHASH(ST_GEOGPOINT(lon, lat), precision).
    Inputs: [lat, lon].
    """
    lat, lon = inputs[0], inputs[1]
    return (
        f"CASE WHEN {lat} IS NOT NULL AND {lon} IS NOT NULL "
        f"THEN ST_GEOHASH(ST_GEOGPOINT({lon}, {lat}), {precision}) "
        f"ELSE NULL END"
    )


@register("lat_lon_bucket")
def lat_lon_bucket(inputs: list[str], grid_size_km: int = 10, **_: Any) -> str:
    """Grid cell blocking key from lat/lon coordinates.

    Divides the globe into grid cells of approximately grid_size_km.
    1 degree latitude ~ 111 km. Returns a string key like '40_-74'.
    Inputs: [lat, lon].
    """
    lat, lon = inputs[0], inputs[1]
    # Approximate degrees per grid cell
    deg = round(grid_size_km / 111.0, 4)
    return (
        f"CASE WHEN {lat} IS NOT NULL AND {lon} IS NOT NULL "
        f"THEN CONCAT(CAST(CAST(FLOOR({lat} / {deg}) AS INT64) AS STRING), "
        f"'_', CAST(CAST(FLOOR({lon} / {deg}) AS INT64) AS STRING)) "
        f"ELSE NULL END"
    )


@register("haversine_distance")
def haversine_distance(inputs: list[str], **_: Any) -> str:
    """Distance in kilometers between two lat/lon points.

    Uses BigQuery ST_DISTANCE for accurate geodesic distance.
    Inputs: [lat1, lon1, lat2, lon2].
    """
    lat1, lon1, lat2, lon2 = inputs[0], inputs[1], inputs[2], inputs[3]
    return (
        f"CASE WHEN {lat1} IS NOT NULL AND {lon1} IS NOT NULL "
        f"AND {lat2} IS NOT NULL AND {lon2} IS NOT NULL "
        f"THEN ST_DISTANCE(ST_GEOGPOINT({lon1}, {lat1}), "
        f"ST_GEOGPOINT({lon2}, {lat2})) / 1000.0 "
        f"ELSE NULL END"
    )


# ---------------------------------------------------------------------------
# Phonetic features (UDF-based)
# ---------------------------------------------------------------------------

_UDF_DATASET_PLACEHOLDER = "{udf_dataset}"


@register("metaphone")
def metaphone(inputs: list[str], udf_dataset: str = "", **_: Any) -> str:
    """Compute Metaphone code via a BigQuery JS UDF.

    The UDF must be deployed to the udf_dataset as ``metaphone(STRING) -> STRING``.
    """
    col = inputs[0]
    ds = udf_dataset or _UDF_DATASET_PLACEHOLDER
    return (
        f"CASE WHEN {col} IS NOT NULL "
        f"THEN `{ds}.metaphone`({col}) "
        f"ELSE NULL END"
    )
