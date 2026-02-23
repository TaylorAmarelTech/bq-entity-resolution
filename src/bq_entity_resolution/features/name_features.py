"""Name feature functions."""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.features.registry import register

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

    OUTPUT TYPE: INT64 — ideal blocking key.
    PERF: Wraps the STRING canonical form in FARM_FINGERPRINT so that
    BOB, BOBBY, ROBERT all hash to the same INT64 value. This enables
    fast INT64 equi-join blocking that automatically groups nicknames.
    """
    col = inputs[0]
    # Reuse nickname_canonical logic
    canonical_expr = nickname_canonical(inputs)
    return f"FARM_FINGERPRINT({canonical_expr})"
