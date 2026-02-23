"""SQL builder for matching/scoring (replaces tier_comparisons.sql.j2 + tier_fellegi_sunter.sql.j2).

Generates SQL for two scoring strategies:
1. Sum-based scoring: weighted binary comparisons with aggregate score
2. Fellegi-Sunter: log-likelihood ratio with m/u probabilities

Both support hard negatives, soft signals, term frequency adjustments,
and audit trail output.

BigQuery Scoring Performance Notes
====================================
The scoring SQL runs per candidate pair. If blocking produces N pairs,
every comparison expression is evaluated N times. Optimizations:

1. COMPARISON ORDERING: Comparisons are emitted in declaration order.
   For best performance, declare cheap comparisons first (exact on INT64,
   numeric_within) and expensive ones last (jaro_winkler, cosine_similarity).
   BigQuery evaluates CASE WHEN sequentially and can skip branches.
   See COMPARISON_COSTS in matching/comparisons.py for cost ranking.

2. JOIN PATTERN: Candidate pairs → source table uses INNER JOIN on
   entity_uid (INT64). This is the tightest possible join — 8 bytes
   per probe. The source table should be CLUSTER BY entity_uid for
   co-located reads.

3. TF JOINS: Each term-frequency-enabled comparison adds a LEFT JOIN
   to the tf_stats table. Keep TF comparisons minimal (1-3 max) to
   avoid join explosion. TF joins use STRING keys (column name + value).

4. HARD NEGATIVES: "disqualify" negatives become WHERE filters, applied
   BEFORE scoring. This eliminates pairs early and avoids wasted
   comparison computation. Place cheap conditions (INT64 inequality,
   NULL checks) as disqualify negatives for best pruning.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bq_entity_resolution.columns import (
    ENTITY_UID,
    LEFT_ENTITY_UID,
    MATCH_CONFIDENCE,
    MATCH_DETAIL,
    MATCH_TIER_NAME,
    MATCH_TIER_PRIORITY,
    MATCH_TOTAL_SCORE,
    MATCHED_AT,
    RIGHT_ENTITY_UID,
    TERM_FREQUENCY_COLUMN,
    TERM_FREQUENCY_RATIO,
    TERM_FREQUENCY_VALUE,
    match_log_weight_column,
    match_score_column,
)
from bq_entity_resolution.sql.expression import SQLExpression


@dataclass(frozen=True)
class ComparisonLevel:
    """A single comparison level (for Fellegi-Sunter)."""
    label: str
    sql_expr: str | None  # None = ELSE clause
    log_weight: float = 0.0
    m: float = 0.9
    u: float = 0.1
    tf_adjusted: bool = False


@dataclass(frozen=True)
class ComparisonDef:
    """A comparison definition."""
    name: str
    sql_expr: str = ""  # For sum-based scoring
    weight: float = 1.0
    levels: list[ComparisonLevel] = field(default_factory=list)  # For F-S
    tf_enabled: bool = False
    tf_column: str = ""
    tf_minimum_u: float = 0.01


@dataclass(frozen=True)
class HardNegative:
    """A hard negative rule."""
    sql_condition: str
    action: str  # 'disqualify' or 'penalize'
    penalty: float = 0.0


@dataclass(frozen=True)
class SoftSignal:
    """A soft signal bonus."""
    sql_condition: str
    bonus: float = 0.0


@dataclass(frozen=True)
class Threshold:
    """Scoring threshold configuration."""
    method: str = "score"
    min_score: float = 0.0
    match_threshold: float | None = None


@dataclass(frozen=True)
class SumScoringParams:
    """Parameters for sum-based scoring."""
    tier_name: str
    tier_index: int
    matches_table: str
    candidates_table: str
    source_table: str
    comparisons: list[ComparisonDef]
    hard_negatives: list[HardNegative] = field(default_factory=list)
    soft_signals: list[SoftSignal] = field(default_factory=list)
    threshold: Threshold = field(default_factory=Threshold)
    confidence: float | None = None
    max_possible_score: float = 1.0
    tf_table: str | None = None
    audit_trail_enabled: bool = False


@dataclass(frozen=True)
class FellegiSunterParams:
    """Parameters for Fellegi-Sunter probabilistic scoring."""
    tier_name: str
    tier_index: int
    matches_table: str
    candidates_table: str
    source_table: str
    comparisons: list[ComparisonDef]
    log_prior_odds: float = 0.0
    hard_negatives: list[HardNegative] = field(default_factory=list)
    soft_signals: list[SoftSignal] = field(default_factory=list)
    threshold: Threshold = field(default_factory=Threshold)
    tf_table: str | None = None
    audit_trail_enabled: bool = False


def _build_tf_joins(comparisons: list[ComparisonDef], tf_table: str | None) -> list[str]:
    """Build LEFT JOINs for term-frequency enabled comparisons."""
    if not tf_table:
        return []

    lines: list[str] = []
    for comp in comparisons:
        if comp.tf_enabled:
            alias = f"tf_{comp.name}"
            lines.append(f"  LEFT JOIN `{tf_table}` {alias}")
            lines.append(f"    ON {alias}.{TERM_FREQUENCY_COLUMN} = '{comp.tf_column}'")
            lines.append(
                f"    AND {alias}.{TERM_FREQUENCY_VALUE} = CAST(l.{comp.tf_column} AS STRING)"
            )
    return lines


def _build_disqualify_filters(hard_negatives: list[HardNegative]) -> list[str]:
    """Build WHERE clause filters for disqualification hard negatives."""
    lines: list[str] = []
    for hn in hard_negatives:
        if hn.action == "disqualify":
            lines.append(f"  AND NOT ({hn.sql_condition})")
    return lines


def build_sum_scoring_sql(params: SumScoringParams) -> SQLExpression:
    """Build sum-based scoring SQL.

    Each comparison produces a 0/1 score, multiplied by weight.
    Total score = sum of weighted scores + soft signals - penalties.
    """
    if not params.comparisons:
        raise ValueError(
            f"Tier '{params.tier_name}' has no comparisons defined. "
            "At least one comparison is required for scoring."
        )

    parts: list[str] = []

    parts.append(f"CREATE OR REPLACE TABLE `{params.matches_table}` AS")
    parts.append("")
    parts.append("WITH scored AS (")
    parts.append("  SELECT")
    parts.append(f"    c.{LEFT_ENTITY_UID},")
    parts.append(f"    c.{RIGHT_ENTITY_UID},")
    parts.append("")

    # Individual comparison scores
    for comp in params.comparisons:
        parts.append(
            f"    CASE WHEN {comp.sql_expr} THEN 1.0 ELSE 0.0 END "
            f"AS {match_score_column(comp.name)},"
        )

    parts.append("")
    # Weighted aggregate score
    parts.append("    (")

    score_terms: list[str] = []
    for comp in params.comparisons:
        if comp.tf_enabled and params.tf_table:
            score_terms.append(
                f"      CASE WHEN {comp.sql_expr}\n"
                f"        THEN {comp.weight} * LOG(1.0 / GREATEST("
                f"COALESCE(tf_{comp.name}.{TERM_FREQUENCY_RATIO}, {comp.tf_minimum_u}), "
                f"{comp.tf_minimum_u}))\n"
                f"        ELSE 0.0\n"
                f"      END"
            )
        else:
            score_terms.append(
                f"      {comp.weight} * (CASE WHEN {comp.sql_expr} "
                f"THEN 1.0 ELSE 0.0 END)"
            )

    parts.append("\n      + ".join(score_terms))

    # Soft signal bonuses
    for ss in params.soft_signals:
        parts.append(
            f"      + CASE WHEN {ss.sql_condition} "
            f"THEN {ss.bonus} ELSE 0.0 END"
        )

    # Hard negative penalties
    for hn in params.hard_negatives:
        if hn.action == "penalize":
            parts.append(
                f"      + CASE WHEN {hn.sql_condition} "
                f"THEN {hn.penalty} ELSE 0.0 END"
            )

    parts.append(f"    ) AS {MATCH_TOTAL_SCORE}")
    parts.append("")

    # FROM clause with joins.
    # PERF: Both JOINs are on entity_uid (INT64) — this is the fastest
    # possible join pattern. The source table should be CLUSTER BY entity_uid
    # so BQ can co-locate the left and right lookups in storage.
    parts.append(f"  FROM `{params.candidates_table}` c")
    parts.append(
        f"  INNER JOIN `{params.source_table}` l ON c.{LEFT_ENTITY_UID} = l.{ENTITY_UID}"
    )
    parts.append(
        f"  INNER JOIN `{params.source_table}` r ON c.{RIGHT_ENTITY_UID} = r.{ENTITY_UID}"
    )

    # TF joins — each adds a LEFT JOIN on STRING keys (column_name, value).
    # PERF: Keep TF-enabled comparisons to a minimum (1-3) to avoid
    # join explosion. Each TF join scans the tf_stats table.
    parts.extend(_build_tf_joins(params.comparisons, params.tf_table))

    # WHERE clause — disqualify hard negatives filter pairs BEFORE scoring.
    # PERF: This is the most important optimization point. Disqualify
    # filters that use INT64 inequality or NULL checks eliminate pairs
    # before any expensive comparison runs.
    parts.append("  WHERE 1=1")
    parts.extend(_build_disqualify_filters(params.hard_negatives))
    parts.append(")")
    parts.append("")

    # Final SELECT with threshold
    parts.append("SELECT")
    parts.append(f"  {LEFT_ENTITY_UID},")
    parts.append(f"  {RIGHT_ENTITY_UID},")
    parts.append(f"  {MATCH_TOTAL_SCORE},")
    parts.append(f"  {params.tier_index} AS {MATCH_TIER_PRIORITY},")
    parts.append(f"  '{params.tier_name}' AS {MATCH_TIER_NAME},")

    if params.confidence is not None:
        parts.append(f"  {params.confidence} AS {MATCH_CONFIDENCE},")
    else:
        parts.append(
            f"  ROUND({MATCH_TOTAL_SCORE} / NULLIF({params.max_possible_score}, 0), 4) "
            f"AS {MATCH_CONFIDENCE},"
        )

    for comp in params.comparisons:
        parts.append(f"  {match_score_column(comp.name)},")

    if params.audit_trail_enabled:
        struct_fields = ", ".join(
            match_score_column(c.name) for c in params.comparisons
        )
        parts.append(f"  TO_JSON_STRING(STRUCT({struct_fields})) AS {MATCH_DETAIL},")

    parts.append(f"  CURRENT_TIMESTAMP() AS {MATCHED_AT}")
    parts.append("FROM scored")
    parts.append(f"WHERE {MATCH_TOTAL_SCORE} >= {params.threshold.min_score}")

    return SQLExpression.from_raw("\n".join(parts))


def _build_fs_level_case(
    comp: ComparisonDef,
    tf_table: str | None,
) -> str:
    """Build COALESCE(CASE ... END, 0.0) for a Fellegi-Sunter comparison."""
    if not comp.levels:
        return "0.0"

    lines: list[str] = ["COALESCE(CASE"]

    for level in comp.levels:
        if level.sql_expr is not None:
            if level.tf_adjusted and tf_table:
                lines.append(
                    f"      WHEN {level.sql_expr} THEN\n"
                    f"        LOG({level.m}) / LOG(2) - LOG(GREATEST({level.u}, "
                    f"COALESCE(tf_{comp.name}.{TERM_FREQUENCY_RATIO}, {comp.tf_minimum_u}))) / LOG(2)"
                )
            else:
                lines.append(f"      WHEN {level.sql_expr} THEN {level.log_weight}")
        else:
            lines.append(f"      ELSE {level.log_weight}")

    lines.append("    END, 0.0)")
    return "\n".join(lines)


def build_fellegi_sunter_sql(params: FellegiSunterParams) -> SQLExpression:
    """Build Fellegi-Sunter probabilistic scoring SQL.

    Uses log-likelihood ratio scoring with comparison levels.
    Total score = log_prior_odds + sum of per-comparison log-weights.
    Match probability = 2^score / (1 + 2^score) with overflow clamping.
    """
    if not params.comparisons:
        raise ValueError(
            f"Tier '{params.tier_name}' has no comparisons defined. "
            "At least one comparison is required for Fellegi-Sunter scoring."
        )

    parts: list[str] = []

    parts.append(f"CREATE OR REPLACE TABLE `{params.matches_table}` AS")
    parts.append("")
    parts.append("WITH scored AS (")
    parts.append("  SELECT")
    parts.append(f"    c.{LEFT_ENTITY_UID},")
    parts.append(f"    c.{RIGHT_ENTITY_UID},")
    parts.append("")

    # Per-comparison level assignment and log-weight
    for comp in params.comparisons:
        case_expr = _build_fs_level_case(comp, params.tf_table)
        parts.append(f"    {case_expr} AS {match_log_weight_column(comp.name)},")

    parts.append("")

    # Total log-likelihood ratio score
    parts.append("    (")
    parts.append(f"      {params.log_prior_odds}")

    for comp in params.comparisons:
        case_expr = _build_fs_level_case(comp, params.tf_table)
        parts.append(f"      + {case_expr}")

    # Soft signal bonuses
    for ss in params.soft_signals:
        parts.append(
            f"      + CASE WHEN {ss.sql_condition} "
            f"THEN {ss.bonus} ELSE 0.0 END"
        )

    # Hard negative penalties
    for hn in params.hard_negatives:
        if hn.action == "penalize":
            parts.append(
                f"      + CASE WHEN {hn.sql_condition} "
                f"THEN {hn.penalty} ELSE 0.0 END"
            )

    parts.append(f"    ) AS {MATCH_TOTAL_SCORE}")
    parts.append("")

    # FROM clause — same INT64 join pattern as sum scoring.
    # PERF: entity_uid is INT64 throughout the pipeline (FARM_FINGERPRINT-based).
    # Both INNER JOINs are 8-byte hash probes — minimal overhead.
    parts.append(f"  FROM `{params.candidates_table}` c")
    parts.append(
        f"  INNER JOIN `{params.source_table}` l ON c.{LEFT_ENTITY_UID} = l.{ENTITY_UID}"
    )
    parts.append(
        f"  INNER JOIN `{params.source_table}` r ON c.{RIGHT_ENTITY_UID} = r.{ENTITY_UID}"
    )

    # TF joins
    parts.extend(_build_tf_joins(params.comparisons, params.tf_table))

    # WHERE clause — disqualify filters eliminate pairs before log-weight computation.
    parts.append("  WHERE 1=1")
    parts.extend(_build_disqualify_filters(params.hard_negatives))
    parts.append(")")
    parts.append("")

    # Final SELECT
    parts.append("SELECT")
    parts.append(f"  {LEFT_ENTITY_UID},")
    parts.append(f"  {RIGHT_ENTITY_UID},")
    parts.append(f"  {MATCH_TOTAL_SCORE},")
    parts.append(f"  {params.tier_index} AS {MATCH_TIER_PRIORITY},")
    parts.append(f"  '{params.tier_name}' AS {MATCH_TIER_NAME},")

    # Match confidence with overflow clamping
    parts.append("  ROUND(CASE")
    parts.append(f"    WHEN {MATCH_TOTAL_SCORE} > 50 THEN 1.0")
    parts.append(f"    WHEN {MATCH_TOTAL_SCORE} < -50 THEN 0.0")
    parts.append(
        f"    ELSE SAFE_DIVIDE(POW(2.0, {MATCH_TOTAL_SCORE}), 1.0 + POW(2.0, {MATCH_TOTAL_SCORE}))"
    )
    parts.append(f"  END, 4) AS {MATCH_CONFIDENCE},")

    for comp in params.comparisons:
        parts.append(f"  {match_log_weight_column(comp.name)},")

    if params.audit_trail_enabled:
        struct_fields = ", ".join(
            match_log_weight_column(c.name) for c in params.comparisons
        )
        parts.append(f"  TO_JSON_STRING(STRUCT({struct_fields})) AS {MATCH_DETAIL},")

    parts.append(f"  CURRENT_TIMESTAMP() AS {MATCHED_AT}")
    parts.append("FROM scored")

    if params.threshold.match_threshold is not None:
        parts.append(f"WHERE {MATCH_TOTAL_SCORE} >= {params.threshold.match_threshold}")
    else:
        parts.append(f"WHERE {MATCH_TOTAL_SCORE} >= {params.threshold.min_score}")

    return SQLExpression.from_raw("\n".join(parts))


def build_init_matches_sql(target_table: str, source_table: str) -> SQLExpression:
    """Build SQL to initialize the all_matches table from the first tier's matches.

    Creates the accumulated matches table with the same schema as the
    per-tier matches table.
    """
    sql = (
        f"CREATE OR REPLACE TABLE `{target_table}` AS\n"
        f"SELECT * FROM `{source_table}`"
    )
    return SQLExpression.from_raw(sql)


def build_accumulate_matches_sql(target_table: str, source_table: str) -> SQLExpression:
    """Build SQL to accumulate matches from a subsequent tier.

    Inserts new matches from the current tier into the accumulated
    matches table, avoiding duplicates.
    """
    sql = (
        f"INSERT INTO `{target_table}`\n"
        f"SELECT s.* FROM `{source_table}` s\n"
        f"LEFT JOIN `{target_table}` t\n"
        f"  ON s.left_entity_uid = t.left_entity_uid\n"
        f"  AND s.right_entity_uid = t.right_entity_uid\n"
        f"WHERE t.left_entity_uid IS NULL"
    )
    return SQLExpression.from_raw(sql)
