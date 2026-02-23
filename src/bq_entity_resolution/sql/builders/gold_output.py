"""SQL builder for gold output (replaces gold_output.sql.j2).

Generates the final resolved entities table with:
- Cluster assignment join
- Canonical record election (completeness, recency, or source_priority)
- Match metadata from accumulated matches
- Partitioning and clustering options
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bq_entity_resolution.sql.expression import SQLExpression


@dataclass(frozen=True)
class GoldOutputParams:
    """Parameters for gold output SQL generation."""
    target_table: str
    source_table: str
    cluster_table: str
    matches_table: str
    canonical_method: str = "completeness"  # 'completeness', 'recency', 'source_priority'
    scoring_columns: list[str] = field(default_factory=list)
    source_columns: list[str] = field(default_factory=list)
    passthrough_columns: list[str] = field(default_factory=list)
    include_match_metadata: bool = True
    entity_id_prefix: str = "ent"
    partition_column: str | None = None
    cluster_columns: list[str] = field(default_factory=list)
    source_priority: list[str] = field(default_factory=list)


def build_gold_output_sql(params: GoldOutputParams) -> SQLExpression:
    """Build gold resolved entities output SQL.

    Joins clusters to featured data, elects canonical records,
    and produces the final output with optional match metadata.
    """
    parts: list[str] = []

    parts.append(f"CREATE OR REPLACE TABLE `{params.target_table}`")

    if params.partition_column:
        parts.append(f"PARTITION BY {params.partition_column}")

    if params.cluster_columns:
        cols = ", ".join(params.cluster_columns)
        parts.append(f"CLUSTER BY {cols}")

    parts.append("AS")
    parts.append("")

    # CTE: clustered
    parts.append("WITH clustered AS (")
    parts.append("  SELECT")
    parts.append("    f.*,")
    parts.append("    c.cluster_id")
    parts.append(f"  FROM `{params.source_table}` f")
    parts.append(f"  INNER JOIN `{params.cluster_table}` c USING (entity_uid)")
    parts.append("),")
    parts.append("")

    # CTE: canonical_scores
    parts.append("canonical_scores AS (")
    parts.append("  SELECT")
    parts.append("    entity_uid,")
    parts.append("    cluster_id,")

    if params.canonical_method == "completeness":
        score_terms: list[str] = []
        for col in params.scoring_columns:
            score_terms.append(
                f"CASE WHEN {col} IS NOT NULL THEN 1 ELSE 0 END"
            )
        if score_terms:
            parts.append(f"    ({' + '.join(score_terms)}) AS canonical_score,")
        else:
            parts.append("    0 AS canonical_score,")
    elif params.canonical_method == "recency":
        parts.append("    UNIX_MICROS(_source_updated_at) AS canonical_score,")
    elif params.canonical_method == "source_priority":
        cases: list[str] = []
        for i, src in enumerate(params.source_priority):
            cases.append(f"WHEN '{src}' THEN {1000 - i}")
        case_expr = " ".join(cases)
        parts.append(
            f"    CASE source_name {case_expr} ELSE 0 END AS canonical_score,"
        )

    # Remove trailing comma from last line
    parts[-1] = parts[-1].rstrip(",")
    parts.append("  FROM clustered")
    parts.append("),")
    parts.append("")

    # CTE: canonicals — elect one canonical per cluster
    # Uses ROW_NUMBER for portability across BigQuery and DuckDB
    parts.append("canonicals AS (")
    parts.append("  SELECT cluster_id, entity_uid AS canonical_entity_uid")
    parts.append("  FROM (")
    parts.append("    SELECT")
    parts.append("      cluster_id,")
    parts.append("      entity_uid,")
    parts.append("      ROW_NUMBER() OVER (")
    parts.append(
        "        PARTITION BY cluster_id "
        "ORDER BY canonical_score DESC, entity_uid ASC"
    )
    parts.append("      ) AS rn")
    parts.append("    FROM canonical_scores")
    parts.append("  )")
    parts.append("  WHERE rn = 1")
    parts.append("),")
    parts.append("")

    # CTE: resolved
    parts.append("resolved AS (")
    parts.append("  SELECT")
    parts.append(
        f"    '{params.entity_id_prefix}_' || "
        f"CAST(cl.cluster_id AS STRING) AS resolved_entity_id,"
    )
    parts.append("    cl.cluster_id,")
    parts.append("    can.canonical_entity_uid,")
    parts.append(
        "    (f.entity_uid = can.canonical_entity_uid) AS is_canonical,"
    )

    # Source columns
    for col in params.source_columns:
        parts.append(f"    f.{col},")

    # Passthrough columns
    for col in params.passthrough_columns:
        parts.append(f"    f.{col},")

    parts.append("    f.source_name,")
    parts.append("    f.entity_uid,")
    parts.append("    f._source_updated_at,")
    parts.append("    f._pipeline_loaded_at")
    parts.append("")
    parts.append("  FROM clustered f")
    parts.append("  JOIN canonicals can ON f.cluster_id = can.cluster_id")
    parts.append(
        f"  JOIN `{params.cluster_table}` cl ON f.entity_uid = cl.entity_uid"
    )
    parts.append(")")
    parts.append("")

    # Final SELECT
    parts.append("SELECT")
    parts.append("  r.*")

    if params.include_match_metadata:
        parts.append("  ,m.tier_name AS matched_by_tier")
        parts.append("  ,m.total_score AS match_score")
        parts.append("  ,m.match_confidence")
        parts.append("  ,m.matched_at")

    parts.append("FROM resolved r")

    if params.include_match_metadata:
        parts.append("LEFT JOIN (")
        parts.append("  SELECT")
        parts.append("    l_entity_uid,")
        parts.append("    r_entity_uid,")
        parts.append("    tier_name,")
        parts.append("    total_score,")
        parts.append("    match_confidence,")
        parts.append("    matched_at,")
        parts.append("    ROW_NUMBER() OVER (")
        parts.append("      PARTITION BY r_entity_uid")
        parts.append("      ORDER BY tier_priority ASC, total_score DESC")
        parts.append("    ) AS rn")
        parts.append(f"  FROM `{params.matches_table}`")
        parts.append(") m")
        parts.append("  ON r.entity_uid = m.r_entity_uid AND m.rn = 1")

    return SQLExpression.from_raw("\n".join(parts))
