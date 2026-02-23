"""SQL builder for multi-path blocking (replaces multi_path_candidates.sql.j2).

Generates SQL to create candidate pairs from multiple blocking paths:
- Intra-batch: new records vs new records
- Cross-batch: new records vs gold canonicals
- Per-path candidate limits
- Deduplication across paths
- Prior-tier exclusion
- LSH bucket keys from embeddings
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bq_entity_resolution.columns import (
    ENTITY_UID,
    LEFT_ENTITY_UID,
    RIGHT_ENTITY_UID,
    BLOCKING_PATH,
    SOURCE_NAME,
    BLOCKING_METRIC_TIER_NAME,
    BLOCKING_METRIC_TOTAL_RECORDS,
    BLOCKING_METRIC_CANDIDATE_PAIRS,
    BLOCKING_METRIC_MATCHED_PAIRS,
    BLOCKING_METRIC_PRECISION,
    BLOCKING_METRIC_REDUCTION_RATIO,
    BLOCKING_METRIC_COMPUTED_AT,
)
from bq_entity_resolution.sql.expression import SQLExpression


@dataclass(frozen=True)
class BlockingPath:
    """A single blocking path definition."""
    index: int
    keys: list[str] = field(default_factory=list)
    lsh_keys: list[str] = field(default_factory=list)
    candidate_limit: int = 0


@dataclass(frozen=True)
class BlockingParams:
    """Parameters for blocking SQL generation."""
    target_table: str
    source_table: str
    blocking_paths: list[BlockingPath]
    tier_name: str
    cross_batch: bool = False
    canonical_table: str | None = None
    excluded_pairs_table: str | None = None
    lsh_table: str | None = None
    link_type: str | None = None  # 'link_and_dedupe', 'dedupe_only', 'link_only'
    cluster_by: list[str] = field(default_factory=list)
    partition_by: str | None = None


def _build_join_conditions(
    path: BlockingPath,
    is_cross_batch: bool,
    link_type: str | None,
) -> list[str]:
    """Build the ON clause conditions for a blocking join."""
    conditions: list[str] = []

    # Entity ordering (intra-batch only)
    if not is_cross_batch:
        conditions.append("l.entity_uid < r.entity_uid")
    else:
        conditions.append("l.entity_uid != r.entity_uid")

    # Link type filter
    if link_type == "dedupe_only":
        conditions.append("l.source_name = r.source_name")
    elif link_type == "link_only":
        conditions.append("l.source_name != r.source_name")

    # Blocking keys
    for key in path.keys:
        conditions.append(f"l.{key} = r.{key}")
        conditions.append(f"l.{key} IS NOT NULL")

    # LSH keys
    for key in path.lsh_keys:
        conditions.append(f"l.{key} = r.{key}")
        conditions.append(f"l.{key} IS NOT NULL")

    return conditions


def _build_path_cte(
    path: BlockingPath,
    tier_name: str,
    source_table: str,
    is_cross_batch: bool,
    canonical_table: str | None,
    lsh_table: str | None,
    link_type: str | None,
) -> str:
    """Build a single blocking path CTE."""
    prefix = "cross" if is_cross_batch else "intra"
    cte_name = f"{prefix}_path_{path.index}"
    path_label = f"{tier_name}_{prefix}_{path.index}"

    # Determine left and right table sources
    has_lsh = bool(path.lsh_keys and lsh_table)
    if has_lsh:
        left_source = "source_with_lsh"
        right_source = "canonical_with_lsh" if is_cross_batch else "source_with_lsh"
    else:
        left_source = f"`{source_table}`"
        right_source = f"`{canonical_table}`" if is_cross_batch else f"`{source_table}`"

    conditions = _build_join_conditions(path, is_cross_batch, link_type)
    on_clause = "\n    AND ".join(conditions)

    lines = [
        f"{cte_name} AS (",
        f"  SELECT",
        f"    l.{ENTITY_UID} AS {LEFT_ENTITY_UID},",
        f"    r.{ENTITY_UID} AS {RIGHT_ENTITY_UID},",
        f"    '{path_label}' AS {BLOCKING_PATH}",
        f"  FROM {left_source} l",
        f"  INNER JOIN {right_source} r",
        f"    ON {on_clause}",
    ]

    if path.candidate_limit > 0:
        lines.append(f"  QUALIFY ROW_NUMBER() OVER (")
        lines.append(f"    PARTITION BY l.entity_uid")
        lines.append(f"    ORDER BY r.entity_uid")
        lines.append(f"  ) <= {path.candidate_limit}")

    lines.append(f")")

    return "\n".join(lines)


def build_blocking_sql(params: BlockingParams) -> SQLExpression:
    """Build multi-path blocking SQL.

    Generates candidate pairs from multiple blocking paths with
    deduplication and optional prior-tier exclusion.
    """
    parts: list[str] = []

    parts.append(f"CREATE OR REPLACE TABLE `{params.target_table}`")
    if params.partition_by:
        parts.append(f"PARTITION BY {params.partition_by}")
    if params.cluster_by:
        parts.append(f"CLUSTER BY {', '.join(params.cluster_by)}")
    parts.append("AS")
    parts.append("")
    parts.append("WITH")

    # LSH pre-join for source
    if params.lsh_table:
        parts.append("source_with_lsh AS (")
        parts.append("  SELECT")
        parts.append("    s.*,")
        parts.append(f"    lsh.* EXCEPT({ENTITY_UID})")
        parts.append(f"  FROM `{params.source_table}` s")
        parts.append(f"  LEFT JOIN `{params.lsh_table}` lsh USING ({ENTITY_UID})")
        parts.append("),")

    # LSH pre-join for canonical (cross-batch only)
    if params.cross_batch and params.lsh_table and params.canonical_table:
        parts.append("canonical_with_lsh AS (")
        parts.append("  SELECT")
        parts.append("    c.*,")
        parts.append(f"    lsh.* EXCEPT({ENTITY_UID})")
        parts.append(f"  FROM `{params.canonical_table}` c")
        parts.append(f"  LEFT JOIN `{params.lsh_table}` lsh USING ({ENTITY_UID})")
        parts.append("),")

    # Intra-batch path CTEs
    for path in params.blocking_paths:
        cte = _build_path_cte(
            path=path,
            tier_name=params.tier_name,
            source_table=params.source_table,
            is_cross_batch=False,
            canonical_table=None,
            lsh_table=params.lsh_table,
            link_type=params.link_type,
        )
        parts.append(f"{cte},")

    # Cross-batch path CTEs
    if params.cross_batch and params.canonical_table:
        for path in params.blocking_paths:
            cte = _build_path_cte(
                path=path,
                tier_name=params.tier_name,
                source_table=params.source_table,
                is_cross_batch=True,
                canonical_table=params.canonical_table,
                lsh_table=params.lsh_table,
                link_type=params.link_type,
            )
            parts.append(f"{cte},")

    # Union all paths
    parts.append("all_candidates AS (")
    union_parts: list[str] = []
    for path in params.blocking_paths:
        union_parts.append(
            f"  SELECT {LEFT_ENTITY_UID}, {RIGHT_ENTITY_UID}, {BLOCKING_PATH} "
            f"FROM intra_path_{path.index}"
        )
    if params.cross_batch and params.canonical_table:
        for path in params.blocking_paths:
            union_parts.append(
                f"  SELECT {LEFT_ENTITY_UID}, {RIGHT_ENTITY_UID}, {BLOCKING_PATH} "
                f"FROM cross_path_{path.index}"
            )

    if not union_parts:
        # Safety: no paths
        parts.append(
            f"  SELECT CAST(NULL AS INT64) AS {LEFT_ENTITY_UID}, "
            f"CAST(NULL AS INT64) AS {RIGHT_ENTITY_UID}, "
            f"CAST(NULL AS STRING) AS {BLOCKING_PATH} WHERE FALSE"
        )
    else:
        parts.append("\n  UNION ALL\n".join(union_parts))

    parts.append("),")

    # Deduplicate
    parts.append("deduplicated AS (")
    parts.append("  SELECT DISTINCT")
    parts.append(f"    {LEFT_ENTITY_UID},")
    parts.append(f"    {RIGHT_ENTITY_UID}")
    parts.append("  FROM all_candidates")
    parts.append(f"  WHERE {LEFT_ENTITY_UID} IS NOT NULL")
    parts.append(")")
    parts.append("")

    # Final select with optional exclusion
    parts.append(f"SELECT d.{LEFT_ENTITY_UID}, d.{RIGHT_ENTITY_UID}")
    parts.append("FROM deduplicated d")

    if params.excluded_pairs_table:
        parts.append(f"LEFT JOIN `{params.excluded_pairs_table}` e")
        parts.append(
            f"  ON (d.{LEFT_ENTITY_UID} = e.{LEFT_ENTITY_UID} AND d.{RIGHT_ENTITY_UID} = e.{RIGHT_ENTITY_UID})"
        )
        parts.append(
            f"  OR (d.{LEFT_ENTITY_UID} = e.{RIGHT_ENTITY_UID} AND d.{RIGHT_ENTITY_UID} = e.{LEFT_ENTITY_UID})"
        )
        parts.append(f"WHERE e.{LEFT_ENTITY_UID} IS NULL")

    return SQLExpression.from_raw("\n".join(parts))


# ---------------------------------------------------------------------------
# Blocking metrics
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BlockingMetricsParams:
    """Parameters for blocking evaluation metrics."""
    candidates_table: str
    matches_table: str
    source_table: str
    tier_name: str


def build_blocking_metrics_sql(params: BlockingMetricsParams) -> SQLExpression:
    """Build SQL to compute blocking evaluation metrics.

    Measures how well blocking reduces the comparison space
    while retaining true matches.
    """
    sql = (
        f"SELECT\n"
        f"  '{params.tier_name}' AS {BLOCKING_METRIC_TIER_NAME},\n"
        f"  (SELECT COUNT(*) FROM `{params.source_table}`) AS {BLOCKING_METRIC_TOTAL_RECORDS},\n"
        f"  (SELECT COUNT(*) FROM `{params.candidates_table}`) AS {BLOCKING_METRIC_CANDIDATE_PAIRS},\n"
        f"  (SELECT COUNT(*) FROM `{params.matches_table}`) AS {BLOCKING_METRIC_MATCHED_PAIRS},\n"
        f"  SAFE_DIVIDE(\n"
        f"    (SELECT COUNT(*) FROM `{params.matches_table}`),\n"
        f"    (SELECT COUNT(*) FROM `{params.candidates_table}`)\n"
        f"  ) AS {BLOCKING_METRIC_PRECISION},\n"
        f"  SAFE_DIVIDE(\n"
        f"    (SELECT COUNT(*) FROM `{params.candidates_table}`),\n"
        f"    (SELECT COUNT(*) FROM `{params.source_table}`) "
        f"* ((SELECT COUNT(*) FROM `{params.source_table}`) - 1) / 2\n"
        f"  ) AS {BLOCKING_METRIC_REDUCTION_RATIO},\n"
        f"  CURRENT_TIMESTAMP() AS {BLOCKING_METRIC_COMPUTED_AT}"
    )
    return SQLExpression.from_raw(sql)
