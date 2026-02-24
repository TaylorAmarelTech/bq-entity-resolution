"""SQL builder for clustering (replaces cluster_assignment.sql.j2 + cluster_quality_metrics.sql.j2).

Generates SQL for:
1. Connected components via iterative minimum-cluster-id propagation
2. Cluster quality metrics (singleton ratio, max size, confidence stats)

Clustering Performance Notes
=============================
Connected components runs iteratively until convergence. Each iteration:
  1. JOINs cluster_table × all_matches on entity_uid (INT64) — fast
  2. Computes MIN(cluster_id) for each entity via GROUP BY — INT64 agg
  3. Replaces cluster_table with updated assignments

All columns involved are INT64:
  - entity_uid: INT64 (FARM_FINGERPRINT from staging)
  - cluster_id: INT64 (initialized to entity_uid, propagated via MIN)
  - left_entity_uid / right_entity_uid: INT64

This means ALL JOINs and aggregations in the clustering loop operate on
fixed-width 8-byte integers — the optimal case for BigQuery's columnar
storage engine. No STRING comparisons occur during clustering.

The OR in the LEFT JOIN (ON uid = uid1 OR uid = uid2) may prevent BQ from
using a pure hash-join. For very large match tables (>100M edges), consider
normalizing edges into a symmetric edge list (uid1→uid2 UNION uid2→uid1)
to enable two separate equi-joins instead of one OR-join.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bq_entity_resolution.columns import (
    ENTITY_UID,
    CLUSTER_ID,
    SOURCE_NAME,
    LEFT_ENTITY_UID,
    RIGHT_ENTITY_UID,
    MATCH_CONFIDENCE,
    CLUSTER_UID1,
    CLUSTER_CID1,
    CLUSTER_UID2,
    CLUSTER_CID2,
    CLUSTER_MIN_NEIGHBOR,
    CLUSTER_METRIC_COUNT,
    CLUSTER_METRIC_SINGLETON_COUNT,
    CLUSTER_METRIC_SINGLETON_RATIO,
    CLUSTER_METRIC_MAX_SIZE,
    CLUSTER_METRIC_AVG_SIZE,
    CLUSTER_METRIC_MEDIAN_SIZE,
    CLUSTER_METRIC_AVG_SOURCE_DIVERSITY,
    CLUSTER_METRIC_AVG_CONFIDENCE,
    CLUSTER_METRIC_MIN_CONFIDENCE,
    CLUSTER_METRIC_COMPUTED_AT,
)
from bq_entity_resolution.sql.expression import SQLExpression


@dataclass(frozen=True)
class ClusteringParams:
    """Parameters for cluster assignment SQL generation."""
    all_matches_table: str
    cluster_table: str
    source_table: str
    max_iterations: int = 20
    cluster_by: list[str] = field(default_factory=lambda: ["entity_uid"])


@dataclass(frozen=True)
class ClusterMetricsParams:
    """Parameters for cluster quality metrics SQL generation."""
    cluster_table: str
    matches_table: str
    source_table: str = ""  # Featured table with source_name column


def build_cluster_assignment_sql(params: ClusteringParams) -> SQLExpression:
    """Build connected components cluster assignment SQL.

    Uses BigQuery scripting (DECLARE/WHILE) to iteratively propagate
    minimum cluster_id through match edges until convergence.

    Note: This uses BQ scripting which is dialect-specific. For DuckDB,
    the iteration loop must be handled in Python.
    """
    lines: list[str] = []

    # Variable declarations
    lines.append("DECLARE iteration INT64 DEFAULT 0;")
    lines.append("DECLARE rows_updated INT64 DEFAULT 1;")
    lines.append("")

    # Step 1: Initialize ALL entities as their own cluster (singletons).
    # PERF: entity_uid is INT64, so cluster_id starts as INT64. All subsequent
    # MIN/LEAST operations stay in INT64 space — no type conversions needed.
    lines.append(
        f"CREATE OR REPLACE TABLE `{params.cluster_table}` AS"
    )
    lines.append(f"SELECT DISTINCT {ENTITY_UID}, {ENTITY_UID} AS {CLUSTER_ID}")
    lines.append(f"FROM `{params.source_table}`;")
    lines.append("")

    # Step 2: Iterative propagation loop
    lines.append(
        f"WHILE rows_updated > 0 AND iteration < {params.max_iterations} DO"
    )
    lines.append("")

    # Build edge list with current cluster assignments
    lines.append("  CREATE OR REPLACE TEMP TABLE _edge_clusters AS")
    lines.append("  SELECT")
    lines.append(f"    c1.{ENTITY_UID} AS {CLUSTER_UID1},")
    lines.append(f"    c1.{CLUSTER_ID} AS {CLUSTER_CID1},")
    lines.append(f"    c2.{ENTITY_UID} AS {CLUSTER_UID2},")
    lines.append(f"    c2.{CLUSTER_ID} AS {CLUSTER_CID2}")
    lines.append(f"  FROM `{params.all_matches_table}` m")
    lines.append(
        f"  JOIN `{params.cluster_table}` c1 ON m.{LEFT_ENTITY_UID} = c1.{ENTITY_UID}"
    )
    lines.append(
        f"  JOIN `{params.cluster_table}` c2 ON m.{RIGHT_ENTITY_UID} = c2.{ENTITY_UID};"
    )
    lines.append("")

    # Compute new cluster_id = minimum reachable cluster_id
    lines.append("  CREATE OR REPLACE TEMP TABLE _new_clusters AS")
    lines.append("  SELECT")
    lines.append(f"    {ENTITY_UID},")
    lines.append("    LEAST(")
    lines.append(f"      {CLUSTER_ID},")
    lines.append(f"      COALESCE({CLUSTER_MIN_NEIGHBOR}, {CLUSTER_ID})")
    lines.append(f"    ) AS {CLUSTER_ID}")
    lines.append("  FROM (")
    lines.append("    SELECT")
    lines.append(f"      c.{ENTITY_UID},")
    lines.append(f"      c.{CLUSTER_ID},")
    lines.append("      MIN(")
    lines.append("        LEAST(")
    lines.append(f"          COALESCE(e.{CLUSTER_CID1}, c.{CLUSTER_ID}),")
    lines.append(f"          COALESCE(e.{CLUSTER_CID2}, c.{CLUSTER_ID})")
    lines.append("        )")
    lines.append(f"      ) AS {CLUSTER_MIN_NEIGHBOR}")
    lines.append(f"    FROM `{params.cluster_table}` c")
    lines.append("    LEFT JOIN _edge_clusters e")
    lines.append(f"      ON c.{ENTITY_UID} = e.{CLUSTER_UID1} OR c.{ENTITY_UID} = e.{CLUSTER_UID2}")
    lines.append(f"    GROUP BY c.{ENTITY_UID}, c.{CLUSTER_ID}")
    lines.append("  );")
    lines.append("")

    # Count how many changed
    lines.append("  SET rows_updated = (")
    lines.append("    SELECT COUNT(*)")
    lines.append("    FROM _new_clusters n")
    lines.append(f"    JOIN `{params.cluster_table}` o USING ({ENTITY_UID})")
    lines.append(f"    WHERE n.{CLUSTER_ID} != o.{CLUSTER_ID}")
    lines.append("  );")
    lines.append("")

    # Replace cluster table
    lines.append(f"  CREATE OR REPLACE TABLE `{params.cluster_table}` AS")
    lines.append("  SELECT * FROM _new_clusters;")
    lines.append("")
    lines.append("  SET iteration = iteration + 1;")
    lines.append("")
    lines.append("END WHILE;")

    return SQLExpression.from_raw("\n".join(lines))


@dataclass(frozen=True)
class IncrementalClusteringParams:
    """Parameters for incremental cluster assignment."""
    all_matches_table: str
    cluster_table: str
    source_table: str
    canonical_table: str
    max_iterations: int = 20


@dataclass(frozen=True)
class PopulateCanonicalIndexParams:
    """Parameters for canonical index population."""
    canonical_table: str
    source_table: str
    cluster_table: str


def build_incremental_cluster_sql(
    params: IncrementalClusteringParams,
) -> SQLExpression:
    """Build incremental cluster assignment SQL.

    Initializes from prior cluster assignments (canonical_index) combined
    with new entities from the current batch as singletons, then propagates
    minimum cluster_id through ALL match edges until convergence.
    """
    lines: list[str] = []

    lines.append("DECLARE iteration INT64 DEFAULT 0;")
    lines.append("DECLARE rows_updated INT64 DEFAULT 1;")
    lines.append("")

    # Step 1: Initialize from prior entities + new singletons
    lines.append(
        f"CREATE OR REPLACE TABLE `{params.cluster_table}` AS"
    )
    lines.append(
        f"SELECT {ENTITY_UID}, {CLUSTER_ID} FROM `{params.canonical_table}`"
    )
    lines.append("UNION ALL")
    lines.append(f"SELECT DISTINCT {ENTITY_UID}, {ENTITY_UID} AS {CLUSTER_ID}")
    lines.append(f"FROM `{params.source_table}`")
    lines.append(
        f"WHERE {ENTITY_UID} NOT IN "
        f"(SELECT {ENTITY_UID} FROM `{params.canonical_table}`);"
    )
    lines.append("")

    # Step 2: Iterative propagation
    lines.append(
        f"WHILE rows_updated > 0 AND iteration < {params.max_iterations} DO"
    )
    lines.append("")

    lines.append("  CREATE OR REPLACE TEMP TABLE _edge_clusters AS")
    lines.append("  SELECT")
    lines.append(f"    c1.{ENTITY_UID} AS {CLUSTER_UID1},")
    lines.append(f"    c1.{CLUSTER_ID} AS {CLUSTER_CID1},")
    lines.append(f"    c2.{ENTITY_UID} AS {CLUSTER_UID2},")
    lines.append(f"    c2.{CLUSTER_ID} AS {CLUSTER_CID2}")
    lines.append(f"  FROM `{params.all_matches_table}` m")
    lines.append(
        f"  JOIN `{params.cluster_table}` c1 ON m.{LEFT_ENTITY_UID} = c1.{ENTITY_UID}"
    )
    lines.append(
        f"  JOIN `{params.cluster_table}` c2 ON m.{RIGHT_ENTITY_UID} = c2.{ENTITY_UID};"
    )
    lines.append("")

    lines.append("  CREATE OR REPLACE TEMP TABLE _new_clusters AS")
    lines.append("  SELECT")
    lines.append(f"    {ENTITY_UID},")
    lines.append("    LEAST(")
    lines.append(f"      {CLUSTER_ID},")
    lines.append(f"      COALESCE({CLUSTER_MIN_NEIGHBOR}, {CLUSTER_ID})")
    lines.append(f"    ) AS {CLUSTER_ID}")
    lines.append("  FROM (")
    lines.append("    SELECT")
    lines.append(f"      c.{ENTITY_UID},")
    lines.append(f"      c.{CLUSTER_ID},")
    lines.append("      MIN(")
    lines.append("        LEAST(")
    lines.append(f"          COALESCE(e.{CLUSTER_CID1}, c.{CLUSTER_ID}),")
    lines.append(f"          COALESCE(e.{CLUSTER_CID2}, c.{CLUSTER_ID})")
    lines.append("        )")
    lines.append(f"      ) AS {CLUSTER_MIN_NEIGHBOR}")
    lines.append(f"    FROM `{params.cluster_table}` c")
    lines.append("    LEFT JOIN _edge_clusters e")
    lines.append(f"      ON c.{ENTITY_UID} = e.{CLUSTER_UID1} OR c.{ENTITY_UID} = e.{CLUSTER_UID2}")
    lines.append(f"    GROUP BY c.{ENTITY_UID}, c.{CLUSTER_ID}")
    lines.append("  );")
    lines.append("")

    lines.append("  SET rows_updated = (")
    lines.append("    SELECT COUNT(*)")
    lines.append("    FROM _new_clusters n")
    lines.append(f"    JOIN `{params.cluster_table}` o USING ({ENTITY_UID})")
    lines.append(f"    WHERE n.{CLUSTER_ID} != o.{CLUSTER_ID}")
    lines.append("  );")
    lines.append("")

    lines.append(f"  CREATE OR REPLACE TABLE `{params.cluster_table}` AS")
    lines.append("  SELECT * FROM _new_clusters;")
    lines.append("")
    lines.append("  SET iteration = iteration + 1;")
    lines.append("")
    lines.append("END WHILE;")

    return SQLExpression.from_raw("\n".join(lines))


@dataclass(frozen=True)
class CanonicalIndexInitParams:
    """Parameters for canonical index initialization."""
    canonical_table: str
    source_table: str  # Featured table (schema reference)
    cluster_by: list[str] = field(default_factory=lambda: ["entity_uid"])
    partition_by: str | None = None


def build_canonical_index_init_sql(
    params: CanonicalIndexInitParams,
) -> SQLExpression:
    """Build SQL to create canonical_index table if it doesn't exist.

    Creates an empty table with the same schema as the featured table
    plus a cluster_id column. Uses CREATE TABLE IF NOT EXISTS so it's
    safe to call on every run — only creates on the first execution.
    """
    lines: list[str] = []
    lines.append(
        f"CREATE TABLE IF NOT EXISTS `{params.canonical_table}`"
    )
    if params.partition_by:
        lines.append(f"PARTITION BY {params.partition_by}")
    if params.cluster_by:
        lines.append(f"CLUSTER BY {', '.join(params.cluster_by)}")
    lines.append("AS")
    lines.append(f"SELECT *, {ENTITY_UID} AS {CLUSTER_ID}")
    lines.append(f"FROM `{params.source_table}`")
    lines.append("WHERE FALSE;")

    return SQLExpression.from_raw("\n".join(lines))


def build_populate_canonical_index_sql(
    params: PopulateCanonicalIndexParams,
) -> SQLExpression:
    """Build SQL to upsert current batch entities into canonical_index.

    Updates cluster_ids for prior entities that were re-clustered,
    and inserts new entities from the current batch.

    PERF: All JOINs use entity_uid (INT64) and cluster_id (INT64).
    The UPDATE...FROM pattern is efficient in BQ for targeted row updates.
    The NOT IN subquery on INT64 is well-optimized by BQ's query engine.
    For canonical tables >100M rows, consider LEFT JOIN WHERE IS NULL instead.
    """
    sql = (
        f"-- Update cluster_ids for prior entities that were re-clustered\n"
        f"-- PERF: UPDATE...FROM on INT64 key — efficient targeted update\n"
        f"UPDATE `{params.canonical_table}` ci\n"
        f"SET {CLUSTER_ID} = cl.{CLUSTER_ID}\n"
        f"FROM `{params.cluster_table}` cl\n"
        f"WHERE ci.{ENTITY_UID} = cl.{ENTITY_UID}\n"
        f"  AND ci.{CLUSTER_ID} != cl.{CLUSTER_ID};\n"
        f"\n"
        f"-- Insert new entities from current batch\n"
        f"-- PERF: NOT IN on INT64 is fast; BQ converts to anti-semi-join\n"
        f"INSERT INTO `{params.canonical_table}`\n"
        f"SELECT f.*, cl.{CLUSTER_ID}\n"
        f"FROM `{params.source_table}` f\n"
        f"JOIN `{params.cluster_table}` cl USING ({ENTITY_UID})\n"
        f"WHERE f.{ENTITY_UID} NOT IN "
        f"(SELECT {ENTITY_UID} FROM `{params.canonical_table}`);"
    )
    return SQLExpression.from_raw(sql)


def build_cluster_quality_metrics_sql(
    params: ClusterMetricsParams,
) -> SQLExpression:
    """Build cluster quality metrics SQL.

    Computes cluster health statistics for monitoring.
    """
    lines: list[str] = []

    # Join cluster table with source table for source_name if available
    if params.source_table:
        lines.append("WITH cluster_stats AS (")
        lines.append("  SELECT")
        lines.append(f"    c.{CLUSTER_ID},")
        lines.append("    COUNT(*) AS cluster_size,")
        lines.append(f"    COUNT(DISTINCT s.{SOURCE_NAME}) AS source_count")
        lines.append(f"  FROM `{params.cluster_table}` c")
        lines.append(f"  LEFT JOIN `{params.source_table}` s")
        lines.append(f"    ON c.{ENTITY_UID} = s.{ENTITY_UID}")
        lines.append(f"  GROUP BY c.{CLUSTER_ID}")
        lines.append("),")
    else:
        lines.append("WITH cluster_stats AS (")
        lines.append("  SELECT")
        lines.append(f"    {CLUSTER_ID},")
        lines.append("    COUNT(*) AS cluster_size,")
        lines.append("    1 AS source_count")
        lines.append(f"  FROM `{params.cluster_table}`")
        lines.append(f"  GROUP BY {CLUSTER_ID}")
        lines.append("),")

    lines.append("confidence_stats AS (")
    lines.append("  SELECT")
    lines.append(f"    AVG({MATCH_CONFIDENCE}) AS {CLUSTER_METRIC_AVG_CONFIDENCE},")
    lines.append(f"    MIN({MATCH_CONFIDENCE}) AS {CLUSTER_METRIC_MIN_CONFIDENCE}")
    lines.append(f"  FROM `{params.matches_table}`")
    lines.append(")")
    lines.append("SELECT")
    lines.append(f"  COUNT(*) AS {CLUSTER_METRIC_COUNT},")
    lines.append(
        f"  SUM(CASE WHEN cluster_size = 1 THEN 1 ELSE 0 END) AS {CLUSTER_METRIC_SINGLETON_COUNT},"
    )
    lines.append("  SAFE_DIVIDE(")
    lines.append(
        "    SUM(CASE WHEN cluster_size = 1 THEN 1 ELSE 0 END),"
    )
    lines.append("    COUNT(*)")
    lines.append(f"  ) AS {CLUSTER_METRIC_SINGLETON_RATIO},")
    lines.append(f"  MAX(cluster_size) AS {CLUSTER_METRIC_MAX_SIZE},")
    lines.append(f"  AVG(cluster_size) AS {CLUSTER_METRIC_AVG_SIZE},")
    lines.append(
        f"  APPROX_QUANTILES(cluster_size, 2)[OFFSET(1)] AS {CLUSTER_METRIC_MEDIAN_SIZE},"
    )
    lines.append(f"  AVG(source_count) AS {CLUSTER_METRIC_AVG_SOURCE_DIVERSITY},")
    lines.append(f"  cs.{CLUSTER_METRIC_AVG_CONFIDENCE},")
    lines.append(f"  cs.{CLUSTER_METRIC_MIN_CONFIDENCE},")
    lines.append(f"  CURRENT_TIMESTAMP() AS {CLUSTER_METRIC_COMPUTED_AT}")
    lines.append("FROM cluster_stats")
    lines.append("CROSS JOIN confidence_stats cs")
    lines.append(
        f"GROUP BY cs.{CLUSTER_METRIC_AVG_CONFIDENCE}, cs.{CLUSTER_METRIC_MIN_CONFIDENCE}"
    )

    return SQLExpression.from_raw("\n".join(lines))
