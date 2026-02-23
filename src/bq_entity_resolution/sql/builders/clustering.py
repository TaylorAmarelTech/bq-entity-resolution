"""SQL builder for clustering (replaces cluster_assignment.sql.j2 + cluster_quality_metrics.sql.j2).

Generates SQL for:
1. Connected components via iterative minimum-cluster-id propagation
2. Cluster quality metrics (singleton ratio, max size, confidence stats)
"""

from __future__ import annotations

from dataclasses import dataclass, field

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

    # Step 1: Initialize ALL entities as their own cluster (singletons)
    lines.append(
        f"CREATE OR REPLACE TABLE `{params.cluster_table}` AS"
    )
    lines.append("SELECT DISTINCT entity_uid, entity_uid AS cluster_id")
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
    lines.append("    c1.entity_uid AS uid1,")
    lines.append("    c1.cluster_id AS cid1,")
    lines.append("    c2.entity_uid AS uid2,")
    lines.append("    c2.cluster_id AS cid2")
    lines.append(f"  FROM `{params.all_matches_table}` m")
    lines.append(
        f"  JOIN `{params.cluster_table}` c1 ON m.l_entity_uid = c1.entity_uid"
    )
    lines.append(
        f"  JOIN `{params.cluster_table}` c2 ON m.r_entity_uid = c2.entity_uid;"
    )
    lines.append("")

    # Compute new cluster_id = minimum reachable cluster_id
    lines.append("  CREATE OR REPLACE TEMP TABLE _new_clusters AS")
    lines.append("  SELECT")
    lines.append("    entity_uid,")
    lines.append("    LEAST(")
    lines.append("      cluster_id,")
    lines.append("      COALESCE(min_neighbor, cluster_id)")
    lines.append("    ) AS cluster_id")
    lines.append("  FROM (")
    lines.append("    SELECT")
    lines.append("      c.entity_uid,")
    lines.append("      c.cluster_id,")
    lines.append("      MIN(")
    lines.append("        LEAST(")
    lines.append("          COALESCE(e.cid1, c.cluster_id),")
    lines.append("          COALESCE(e.cid2, c.cluster_id)")
    lines.append("        )")
    lines.append("      ) AS min_neighbor")
    lines.append(f"    FROM `{params.cluster_table}` c")
    lines.append("    LEFT JOIN _edge_clusters e")
    lines.append("      ON c.entity_uid = e.uid1 OR c.entity_uid = e.uid2")
    lines.append("    GROUP BY c.entity_uid, c.cluster_id")
    lines.append("  );")
    lines.append("")

    # Count how many changed
    lines.append("  SET rows_updated = (")
    lines.append("    SELECT COUNT(*)")
    lines.append("    FROM _new_clusters n")
    lines.append(f"    JOIN `{params.cluster_table}` o USING (entity_uid)")
    lines.append("    WHERE n.cluster_id != o.cluster_id")
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


def build_cluster_quality_metrics_sql(
    params: ClusterMetricsParams,
) -> SQLExpression:
    """Build cluster quality metrics SQL.

    Computes cluster health statistics for monitoring.
    """
    lines: list[str] = []

    lines.append("WITH cluster_stats AS (")
    lines.append("  SELECT")
    lines.append("    cluster_id,")
    lines.append("    COUNT(*) AS cluster_size,")
    lines.append("    COUNT(DISTINCT source_name) AS source_count")
    lines.append(f"  FROM `{params.cluster_table}`")
    lines.append("  GROUP BY cluster_id")
    lines.append("),")
    lines.append("confidence_stats AS (")
    lines.append("  SELECT")
    lines.append("    AVG(match_confidence) AS avg_match_confidence,")
    lines.append("    MIN(match_confidence) AS min_match_confidence")
    lines.append(f"  FROM `{params.matches_table}`")
    lines.append(")")
    lines.append("SELECT")
    lines.append("  COUNT(*) AS cluster_count,")
    lines.append(
        "  SUM(CASE WHEN cluster_size = 1 THEN 1 ELSE 0 END) AS singleton_count,"
    )
    lines.append("  SAFE_DIVIDE(")
    lines.append(
        "    SUM(CASE WHEN cluster_size = 1 THEN 1 ELSE 0 END),"
    )
    lines.append("    COUNT(*)")
    lines.append("  ) AS singleton_ratio,")
    lines.append("  MAX(cluster_size) AS max_cluster_size,")
    lines.append("  AVG(cluster_size) AS avg_cluster_size,")
    lines.append(
        "  APPROX_QUANTILES(cluster_size, 2)[OFFSET(1)] AS median_cluster_size,"
    )
    lines.append("  AVG(source_count) AS avg_source_diversity,")
    lines.append("  cs.avg_match_confidence,")
    lines.append("  cs.min_match_confidence,")
    lines.append("  CURRENT_TIMESTAMP() AS computed_at")
    lines.append("FROM cluster_stats")
    lines.append("CROSS JOIN confidence_stats cs")
    lines.append(
        "GROUP BY cs.avg_match_confidence, cs.min_match_confidence"
    )

    return SQLExpression.from_raw("\n".join(lines))
