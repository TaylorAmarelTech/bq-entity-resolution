"""
Reconciliation engine.

Orchestrates post-matching operations:
1. Union all tier matches into a single matches table
2. Assign entity clusters via connected components
3. Elect canonical records per cluster
4. Generate gold output table
"""

from __future__ import annotations

import logging

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.naming import (
    all_matches_table,
    canonical_index_table,
    cluster_table,
    featured_table,
    resolved_table,
)
from bq_entity_resolution.sql.generator import SQLGenerator

logger = logging.getLogger(__name__)


class ReconciliationEngine:
    """Generates SQL for cross-tier reconciliation and gold output."""

    def __init__(self, config: PipelineConfig, sql_gen: SQLGenerator | None = None):
        self.config = config
        self.sql_gen = sql_gen or SQLGenerator()

    def generate_create_matches_table_sql(self) -> str:
        """Generate SQL to create the accumulated matches table."""
        table = all_matches_table(self.config)
        audit_enabled = self.config.reconciliation.output.audit_trail.enabled
        audit_col = "  match_detail STRING,\n" if audit_enabled else ""
        clustering = self.config.scale.matches_clustering
        cluster_clause = ""
        if clustering:
            cluster_clause = f"\nCLUSTER BY {', '.join(clustering)}"
        return (
            f"CREATE TABLE IF NOT EXISTS `{table}` (\n"
            f"  l_entity_uid STRING NOT NULL,\n"
            f"  r_entity_uid STRING NOT NULL,\n"
            f"  total_score FLOAT64,\n"
            f"  tier_priority INT64,\n"
            f"  tier_name STRING,\n"
            f"  match_confidence FLOAT64,\n"
            f"{audit_col}"
            f"  matched_at TIMESTAMP\n"
            f")\n"
            f"PARTITION BY DATE(matched_at)"
            f"{cluster_clause}"
        )

    def generate_cluster_sql(self, cross_batch: bool = False) -> str:
        """Generate connected-components cluster assignment SQL.

        When *cross_batch* is True, uses the incremental template that
        initialises from the canonical_index (prior entities keep prior
        cluster_ids) and only adds new entities as singletons.
        """
        clustering = self.config.reconciliation.clustering
        if cross_batch:
            return self.sql_gen.render(
                "reconciliation/incremental_cluster_assignment.sql.j2",
                all_matches_table=all_matches_table(self.config),
                cluster_table=cluster_table(self.config),
                source_table=featured_table(self.config),
                canonical_table=canonical_index_table(self.config),
                max_iterations=clustering.max_iterations,
            )
        return self.sql_gen.render(
            "reconciliation/cluster_assignment.sql.j2",
            all_matches_table=all_matches_table(self.config),
            cluster_table=cluster_table(self.config),
            source_table=featured_table(self.config),
            max_iterations=clustering.max_iterations,
        )

    def generate_quality_metrics_sql(self) -> str:
        """Generate SQL to compute cluster quality metrics."""
        return self.sql_gen.render(
            "reconciliation/cluster_quality_metrics.sql.j2",
            cluster_table=cluster_table(self.config),
            matches_table=all_matches_table(self.config),
        )

    def generate_gold_output_sql(self, use_canonical: bool = False) -> str:
        """Generate the gold resolved entities output SQL.

        When *use_canonical* is True, reads ALL entities from the
        canonical_index table (which has accumulated entities from all
        prior batches) for cross-batch canonical election.
        """
        recon = self.config.reconciliation
        out = recon.output

        # Build scoring columns for canonical election
        scoring_columns = []
        for source in self.config.sources:
            for col in source.columns:
                scoring_columns.append(col.name)
        for group in self.config.feature_engineering.all_groups():
            if group.enabled:
                for feat in group.features:
                    scoring_columns.append(feat.name)

        # Source columns to include
        source_columns = []
        for source in self.config.sources:
            for col in source.columns:
                if col.name not in source_columns:
                    source_columns.append(col.name)

        passthrough_columns = []
        for source in self.config.sources:
            for col in source.passthrough_columns:
                if col not in passthrough_columns:
                    passthrough_columns.append(col)

        canon_table = (
            canonical_index_table(self.config) if use_canonical else None
        )

        return self.sql_gen.render(
            "reconciliation/gold_output.sql.j2",
            target_table=resolved_table(self.config),
            source_table=featured_table(self.config),
            cluster_table=cluster_table(self.config),
            matches_table=all_matches_table(self.config),
            canonical_method=recon.canonical_selection.method,
            source_priority=recon.canonical_selection.source_priority,
            scoring_columns=scoring_columns,
            source_columns=source_columns,
            passthrough_columns=passthrough_columns,
            include_match_metadata=out.include_match_metadata,
            entity_id_prefix=out.entity_id_prefix,
            partition_column=out.partition_column,
            cluster_columns=out.cluster_columns,
            canonical_index_table=canon_table,
        )

    def generate_create_canonical_index_sql(self) -> str:
        """Generate DDL to create the canonical_index table.

        Mirrors the ``featured`` table schema plus a ``cluster_id``
        column.  Uses ``CREATE TABLE IF NOT EXISTS ... AS SELECT ...
        WHERE FALSE`` so the DDL is a no-op after the first run.
        """
        return (
            f"CREATE TABLE IF NOT EXISTS "
            f"`{canonical_index_table(self.config)}`\n"
            f"CLUSTER BY entity_uid\n"
            f"AS\n"
            f"SELECT f.*, CAST(NULL AS STRING) AS cluster_id\n"
            f"FROM `{featured_table(self.config)}` f\n"
            f"WHERE FALSE"
        )

    def generate_populate_canonical_index_sql(self) -> str:
        """Generate SQL to upsert current batch entities into canonical_index.

        Also updates cluster_ids for prior entities that were
        re-clustered during the current run.
        """
        return self.sql_gen.render(
            "reconciliation/populate_canonical_index.sql.j2",
            canonical_table=canonical_index_table(self.config),
            source_table=featured_table(self.config),
            cluster_table=cluster_table(self.config),
        )
