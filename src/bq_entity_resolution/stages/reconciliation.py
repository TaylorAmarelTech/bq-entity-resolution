"""Reconciliation stage: clustering + gold output.

Extracted from PipelineOrchestrator._reconcile().
Uses clustering and gold_output builders.
"""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.naming import (
    all_matches_table,
    cluster_table as _cluster_table,
    featured_table,
    resolved_table,
)
from bq_entity_resolution.sql.builders.clustering import (
    ClusteringParams,
    ClusterMetricsParams,
    build_cluster_assignment_sql,
    build_cluster_quality_metrics_sql,
)
from bq_entity_resolution.sql.builders.gold_output import (
    GoldOutputParams,
    build_gold_output_sql,
)
from bq_entity_resolution.sql.builders.golden_record import FieldStrategy
from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.stages.base import Stage, TableRef


class ClusteringStage(Stage):
    """Assign entity clusters using connected components."""

    def __init__(self, config: PipelineConfig):
        self._config = config

    @property
    def name(self) -> str:
        return "clustering"

    @property
    def inputs(self) -> dict[str, TableRef]:
        return {
            "all_matches": TableRef(
                name="all_matches",
                fq_name=all_matches_table(self._config),
            ),
            "featured": TableRef(
                name="featured",
                fq_name=featured_table(self._config),
            ),
        }

    @property
    def outputs(self) -> dict[str, TableRef]:
        target = _cluster_table(self._config)
        return {
            "clusters": TableRef(
                name="clusters",
                fq_name=target,
                description="Entity cluster assignments",
            ),
        }

    def plan(self, **kwargs: Any) -> list[SQLExpression]:
        """Generate cluster assignment SQL."""
        max_iter = getattr(
            self._config.reconciliation.clustering, "max_iterations", 20
        )

        params = ClusteringParams(
            all_matches_table=self.inputs["all_matches"].fq_name,
            cluster_table=self.outputs["clusters"].fq_name,
            source_table=self.inputs["featured"].fq_name,
            max_iterations=max_iter,
        )

        return [build_cluster_assignment_sql(params)]


class GoldOutputStage(Stage):
    """Generate resolved entities with canonical record election."""

    def __init__(self, config: PipelineConfig):
        self._config = config

    @property
    def name(self) -> str:
        return "gold_output"

    @property
    def inputs(self) -> dict[str, TableRef]:
        return {
            "featured": TableRef(
                name="featured",
                fq_name=featured_table(self._config),
            ),
            "clusters": TableRef(
                name="clusters",
                fq_name=_cluster_table(self._config),
            ),
            "all_matches": TableRef(
                name="all_matches",
                fq_name=all_matches_table(self._config),
            ),
        }

    @property
    def outputs(self) -> dict[str, TableRef]:
        target = resolved_table(self._config)
        return {
            "gold": TableRef(
                name="gold",
                fq_name=target,
                description="Final resolved entities",
            ),
        }

    def plan(self, **kwargs: Any) -> list[SQLExpression]:
        """Generate gold output SQL."""
        recon = self._config.reconciliation
        output = getattr(self._config, "output", None) or getattr(recon, "output", None)

        # Collect scoring columns for completeness method
        scoring_cols: list[str] = []
        source_cols: list[str] = []
        for source in self._config.sources:
            for col in source.columns:
                if col.name not in source_cols:
                    source_cols.append(col.name)
                    scoring_cols.append(col.name)

        canonical_method = getattr(
            recon.canonical_selection, "method", "completeness"
        )
        source_priority = getattr(
            recon.canonical_selection, "source_priority", []
        )

        # Field-level merge strategies
        field_strategies = []
        for fs in getattr(recon.canonical_selection, "field_strategies", []):
            field_strategies.append(FieldStrategy(
                column=fs.column,
                strategy=fs.strategy,
                source_priority=fs.source_priority,
            ))
        default_field_strategy = getattr(
            recon.canonical_selection, "default_field_strategy", "most_complete"
        )

        # Reconciliation strategy for match deduplication
        reconciliation_strategy = getattr(recon, "strategy", "tier_priority")

        params = GoldOutputParams(
            target_table=self.outputs["gold"].fq_name,
            source_table=self.inputs["featured"].fq_name,
            cluster_table=self.inputs["clusters"].fq_name,
            matches_table=self.inputs["all_matches"].fq_name,
            canonical_method=canonical_method,
            scoring_columns=scoring_cols,
            source_columns=source_cols,
            include_match_metadata=getattr(
                output, "include_match_metadata", True
            ),
            entity_id_prefix=getattr(output, "entity_id_prefix", "ent"),
            partition_column=getattr(output, "partition_column", None),
            cluster_columns=getattr(output, "cluster_columns", []),
            source_priority=source_priority,
            field_strategies=field_strategies,
            default_field_strategy=default_field_strategy,
            reconciliation_strategy=reconciliation_strategy,
        )

        return [build_gold_output_sql(params)]


class ClusterQualityStage(Stage):
    """Compute cluster quality metrics for monitoring."""

    def __init__(self, config: PipelineConfig):
        self._config = config

    @property
    def name(self) -> str:
        return "cluster_quality"

    @property
    def inputs(self) -> dict[str, TableRef]:
        return {
            "clusters": TableRef(
                name="clusters",
                fq_name=_cluster_table(self._config),
            ),
            "all_matches": TableRef(
                name="all_matches",
                fq_name=all_matches_table(self._config),
            ),
        }

    def plan(self, **kwargs: Any) -> list[SQLExpression]:
        """Generate cluster quality metrics SQL."""
        params = ClusterMetricsParams(
            cluster_table=self.inputs["clusters"].fq_name,
            matches_table=self.inputs["all_matches"].fq_name,
        )

        return [build_cluster_quality_metrics_sql(params)]
