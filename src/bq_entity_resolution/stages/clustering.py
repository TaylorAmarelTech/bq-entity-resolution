"""Clustering stage: assign entity clusters using connected components.

Supports both full-refresh and incremental modes. In incremental mode,
initializes from canonical_index (prior entities) and adds new entities
as singletons before propagating minimum cluster_id through match edges.
"""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.naming import (
    all_matches_table,
    canonical_index_table,
    cluster_table as _cluster_table,
    featured_table,
)
from bq_entity_resolution.sql.builders.clustering import (
    ClusteringParams,
    IncrementalClusteringParams,
    build_cluster_assignment_sql,
    build_incremental_cluster_sql,
)
from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.stages.base import Stage, TableRef


class ClusteringStage(Stage):
    """Assign entity clusters using connected components.

    When incremental processing is enabled, uses incremental clustering
    that initializes from the canonical_index (prior entities with their
    cluster assignments) and adds new entities as singletons before
    propagating minimum cluster_id through match edges.
    """

    def __init__(self, config: PipelineConfig):
        self._config = config

    @property
    def name(self) -> str:
        return "clustering"

    @property
    def _is_incremental(self) -> bool:
        inc = getattr(self._config, "incremental", None)
        return bool(inc and getattr(inc, "enabled", False))

    @property
    def inputs(self) -> dict[str, TableRef]:
        refs = {
            "all_matches": TableRef(
                name="all_matches",
                fq_name=all_matches_table(self._config),
            ),
            "featured": TableRef(
                name="featured",
                fq_name=featured_table(self._config),
            ),
        }
        if self._is_incremental:
            refs["canonical_index"] = TableRef(
                name="canonical_index",
                fq_name=canonical_index_table(self._config),
            )
        return refs

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
        """Generate cluster assignment SQL.

        Uses incremental clustering when incremental processing is enabled,
        which initializes from canonical_index + new singletons.
        """
        max_iter = getattr(
            self._config.reconciliation.clustering, "max_iterations", 20
        )

        if self._is_incremental:
            params = IncrementalClusteringParams(
                all_matches_table=self.inputs["all_matches"].fq_name,
                cluster_table=self.outputs["clusters"].fq_name,
                source_table=self.inputs["featured"].fq_name,
                canonical_table=self.inputs["canonical_index"].fq_name,
                max_iterations=max_iter,
            )
            return [build_incremental_cluster_sql(params)]

        params = ClusteringParams(
            all_matches_table=self.inputs["all_matches"].fq_name,
            cluster_table=self.outputs["clusters"].fq_name,
            source_table=self.inputs["featured"].fq_name,
            max_iterations=max_iter,
        )
        return [build_cluster_assignment_sql(params)]
