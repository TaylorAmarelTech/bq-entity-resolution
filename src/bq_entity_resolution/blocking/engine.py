"""
Blocking engine: generates candidate pair SQL for each matching tier.

Supports standard equi-join blocking and LSH-based blocking, with
per-path candidate limits and deduplication.
"""

from __future__ import annotations

import logging

from bq_entity_resolution.config.schema import (
    MatchingTierConfig,
    PipelineConfig,
)
from bq_entity_resolution.naming import (
    candidates_table,
    canonical_index_table,
    featured_table,
    lsh_buckets_table,
    matches_table,
)
from bq_entity_resolution.sql.generator import SQLGenerator

logger = logging.getLogger(__name__)


class BlockingEngine:
    """Generates blocking candidate SQL for matching tiers."""

    def __init__(self, config: PipelineConfig, sql_gen: SQLGenerator | None = None):
        self.config = config
        self.sql_gen = sql_gen or SQLGenerator()

    def generate_candidates_sql(
        self,
        tier: MatchingTierConfig,
        tier_index: int,
        excluded_pairs_table: str | None = None,
    ) -> str:
        """Generate multi-path blocking SQL for a tier."""
        # Build blocking path parameters
        lsh_prefix = self.config.embeddings.lsh.bucket_column_prefix
        lsh_columns = set()
        blocking_paths = []
        for i, path in enumerate(tier.blocking.paths):
            # Detect which keys are LSH bucket columns
            path_lsh_keys = [
                k for k in path.keys if k.startswith(lsh_prefix + "_")
            ]
            path_standard_keys = [
                k for k in path.keys if not k.startswith(lsh_prefix + "_")
            ]
            lsh_columns.update(path_lsh_keys)
            blocking_paths.append({
                "keys": path_standard_keys,
                "lsh_keys": path_lsh_keys,
                "candidate_limit": path.candidate_limit,
                "index": i,
            })

        # Pass LSH table if any blocking path uses LSH keys
        lsh_tbl = None
        if lsh_columns and self.config.embeddings.enabled:
            lsh_tbl = lsh_buckets_table(self.config)

        return self.sql_gen.render(
            "blocking/multi_path_candidates.sql.j2",
            target_table=candidates_table(self.config, tier.name),
            source_table=featured_table(self.config),
            canonical_table=canonical_index_table(self.config),
            blocking_paths=blocking_paths,
            cross_batch=tier.blocking.cross_batch,
            excluded_pairs_table=excluded_pairs_table,
            tier_name=tier.name,
            lsh_table=lsh_tbl,
            link_type=self.config.link_type,
            cluster_by=self.config.scale.candidates_clustering,
        )

    def generate_metrics_sql(self, tier: MatchingTierConfig) -> str:
        """Generate SQL to compute blocking evaluation metrics for a tier."""
        return self.sql_gen.render(
            "blocking/blocking_metrics.sql.j2",
            candidates_table=candidates_table(self.config, tier.name),
            matches_table=matches_table(self.config, tier.name),
            source_table=featured_table(self.config),
            tier_name=tier.name,
        )

    def generate_lsh_blocking_sql(self) -> str:
        """Generate LSH bucket computation SQL from embeddings."""
        emb = self.config.embeddings
        if not emb.enabled:
            return ""

        return self.sql_gen.render(
            "blocking/lsh_block.sql.j2",
            target_table=lsh_buckets_table(self.config),
            embedding_table=self.config.fq_table(
                "bq_dataset_silver", "entity_embeddings"
            ),
            num_tables=emb.lsh.num_hash_tables,
            num_functions=emb.lsh.num_hash_functions_per_table,
            dimensions=emb.dimensions,
            seed=emb.lsh.projection_seed,
            bucket_prefix=emb.lsh.bucket_column_prefix,
        )
