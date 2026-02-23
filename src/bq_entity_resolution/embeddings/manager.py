"""
Embedding computation and management.

Handles embedding generation via BigQuery ML and storage.
"""

from __future__ import annotations

import logging

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.naming import (
    embeddings_table,
    featured_table,
    lsh_buckets_table,
)
from bq_entity_resolution.sql.generator import SQLGenerator

logger = logging.getLogger(__name__)


class EmbeddingManager:
    """Manages embedding computation and LSH bucket generation."""

    def __init__(self, config: PipelineConfig, sql_gen: SQLGenerator | None = None):
        self.config = config
        self.sql_gen = sql_gen or SQLGenerator()
        self.emb_config = config.embeddings

    def generate_embedding_sql(self) -> str:
        """Generate SQL to compute embeddings using BigQuery ML."""
        source_cols = self.emb_config.source_columns
        sep = self.emb_config.concat_separator
        concat_parts = f", '{sep}', ".join(
            f"COALESCE({c}, '')" for c in source_cols
        )
        concat_expr = f"CONCAT({concat_parts})"

        return self.sql_gen.render(
            "features/embeddings.sql.j2",
            target_table=embeddings_table(self.config),
            source_table=featured_table(self.config),
            concat_expression=concat_expr,
            model_name=self.emb_config.model,
            dimensions=self.emb_config.dimensions,
        )

    def generate_lsh_sql(self) -> str:
        """Generate SQL to compute LSH bucket assignments."""
        lsh = self.emb_config.lsh
        return self.sql_gen.render(
            "blocking/lsh_block.sql.j2",
            target_table=lsh_buckets_table(self.config),
            embedding_table=embeddings_table(self.config),
            num_tables=lsh.num_hash_tables,
            num_functions=lsh.num_hash_functions_per_table,
            dimensions=self.emb_config.dimensions,
            seed=lsh.projection_seed,
            bucket_prefix=lsh.bucket_column_prefix,
        )
