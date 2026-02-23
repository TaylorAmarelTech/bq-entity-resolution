"""Blocking stage: generates candidate pairs for a matching tier.

Extracted from PipelineOrchestrator._execute_tiers() blocking portion.
Uses the blocking SQL builder for SQL generation.
"""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.config.schema import (
    MatchingTierConfig,
    PipelineConfig,
)
from bq_entity_resolution.naming import (
    candidates_table,
    featured_table,
)
from bq_entity_resolution.sql.builders.blocking import (
    BlockingParams,
    BlockingPath,
    build_blocking_sql,
)
from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.stages.base import Stage, TableRef


class BlockingStage(Stage):
    """Generate candidate pairs for a single matching tier.

    Supports multiple blocking paths, LSH keys, cross-batch
    matching, and prior-tier exclusion.
    """

    def __init__(
        self,
        tier: MatchingTierConfig,
        tier_index: int,
        config: PipelineConfig,
    ):
        self._tier = tier
        self._tier_index = tier_index
        self._config = config

    @property
    def name(self) -> str:
        return f"blocking_{self._tier.name}"

    @property
    def inputs(self) -> dict[str, TableRef]:
        refs = {
            "featured": TableRef(
                name="featured",
                fq_name=featured_table(self._config),
            ),
        }
        return refs

    @property
    def outputs(self) -> dict[str, TableRef]:
        target = candidates_table(self._config, self._tier.name)
        return {
            "candidates": TableRef(
                name=f"candidates_{self._tier.name}",
                fq_name=target,
                description=f"Candidate pairs for tier {self._tier.name}",
            ),
        }

    def plan(self, **kwargs: Any) -> list[SQLExpression]:
        """Generate blocking SQL.

        kwargs:
            excluded_pairs_table: str | None — table of already-matched pairs
        """
        excluded = kwargs.get("excluded_pairs_table")
        tier = self._tier
        blocking = tier.blocking

        # Build blocking paths
        paths: list[BlockingPath] = []
        for i, path in enumerate(blocking.paths):
            paths.append(
                BlockingPath(
                    index=i,
                    keys=list(path.keys),
                    lsh_keys=list(getattr(path, "lsh_keys", [])),
                    candidate_limit=getattr(path, "candidate_limit", 0),
                )
            )

        # Determine LSH table
        lsh_table = None
        if any(p.lsh_keys for p in paths):
            from bq_entity_resolution.naming import lsh_buckets_table
            lsh_table = lsh_buckets_table(self._config)

        # Determine canonical table for cross-batch
        canonical_table = None
        if getattr(blocking, "cross_batch", False):
            from bq_entity_resolution.naming import canonical_index_table
            canonical_table = canonical_index_table(self._config)

        params = BlockingParams(
            target_table=self.outputs["candidates"].fq_name,
            source_table=featured_table(self._config),
            blocking_paths=paths,
            tier_name=tier.name,
            cross_batch=getattr(blocking, "cross_batch", False),
            canonical_table=canonical_table,
            excluded_pairs_table=excluded,
            lsh_table=lsh_table,
            link_type=getattr(self._config, "link_type", None),
        )

        return [build_blocking_sql(params)]

    def validate(self) -> list[str]:
        errors = []
        if not self._tier.blocking.paths:
            errors.append(
                f"Tier '{self._tier.name}' has no blocking paths defined"
            )
        return errors
