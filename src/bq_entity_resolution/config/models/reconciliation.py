"""
Reconciliation and output configuration models.

Defines clustering, canonical selection, audit trail, output, and
reconciliation configuration for cross-tier entity resolution.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

__all__ = [
    "ClusteringConfig",
    "FieldMergeStrategy",
    "CanonicalSelectionConfig",
    "AuditTrailConfig",
    "OutputConfig",
    "ReconciliationConfig",
]


class ClusteringConfig(BaseModel):
    """How to assign entity clusters from pairwise matches."""

    method: Literal["connected_components", "star", "best_match"] = "connected_components"
    max_iterations: int = 20
    min_cluster_confidence: float = 0.0


class FieldMergeStrategy(BaseModel):
    """Per-field strategy for golden record assembly.

    Used when canonical_selection.method = 'field_merge'.
    Each field can independently choose which record's value to pick.

    Strategies:
        most_complete:    Value from the record with the most non-null fields.
        most_recent:      Value from the most recently updated record.
        source_priority:  Value from the highest-priority source.
        most_common:      Most frequently occurring non-null value (majority vote).
        weighted_vote:    Value with the highest recency-weighted vote count.
                          Uses exponential time decay (configurable via decay_rate).
    """

    column: str
    strategy: Literal[
        "most_complete", "most_recent", "source_priority",
        "most_common", "weighted_vote",
    ] = "most_complete"
    source_priority: list[str] = Field(default_factory=list)
    decay_rate: float = 0.01  # Daily decay rate for weighted_vote strategy


class CanonicalSelectionConfig(BaseModel):
    """How to elect the canonical record within a cluster."""

    method: Literal[
        "completeness", "recency", "source_priority", "field_merge"
    ] = "completeness"
    source_priority: list[str] = Field(default_factory=list)
    field_strategies: list[FieldMergeStrategy] = Field(default_factory=list)
    default_field_strategy: Literal[
        "most_complete", "most_recent", "source_priority",
        "most_common", "weighted_vote",
    ] = "most_complete"


class AuditTrailConfig(BaseModel):
    """Per-match audit trail for debugging and compliance.

    When enabled, each match row includes a JSON column with individual
    comparison scores/weights so you can see exactly why a pair matched.
    """

    enabled: bool = False
    include_individual_scores: bool = True


class OutputConfig(BaseModel):
    """Gold layer output configuration."""

    include_match_metadata: bool = True
    include_passthrough: bool = True
    entity_id_prefix: str = "ENT"
    partition_column: Optional[str] = None
    cluster_columns: list[str] = Field(default_factory=list)
    audit_trail: AuditTrailConfig = Field(default_factory=AuditTrailConfig)


class ReconciliationConfig(BaseModel):
    """Cross-tier reconciliation configuration."""

    strategy: Literal["tier_priority", "highest_score", "manual_review"] = "tier_priority"
    clustering: ClusteringConfig = Field(default_factory=ClusteringConfig)
    canonical_selection: CanonicalSelectionConfig = Field(
        default_factory=CanonicalSelectionConfig
    )
    output: OutputConfig = Field(default_factory=OutputConfig)
