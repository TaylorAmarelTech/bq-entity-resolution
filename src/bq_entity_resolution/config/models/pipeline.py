"""
Root pipeline configuration model.

Defines PipelineConfig, the top-level Pydantic model that composes
all domain-specific configuration models into a single validated schema.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from bq_entity_resolution.config.models.source import SourceConfig
from bq_entity_resolution.config.models.features import FeatureEngineeringConfig
from bq_entity_resolution.config.models.matching import (
    ComparisonDef,
    HardNegativeDef,
    MatchingTierConfig,
    SoftSignalDef,
    TrainingConfig,
)
from bq_entity_resolution.config.models.reconciliation import ReconciliationConfig
from bq_entity_resolution.config.models.infrastructure import (
    EmbeddingConfig,
    IncrementalConfig,
    MonitoringConfig,
    ProjectConfig,
    ScaleConfig,
)

__all__ = [
    "PipelineConfig",
]


class PipelineConfig(BaseModel):
    """Root configuration for the entire entity resolution pipeline."""

    version: str = "1.0"
    project: ProjectConfig
    sources: list[SourceConfig]
    feature_engineering: FeatureEngineeringConfig = Field(
        default_factory=FeatureEngineeringConfig
    )
    embeddings: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    comparison_pool: dict[str, ComparisonDef] = Field(default_factory=dict)
    matching_tiers: list[MatchingTierConfig]
    global_hard_negatives: list[HardNegativeDef] = Field(default_factory=list)
    global_soft_signals: list[SoftSignalDef] = Field(default_factory=list)
    reconciliation: ReconciliationConfig = Field(default_factory=ReconciliationConfig)
    incremental: IncrementalConfig = Field(default_factory=IncrementalConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    training: TrainingConfig = Field(default_factory=TrainingConfig)  # global default
    scale: ScaleConfig = Field(default_factory=lambda: ScaleConfig())
    link_type: Literal["link_and_dedupe", "dedupe_only", "link_only"] = "link_and_dedupe"

    @model_validator(mode="after")
    def _resolve_comparison_refs(self) -> "PipelineConfig":
        """Resolve comparison pool references in tier comparisons.

        When a ComparisonDef has ``ref`` set, it is resolved against
        the ``comparison_pool`` dict.  Any fields set alongside ``ref``
        act as tier-level overrides (e.g. weight).
        """
        for tier in self.matching_tiers:
            resolved: list[ComparisonDef] = []
            for comp in tier.comparisons:
                if comp.ref:
                    pool_entry = self.comparison_pool.get(comp.ref)
                    if not pool_entry:
                        raise ValueError(
                            f"Tier '{tier.name}' references unknown "
                            f"comparison_pool ref '{comp.ref}'. "
                            f"Available: {sorted(self.comparison_pool.keys())}"
                        )
                    # Merge: pool base + any tier-level overrides
                    overrides = comp.model_dump(
                        exclude_defaults=True, exclude={"ref"}
                    )
                    merged_data = pool_entry.model_dump()
                    merged_data.update(overrides)
                    merged_data.pop("ref", None)  # resolved refs don't carry ref
                    resolved.append(ComparisonDef(**merged_data))
                else:
                    resolved.append(comp)
            tier.comparisons = resolved
        return self

    @model_validator(mode="after")
    def unique_tier_names(self) -> "PipelineConfig":
        names = [t.name for t in self.matching_tiers]
        dupes = [n for n in names if names.count(n) > 1]
        if dupes:
            raise ValueError(f"Duplicate tier names: {set(dupes)}")
        return self

    @model_validator(mode="after")
    def blocking_keys_exist(self) -> "PipelineConfig":
        """Verify all blocking keys referenced in tiers are defined."""
        defined = {bk.name for bk in self.feature_engineering.blocking_keys}
        defined |= {ck.name for ck in self.feature_engineering.composite_keys}
        if self.embeddings.enabled:
            for i in range(self.embeddings.lsh.num_hash_tables):
                defined.add(f"{self.embeddings.lsh.bucket_column_prefix}_{i}")
        for tier in self.matching_tiers:
            if not tier.enabled:
                continue
            for path in tier.blocking.paths:
                for key in path.keys:
                    if key not in defined:
                        raise ValueError(
                            f"Tier '{tier.name}' references undefined "
                            f"blocking key '{key}'. Defined keys: {sorted(defined)}"
                        )
        return self

    @model_validator(mode="after")
    def at_least_one_source(self) -> "PipelineConfig":
        if not self.sources:
            raise ValueError("At least one source must be defined")
        return self

    def enabled_tiers(self) -> list[MatchingTierConfig]:
        """Return only enabled matching tiers in order."""
        return [t for t in self.matching_tiers if t.enabled]

    def effective_hard_negatives(self, tier: MatchingTierConfig) -> list[HardNegativeDef]:
        """Return combined global + tier-level hard negatives for a tier.

        Global hard negatives are applied first, then tier-specific ones.
        This enables defining disqualification rules once and reusing
        across all tiers.
        """
        return list(self.global_hard_negatives) + list(tier.hard_negatives)

    def effective_soft_signals(self, tier: MatchingTierConfig) -> list[SoftSignalDef]:
        """Return combined global + tier-level soft signals for a tier.

        Global soft signals are applied first, then tier-specific ones.
        """
        return list(self.global_soft_signals) + list(tier.soft_signals)

    def effective_training_config(self, tier: MatchingTierConfig) -> TrainingConfig:
        """Return the effective training config for a tier.

        Resolution order:
        1. Tier-level training config (if method != "none")
        2. Auto-retrain from label feedback (if enabled + auto_retrain)
        3. Global training config
        """
        # Tier-level explicit training takes priority
        if tier.training.method != "none":
            return tier.training

        # Auto-retrain: when label feedback is enabled and auto_retrain=True,
        # wire the labels table as the training source for the next run
        feedback = tier.active_learning.label_feedback
        if feedback.enabled and feedback.auto_retrain:
            from bq_entity_resolution.naming import labels_table
            return TrainingConfig(
                method="labeled",
                labeled_pairs_table=labels_table(self),
            )

        # Fall back to global training config
        return self.training

    def fq_table(self, dataset_attr: str, suffix: str) -> str:
        """Build a fully-qualified BigQuery table name."""
        dataset = getattr(self.project, dataset_attr)
        return f"{self.project.bq_project}.{dataset}.{suffix}"

    def to_yaml(self) -> str:
        """Serialize the full config to YAML for inspection and editing.

        Enables the workflow: quick_config() -> inspect -> tweak -> reload.
        """
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "PyYAML is required for to_yaml(). Install with: pip install pyyaml"
            )

        data = self.model_dump(exclude_defaults=True, exclude_none=True)
        return yaml.dump(data, default_flow_style=False, sort_keys=False, width=100)
