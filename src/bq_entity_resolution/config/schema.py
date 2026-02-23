"""
Pydantic v2 models defining the complete YAML configuration schema.

Every pipeline behavior is driven by these models. Validated at load time
so configuration errors surface before any BigQuery SQL is generated.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Project & environment
# ---------------------------------------------------------------------------

class ProjectConfig(BaseModel):
    """BigQuery project and dataset routing."""

    name: str
    description: str = ""
    bq_project: str
    bq_dataset_bronze: str = "er_bronze"
    bq_dataset_silver: str = "er_silver"
    bq_dataset_gold: str = "er_gold"
    bq_location: str = "US"
    watermark_dataset: str = "er_meta"
    udf_dataset: str = "er_udfs"


# ---------------------------------------------------------------------------
# Source definitions
# ---------------------------------------------------------------------------

class ColumnMapping(BaseModel):
    """Maps a source column to a semantic role for automatic feature engineering."""

    name: str
    type: str = "STRING"
    role: Optional[str] = None  # first_name, last_name, address_line_1, etc.
    nullable: bool = True


class JoinConfig(BaseModel):
    """Defines how to join a supplemental source to the primary source."""

    table: str
    alias: str = ""
    on: str  # SQL join condition
    type: Literal["LEFT", "INNER"] = "LEFT"


class SourceConfig(BaseModel):
    """A source table that feeds entities into the pipeline."""

    name: str
    table: str
    unique_key: str
    updated_at: str
    partition_column: Optional[str] = None
    columns: list[ColumnMapping]
    passthrough_columns: list[str] = Field(default_factory=list)
    joins: list[JoinConfig] = Field(default_factory=list)
    filter: Optional[str] = None  # optional WHERE clause fragment
    entity_type_column: Optional[str] = None
    batch_size: int = 2_000_000

    @field_validator("columns")
    @classmethod
    def unique_column_names(cls, v: list[ColumnMapping]) -> list[ColumnMapping]:
        names = [c.name for c in v]
        dupes = [n for n in names if names.count(n) > 1]
        if dupes:
            raise ValueError(f"Duplicate column names: {set(dupes)}")
        return v


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

class FeatureDef(BaseModel):
    """A single engineered feature."""

    name: str
    function: str  # registered function name (e.g. 'name_clean', 'soundex')
    input: Optional[str] = None  # single-input functions
    inputs: Optional[list[str]] = None  # multi-input functions
    params: dict[str, Any] = Field(default_factory=dict)
    sql: Optional[str] = None  # raw SQL override (for custom features)
    depends_on: list[str] = Field(default_factory=list)
    join: Optional[JoinConfig] = None


class FeatureGroupConfig(BaseModel):
    """A logical group of related features (e.g. name features, address features)."""

    enabled: bool = True
    features: list[FeatureDef] = Field(default_factory=list)


class BlockingKeyDef(BaseModel):
    """A blocking key used for candidate pair generation."""

    name: str
    function: str  # farm_fingerprint, farm_fingerprint_concat, etc.
    inputs: list[str]


class CompositeKeyDef(BaseModel):
    """A composite key for exact-match tiers."""

    name: str
    function: str
    inputs: list[str]


class FeatureEngineeringConfig(BaseModel):
    """All feature engineering configuration."""

    name_features: FeatureGroupConfig = Field(default_factory=FeatureGroupConfig)
    address_features: FeatureGroupConfig = Field(default_factory=FeatureGroupConfig)
    contact_features: FeatureGroupConfig = Field(default_factory=FeatureGroupConfig)
    extra_groups: dict[str, FeatureGroupConfig] = Field(default_factory=dict)
    blocking_keys: list[BlockingKeyDef] = Field(default_factory=list)
    composite_keys: list[CompositeKeyDef] = Field(default_factory=list)
    custom_features: list[FeatureDef] = Field(default_factory=list)

    def all_groups(self) -> list[FeatureGroupConfig]:
        """Return all feature groups (built-in + extra) for iteration."""
        groups = [self.name_features, self.address_features, self.contact_features]
        groups.extend(self.extra_groups.values())
        return groups

    def all_feature_names(self) -> set[str]:
        """Return all feature names across all groups + custom features."""
        names: set[str] = set()
        for group in self.all_groups():
            for feat in group.features:
                names.add(feat.name)
        for feat in self.custom_features:
            names.add(feat.name)
        for bk in self.blocking_keys:
            names.add(bk.name)
        for ck in self.composite_keys:
            names.add(ck.name)
        return names


# ---------------------------------------------------------------------------
# Embeddings & LSH
# ---------------------------------------------------------------------------

class LSHConfig(BaseModel):
    """Locality-Sensitive Hashing configuration for embedding-based blocking."""

    num_hash_tables: int = 20
    num_hash_functions_per_table: int = 8
    bucket_column_prefix: str = "lsh_bucket"
    projection_seed: int = 42


class EmbeddingConfig(BaseModel):
    """Embedding computation and storage configuration."""

    enabled: bool = False
    model: str = "text-embedding-004"
    source_columns: list[str] = Field(default_factory=list)
    concat_separator: str = " | "
    dimensions: int = 768
    batch_size: int = 5000
    lsh: LSHConfig = Field(default_factory=LSHConfig)


# ---------------------------------------------------------------------------
# Matching tiers
# ---------------------------------------------------------------------------

class BlockingPathDef(BaseModel):
    """A single blocking path within a tier."""

    keys: list[str]
    candidate_limit: int = 200
    lsh_min_bands: int = 1  # for LSH blocking: min matching bands


class TierBlockingConfig(BaseModel):
    """Blocking configuration for a matching tier."""

    paths: list[BlockingPathDef]
    cross_batch: bool = True  # also compare against gold canonicals


class ComparisonLevelDef(BaseModel):
    """One outcome level within a multi-level comparison.

    Levels are evaluated top-to-bottom; the first matching level wins.
    The last level should have method=None (else/fallthrough).
    m and u can be set manually or learned from training data / EM.
    """

    label: str  # "exact", "fuzzy_high", "else"
    method: Optional[str] = None  # comparison function name (None = else/fallthrough)
    params: dict[str, Any] = Field(default_factory=dict)
    m: Optional[float] = None  # P(level | match)
    u: Optional[float] = None  # P(level | non-match)

    @field_validator("m", "u")
    @classmethod
    def probability_range(cls, v: float | None) -> float | None:
        if v is not None and not (0.0 <= v <= 1.0):
            raise ValueError(f"Probability must be in [0, 1], got {v}")
        return v


class TermFrequencyConfig(BaseModel):
    """Term frequency adjustment for a comparison.

    Adjusts match evidence based on value rarity — a match on "Smith"
    (common) provides less evidence than "Worthington" (rare).
    Follows the Splink approach of adjusting u-probability per-value.
    """

    enabled: bool = False
    tf_minimum_u_value: float = 0.001  # Floor prevents extreme weights from rare values
    tf_adjustment_column: Optional[str] = None  # Override column (defaults to comp.left)


class ComparisonDef(BaseModel):
    """A comparison between two columns using a registered method."""

    left: str
    right: str
    method: str  # exact, levenshtein, jaro_winkler, cosine_similarity, etc.
    params: dict[str, Any] = Field(default_factory=dict)
    weight: float = 1.0
    levels: Optional[list[ComparisonLevelDef]] = None  # multi-level outcomes for F-S
    tf_adjustment: Optional[TermFrequencyConfig] = None  # term frequency adjustment


class ThresholdConfig(BaseModel):
    """How to aggregate comparison scores and apply the match threshold."""

    method: Literal["sum", "weighted_sum", "min_all", "fellegi_sunter"] = "sum"
    min_score: float = 0.0
    match_threshold: Optional[float] = None  # F-S: log-likelihood threshold for match


class HardNegativeDef(BaseModel):
    """A rule that disqualifies or penalizes candidate pairs."""

    left: str
    right: Optional[str] = None  # defaults to same as left
    method: str  # different, null_either, custom
    action: Literal["disqualify", "penalize"] = "disqualify"
    penalty: float = 0.0
    sql: Optional[str] = None  # raw SQL condition override


class SoftSignalDef(BaseModel):
    """A signal that adjusts the match score (positive or negative)."""

    left: str
    right: Optional[str] = None
    method: str  # exact, similar, both_null, custom
    bonus: float = 1.0
    sql: Optional[str] = None


class TrainingConfig(BaseModel):
    """Parameter estimation configuration for Fellegi-Sunter m/u probabilities."""

    method: Literal["labeled", "em", "none"] = "none"
    labeled_pairs_table: Optional[str] = None  # FQ BQ table of labeled pairs
    em_max_iterations: int = 10
    em_convergence_threshold: float = 0.001
    em_sample_size: int = 100_000
    em_initial_match_proportion: float = 0.1
    parameters_table: Optional[str] = None  # FQ table to persist estimated params


class LabelFeedbackConfig(BaseModel):
    """Configuration for label ingestion and retrain feedback loop.

    Closes the active learning cycle: review queue → human labels → retrain m/u.
    """

    enabled: bool = False
    feedback_table: Optional[str] = None  # FQ table to store ingested labels
    min_labels_for_retrain: int = 50  # Minimum labels before retraining
    auto_retrain: bool = False  # Automatically retrain when threshold met


class ActiveLearningConfig(BaseModel):
    """Active learning review queue configuration."""

    enabled: bool = False
    review_queue_table: Optional[str] = None
    queue_size: int = 200
    uncertainty_window: float = 0.15
    label_feedback: LabelFeedbackConfig = Field(default_factory=LabelFeedbackConfig)


class MatchingTierConfig(BaseModel):
    """A single matching tier in the pipeline."""

    name: str
    description: str = ""
    enabled: bool = True
    blocking: TierBlockingConfig
    comparisons: list[ComparisonDef]
    threshold: ThresholdConfig
    hard_negatives: list[HardNegativeDef] = Field(default_factory=list)
    soft_signals: list[SoftSignalDef] = Field(default_factory=list)
    confidence: Optional[float] = None  # fixed confidence score for this tier
    training: TrainingConfig = Field(default_factory=TrainingConfig)
    active_learning: ActiveLearningConfig = Field(default_factory=ActiveLearningConfig)

    @field_validator("name")
    @classmethod
    def valid_tier_name(cls, v: str) -> str:
        if not v.replace("_", "").replace("-", "").isalnum():
            raise ValueError(f"Tier name must be alphanumeric/underscore/dash: {v}")
        return v


# ---------------------------------------------------------------------------
# Reconciliation & output
# ---------------------------------------------------------------------------

class ClusteringConfig(BaseModel):
    """How to assign entity clusters from pairwise matches."""

    method: Literal["connected_components", "star", "best_match"] = "connected_components"
    max_iterations: int = 20
    min_cluster_confidence: float = 0.0


class CanonicalSelectionConfig(BaseModel):
    """How to elect the canonical record within a cluster."""

    method: Literal["completeness", "recency", "source_priority"] = "completeness"
    source_priority: list[str] = Field(default_factory=list)


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


# ---------------------------------------------------------------------------
# Incremental processing
# ---------------------------------------------------------------------------

class IncrementalConfig(BaseModel):
    """Incremental processing and watermark configuration."""

    enabled: bool = True
    grace_period_hours: int = 48
    cursor_columns: list[str] = Field(default_factory=lambda: ["updated_at"])
    batch_size: int = 2_000_000
    full_refresh_on_schema_change: bool = True


# ---------------------------------------------------------------------------
# Monitoring
# ---------------------------------------------------------------------------

class MetricsConfig(BaseModel):
    """Pipeline metrics collection configuration."""

    enabled: bool = True
    destination: Literal["bigquery", "stdout"] = "bigquery"


class ProfilingConfig(BaseModel):
    """Data quality profiling configuration."""

    enabled: bool = False
    sample_rate: float = 0.01


class BlockingMetricsConfig(BaseModel):
    """Blocking evaluation metrics to assess blocking strategy quality."""

    enabled: bool = False
    persist_to_table: bool = False


class ClusterQualityConfig(BaseModel):
    """Cluster quality metrics and alerts.

    Monitors cluster health to catch false positives that merge
    unrelated entities via transitivity.
    """

    enabled: bool = False
    persist_to_table: bool = False
    alert_max_cluster_size: int = 100
    alert_singleton_ratio: float = 0.95
    abort_on_explosion: bool = False  # Hard abort if max cluster exceeds threshold


class MonitoringConfig(BaseModel):
    """Observability configuration."""

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "text"] = "json"
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    profiling: ProfilingConfig = Field(default_factory=ProfilingConfig)
    blocking_metrics: BlockingMetricsConfig = Field(default_factory=BlockingMetricsConfig)
    cluster_quality: ClusterQualityConfig = Field(default_factory=ClusterQualityConfig)
    persist_sql_log: bool = False  # Write sql_log to BQ table after run


# ---------------------------------------------------------------------------
# Scale
# ---------------------------------------------------------------------------

class ScaleConfig(BaseModel):
    """Scale optimizations for high-volume processing (5-10M+ records/day).

    All fields are opt-in (off by default) to preserve backwards compatibility.
    """

    max_bytes_billed: Optional[int] = None  # Safety cap per query (bytes)
    featured_table_clustering: list[str] = Field(default_factory=list)
    candidates_clustering: list[str] = Field(
        default_factory=lambda: ["l_entity_uid"]
    )
    matches_clustering: list[str] = Field(
        default_factory=lambda: ["l_entity_uid", "r_entity_uid"]
    )
    checkpoint_enabled: bool = False


# ---------------------------------------------------------------------------
# Root config
# ---------------------------------------------------------------------------

class PipelineConfig(BaseModel):
    """Root configuration for the entire entity resolution pipeline."""

    version: str = "1.0"
    project: ProjectConfig
    sources: list[SourceConfig]
    feature_engineering: FeatureEngineeringConfig = Field(
        default_factory=FeatureEngineeringConfig
    )
    embeddings: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    matching_tiers: list[MatchingTierConfig]
    reconciliation: ReconciliationConfig = Field(default_factory=ReconciliationConfig)
    incremental: IncrementalConfig = Field(default_factory=IncrementalConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    training: TrainingConfig = Field(default_factory=TrainingConfig)  # global default
    scale: ScaleConfig = Field(default_factory=lambda: ScaleConfig())
    link_type: Literal["link_and_dedupe", "dedupe_only", "link_only"] = "link_and_dedupe"

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

    def fq_table(self, dataset_attr: str, suffix: str) -> str:
        """Build a fully-qualified BigQuery table name."""
        dataset = getattr(self.project, dataset_attr)
        return f"{self.project.bq_project}.{dataset}.{suffix}"
