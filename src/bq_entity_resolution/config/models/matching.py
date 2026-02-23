"""
Matching configuration models.

Defines comparisons, thresholds, hard negatives, soft signals,
training, active learning, and matching tier configuration.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from bq_entity_resolution.config.models.blocking import TierBlockingConfig

__all__ = [
    "ComparisonLevelDef",
    "TermFrequencyConfig",
    "ComparisonDef",
    "ThresholdConfig",
    "HardNegativeDef",
    "SoftSignalDef",
    "TrainingConfig",
    "LabelFeedbackConfig",
    "ActiveLearningConfig",
    "MatchingTierConfig",
]


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

    Adjusts match evidence based on value rarity --- a match on "Smith"
    (common) provides less evidence than "Worthington" (rare).
    Follows the Splink approach of adjusting u-probability per-value.
    """

    enabled: bool = False
    tf_minimum_u_value: float = 0.001  # Floor prevents extreme weights from rare values
    tf_adjustment_column: Optional[str] = None  # Override column (defaults to comp.left)


class ComparisonDef(BaseModel):
    """A comparison between two columns using a registered method.

    Can be defined inline or reference a pool entry via ``ref``.
    When ``ref`` is set, the comparison inherits all fields from the pool
    entry.  Any additional fields set alongside ``ref`` act as overrides
    (e.g. ``ref: email_exact, weight: 1.5`` to override the pool weight
    for a specific tier).
    """

    ref: Optional[str] = None  # Pool reference name
    left: str = ""
    right: str = ""
    method: str = ""  # exact, levenshtein, jaro_winkler, cosine_similarity, etc.
    params: dict[str, Any] = Field(default_factory=dict)
    weight: float = 1.0
    weight_mode: Literal["manual", "auto", "profile"] = "manual"
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

    Closes the active learning cycle: review queue -> human labels -> retrain m/u.
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
