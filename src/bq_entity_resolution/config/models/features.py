"""
Feature engineering configuration models.

Defines feature definitions, feature groups, blocking/composite keys,
enrichment joins, and the top-level feature engineering config.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, model_validator

from bq_entity_resolution.config.models.source import JoinConfig

__all__ = [
    "FeatureDef",
    "FeatureGroupConfig",
    "BlockingKeyDef",
    "CompositeKeyDef",
    "EnrichmentJoinConfig",
    "CompoundDetectionConfig",
    "FeatureEngineeringConfig",
]


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

    @model_validator(mode="after")
    def _normalize_input(self) -> "FeatureDef":
        """Normalize singular 'input' into 'inputs' list."""
        if self.input and not self.inputs:
            object.__setattr__(self, "inputs", [self.input])
        return self


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


class EnrichmentJoinConfig(BaseModel):
    """A lookup table join for enriching entities with external data.

    Used during feature engineering to bring in standardized addresses,
    GPS coordinates, or any reference data from an external BigQuery table.

    The join key is computed from source columns using a registered feature
    function (e.g., farm_fingerprint_concat). The lookup table must have a
    column matching the computed key's output type (typically INT64 for
    FARM_FINGERPRINT-based keys).

    PERF: When source_key_function produces INT64 (e.g., farm_fingerprint),
    the enrichment join runs at INT64 speed — ~3-5x faster than STRING joins.

    Example YAML::

        enrichment_joins:
          - name: "census_geocode"
            table: "${BQ_PROJECT}.census.address_lookup"
            lookup_key: "address_fp"
            source_key_function: "farm_fingerprint_concat"
            source_key_inputs: ["address_line_1", "city", "state"]
            columns: ["matched_address", "latitude", "longitude"]
            column_prefix: "census_"
            match_flag: "has_census_match"
    """

    name: str  # Human-readable name, also used as JOIN alias
    table: str  # Fully-qualified BQ table (e.g., "project.dataset.table")
    lookup_key: str  # Column in lookup table to join on
    source_key_function: str  # Registered feature function name
    source_key_inputs: list[str]  # Source columns passed to the function
    source_key_params: dict[str, Any] = Field(default_factory=dict)
    columns: list[str]  # Columns to pull from the lookup table
    column_prefix: str = ""  # Prefix for pulled columns (avoids collisions)
    match_flag: str = ""  # If set, auto-generates a 0/1 INT64 match flag column
    type: Literal["LEFT", "INNER"] = "LEFT"


class CompoundDetectionConfig(BaseModel):
    """Compound record detection and handling configuration.

    When enabled, automatically injects compound detection features
    (is_compound_name, compound_pattern) into feature engineering.

    Actions:
        flag:  Add detection columns only (default).
        split: Expand compound rows into individual records.
        both:  Split first, then flag the originals.
    """

    enabled: bool = False
    name_column: str = "first_name"
    last_name_column: str = "last_name"
    action: Literal["flag", "split", "both"] = "flag"
    flag_column: str = "is_compound_name"
    custom_patterns: list[str] = Field(default_factory=list)


class FeatureEngineeringConfig(BaseModel):
    """All feature engineering configuration."""

    name_features: FeatureGroupConfig = Field(default_factory=FeatureGroupConfig)
    address_features: FeatureGroupConfig = Field(default_factory=FeatureGroupConfig)
    contact_features: FeatureGroupConfig = Field(default_factory=FeatureGroupConfig)
    extra_groups: dict[str, FeatureGroupConfig] = Field(default_factory=dict)
    blocking_keys: list[BlockingKeyDef] = Field(default_factory=list)
    composite_keys: list[CompositeKeyDef] = Field(default_factory=list)
    custom_features: list[FeatureDef] = Field(default_factory=list)
    enrichment_joins: list[EnrichmentJoinConfig] = Field(default_factory=list)
    compound_detection: CompoundDetectionConfig = Field(
        default_factory=CompoundDetectionConfig
    )

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
