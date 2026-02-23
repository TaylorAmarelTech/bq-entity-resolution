"""Tests for cross-field configuration validators."""

from __future__ import annotations

import pytest

from bq_entity_resolution.config.schema import (
    BlockingKeyDef,
    BlockingPathDef,
    ColumnMapping,
    ComparisonDef,
    FeatureDef,
    FeatureEngineeringConfig,
    FeatureGroupConfig,
    HardNegativeDef,
    MatchingTierConfig,
    PipelineConfig,
    ProjectConfig,
    SoftSignalDef,
    SourceConfig,
    ThresholdConfig,
    TierBlockingConfig,
)
from bq_entity_resolution.config.validators import (
    validate_comparison_methods_registered,
    validate_source_schema_alignment,
    validate_full,
)
from bq_entity_resolution.exceptions import ConfigurationError


def _make_source(name: str, columns: list[str]) -> SourceConfig:
    return SourceConfig(
        name=name,
        table=f"proj.ds.{name}",
        unique_key="id",
        updated_at="updated_at",
        columns=[ColumnMapping(name=c) for c in columns],
    )


def _make_config(sources: list[SourceConfig]) -> PipelineConfig:
    return PipelineConfig(
        project=ProjectConfig(name="test", bq_project="proj"),
        sources=sources,
        feature_engineering=FeatureEngineeringConfig(
            blocking_keys=[
                BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
            ],
        ),
        matching_tiers=[
            MatchingTierConfig(
                name="t1",
                blocking=TierBlockingConfig(
                    paths=[BlockingPathDef(keys=["bk"])],
                ),
                comparisons=[
                    ComparisonDef(left="a", right="a", method="exact"),
                ],
                threshold=ThresholdConfig(method="sum", min_score=1.0),
            ),
        ],
    )


class TestSourceSchemaAlignment:
    """Tests for validate_source_schema_alignment."""

    def test_single_source_passes(self):
        config = _make_config([_make_source("s1", ["a", "b"])])
        validate_source_schema_alignment(config)  # no error

    def test_identical_sources_pass(self):
        config = _make_config([
            _make_source("s1", ["a", "b"]),
            _make_source("s2", ["a", "b"]),
        ])
        validate_source_schema_alignment(config)  # no error

    def test_missing_column_fails(self):
        config = _make_config([
            _make_source("crm", ["a", "b", "c"]),
            _make_source("erp", ["a", "b"]),
        ])
        with pytest.raises(ConfigurationError, match="missing columns"):
            validate_source_schema_alignment(config)

    def test_extra_column_fails(self):
        config = _make_config([
            _make_source("crm", ["a", "b"]),
            _make_source("erp", ["a", "b", "extra"]),
        ])
        with pytest.raises(ConfigurationError, match="extra columns"):
            validate_source_schema_alignment(config)

    def test_both_missing_and_extra_fails(self):
        config = _make_config([
            _make_source("crm", ["a", "b"]),
            _make_source("erp", ["a", "c"]),
        ])
        with pytest.raises(ConfigurationError, match="Source schema alignment"):
            validate_source_schema_alignment(config)

    def test_three_sources_all_must_match(self):
        config = _make_config([
            _make_source("s1", ["a", "b"]),
            _make_source("s2", ["a", "b"]),
            _make_source("s3", ["a"]),
        ])
        with pytest.raises(ConfigurationError, match="s3"):
            validate_source_schema_alignment(config)

    def test_runs_in_validate_full(self):
        config = _make_config([
            _make_source("s1", ["a", "b"]),
            _make_source("s2", ["a"]),
        ])
        with pytest.raises(ConfigurationError, match="Source schema alignment"):
            validate_full(config)


class TestMethodRegistryValidation:
    """Tests for validate_comparison_methods_registered."""

    def test_valid_methods_pass(self):
        """Config with all valid methods passes validation."""
        config = _make_config([_make_source("s1", ["a"])])
        validate_comparison_methods_registered(config)

    def test_invalid_comparison_method_fails(self):
        """Typo in comparison method is caught at config time."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="levenstein"),  # typo!
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        with pytest.raises(ConfigurationError, match="levenstein"):
            validate_comparison_methods_registered(config)

    def test_invalid_hard_negative_method_fails(self):
        """Typo in hard negative method is caught."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="exact"),
                    ],
                    hard_negatives=[
                        HardNegativeDef(left="a", method="diferent"),  # typo!
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        with pytest.raises(ConfigurationError, match="diferent"):
            validate_comparison_methods_registered(config)

    def test_raw_sql_override_skips_validation(self):
        """Hard negative with sql override skips method validation."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="exact"),
                    ],
                    hard_negatives=[
                        HardNegativeDef(
                            left="a", method="custom_thing",
                            sql="l.a != r.a",
                        ),
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        validate_comparison_methods_registered(config)  # should pass

    def test_invalid_feature_function_fails(self):
        """Typo in feature function name is caught."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a"])],
            feature_engineering=FeatureEngineeringConfig(
                name_features=FeatureGroupConfig(features=[
                    FeatureDef(name="a_clean", function="name_kleen", inputs=["a"]),
                ]),
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="exact"),
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        with pytest.raises(ConfigurationError, match="name_kleen"):
            validate_comparison_methods_registered(config)

    def test_invalid_blocking_key_function_fails(self):
        """Typo in blocking key function name is caught."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farmm_fingerprint", inputs=["a"]),
                ],
            ),
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="exact"),
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        with pytest.raises(ConfigurationError, match="farmm_fingerprint"):
            validate_comparison_methods_registered(config)

    def test_runs_in_validate_full(self):
        """Method validation runs as part of validate_full."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="typo_method"),
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        with pytest.raises(ConfigurationError, match="typo_method"):
            validate_full(config)


class TestGlobalHardNegativesAndSoftSignals:
    """Tests for global hard negatives and soft signals."""

    def test_global_hard_negatives_in_config(self):
        """Global hard negatives are accepted in PipelineConfig."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            global_hard_negatives=[
                HardNegativeDef(left="a", method="different", action="disqualify"),
            ],
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="exact"),
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        assert len(config.global_hard_negatives) == 1

    def test_effective_hard_negatives_combines(self):
        """effective_hard_negatives merges global + tier-level."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a", "b"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            global_hard_negatives=[
                HardNegativeDef(left="a", method="different", action="disqualify"),
            ],
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="exact"),
                    ],
                    hard_negatives=[
                        HardNegativeDef(left="b", method="null_either", action="disqualify"),
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        effective = config.effective_hard_negatives(config.matching_tiers[0])
        assert len(effective) == 2
        assert effective[0].left == "a"  # global first
        assert effective[1].left == "b"  # tier-specific second

    def test_effective_soft_signals_combines(self):
        """effective_soft_signals merges global + tier-level."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a", "b"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            global_soft_signals=[
                SoftSignalDef(left="a", method="exact", bonus=1.0),
            ],
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="exact"),
                    ],
                    soft_signals=[
                        SoftSignalDef(left="b", method="exact", bonus=2.0),
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        effective = config.effective_soft_signals(config.matching_tiers[0])
        assert len(effective) == 2
        assert effective[0].bonus == 1.0  # global first
        assert effective[1].bonus == 2.0  # tier-specific second

    def test_global_only_no_tier_level(self):
        """Global signals work when tier has none."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            global_hard_negatives=[
                HardNegativeDef(left="a", method="different", action="disqualify"),
            ],
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="exact"),
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        effective = config.effective_hard_negatives(config.matching_tiers[0])
        assert len(effective) == 1

    def test_global_column_reference_validated(self):
        """Global hard negative referencing unknown column is caught."""
        config = PipelineConfig(
            project=ProjectConfig(name="test", bq_project="proj"),
            sources=[_make_source("s1", ["a"])],
            feature_engineering=FeatureEngineeringConfig(
                blocking_keys=[
                    BlockingKeyDef(name="bk", function="farm_fingerprint", inputs=["a"]),
                ],
            ),
            global_hard_negatives=[
                HardNegativeDef(left="nonexistent", method="different"),
            ],
            matching_tiers=[
                MatchingTierConfig(
                    name="t1",
                    blocking=TierBlockingConfig(
                        paths=[BlockingPathDef(keys=["bk"])],
                    ),
                    comparisons=[
                        ComparisonDef(left="a", right="a", method="exact"),
                    ],
                    threshold=ThresholdConfig(method="sum", min_score=1.0),
                ),
            ],
        )
        with pytest.raises(ConfigurationError, match="nonexistent"):
            validate_full(config)


class TestToYaml:
    """Tests for PipelineConfig.to_yaml()."""

    def test_to_yaml_returns_string(self):
        config = _make_config([_make_source("s1", ["a"])])
        yaml_str = config.to_yaml()
        assert isinstance(yaml_str, str)
        assert "project:" in yaml_str
        assert "bq_project: proj" in yaml_str

    def test_to_yaml_includes_tiers(self):
        config = _make_config([_make_source("s1", ["a"])])
        yaml_str = config.to_yaml()
        assert "matching_tiers:" in yaml_str
        assert "method: exact" in yaml_str

    def test_to_yaml_roundtrip_structure(self):
        """YAML output can be parsed back as a dict."""
        import yaml
        config = _make_config([_make_source("s1", ["a"])])
        yaml_str = config.to_yaml()
        data = yaml.safe_load(yaml_str)
        assert "project" in data
        assert "sources" in data
        assert "matching_tiers" in data
