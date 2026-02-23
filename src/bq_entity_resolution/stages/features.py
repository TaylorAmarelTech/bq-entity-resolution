"""Feature engineering stage: computes features, blocking keys, and term frequencies.

Extracted from PipelineOrchestrator._engineer_features() and
_compute_term_frequencies().
Uses the features SQL builder for SQL generation.
"""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.features.registry import FEATURE_FUNCTIONS
from bq_entity_resolution.naming import featured_table, staged_table
from bq_entity_resolution.sql.builders.features import (
    FeatureExpr,
    FeatureParams,
    TFColumn,
    build_features_sql,
    build_term_frequencies_sql,
)
from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.stages.base import Stage, TableRef


def _get_features_config(config):
    """Get the features config, supporting both attribute names.

    Real PipelineConfig uses 'feature_engineering', test mocks use 'features'.
    """
    return getattr(
        config, "feature_engineering",
        getattr(config, "features", None),
    )


def _get_feature_groups(features_config):
    """Get feature groups, supporting both real config and test mocks.

    Real FeatureEngineeringConfig has all_groups() method.
    Test mocks have a 'groups' attribute.
    """
    if hasattr(features_config, "all_groups"):
        return features_config.all_groups()
    return getattr(features_config, "groups", [])


class FeatureEngineeringStage(Stage):
    """Compute all engineered features from staged source data.

    Multi-pass feature computation:
    1. Independent features (from source columns)
    2. Dependent features (reference pass 1 features)
    3. Blocking keys and composite keys
    """

    def __init__(self, config: PipelineConfig):
        self._config = config

    @property
    def name(self) -> str:
        return "feature_engineering"

    @property
    def inputs(self) -> dict[str, TableRef]:
        refs = {}
        for source in self._config.sources:
            target = staged_table(self._config, source.name)
            refs[f"staged_{source.name}"] = TableRef(
                name=f"staged_{source.name}",
                fq_name=target,
            )
        return refs

    @property
    def outputs(self) -> dict[str, TableRef]:
        target = featured_table(self._config)
        return {
            "featured": TableRef(
                name="featured",
                fq_name=target,
                description="Featured table with all engineered features",
            ),
        }

    def plan(self, **kwargs: Any) -> list[SQLExpression]:
        """Generate feature engineering SQL."""
        config = self._config
        target = self.outputs["featured"].fq_name
        source_tables = [
            staged_table(config, s.name) for s in config.sources
        ]

        # Collect source columns
        source_columns = set()
        for source in config.sources:
            for col in source.columns:
                source_columns.add(col.name)
        source_col_list = sorted(source_columns)

        # Resolve features
        all_features: list[dict] = []
        for group in _get_feature_groups(_get_features_config(config)):
            for feat in group.features:
                func = FEATURE_FUNCTIONS.get(feat.function)
                if func is None:
                    continue
                inputs = feat.inputs if isinstance(feat.inputs, list) else [feat.inputs]
                params = feat.params or {}
                try:
                    expression = func(inputs, **params)
                except Exception:
                    continue
                all_features.append({
                    "name": feat.name,
                    "expression": expression,
                    "inputs": inputs,
                })

        # Split into independent and dependent
        feature_names = {f["name"] for f in all_features}
        pass1 = []
        pass2 = []
        for f in all_features:
            if any(inp in feature_names for inp in f["inputs"]):
                pass2.append(FeatureExpr(f["name"], f["expression"]))
            else:
                pass1.append(FeatureExpr(f["name"], f["expression"]))

        # Blocking keys
        blocking_keys = []
        fc = _get_features_config(config)
        for bk in fc.blocking_keys:
            func = FEATURE_FUNCTIONS.get(bk.function)
            if func is None:
                continue
            inputs = bk.inputs if isinstance(bk.inputs, list) else [bk.inputs]
            params = getattr(bk, "params", None) or {}
            try:
                expression = func(inputs, **params)
            except Exception:
                continue
            blocking_keys.append(FeatureExpr(bk.name, expression))

        # Composite keys
        composite_keys = []
        for ck in fc.composite_keys:
            composite_keys.append(FeatureExpr(ck.name, ck.expression))

        params = FeatureParams(
            target_table=target,
            source_tables=source_tables,
            source_columns=source_col_list,
            feature_expressions=pass1,
            dependent_features=pass2,
            blocking_keys=blocking_keys,
            composite_keys=composite_keys,
        )

        return [build_features_sql(params)]

    def validate(self) -> list[str]:
        errors = []
        fc = _get_features_config(self._config)
        for group in _get_feature_groups(fc):
            for feat in group.features:
                if feat.function not in FEATURE_FUNCTIONS:
                    errors.append(
                        f"Unknown feature function: '{feat.function}' "
                        f"for feature '{feat.name}'"
                    )
        return errors


class TermFrequencyStage(Stage):
    """Compute term frequency statistics for TF-adjusted matching."""

    def __init__(self, config: PipelineConfig):
        self._config = config

    @property
    def name(self) -> str:
        return "term_frequencies"

    @property
    def inputs(self) -> dict[str, TableRef]:
        target = featured_table(self._config)
        return {
            "featured": TableRef(name="featured", fq_name=target),
        }

    @property
    def outputs(self) -> dict[str, TableRef]:
        from bq_entity_resolution.naming import term_frequency_table
        target = term_frequency_table(self._config)
        return {
            "tf_stats": TableRef(
                name="tf_stats",
                fq_name=target,
                description="Term frequency statistics",
            ),
        }

    def plan(self, **kwargs: Any) -> list[SQLExpression]:
        """Generate TF computation SQL, or empty if no TF-enabled comparisons."""
        tf_columns: list[TFColumn] = []

        for tier in self._config.enabled_tiers():
            for comp in tier.comparisons:
                if getattr(comp, "tf_enabled", False):
                    col_name = getattr(comp, "tf_column", comp.left)
                    if col_name and not any(
                        c.column_name == col_name for c in tf_columns
                    ):
                        tf_columns.append(TFColumn(col_name))

        if not tf_columns:
            return []  # No TF-enabled comparisons

        return [
            build_term_frequencies_sql(
                target_table=self.outputs["tf_stats"].fq_name,
                source_table=self.inputs["featured"].fq_name,
                tf_columns=tf_columns,
            )
        ]
