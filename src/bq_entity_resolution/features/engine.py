"""
Feature engineering engine.

Translates config-driven feature definitions into BigQuery SQL,
orchestrating staging, feature computation, blocking key generation,
and composite key generation.
"""

from __future__ import annotations

import logging
from typing import Any

from bq_entity_resolution.config.schema import (
    FeatureDef,
    FeatureGroupConfig,
    PipelineConfig,
    SourceConfig,
)
from bq_entity_resolution.exceptions import SQLGenerationError
from bq_entity_resolution.features.registry import FEATURE_FUNCTIONS
from bq_entity_resolution.naming import featured_table, staged_table, term_frequency_table
from bq_entity_resolution.sql.generator import SQLGenerator

logger = logging.getLogger(__name__)


class FeatureEngine:
    """Generates SQL for staging, feature engineering, and key generation."""

    def __init__(self, config: PipelineConfig, sql_gen: SQLGenerator | None = None):
        self.config = config
        self.sql_gen = sql_gen or SQLGenerator()

    # ------------------------------------------------------------------
    # Staging
    # ------------------------------------------------------------------

    def generate_staging_sql(
        self,
        source: SourceConfig,
        watermark: dict[str, Any] | None,
        full_refresh: bool = False,
    ) -> str:
        """Generate incremental load SQL for a source."""
        target = staged_table(self.config, source.name)
        return self.sql_gen.render(
            "staging/incremental_load.sql.j2",
            target_table=target,
            source=source,
            watermark=watermark,
            grace_period_hours=self.config.incremental.grace_period_hours,
            full_refresh=full_refresh,
            cluster_by=["entity_uid", "source_name"],
        )

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------

    def generate_feature_sql(self) -> str:
        """Generate the complete feature engineering SQL."""
        fe = self.config.feature_engineering

        # Collect feature expressions from all configured groups (dynamic)
        feature_expressions = []
        for group in fe.all_groups():
            if not group.enabled:
                continue
            for feat in group.features:
                expr = self._feature_to_sql(feat)
                inputs = [feat.input] if feat.input else (feat.inputs or [])
                feature_expressions.append({
                    "name": feat.name,
                    "expression": expr,
                    "inputs": inputs,
                })

        # Custom features
        custom_joins: list[dict[str, str]] = []
        for feat in fe.custom_features:
            if feat.sql:
                feature_expressions.append({
                    "name": feat.name,
                    "expression": feat.sql,
                    "inputs": [],
                })
            else:
                inputs = [feat.input] if feat.input else (feat.inputs or [])
                feature_expressions.append({
                    "name": feat.name,
                    "expression": self._feature_to_sql(feat),
                    "inputs": inputs,
                })
            if feat.join:
                custom_joins.append({
                    "table": feat.join.table,
                    "alias": feat.join.alias or f"cj_{feat.name}",
                    "on": feat.join.on,
                })

        # Split features into independent (pass 1) and dependent (pass 2).
        # A feature is dependent if any of its inputs match another feature name.
        pass1, pass2 = self._split_feature_passes(feature_expressions)

        # Blocking keys
        blocking_keys = []
        for bk in fe.blocking_keys:
            expr = self._resolve_function(bk.function, bk.inputs)
            blocking_keys.append({"name": bk.name, "expression": expr})

        # Composite keys
        composite_keys = []
        for ck in fe.composite_keys:
            expr = self._resolve_function(ck.function, ck.inputs)
            composite_keys.append({"name": ck.name, "expression": expr})

        # Build source table references — union of all staged sources
        source_tables = [
            staged_table(self.config, s.name)
            for s in self.config.sources
        ]

        cluster_by = (
            self.config.scale.featured_table_clustering or ["entity_uid"]
        )
        return self.sql_gen.render(
            "features/all_features.sql.j2",
            target_table=featured_table(self.config),
            source_tables=source_tables,
            feature_expressions=pass1,
            dependent_features=pass2,
            blocking_keys=blocking_keys,
            composite_keys=composite_keys,
            source_columns=self._all_source_columns(),
            passthrough_columns=self._all_passthrough_columns(),
            custom_joins=custom_joins,
            cluster_by=cluster_by,
        )

    # ------------------------------------------------------------------
    # Term frequency computation
    # ------------------------------------------------------------------

    def generate_term_frequency_sql(self) -> str | None:
        """Generate SQL to compute term frequencies for TF-enabled comparisons.

        Returns None if no comparison has tf_adjustment.enabled = True.
        """
        tf_columns = self._collect_tf_columns()
        if not tf_columns:
            return None

        return self.sql_gen.render(
            "features/term_frequencies.sql.j2",
            target_table=term_frequency_table(self.config),
            source_table=featured_table(self.config),
            tf_columns=tf_columns,
        )

    def _collect_tf_columns(self) -> list[dict[str, str]]:
        """Collect unique TF-enabled columns from all tier comparisons."""
        seen: set[str] = set()
        result: list[dict[str, str]] = []
        for tier in self.config.enabled_tiers():
            for comp in tier.comparisons:
                if comp.tf_adjustment and comp.tf_adjustment.enabled:
                    col = comp.tf_adjustment.tf_adjustment_column or comp.left
                    if col not in seen:
                        result.append({"column_name": col})
                        seen.add(col)
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_feature_passes(
        features: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """Split features into independent (pass 1) and dependent (pass 2).

        A feature is dependent if any of its inputs match another feature's
        output name. This prevents undefined column references in the SQL
        when feature B depends on feature A's output.
        """
        feature_names = {f["name"] for f in features}
        pass1: list[dict] = []
        pass2: list[dict] = []
        for f in features:
            inputs = f.get("inputs", [])
            if any(inp in feature_names for inp in inputs):
                pass2.append(f)
            else:
                pass1.append(f)
        return pass1, pass2

    def _feature_to_sql(self, feat: FeatureDef) -> str:
        """Convert a feature definition to a SQL expression."""
        inputs = [feat.input] if feat.input else (feat.inputs or [])
        return self._resolve_function(feat.function, inputs, **feat.params)

    def _resolve_function(
        self, func_name: str, inputs: list[str], **params: Any
    ) -> str:
        """Look up a registered function and call it."""
        func = FEATURE_FUNCTIONS.get(func_name)
        if func is None:
            available = sorted(FEATURE_FUNCTIONS.keys())
            raise SQLGenerationError(
                f"Unknown feature function: '{func_name}'. "
                f"Available: {available}"
            )
        return func(inputs, **params)

    def _all_source_columns(self) -> list[str]:
        """Collect all source column names (deduplicated, ordered)."""
        seen: set[str] = set()
        columns: list[str] = []
        for source in self.config.sources:
            for col in source.columns:
                if col.name not in seen:
                    columns.append(col.name)
                    seen.add(col.name)
        return columns

    def _all_passthrough_columns(self) -> list[str]:
        """Collect all passthrough column names (deduplicated, ordered)."""
        seen: set[str] = set()
        columns: list[str] = []
        for source in self.config.sources:
            for col in source.passthrough_columns:
                if col not in seen:
                    columns.append(col)
                    seen.add(col)
        return columns
