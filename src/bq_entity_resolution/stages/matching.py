"""Matching stage: scores candidate pairs and filters by threshold.

Extracted from PipelineOrchestrator._execute_tiers() matching portion.
Supports both sum-based and Fellegi-Sunter scoring strategies.
"""

from __future__ import annotations

from typing import Any

from bq_entity_resolution.config.schema import (
    MatchingTierConfig,
    PipelineConfig,
)
from bq_entity_resolution.matching.comparisons import COMPARISON_FUNCTIONS
from bq_entity_resolution.naming import (
    candidates_table,
    featured_table,
    matches_table,
)
from bq_entity_resolution.sql.builders.comparison import (
    ComparisonDef,
    ComparisonLevel,
    HardNegative,
    SoftSignal,
    Threshold,
    SumScoringParams,
    FellegiSunterParams,
    build_sum_scoring_sql,
    build_fellegi_sunter_sql,
)
from bq_entity_resolution.sql.expression import SQLExpression
from bq_entity_resolution.stages.base import Stage, TableRef


class MatchingStage(Stage):
    """Score candidate pairs for a single matching tier.

    Generates comparison SQL, applies hard negatives and soft signals,
    computes aggregate score, and filters by threshold.
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
        self._estimated_params: dict[str, Any] | None = None

    @property
    def name(self) -> str:
        return f"matching_{self._tier.name}"

    @property
    def inputs(self) -> dict[str, TableRef]:
        return {
            "candidates": TableRef(
                name=f"candidates_{self._tier.name}",
                fq_name=candidates_table(self._config, self._tier.name),
            ),
            "featured": TableRef(
                name="featured",
                fq_name=featured_table(self._config),
            ),
        }

    @property
    def outputs(self) -> dict[str, TableRef]:
        target = matches_table(self._config, self._tier.name)
        return {
            "matches": TableRef(
                name=f"matches_{self._tier.name}",
                fq_name=target,
                description=f"Scored matches for tier {self._tier.name}",
            ),
        }

    def set_estimated_params(self, params: dict[str, Any]) -> None:
        """Inject estimated m/u parameters from EM or labeled data."""
        self._estimated_params = params

    def plan(self, **kwargs: Any) -> list[SQLExpression]:
        """Generate matching/scoring SQL."""
        tier = self._tier
        is_fs = tier.threshold.method == "fellegi_sunter"

        if is_fs:
            return self._plan_fellegi_sunter()
        else:
            return self._plan_sum_scoring()

    def _plan_sum_scoring(self) -> list[SQLExpression]:
        """Generate sum-based scoring SQL."""
        tier = self._tier
        config = self._config
        udf_dataset = getattr(config.project, "udf_dataset", "")

        comparisons: list[ComparisonDef] = []
        for comp in tier.comparisons:
            func = COMPARISON_FUNCTIONS.get(comp.method)
            if func is None:
                continue
            params = comp.params or {}
            if udf_dataset:
                params["udf_dataset"] = udf_dataset
            try:
                sql_expr = func(comp.left, comp.right, **params)
            except Exception:
                continue
            comp_name = getattr(
                comp, "name", f"{comp.left}_{comp.method}"
            )
            tf_adj = getattr(comp, "tf_adjustment", None)
            comparisons.append(
                ComparisonDef(
                    name=comp_name,
                    sql_expr=sql_expr,
                    weight=getattr(comp, "weight", 1.0),
                    tf_enabled=(
                        getattr(comp, "tf_enabled", False)
                        or (tf_adj is not None and getattr(tf_adj, "enabled", False))
                    ),
                    tf_column=(
                        getattr(comp, "tf_column", "")
                        or (getattr(tf_adj, "tf_adjustment_column", "") if tf_adj else "")
                        or comp.left
                    ),
                    tf_minimum_u=(
                        getattr(comp, "tf_minimum_u", 0.01)
                        if getattr(comp, "tf_enabled", False)
                        else (getattr(tf_adj, "tf_minimum_u_value", 0.01) if tf_adj else 0.01)
                    ),
                )
            )

        hard_negatives = self._build_hard_negatives(tier)
        soft_signals = self._build_soft_signals(tier)

        max_score = sum(c.weight for c in comparisons)
        tf_table = None
        if any(c.tf_enabled for c in comparisons):
            from bq_entity_resolution.naming import term_frequency_table
            tf_table = term_frequency_table(config)

        params = SumScoringParams(
            tier_name=tier.name,
            tier_index=self._tier_index,
            matches_table=self.outputs["matches"].fq_name,
            candidates_table=self.inputs["candidates"].fq_name,
            source_table=self.inputs["featured"].fq_name,
            comparisons=comparisons,
            hard_negatives=hard_negatives,
            soft_signals=soft_signals,
            threshold=Threshold(
                method=tier.threshold.method,
                min_score=tier.threshold.min_score,
            ),
            confidence=getattr(tier, "confidence", None),
            max_possible_score=max_score,
            tf_table=tf_table,
            audit_trail_enabled=getattr(
                config.monitoring, "audit_trail_enabled", False
            ),
        )

        return [build_sum_scoring_sql(params)]

    def _plan_fellegi_sunter(self) -> list[SQLExpression]:
        """Generate Fellegi-Sunter scoring SQL."""
        tier = self._tier
        config = self._config

        comparisons: list[ComparisonDef] = []
        for comp in tier.comparisons:
            levels: list[ComparisonLevel] = []
            for level in getattr(comp, "levels", []):
                levels.append(
                    ComparisonLevel(
                        label=level.label,
                        sql_expr=getattr(level, "sql_expr", None),
                        log_weight=getattr(level, "log_weight", 0.0),
                        m=getattr(level, "m", 0.9),
                        u=getattr(level, "u", 0.1),
                        tf_adjusted=getattr(level, "tf_adjusted", False),
                    )
                )
            comp_name = getattr(
                comp, "name", f"{comp.left}_{comp.method}"
            )
            tf_adj = getattr(comp, "tf_adjustment", None)
            comparisons.append(
                ComparisonDef(
                    name=comp_name,
                    levels=levels,
                    tf_enabled=(
                        getattr(comp, "tf_enabled", False)
                        or (tf_adj is not None and getattr(tf_adj, "enabled", False))
                    ),
                    tf_column=(
                        getattr(comp, "tf_column", "")
                        or (getattr(tf_adj, "tf_adjustment_column", "") if tf_adj else "")
                        or comp.left
                    ),
                    tf_minimum_u=(
                        getattr(comp, "tf_minimum_u", 0.01)
                        if getattr(comp, "tf_enabled", False)
                        else (getattr(tf_adj, "tf_minimum_u_value", 0.01) if tf_adj else 0.01)
                    ),
                )
            )

        hard_negatives = self._build_hard_negatives(tier)
        soft_signals = self._build_soft_signals(tier)

        log_prior = getattr(tier.threshold, "log_prior_odds", 0.0)

        tf_table = None
        if any(c.tf_enabled for c in comparisons):
            from bq_entity_resolution.naming import term_frequency_table
            tf_table = term_frequency_table(config)

        params = FellegiSunterParams(
            tier_name=tier.name,
            tier_index=self._tier_index,
            matches_table=self.outputs["matches"].fq_name,
            candidates_table=self.inputs["candidates"].fq_name,
            source_table=self.inputs["featured"].fq_name,
            comparisons=comparisons,
            log_prior_odds=log_prior,
            hard_negatives=hard_negatives,
            soft_signals=soft_signals,
            threshold=Threshold(
                method=tier.threshold.method,
                min_score=tier.threshold.min_score,
                match_threshold=getattr(tier.threshold, "match_threshold", None),
            ),
            tf_table=tf_table,
            audit_trail_enabled=getattr(
                config.monitoring, "audit_trail_enabled", False
            ),
        )

        return [build_fellegi_sunter_sql(params)]

    def _build_hard_negatives(self, tier: MatchingTierConfig) -> list[HardNegative]:
        """Convert tier hard negatives to builder format."""
        result: list[HardNegative] = []
        for hn in getattr(tier, "hard_negatives", []):
            result.append(
                HardNegative(
                    sql_condition=hn.sql_condition,
                    action=hn.action,
                    penalty=getattr(hn, "penalty", 0.0),
                )
            )
        return result

    def _build_soft_signals(self, tier: MatchingTierConfig) -> list[SoftSignal]:
        """Convert tier soft signals to builder format."""
        result: list[SoftSignal] = []
        for ss in getattr(tier, "soft_signals", []):
            result.append(
                SoftSignal(
                    sql_condition=ss.sql_condition,
                    bonus=getattr(ss, "bonus", 0.0),
                )
            )
        return result

    def validate(self) -> list[str]:
        errors = []
        if not self._tier.comparisons:
            errors.append(
                f"Tier '{self._tier.name}' has no comparisons defined"
            )
        for comp in self._tier.comparisons:
            if comp.method not in COMPARISON_FUNCTIONS:
                errors.append(
                    f"Unknown comparison method: '{comp.method}' "
                    f"in tier '{self._tier.name}'"
                )
        return errors
