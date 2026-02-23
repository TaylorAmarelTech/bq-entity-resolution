"""
Matching tier engine.

Generates the comparison and scoring SQL for each matching tier,
using comparison functions from the registry. Supports both
sum-based scoring and Fellegi-Sunter probabilistic scoring.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from bq_entity_resolution.config.schema import (
    ComparisonDef,
    MatchingTierConfig,
    PipelineConfig,
)
from bq_entity_resolution.exceptions import SQLGenerationError
from bq_entity_resolution.matching.comparisons import COMPARISON_FUNCTIONS
from bq_entity_resolution.matching.hard_negatives import build_hard_negative_expr
from bq_entity_resolution.matching.soft_signals import build_soft_signal_expr
from bq_entity_resolution.naming import (
    candidates_table,
    featured_table,
    matches_table,
    term_frequency_table,
    udf_dataset,
)
from bq_entity_resolution.sql.generator import SQLGenerator

logger = logging.getLogger(__name__)


class MatchingEngine:
    """Generates SQL for tier-level comparisons, scoring, and thresholding."""

    def __init__(self, config: PipelineConfig, sql_gen: SQLGenerator | None = None):
        self.config = config
        self.sql_gen = sql_gen or SQLGenerator()
        self._tier_params: dict[str, Any] = {}

    def set_tier_parameters(self, tier_name: str, params: Any) -> None:
        """Store estimated Fellegi-Sunter parameters for a tier."""
        self._tier_params[tier_name] = params

    def generate_tier_sql(self, tier: MatchingTierConfig, tier_index: int) -> str:
        """Generate the full comparison + scoring SQL for a tier."""
        if tier.threshold.method == "fellegi_sunter":
            return self._generate_fellegi_sunter_sql(tier, tier_index)
        return self._generate_sum_sql(tier, tier_index)

    # ------------------------------------------------------------------
    # Sum-based scoring (existing behavior)
    # ------------------------------------------------------------------

    def _generate_sum_sql(self, tier: MatchingTierConfig, tier_index: int) -> str:
        """Generate sum-based scoring SQL (original behavior)."""
        comparisons = self._build_comparisons(tier)
        hard_negatives = self._build_hard_negatives(tier)
        soft_signals = self._build_soft_signals(tier)

        max_possible_score = sum(c["weight"] for c in comparisons)
        max_possible_score += sum(
            ss["bonus"] for ss in soft_signals if ss["bonus"] > 0
        )

        # Determine if any comparison uses TF adjustments
        has_tf = any(c.get("tf_enabled") for c in comparisons)
        tf_table = term_frequency_table(self.config) if has_tf else None

        # Audit trail
        audit_enabled = self.config.reconciliation.output.audit_trail.enabled

        return self.sql_gen.render(
            "matching/tier_comparisons.sql.j2",
            tier_name=tier.name,
            tier_index=tier_index,
            matches_table=matches_table(self.config, tier.name),
            candidates_table=candidates_table(self.config, tier.name),
            source_table=featured_table(self.config),
            comparisons=comparisons,
            hard_negatives=hard_negatives,
            soft_signals=soft_signals,
            threshold=tier.threshold,
            confidence=tier.confidence,
            max_possible_score=max_possible_score if max_possible_score > 0 else 1.0,
            tf_table=tf_table,
            audit_trail_enabled=audit_enabled,
        )

    # ------------------------------------------------------------------
    # Fellegi-Sunter probabilistic scoring
    # ------------------------------------------------------------------

    def _generate_fellegi_sunter_sql(
        self, tier: MatchingTierConfig, tier_index: int
    ) -> str:
        """Generate Fellegi-Sunter log-likelihood scoring SQL."""
        params = self._tier_params.get(tier.name)
        comparisons = self._build_level_comparisons(tier, params)
        hard_negatives = self._build_hard_negatives(tier)
        soft_signals = self._build_soft_signals(tier)

        # Compute prior odds from params or default
        if params and hasattr(params, "log_prior_odds"):
            log_prior_odds = params.log_prior_odds
        else:
            log_prior_odds = math.log2(0.1 / 0.9)  # default 10% prior

        # Determine if any comparison uses TF adjustments
        has_tf = any(c.get("tf_enabled") for c in comparisons)
        tf_table = term_frequency_table(self.config) if has_tf else None

        # Audit trail
        audit_enabled = self.config.reconciliation.output.audit_trail.enabled

        return self.sql_gen.render(
            "matching/tier_fellegi_sunter.sql.j2",
            tier_name=tier.name,
            tier_index=tier_index,
            matches_table=matches_table(self.config, tier.name),
            candidates_table=candidates_table(self.config, tier.name),
            source_table=featured_table(self.config),
            comparisons=comparisons,
            hard_negatives=hard_negatives,
            soft_signals=soft_signals,
            threshold=tier.threshold,
            log_prior_odds=round(log_prior_odds, 6),
            tf_table=tf_table,
            audit_trail_enabled=audit_enabled,
        )

    def _build_level_comparisons(
        self, tier: MatchingTierConfig, params: Any = None
    ) -> list[dict]:
        """Build comparison dicts with per-level SQL and log-weights.

        Each comparison produces:
        {
            "name": "first_name__jaro_winkler",
            "levels": [
                {"label": "exact", "sql_expr": "...", "log_weight": 6.32},
                {"label": "fuzzy", "sql_expr": "...", "log_weight": 2.15},
                {"label": "else", "sql_expr": None, "log_weight": -1.89},
            ]
        }
        """
        udf_ds = udf_dataset(self.config)
        result = []

        # Build a lookup for estimated params if available
        param_lookup: dict[str, dict[str, dict[str, float]]] = {}
        if params and hasattr(params, "comparisons"):
            for cp in params.comparisons:
                param_lookup[cp.comparison_name] = {
                    lvl["label"]: {"m": lvl["m"], "u": lvl["u"]}
                    for lvl in cp.levels
                }

        for comp in tier.comparisons:
            safe_name = f"{comp.left}__{comp.method}".replace(".", "_")
            levels = self._resolve_levels(comp, udf_ds, safe_name, param_lookup)
            entry: dict = {"name": safe_name, "levels": levels}
            # Term frequency adjustment
            if comp.tf_adjustment and comp.tf_adjustment.enabled:
                entry["tf_enabled"] = True
                entry["tf_column"] = comp.tf_adjustment.tf_adjustment_column or comp.left
                entry["tf_minimum_u"] = comp.tf_adjustment.tf_minimum_u_value
            else:
                entry["tf_enabled"] = False
            result.append(entry)

        return result

    def _resolve_levels(
        self,
        comp: ComparisonDef,
        udf_ds: str,
        safe_name: str,
        param_lookup: dict,
    ) -> list[dict]:
        """Resolve comparison levels to SQL expressions with log-weights."""
        # Check if this comparison has TF enabled
        has_tf = (
            comp.tf_adjustment is not None
            and comp.tf_adjustment.enabled
        )

        if comp.levels:
            # Explicit levels defined in config
            levels = []
            for lvl in comp.levels:
                sql_expr = None
                if lvl.method is not None:
                    func = COMPARISON_FUNCTIONS.get(lvl.method)
                    if func is None:
                        raise SQLGenerationError(
                            f"Unknown comparison method '{lvl.method}' in level "
                            f"'{lvl.label}'. Available: {sorted(COMPARISON_FUNCTIONS.keys())}"
                        )
                    params = {**lvl.params, "udf_dataset": udf_ds}
                    sql_expr = func(comp.left, comp.right, **params)

                # Get m/u from level config, param estimation, or defaults
                m, u = self._get_mu(lvl.label, lvl.m, lvl.u, safe_name, param_lookup)
                log_weight = self._log_weight(m, u)

                entry: dict = {
                    "label": lvl.label,
                    "sql_expr": sql_expr,
                    "log_weight": round(log_weight, 6),
                }
                # Pass m/u for TF-enabled exact/match levels so template can compute dynamically
                if has_tf and lvl.label in ("exact", "match"):
                    entry["m"] = round(m, 6)
                    entry["u"] = round(u, 6)
                    entry["tf_adjusted"] = True
                else:
                    entry["tf_adjusted"] = False
                levels.append(entry)
            return levels
        else:
            # Auto-create binary levels: match + else
            func = COMPARISON_FUNCTIONS.get(comp.method)
            if func is None:
                raise SQLGenerationError(
                    f"Unknown comparison method '{comp.method}'. "
                    f"Available: {sorted(COMPARISON_FUNCTIONS.keys())}"
                )
            params = {**comp.params, "udf_dataset": udf_ds}
            sql_expr = func(comp.left, comp.right, **params)

            m_match, u_match = self._get_mu(
                "match", None, None, safe_name, param_lookup
            )
            m_else, u_else = self._get_mu(
                "else", None, None, safe_name, param_lookup
            )

            match_entry: dict = {
                "label": "match",
                "sql_expr": sql_expr,
                "log_weight": round(self._log_weight(m_match, u_match), 6),
            }
            if has_tf:
                match_entry["m"] = round(m_match, 6)
                match_entry["u"] = round(u_match, 6)
                match_entry["tf_adjusted"] = True
            else:
                match_entry["tf_adjusted"] = False

            return [
                match_entry,
                {
                    "label": "else",
                    "sql_expr": None,
                    "log_weight": round(self._log_weight(m_else, u_else), 6),
                    "tf_adjusted": False,
                },
            ]

    @staticmethod
    def _get_mu(
        label: str,
        config_m: float | None,
        config_u: float | None,
        comp_name: str,
        param_lookup: dict,
    ) -> tuple[float, float]:
        """Get m/u from config, estimation, or defaults."""
        # Config takes precedence
        if config_m is not None and config_u is not None:
            return config_m, config_u
        # Then estimation results
        comp_params = param_lookup.get(comp_name, {})
        level_params = comp_params.get(label, {})
        m = config_m or level_params.get("m")
        u = config_u or level_params.get("u")
        if m is not None and u is not None:
            return m, u
        # Defaults for common labels
        defaults = {
            "exact": (0.9, 0.1),
            "match": (0.9, 0.1),
            "fuzzy": (0.7, 0.2),
            "fuzzy_high": (0.7, 0.15),
            "fuzzy_low": (0.5, 0.3),
            "else": (0.1, 0.9),
        }
        return defaults.get(label, (0.5, 0.5))

    @staticmethod
    def _log_weight(m: float, u: float) -> float:
        """Compute log2(m/u) with clamping to prevent log(0)."""
        m = max(0.001, min(0.999, m))
        u = max(0.001, min(0.999, u))
        return math.log2(m / u)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _build_comparisons(self, tier: MatchingTierConfig) -> list[dict]:
        """Build comparison SQL expressions from config (sum-based scoring)."""
        udf_ds = udf_dataset(self.config)
        comparisons = []
        for comp in tier.comparisons:
            func = COMPARISON_FUNCTIONS.get(comp.method)
            if func is None:
                raise SQLGenerationError(
                    f"Unknown comparison method '{comp.method}' in tier '{tier.name}'. "
                    f"Available: {sorted(COMPARISON_FUNCTIONS.keys())}"
                )
            params = {**comp.params, "udf_dataset": udf_ds}
            sql_expr = func(comp.left, comp.right, **params)
            safe_name = f"{comp.left}__{comp.method}".replace(".", "_")
            entry: dict = {
                "name": safe_name,
                "sql_expr": sql_expr,
                "weight": comp.weight,
            }
            # Term frequency adjustment
            if comp.tf_adjustment and comp.tf_adjustment.enabled:
                entry["tf_enabled"] = True
                entry["tf_column"] = comp.tf_adjustment.tf_adjustment_column or comp.left
                entry["tf_minimum_u"] = comp.tf_adjustment.tf_minimum_u_value
            else:
                entry["tf_enabled"] = False
            comparisons.append(entry)
        return comparisons

    def _build_hard_negatives(self, tier: MatchingTierConfig) -> list[dict]:
        """Build hard negative SQL expressions from config."""
        return [build_hard_negative_expr(hn) for hn in tier.hard_negatives]

    def _build_soft_signals(self, tier: MatchingTierConfig) -> list[dict]:
        """Build soft signal SQL expressions from config."""
        return [build_soft_signal_expr(ss) for ss in tier.soft_signals]

    def generate_accumulate_matches_sql(
        self,
        tier: MatchingTierConfig,
        all_matches_tbl: str,
    ) -> str:
        """Generate SQL to append this tier's matches to the accumulated matches table."""
        tier_matches = matches_table(self.config, tier.name)
        audit_enabled = self.config.reconciliation.output.audit_trail.enabled

        cols = (
            "l_entity_uid, r_entity_uid, total_score, tier_priority, "
            "tier_name, match_confidence, matched_at"
        )
        if audit_enabled:
            cols += ", match_detail"

        return (
            f"INSERT INTO `{all_matches_tbl}` ({cols})\n"
            f"SELECT {cols}\n"
            f"FROM `{tier_matches}`"
        )

    def generate_create_udfs_sql(self) -> str:
        """Generate SQL to create required UDFs (e.g., jaro_winkler)."""
        needs_jaro = self._needs_jaro_winkler()
        if not needs_jaro:
            return ""
        return self.sql_gen.render(
            "udfs/jaro_winkler.sql.j2",
            udf_dataset=udf_dataset(self.config),
        )

    def _needs_jaro_winkler(self) -> bool:
        """Check if any tier uses Jaro-Winkler (in comparisons or levels)."""
        jw_methods = {"jaro_winkler", "jaro_winkler_score"}
        for tier in self.config.enabled_tiers():
            for comp in tier.comparisons:
                if comp.method in jw_methods:
                    return True
                if comp.levels:
                    for lvl in comp.levels:
                        if lvl.method in jw_methods:
                            return True
        return False
