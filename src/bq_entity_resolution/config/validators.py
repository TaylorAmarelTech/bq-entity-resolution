"""
Cross-field validation helpers for pipeline configuration.

These validators check relationships between config sections that
Pydantic's per-model validation cannot catch.
"""

from __future__ import annotations

import difflib

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.exceptions import ConfigurationError


def _suggest_closest(name: str, valid: set[str], n: int = 3) -> str:
    """Suggest closest matches for a typo."""
    matches = difflib.get_close_matches(name, sorted(valid), n=n, cutoff=0.6)
    if matches:
        return f" Did you mean: {', '.join(repr(m) for m in matches)}?"
    return ""


def validate_comparison_columns_exist(config: PipelineConfig) -> None:
    """Ensure all columns referenced in tier comparisons are defined as features or source columns."""
    # Collect all known column names
    known: set[str] = set()
    for source in config.sources:
        for col in source.columns:
            known.add(col.name)
        known.update(source.passthrough_columns)

    known |= config.feature_engineering.all_feature_names()

    # Check tier references
    errors: list[str] = []
    for tier in config.enabled_tiers():
        for comp in tier.comparisons:
            if comp.left not in known:
                errors.append(f"Tier '{tier.name}' comparison references unknown column '{comp.left}'")
            if comp.right not in known:
                errors.append(f"Tier '{tier.name}' comparison references unknown column '{comp.right}'")
        for hn in tier.hard_negatives:
            if hn.left not in known:
                errors.append(f"Tier '{tier.name}' hard_negative references unknown column '{hn.left}'")
        for ss in tier.soft_signals:
            if ss.left not in known:
                errors.append(f"Tier '{tier.name}' soft_signal references unknown column '{ss.left}'")

    # Global hard negatives / soft signals
    for hn in config.global_hard_negatives:
        if hn.left not in known:
            errors.append(f"Global hard_negative references unknown column '{hn.left}'")
    for ss in config.global_soft_signals:
        if ss.left not in known:
            errors.append(f"Global soft_signal references unknown column '{ss.left}'")

    if errors:
        raise ConfigurationError(
            "Column reference validation failed:\n  " + "\n  ".join(errors)
        )


def validate_feature_inputs_exist(config: PipelineConfig) -> None:
    """Ensure feature function inputs reference known source columns or prior features."""
    source_cols = set()
    for source in config.sources:
        for col in source.columns:
            source_cols.add(col.name)

    defined: set[str] = set(source_cols)
    errors: list[str] = []

    for group in config.feature_engineering.all_groups():
        if not group.enabled:
            continue
        for feat in group.features:
            inputs = [feat.input] if feat.input else (feat.inputs or [])
            for inp in inputs:
                if inp not in defined:
                    errors.append(
                        f"Feature '{feat.name}' references unknown input '{inp}'. "
                        f"Ensure it is defined in sources or prior features."
                    )
            defined.add(feat.name)

    if errors:
        raise ConfigurationError(
            "Feature input validation failed:\n  " + "\n  ".join(errors)
        )


def validate_feature_dependencies(config: PipelineConfig) -> None:
    """Validate that feature depends_on references exist and form a DAG."""
    all_names = config.feature_engineering.all_feature_names()
    source_cols = {col.name for src in config.sources for col in src.columns}
    known = all_names | source_cols
    errors: list[str] = []

    for group in config.feature_engineering.all_groups():
        for feat in group.features:
            for dep in feat.depends_on:
                if dep not in known:
                    errors.append(
                        f"Feature '{feat.name}' depends_on unknown feature '{dep}'"
                    )

    for feat in config.feature_engineering.custom_features:
        for dep in feat.depends_on:
            if dep not in known:
                errors.append(
                    f"Custom feature '{feat.name}' depends_on unknown feature '{dep}'"
                )

    if errors:
        raise ConfigurationError(
            "Feature dependency validation failed:\n  " + "\n  ".join(errors)
        )


def validate_comparison_weights(config: PipelineConfig) -> None:
    """Validate comparison weights are positive numbers."""
    errors: list[str] = []
    for tier in config.enabled_tiers():
        for comp in tier.comparisons:
            if comp.weight < 0:
                errors.append(
                    f"Tier '{tier.name}' comparison {comp.left}/{comp.right} "
                    f"has negative weight {comp.weight}"
                )

    if errors:
        raise ConfigurationError(
            "Comparison weight validation failed:\n  " + "\n  ".join(errors)
        )


def validate_fellegi_sunter_config(config: PipelineConfig) -> None:
    """Validate Fellegi-Sunter tier configuration completeness."""
    errors: list[str] = []
    for tier in config.enabled_tiers():
        if tier.threshold.method != "fellegi_sunter":
            continue

        # Resolve training config (tier-level overrides global)
        training = tier.training if tier.training.method != "none" else config.training

        for comp in tier.comparisons:
            if not comp.levels:
                # No levels = auto-created binary; only valid with training
                if training.method == "none":
                    errors.append(
                        f"Tier '{tier.name}' comparison '{comp.left}/{comp.right}': "
                        f"fellegi_sunter requires either comparison levels with m/u "
                        f"or a training config (labeled/em)"
                    )
                continue

            # Verify last level is else (no method)
            if comp.levels[-1].method is not None:
                errors.append(
                    f"Tier '{tier.name}' comparison '{comp.left}/{comp.right}': "
                    f"last level must have method=null (else/fallthrough)"
                )

            # If no training, all levels must have manual m/u
            if training.method == "none":
                for lvl in comp.levels:
                    if lvl.m is None or lvl.u is None:
                        errors.append(
                            f"Tier '{tier.name}' comparison '{comp.left}/{comp.right}' "
                            f"level '{lvl.label}': m and u required when training.method=none"
                        )

        # Validate labeled training has table
        if training.method == "labeled" and not training.labeled_pairs_table:
            errors.append(
                f"Tier '{tier.name}': labeled training requires labeled_pairs_table"
            )

    if errors:
        raise ConfigurationError(
            "Fellegi-Sunter config validation failed:\n  " + "\n  ".join(errors)
        )


def validate_tf_columns_exist(config: PipelineConfig) -> None:
    """Ensure TF adjustment columns reference known features or source columns."""
    known: set[str] = set()
    for source in config.sources:
        for col in source.columns:
            known.add(col.name)
    known |= config.feature_engineering.all_feature_names()

    errors: list[str] = []
    for tier in config.enabled_tiers():
        for comp in tier.comparisons:
            if comp.tf_adjustment and comp.tf_adjustment.enabled:
                col = comp.tf_adjustment.tf_adjustment_column or comp.left
                if col not in known:
                    errors.append(
                        f"Tier '{tier.name}' comparison '{comp.left}/{comp.right}': "
                        f"tf_adjustment_column '{col}' not found in features or source columns"
                    )

    if errors:
        raise ConfigurationError(
            "Term frequency column validation failed:\n  " + "\n  ".join(errors)
        )


def validate_source_schema_alignment(config: PipelineConfig) -> None:
    """Ensure all sources define the same column names for safe UNION ALL.

    When multiple sources are UNION-ALLed during feature engineering,
    they must share the same column set.  This validator catches schema
    mismatches at config load time rather than at BigQuery runtime.
    """
    if len(config.sources) < 2:
        return

    reference = config.sources[0]
    ref_names = {c.name for c in reference.columns}
    errors: list[str] = []

    for source in config.sources[1:]:
        src_names = {c.name for c in source.columns}
        missing = ref_names - src_names
        extra = src_names - ref_names
        if missing:
            errors.append(
                f"Source '{source.name}' missing columns present in "
                f"'{reference.name}': {sorted(missing)}"
            )
        if extra:
            errors.append(
                f"Source '{source.name}' has extra columns not in "
                f"'{reference.name}': {sorted(extra)}"
            )

    if errors:
        raise ConfigurationError(
            "Source schema alignment failed:\n  " + "\n  ".join(errors)
        )


def validate_comparison_methods_registered(config: PipelineConfig) -> None:
    """Validate that all comparison/hard_negative/soft_signal method names are registered.

    Catches typos like ``method: "levenstein"`` at config load time
    instead of at SQL generation time.
    """
    from bq_entity_resolution.features.registry import FEATURE_FUNCTIONS
    from bq_entity_resolution.matching.comparisons import COMPARISON_FUNCTIONS

    known_comparison = set(COMPARISON_FUNCTIONS.keys())
    known_feature = set(FEATURE_FUNCTIONS.keys())
    errors: list[str] = []

    # Feature function names
    for group in config.feature_engineering.all_groups():
        if not group.enabled:
            continue
        for feat in group.features:
            if feat.sql:
                continue  # raw SQL override — no function to validate
            if feat.function not in known_feature:
                suggestion = _suggest_closest(feat.function, known_feature)
                errors.append(
                    f"Feature '{feat.name}' references unknown function "
                    f"'{feat.function}'.{suggestion}"
                )

    for feat in config.feature_engineering.custom_features:
        if feat.sql:
            continue
        if feat.function not in known_feature:
            suggestion = _suggest_closest(feat.function, known_feature)
            errors.append(
                f"Custom feature '{feat.name}' references unknown function "
                f"'{feat.function}'.{suggestion}"
            )

    # Blocking key function names
    for bk in config.feature_engineering.blocking_keys:
        if bk.function not in known_feature:
            suggestion = _suggest_closest(bk.function, known_feature)
            errors.append(
                f"Blocking key '{bk.name}' references unknown function "
                f"'{bk.function}'.{suggestion}"
            )

    # Comparison method names
    for tier in config.enabled_tiers():
        for comp in tier.comparisons:
            if comp.method not in known_comparison:
                suggestion = _suggest_closest(comp.method, known_comparison)
                errors.append(
                    f"Tier '{tier.name}' comparison '{comp.left}/{comp.right}' "
                    f"references unknown method '{comp.method}'.{suggestion}"
                )
            # Multi-level comparisons
            if comp.levels:
                for lvl in comp.levels:
                    if lvl.method and lvl.method not in known_comparison:
                        suggestion = _suggest_closest(lvl.method, known_comparison)
                        errors.append(
                            f"Tier '{tier.name}' comparison '{comp.left}/{comp.right}' "
                            f"level '{lvl.label}' references unknown method "
                            f"'{lvl.method}'.{suggestion}"
                        )

        # Hard negative methods
        for hn in tier.hard_negatives:
            if hn.sql:
                continue  # raw SQL override
            if hn.method not in known_comparison:
                suggestion = _suggest_closest(hn.method, known_comparison)
                errors.append(
                    f"Tier '{tier.name}' hard_negative references unknown method "
                    f"'{hn.method}'.{suggestion}"
                )

        # Soft signal methods
        for ss in tier.soft_signals:
            if ss.sql:
                continue  # raw SQL override
            if ss.method not in known_comparison:
                suggestion = _suggest_closest(ss.method, known_comparison)
                errors.append(
                    f"Tier '{tier.name}' soft_signal references unknown method "
                    f"'{ss.method}'.{suggestion}"
                )

    if errors:
        raise ConfigurationError(
            "Method/function registry validation failed:\n  " + "\n  ".join(errors)
        )


def validate_full(config: PipelineConfig) -> None:
    """Run all cross-field validations."""
    validate_source_schema_alignment(config)
    validate_feature_inputs_exist(config)
    validate_feature_dependencies(config)
    validate_comparison_columns_exist(config)
    validate_comparison_weights(config)
    validate_fellegi_sunter_config(config)
    validate_tf_columns_exist(config)
    validate_comparison_methods_registered(config)
