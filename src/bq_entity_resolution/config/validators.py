"""
Cross-field validation helpers for pipeline configuration.

These validators check relationships between config sections that
Pydantic's per-model validation cannot catch.
"""

from __future__ import annotations

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.exceptions import ConfigurationError


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


def validate_full(config: PipelineConfig) -> None:
    """Run all cross-field validations."""
    validate_feature_inputs_exist(config)
    validate_feature_dependencies(config)
    validate_comparison_columns_exist(config)
    validate_comparison_weights(config)
    validate_fellegi_sunter_config(config)
    validate_tf_columns_exist(config)
