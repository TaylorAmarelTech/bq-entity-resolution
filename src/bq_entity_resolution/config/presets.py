"""Configuration presets for common entity resolution use cases.

Presets provide progressive disclosure:
- Level 1: quick_config() — 5 lines, auto-detects roles from column names
- Level 2: role-based — assign semantic roles, auto-generate features
- Level 3: Full YAML control — complete manual configuration

Each preset function takes minimal inputs and returns a full
PipelineConfig.
"""

from __future__ import annotations

from bq_entity_resolution.exceptions import ConfigurationError
from bq_entity_resolution.config.schema import (
    ActiveLearningConfig,
    BlockingKeyDef,
    BlockingPathDef,
    CanonicalSelectionConfig,
    ClusteringConfig,
    ColumnMapping,
    ComparisonDef,
    FeatureDef,
    FeatureEngineeringConfig,
    FeatureGroupConfig,
    MatchingTierConfig,
    OutputConfig,
    PipelineConfig,
    ProjectConfig,
    ReconciliationConfig,
    SourceConfig,
    ThresholdConfig,
    TierBlockingConfig,
)
from bq_entity_resolution.config.roles import (
    blocking_keys_for_role,
    comparisons_for_role,
    detect_role,
    features_for_role,
)


def quick_config(
    bq_project: str,
    source_table: str,
    unique_key: str = "id",
    updated_at: str = "updated_at",
    columns: list[str] | None = None,
    column_roles: dict[str, str] | None = None,
    project_name: str | None = None,
) -> PipelineConfig:
    """Generate a complete PipelineConfig from minimal inputs.

    This is Level 1 progressive disclosure: provide a source table
    and column list, and the system auto-detects roles and generates
    all features, blocking keys, comparisons, and tiers.

    Args:
        bq_project: GCP project ID.
        source_table: Fully-qualified BigQuery source table.
        unique_key: Primary key column name.
        updated_at: Timestamp column for incremental processing.
        columns: Column names to include. If None, must provide
            column_roles.
        column_roles: Explicit role assignments {column_name: role}.
            Overrides auto-detection.
        project_name: Optional project name (derived from table if not set).
    """
    # Resolve column roles
    role_map = _resolve_roles(columns, column_roles)
    if not role_map:
        raise ConfigurationError(
            "No columns with recognized roles. Provide column_roles "
            "explicitly or use column names that match common patterns "
            "(first_name, last_name, email, etc.)."
        )

    # Derive project name
    if not project_name:
        parts = source_table.split(".")
        project_name = parts[-1] if parts else "er_pipeline"

    return _build_config(
        bq_project=bq_project,
        project_name=project_name,
        source_table=source_table,
        unique_key=unique_key,
        updated_at=updated_at,
        role_map=role_map,
    )


def person_dedup_preset(
    bq_project: str,
    source_table: str,
    unique_key: str = "id",
    updated_at: str = "updated_at",
    columns: dict[str, str] | None = None,
    project_name: str = "person_dedup",
) -> PipelineConfig:
    """Preset for person deduplication.

    Auto-generates features for person attributes: name, DOB, email,
    phone, address. Creates two tiers: exact (deterministic) and
    fuzzy (probabilistic with Jaro-Winkler).

    Args:
        bq_project: GCP project ID.
        source_table: Source table with person records.
        unique_key: Primary key column.
        updated_at: Timestamp column.
        columns: {column_name: role} mapping. Common roles:
            first_name, last_name, date_of_birth, email, phone,
            address_line_1, city, state, zip_code, ssn.
        project_name: Project name for dataset naming.
    """
    if not columns:
        raise ConfigurationError(
            "columns dict required: {column_name: role}. "
            "Common roles: first_name, last_name, date_of_birth, "
            "email, phone, address_line_1, city, state, zip_code, ssn"
        )

    return _build_config(
        bq_project=bq_project,
        project_name=project_name,
        source_table=source_table,
        unique_key=unique_key,
        updated_at=updated_at,
        role_map=columns,
        link_type="dedupe_only",
    )


def person_linkage_preset(
    bq_project: str,
    source_tables: list[dict],
    columns: dict[str, str],
    unique_key: str = "id",
    updated_at: str = "updated_at",
    project_name: str = "person_linkage",
) -> PipelineConfig:
    """Preset for person record linkage across multiple sources.

    Args:
        bq_project: GCP project ID.
        source_tables: List of source table dicts, each with:
            {"name": "crm", "table": "proj.ds.table"}
        columns: {column_name: role} mapping (shared across sources).
        unique_key: Primary key column (same in all sources).
        updated_at: Timestamp column (same in all sources).
        project_name: Project name.
    """
    if not source_tables or len(source_tables) < 2:
        raise ConfigurationError("At least 2 source tables required for linkage")

    # Build column mappings
    col_mappings = [
        ColumnMapping(name=col, role=role)
        for col, role in columns.items()
    ]

    # Build sources
    sources = []
    for st in source_tables:
        sources.append(SourceConfig(
            name=st["name"],
            table=st["table"],
            unique_key=unique_key,
            updated_at=updated_at,
            columns=col_mappings,
        ))

    # Generate features, blocking, comparison pool from roles
    features, blocking_keys, comparison_pool = _generate_from_roles(columns)

    # Build tiers (using pool references)
    blocking_key_names = [bk.name for bk in blocking_keys]
    tiers = _build_default_tiers(blocking_key_names, comparison_pool)

    return PipelineConfig(
        project=ProjectConfig(name=project_name, bq_project=bq_project),
        sources=sources,
        feature_engineering=FeatureEngineeringConfig(
            name_features=FeatureGroupConfig(features=features),
            blocking_keys=blocking_keys,
        ),
        comparison_pool=comparison_pool,
        matching_tiers=tiers,
        link_type="link_only",
    )


def insurance_dedup_preset(
    bq_project: str,
    source_table: str,
    unique_key: str = "id",
    updated_at: str = "updated_at",
    columns: dict[str, str] | None = None,
    project_name: str = "insurance_dedup",
) -> PipelineConfig:
    """Preset for insurance entity resolution (claims, policies).

    Auto-generates features for insurance attributes: policy number,
    claim number, insured name, DOB, address, phone, SSN.

    Args:
        bq_project: GCP project ID.
        source_table: Source table with insurance records.
        unique_key: Primary key column.
        updated_at: Timestamp column.
        columns: {column_name: role} mapping. Common roles:
            policy_number, claim_number, first_name, last_name,
            date_of_birth, date_of_loss, address_line_1, city,
            state, zip_code, phone, email, ssn.
        project_name: Project name.
    """
    if not columns:
        raise ConfigurationError(
            "columns dict required: {column_name: role}. "
            "Common roles: policy_number, claim_number, first_name, "
            "last_name, date_of_birth, date_of_loss, ssn, phone, email"
        )

    return _build_config(
        bq_project=bq_project,
        project_name=project_name,
        source_table=source_table,
        unique_key=unique_key,
        updated_at=updated_at,
        role_map=columns,
        link_type="dedupe_only",
    )


def financial_transaction_preset(
    bq_project: str,
    source_table: str,
    unique_key: str = "id",
    updated_at: str = "updated_at",
    columns: dict[str, str] | None = None,
    project_name: str = "financial_txn_match",
) -> PipelineConfig:
    """Preset for financial transaction matching.

    Auto-generates features for financial attributes: account number,
    routing number, transaction amount/date, customer name.

    Args:
        bq_project: GCP project ID.
        source_table: Source table with transaction records.
        unique_key: Primary key column.
        updated_at: Timestamp column.
        columns: {column_name: role} mapping. Common roles:
            account_number, routing_number, transaction_amount,
            transaction_date, first_name, last_name, email, phone.
        project_name: Project name.
    """
    if not columns:
        raise ConfigurationError(
            "columns dict required: {column_name: role}. "
            "Common roles: account_number, routing_number, "
            "transaction_amount, transaction_date, first_name, last_name"
        )

    return _build_config(
        bq_project=bq_project,
        project_name=project_name,
        source_table=source_table,
        unique_key=unique_key,
        updated_at=updated_at,
        role_map=columns,
        link_type="link_and_dedupe",
    )


def healthcare_patient_preset(
    bq_project: str,
    source_table: str,
    unique_key: str = "id",
    updated_at: str = "updated_at",
    columns: dict[str, str] | None = None,
    project_name: str = "patient_match",
) -> PipelineConfig:
    """Preset for healthcare patient matching.

    Auto-generates features for healthcare attributes: NPI, MRN,
    patient name, DOB, address, phone, SSN.

    Args:
        bq_project: GCP project ID.
        source_table: Source table with patient records.
        unique_key: Primary key column.
        updated_at: Timestamp column.
        columns: {column_name: role} mapping. Common roles:
            npi, mrn, first_name, last_name, date_of_birth,
            address_line_1, city, state, zip_code, phone, email, ssn.
        project_name: Project name.
    """
    if not columns:
        raise ConfigurationError(
            "columns dict required: {column_name: role}. "
            "Common roles: npi, mrn, first_name, last_name, "
            "date_of_birth, address_line_1, city, state, zip_code, "
            "phone, email, ssn"
        )

    return _build_config(
        bq_project=bq_project,
        project_name=project_name,
        source_table=source_table,
        unique_key=unique_key,
        updated_at=updated_at,
        role_map=columns,
        link_type="dedupe_only",
    )


def business_dedup_preset(
    bq_project: str,
    source_table: str,
    unique_key: str = "id",
    updated_at: str = "updated_at",
    columns: dict[str, str] | None = None,
    project_name: str = "business_dedup",
) -> PipelineConfig:
    """Preset for business/company deduplication.

    Args:
        bq_project: GCP project ID.
        source_table: Source table with business records.
        unique_key: Primary key column.
        updated_at: Timestamp column.
        columns: {column_name: role} mapping. Common roles:
            company_name, ein, address_line_1, city, state,
            zip_code, phone, email.
        project_name: Project name.
    """
    if not columns:
        raise ConfigurationError(
            "columns dict required: {column_name: role}. "
            "Common roles: company_name, ein, address_line_1, "
            "city, state, zip_code, phone, email"
        )

    return _build_config(
        bq_project=bq_project,
        project_name=project_name,
        source_table=source_table,
        unique_key=unique_key,
        updated_at=updated_at,
        role_map=columns,
        link_type="dedupe_only",
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_roles(
    columns: list[str] | None,
    column_roles: dict[str, str] | None,
) -> dict[str, str]:
    """Resolve column-to-role mapping.

    Explicit roles take priority. Auto-detection fills in the rest.
    """
    result: dict[str, str] = {}

    if column_roles:
        result.update(column_roles)

    if columns:
        for col in columns:
            if col not in result:
                role = detect_role(col)
                if role:
                    result[col] = role

    return result


def _build_config(
    bq_project: str,
    project_name: str,
    source_table: str,
    unique_key: str,
    updated_at: str,
    role_map: dict[str, str],
    link_type: str = "link_and_dedupe",
) -> PipelineConfig:
    """Build a full PipelineConfig from a role map."""
    # Column mappings
    col_mappings = [
        ColumnMapping(name=col, role=role)
        for col, role in role_map.items()
    ]

    # Source
    source = SourceConfig(
        name=source_table.rsplit(".", 1)[-1],
        table=source_table,
        unique_key=unique_key,
        updated_at=updated_at,
        columns=col_mappings,
    )

    # Generate features, blocking keys, comparison pool
    features, blocking_keys, comparison_pool = _generate_from_roles(role_map)

    # Build tiers (using pool references)
    blocking_key_names = [bk.name for bk in blocking_keys]
    tiers = _build_default_tiers(blocking_key_names, comparison_pool)

    return PipelineConfig(
        project=ProjectConfig(name=project_name, bq_project=bq_project),
        sources=[source],
        feature_engineering=FeatureEngineeringConfig(
            name_features=FeatureGroupConfig(features=features),
            blocking_keys=blocking_keys,
        ),
        comparison_pool=comparison_pool,
        matching_tiers=tiers,
        link_type=link_type,
    )


def _generate_from_roles(
    role_map: dict[str, str],
) -> tuple[list[FeatureDef], list[BlockingKeyDef], dict[str, ComparisonDef]]:
    """Generate features, blocking keys, and comparison pool from roles.

    Returns comparison pool as a dict keyed by name (e.g. "email_exact")
    instead of a flat list.  Tiers reference these by name via ``ref``.
    """
    all_features: list[FeatureDef] = []
    all_blocking_keys: list[BlockingKeyDef] = []
    comparison_pool: dict[str, ComparisonDef] = {}
    seen_bk_names: set[str] = set()

    for col, role in role_map.items():
        # Features
        for feat_dict in features_for_role(col, role):
            all_features.append(FeatureDef(
                name=feat_dict["name"],
                function=feat_dict["function"],
                inputs=feat_dict["inputs"],
            ))

        # Blocking keys (deduplicate by name)
        for bk_dict in blocking_keys_for_role(col, role):
            if bk_dict["name"] not in seen_bk_names:
                all_blocking_keys.append(BlockingKeyDef(
                    name=bk_dict["name"],
                    function=bk_dict["function"],
                    inputs=bk_dict["inputs"],
                ))
                seen_bk_names.add(bk_dict["name"])

        # Comparisons → pool entries keyed by name
        for comp_dict in comparisons_for_role(col, role):
            pool_name = comp_dict["name"]
            comparison_pool[pool_name] = ComparisonDef(
                left=comp_dict["left"],
                right=comp_dict["right"],
                method=comp_dict["method"],
                weight=comp_dict["weight"],
            )

    return all_features, all_blocking_keys, comparison_pool


def _build_default_tiers(
    blocking_key_names: list[str],
    comparison_pool: dict[str, ComparisonDef],
) -> list[MatchingTierConfig]:
    """Build default matching tiers using comparison pool references.

    Creates two tiers:
    1. "exact" — high-confidence exact matches using all blocking keys
    2. "fuzzy" — lower-threshold fuzzy matches

    Tiers reference the comparison pool by name.  The PipelineConfig
    ``_resolve_comparison_refs`` validator hydrates them at load time.

    If only one blocking key exists, both tiers use the same key.
    """
    if not blocking_key_names:
        raise ConfigurationError("No blocking keys generated from roles")
    if not comparison_pool:
        raise ConfigurationError("No comparisons generated from roles")

    # Build pool ref comparisons (tier references the pool by name)
    pool_refs = [ComparisonDef(ref=name) for name in comparison_pool]

    # Compute total weight from pool entries for threshold calculation
    total_weight = sum(c.weight for c in comparison_pool.values())

    # Split blocking keys across tiers if we have enough
    if len(blocking_key_names) >= 2:
        exact_keys = blocking_key_names[:len(blocking_key_names) // 2]
        fuzzy_keys = blocking_key_names[len(blocking_key_names) // 2:]
    else:
        exact_keys = blocking_key_names
        fuzzy_keys = blocking_key_names

    # Exact tier: higher threshold
    exact_tier = MatchingTierConfig(
        name="exact",
        description="High-confidence exact matches",
        blocking=TierBlockingConfig(
            paths=[BlockingPathDef(keys=exact_keys)],
        ),
        comparisons=pool_refs,
        threshold=ThresholdConfig(
            method="sum",
            min_score=total_weight * 0.7,
        ),
    )

    # Fuzzy tier: lower threshold, different blocking path
    fuzzy_tier = MatchingTierConfig(
        name="fuzzy",
        description="Fuzzy probabilistic matches",
        blocking=TierBlockingConfig(
            paths=[BlockingPathDef(keys=fuzzy_keys)],
        ),
        comparisons=list(pool_refs),  # fresh copy
        threshold=ThresholdConfig(
            method="sum",
            min_score=total_weight * 0.4,
        ),
    )

    return [exact_tier, fuzzy_tier]
