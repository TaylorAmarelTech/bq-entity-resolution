"""BigQuery Entity Resolution Pipeline -- configurable multi-tier matching.

Quick start::

    from bq_entity_resolution import Pipeline, quick_config

    config = quick_config(
        bq_project="my-gcp-project",
        source_table="my-gcp-project.raw.customers",
        columns=["first_name", "last_name", "email", "phone"],
    )
    pipeline = Pipeline(config)
    plan = pipeline.plan()
    print(plan.preview())
"""

from bq_entity_resolution.version import __version__

# Core API
from bq_entity_resolution.pipeline.pipeline import Pipeline
from bq_entity_resolution.config.schema import PipelineConfig, SourceConfig
from bq_entity_resolution.config.loader import load_config

# Presets (progressive disclosure)
from bq_entity_resolution.config.presets import (
    quick_config,
    person_dedup_preset,
    person_linkage_preset,
    business_dedup_preset,
    insurance_dedup_preset,
    financial_transaction_preset,
    healthcare_patient_preset,
)

# Registries (for custom extensions)
from bq_entity_resolution.features.registry import (
    FEATURE_FUNCTIONS,
    register as register_feature,
)
from bq_entity_resolution.matching.comparisons import (
    COMPARISON_FUNCTIONS,
    register as register_comparison,
)

# Backends (for local testing and production)
from bq_entity_resolution.backends.duckdb import DuckDBBackend

# Lazy import for BigQueryBackend (requires google-cloud-bigquery)
def __getattr__(name: str):
    if name == "BigQueryBackend":
        from bq_entity_resolution.backends.bigquery import BigQueryBackend
        return BigQueryBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# Role detection utilities
from bq_entity_resolution.config.roles import (
    detect_role,
    features_for_role,
    blocking_keys_for_role,
    comparisons_for_role,
)

__all__ = [
    "__version__",
    # Core
    "Pipeline",
    "PipelineConfig",
    "SourceConfig",
    "load_config",
    # Presets
    "quick_config",
    "person_dedup_preset",
    "person_linkage_preset",
    "business_dedup_preset",
    "insurance_dedup_preset",
    "financial_transaction_preset",
    "healthcare_patient_preset",
    # Extensibility
    "FEATURE_FUNCTIONS",
    "COMPARISON_FUNCTIONS",
    "register_feature",
    "register_comparison",
    # Backends
    "DuckDBBackend",
    "BigQueryBackend",
    # Role utilities
    "detect_role",
    "features_for_role",
    "blocking_keys_for_role",
    "comparisons_for_role",
]
