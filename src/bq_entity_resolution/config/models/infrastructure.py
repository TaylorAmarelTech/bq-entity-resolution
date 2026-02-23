"""
Infrastructure configuration models.

Defines BigQuery project/environment settings, embeddings/LSH,
incremental processing, monitoring, and scale configuration.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

__all__ = [
    "ProjectConfig",
    "LSHConfig",
    "EmbeddingConfig",
    "PartitionCursorConfig",
    "IncrementalConfig",
    "MetricsConfig",
    "ProfilingConfig",
    "BlockingMetricsConfig",
    "ClusterQualityConfig",
    "MonitoringConfig",
    "ScaleConfig",
]


# ---------------------------------------------------------------------------
# Project & environment
# ---------------------------------------------------------------------------

class ProjectConfig(BaseModel):
    """BigQuery project and dataset routing."""

    name: str
    description: str = ""
    bq_project: str
    bq_dataset_bronze: str = "er_bronze"
    bq_dataset_silver: str = "er_silver"
    bq_dataset_gold: str = "er_gold"
    bq_location: str = "US"
    watermark_dataset: str = "er_meta"
    udf_dataset: str = "er_udfs"


# ---------------------------------------------------------------------------
# Embeddings & LSH
# ---------------------------------------------------------------------------

class LSHConfig(BaseModel):
    """Locality-Sensitive Hashing configuration for embedding-based blocking."""

    num_hash_tables: int = 20
    num_hash_functions_per_table: int = 8
    bucket_column_prefix: str = "lsh_bucket"
    projection_seed: int = 42


class EmbeddingConfig(BaseModel):
    """Embedding computation and storage configuration."""

    enabled: bool = False
    model: str = "text-embedding-004"
    source_columns: list[str] = Field(default_factory=list)
    concat_separator: str = " | "
    dimensions: int = 768
    batch_size: int = 5000
    lsh: LSHConfig = Field(default_factory=LSHConfig)


# ---------------------------------------------------------------------------
# Incremental processing
# ---------------------------------------------------------------------------

class PartitionCursorConfig(BaseModel):
    """A partition-aware cursor dimension for scan optimization.

    When sources are partitioned by columns beyond the timestamp (e.g.
    state, policy_id, region), adding them as partition cursors generates
    AND predicates in the staging WHERE clause. BigQuery uses these for
    partition pruning, dramatically reducing bytes scanned.

    Example config::

        incremental:
          partition_cursors:
            - column: state
              strategy: range      # >= last processed value
            - column: policy_year
              strategy: equality   # = current year

    This generates::

        WHERE updated_at > WATERMARK_TS
          AND state >= 'last_processed_state'
          AND policy_year = 2024
    """

    column: str
    strategy: Literal["range", "equality", "in_list"] = "range"
    value: Optional[Any] = None  # Static value for equality strategy


class IncrementalConfig(BaseModel):
    """Incremental processing and watermark configuration."""

    enabled: bool = True
    grace_period_hours: int = 48
    cursor_columns: list[str] = Field(default_factory=lambda: ["updated_at"])
    partition_cursors: list[PartitionCursorConfig] = Field(default_factory=list)
    batch_size: int = 2_000_000
    full_refresh_on_schema_change: bool = True


# ---------------------------------------------------------------------------
# Monitoring
# ---------------------------------------------------------------------------

class MetricsConfig(BaseModel):
    """Pipeline metrics collection configuration."""

    enabled: bool = True
    destination: Literal["bigquery", "stdout"] = "bigquery"


class ProfilingConfig(BaseModel):
    """Data quality profiling configuration."""

    enabled: bool = False
    sample_rate: float = 0.01


class BlockingMetricsConfig(BaseModel):
    """Blocking evaluation metrics to assess blocking strategy quality."""

    enabled: bool = False
    persist_to_table: bool = False


class ClusterQualityConfig(BaseModel):
    """Cluster quality metrics and alerts.

    Monitors cluster health to catch false positives that merge
    unrelated entities via transitivity.
    """

    enabled: bool = False
    persist_to_table: bool = False
    alert_max_cluster_size: int = 100
    alert_singleton_ratio: float = 0.95
    abort_on_explosion: bool = False  # Hard abort if max cluster exceeds threshold


class MonitoringConfig(BaseModel):
    """Observability configuration."""

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "text"] = "json"
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    profiling: ProfilingConfig = Field(default_factory=ProfilingConfig)
    blocking_metrics: BlockingMetricsConfig = Field(default_factory=BlockingMetricsConfig)
    cluster_quality: ClusterQualityConfig = Field(default_factory=ClusterQualityConfig)
    persist_sql_log: bool = False  # Write sql_log to BQ table after run


# ---------------------------------------------------------------------------
# Scale
# ---------------------------------------------------------------------------

class ScaleConfig(BaseModel):
    """Scale optimizations for high-volume processing (5-10M+ records/day).

    All fields are opt-in (off by default) to preserve backwards compatibility.

    **Partitioning** controls BigQuery PARTITION BY on generated tables.
    Good partition columns: date/timestamp columns with daily/monthly granularity.
    BigQuery limit: max 1 partition column per table, 4000 partitions.

    **Clustering** controls BigQuery CLUSTER BY on generated tables.
    Up to 4 columns per table. Put highest-cardinality filter columns first.

    Example config::

        scale:
          staging_partition_by: "DATE(source_updated_at)"
          staging_clustering: [entity_uid, source_name]
          candidates_clustering: [l_entity_uid]
          matches_partition_by: "DATE(matched_at)"
    """

    max_bytes_billed: Optional[int] = None  # Safety cap per query (bytes)

    # Staging tables
    staging_partition_by: Optional[str] = None  # e.g. "DATE(source_updated_at)"
    staging_clustering: list[str] = Field(
        default_factory=lambda: ["entity_uid"]
    )

    # Featured tables
    featured_partition_by: Optional[str] = None
    featured_table_clustering: list[str] = Field(default_factory=list)

    # Candidate pair tables
    candidates_partition_by: Optional[str] = None
    candidates_clustering: list[str] = Field(
        default_factory=lambda: ["l_entity_uid"]
    )

    # Match result tables
    matches_partition_by: Optional[str] = None  # e.g. "DATE(matched_at)"
    matches_clustering: list[str] = Field(
        default_factory=lambda: ["l_entity_uid", "r_entity_uid"]
    )

    # Canonical index
    canonical_index_partition_by: Optional[str] = None
    canonical_index_clustering: list[str] = Field(
        default_factory=lambda: ["entity_uid"]
    )

    checkpoint_enabled: bool = False
