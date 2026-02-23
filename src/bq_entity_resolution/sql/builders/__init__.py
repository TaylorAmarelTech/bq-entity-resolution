"""SQL builders: composable, testable SQL generation replacing Jinja2 templates.

Each builder module corresponds to a pipeline stage and contains:
- Frozen dataclass parameter definitions (type-safe inputs)
- Builder functions that return SQLExpression objects
- Full dialect support (bigquery, duckdb) via SQLExpression.render()
"""

from bq_entity_resolution.sql.builders.staging import build_staging_sql, StagingParams
from bq_entity_resolution.sql.builders.features import (
    build_features_sql,
    build_term_frequencies_sql,
    FeatureParams,
)
from bq_entity_resolution.sql.builders.blocking import (
    build_blocking_sql,
    build_blocking_metrics_sql,
    BlockingParams,
    BlockingMetricsParams,
)
from bq_entity_resolution.sql.builders.comparison import (
    build_sum_scoring_sql,
    build_fellegi_sunter_sql,
    SumScoringParams,
    FellegiSunterParams,
)
from bq_entity_resolution.sql.builders.clustering import (
    build_cluster_assignment_sql,
    build_cluster_quality_metrics_sql,
    build_incremental_cluster_sql,
    build_populate_canonical_index_sql,
    ClusteringParams,
    ClusterMetricsParams,
    IncrementalClusteringParams,
    PopulateCanonicalIndexParams,
)
from bq_entity_resolution.sql.builders.gold_output import (
    build_gold_output_sql,
    GoldOutputParams,
)
from bq_entity_resolution.sql.builders.golden_record import (
    build_golden_record_cte,
    GoldenRecordParams,
    FieldStrategy,
)
from bq_entity_resolution.sql.builders.em import (
    build_em_estimation_sql,
    build_estimate_from_labels_sql,
    EMParams,
    LabelEstimationParams,
)
from bq_entity_resolution.sql.builders.embeddings import (
    build_embeddings_sql,
    build_lsh_buckets_sql,
    EmbeddingsParams,
    LSHParams,
)
from bq_entity_resolution.sql.builders.active_learning import (
    build_active_learning_sql,
    build_ingest_labels_sql,
    ActiveLearningParams,
    IngestLabelsParams,
)
from bq_entity_resolution.sql.builders.watermark import (
    build_create_watermark_table_sql,
    build_read_watermark_sql,
    build_update_watermark_sql,
    build_create_checkpoint_table_sql,
)
from bq_entity_resolution.sql.builders.udf import build_jaro_winkler_udf_sql
from bq_entity_resolution.sql.builders.monitoring import build_persist_sql_log_sql

__all__ = [
    # Staging
    "build_staging_sql",
    "StagingParams",
    # Features
    "build_features_sql",
    "build_term_frequencies_sql",
    "FeatureParams",
    # Blocking
    "build_blocking_sql",
    "build_blocking_metrics_sql",
    "BlockingParams",
    "BlockingMetricsParams",
    # Comparison / Scoring
    "build_sum_scoring_sql",
    "build_fellegi_sunter_sql",
    "SumScoringParams",
    "FellegiSunterParams",
    # Clustering
    "build_cluster_assignment_sql",
    "build_cluster_quality_metrics_sql",
    "build_incremental_cluster_sql",
    "build_populate_canonical_index_sql",
    "ClusteringParams",
    "ClusterMetricsParams",
    "IncrementalClusteringParams",
    "PopulateCanonicalIndexParams",
    # Gold Output
    "build_gold_output_sql",
    "GoldOutputParams",
    "build_golden_record_cte",
    "GoldenRecordParams",
    "FieldStrategy",
    # EM / Parameter Estimation
    "build_em_estimation_sql",
    "build_estimate_from_labels_sql",
    "EMParams",
    "LabelEstimationParams",
    # Embeddings
    "build_embeddings_sql",
    "build_lsh_buckets_sql",
    "EmbeddingsParams",
    "LSHParams",
    # Active Learning
    "build_active_learning_sql",
    "build_ingest_labels_sql",
    "ActiveLearningParams",
    "IngestLabelsParams",
    # Watermark / Checkpoint
    "build_create_watermark_table_sql",
    "build_read_watermark_sql",
    "build_update_watermark_sql",
    "build_create_checkpoint_table_sql",
    # UDF
    "build_jaro_winkler_udf_sql",
    # Monitoring
    "build_persist_sql_log_sql",
]
