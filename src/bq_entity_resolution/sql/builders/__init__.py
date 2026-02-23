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
from bq_entity_resolution.sql.builders.blocking import build_blocking_sql, BlockingParams
from bq_entity_resolution.sql.builders.comparison import (
    build_sum_scoring_sql,
    build_fellegi_sunter_sql,
    SumScoringParams,
    FellegiSunterParams,
)
from bq_entity_resolution.sql.builders.clustering import (
    build_cluster_assignment_sql,
    build_cluster_quality_metrics_sql,
    ClusteringParams,
    ClusterMetricsParams,
)
from bq_entity_resolution.sql.builders.gold_output import (
    build_gold_output_sql,
    GoldOutputParams,
)
from bq_entity_resolution.sql.builders.em import build_em_estimation_sql, EMParams
from bq_entity_resolution.sql.builders.active_learning import (
    build_active_learning_sql,
    ActiveLearningParams,
)

__all__ = [
    "build_staging_sql",
    "StagingParams",
    "build_features_sql",
    "build_term_frequencies_sql",
    "FeatureParams",
    "build_blocking_sql",
    "BlockingParams",
    "build_sum_scoring_sql",
    "build_fellegi_sunter_sql",
    "SumScoringParams",
    "FellegiSunterParams",
    "build_cluster_assignment_sql",
    "build_cluster_quality_metrics_sql",
    "ClusteringParams",
    "ClusterMetricsParams",
    "build_gold_output_sql",
    "GoldOutputParams",
    "build_em_estimation_sql",
    "EMParams",
    "build_active_learning_sql",
    "ActiveLearningParams",
]
