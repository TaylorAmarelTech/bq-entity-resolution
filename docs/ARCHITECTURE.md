# Architecture

## Module Dependency Graph

```
                        config/schema.py
                     (Pydantic v2 models)
                              │
              ┌───────────────┼───────────────────┐
              │               │                   │
              ▼               ▼                   ▼
    config/loader.py   config/validators.py   naming.py
    (YAML + env vars)  (cross-field checks)   (table names)
                                                  │
              ┌───────────────┬───────────────────┤
              │               │                   │
              ▼               ▼                   ▼
    features/registry   matching/comparisons   sql/generator.py
    (45+ functions)     (22+ functions)        (Jinja2 renderer)
              │               │                   │
              ▼               ▼                   │
    features/engine     matching/engine     ◄─────┘
              │               │
              │         ┌─────┴─────┐
              │         │           │
              │    hard_negatives  soft_signals
              │         │           │
              ▼         ▼           ▼
    blocking/engine     matching/engine
              │               │
              └───────┬───────┘
                      │
                      ▼
           reconciliation/engine
                      │
                      ▼
           pipeline/orchestrator  ◄── pipeline/context
                      │                pipeline/runner
                      ▼
           clients/bigquery.py
           (execution + retries)
```

No circular dependencies. Config modules never import engines. Engines never import the BigQuery client directly (the orchestrator bridges them via the runner).

## Probabilistic Matching (Fellegi-Sunter)

When a tier uses `threshold.method: fellegi_sunter`, the pipeline uses log-likelihood ratio scoring instead of simple weight sums:

```
                              ┌──────────────────┐
                              │  Training Config  │
                              │  (em / labeled /  │
                              │   none)           │
                              └────────┬─────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
                    ▼                  ▼                  ▼
           EM Estimation      Labeled Training     Manual m/u
           (BQ scripting)     (BQ SQL query)       (from config)
           em_estimation      estimate_from_       ComparisonLevelDef
           .sql.j2            labels.sql.j2        .m / .u
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │ TierParameters    │
                              │  per-comparison   │
                              │  per-level m/u    │
                              │  log_prior_odds   │
                              └────────┬─────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │ Log-weight calc   │
                              │ log2(m/u) per     │
                              │ level, clamped    │
                              │ [0.001, 0.999]    │
                              └────────┬─────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │ tier_fellegi_     │
                              │ sunter.sql.j2     │
                              │                   │
                              │ CASE/WHEN per     │
                              │ comparison level  │
                              │ total_score =     │
                              │ Σ log_weights     │
                              │ confidence =      │
                              │ 2^W / (1 + 2^W)  │
                              └────────┬─────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │ Active Learning   │
                              │ (optional)        │
                              │ Review queue of   │
                              │ uncertain pairs   │
                              └──────────────────┘
```

### m/u Precedence

When resolving m and u probabilities for each comparison level:

1. **Config values** — `ComparisonLevelDef.m` / `.u` from YAML (highest priority)
2. **Estimation results** — From EM or labeled training
3. **Label defaults** — `exact` → (0.9, 0.1), `fuzzy` → (0.7, 0.2), `else` → (0.1, 0.9)

### EM Algorithm (BigQuery Scripting)

The EM estimation runs entirely within BigQuery using scripting (`DECLARE`/`LOOP`/`SET`):

1. **Sample** — Random candidate pairs with comparison outcomes → temp table
2. **Initialize** — m/u priors per level (match=0.9/0.1, else=0.1/0.9)
3. **E-step** — For each pair, compute match probability using current m/u (log-space)
4. **M-step** — Update m/u as weighted averages using soft match assignments
5. **Converge** — Stop when max delta < threshold or max iterations reached
6. **Clamp** — Final m/u clamped to [0.001, 0.999] to prevent log(0)

## Data Flow (Bronze → Silver → Gold)

```
Source Tables                    Bronze Layer                  Silver Layer                    Gold Layer
┌──────────┐    Staging SQL    ┌──────────────┐  Feature SQL  ┌──────────────┐               ┌──────────────┐
│ source_1 │ ──────────────► │staged_source1│ ──────────► │  featured    │               │  resolved    │
└──────────┘                  └──────────────┘              │  (all cols   │  Cluster SQL  │  _entities   │
┌──────────┐    Staging SQL    ┌──────────────┐  UNION ALL   │  + features  │ ──────────► │  (cluster_id │
│ source_2 │ ──────────────► │staged_source2│ ──────────► │  + blocking  │               │  canonical   │
└──────────┘                  └──────────────┘              │  + composite)│               │  is_canonical│
                                                            └──────┬───────┘               │  match_meta) │
                                                                   │                       └──────────────┘
                                                                   ▼
                                                    ┌──────────────────────────┐
                                                    │  Per-Tier Tables:         │
                                                    │  candidates_{tier_name}   │
                                                    │  matches_{tier_name}      │
                                                    │  all_matched_pairs        │
                                                    │  entity_clusters          │
                                                    └──────────────────────────┘
```

## Table Naming Convention

All table names are constructed in `naming.py`. Pattern: `{bq_project}.{dataset}.{suffix}`

| Function | Dataset | Suffix | Example |
|----------|---------|--------|---------|
| `staged_table(cfg, "src1")` | bronze | `staged_src1` | `proj.er_bronze.staged_src1` |
| `featured_table(cfg)` | silver | `featured` | `proj.er_silver.featured` |
| `candidates_table(cfg, "fuzzy")` | silver | `candidates_fuzzy` | `proj.er_silver.candidates_fuzzy` |
| `matches_table(cfg, "fuzzy")` | silver | `matches_fuzzy` | `proj.er_silver.matches_fuzzy` |
| `all_matches_table(cfg)` | silver | `all_matched_pairs` | `proj.er_silver.all_matched_pairs` |
| `cluster_table(cfg)` | silver | `entity_clusters` | `proj.er_silver.entity_clusters` |
| `resolved_table(cfg)` | gold | `resolved_entities` | `proj.er_gold.resolved_entities` |
| `embeddings_table(cfg)` | silver | `entity_embeddings` | `proj.er_silver.entity_embeddings` |
| `lsh_buckets_table(cfg)` | silver | `lsh_buckets` | `proj.er_silver.lsh_buckets` |
| `parameters_table(cfg, "tier")` | silver | `fs_parameters_tier` | `proj.er_silver.fs_parameters_tier` |
| `review_queue_table(cfg, "tier")` | silver | `al_review_queue_tier` | `proj.er_silver.al_review_queue_tier` |
| `labels_table(cfg)` | silver | `al_labels` | `proj.er_silver.al_labels` |

## SQL Template Inventory

| Template | Purpose | Key Parameters |
|----------|---------|----------------|
| `staging/incremental_load.sql.j2` | Incremental load with grace period | source, watermark, grace_period_hours |
| `features/all_features.sql.j2` | Feature engineering (UNION sources + compute) | source_tables, feature_expressions, blocking_keys |
| `features/embeddings.sql.j2` | BigQuery ML text embedding generation | concat_expression, model_name, dimensions |
| `blocking/multi_path_candidates.sql.j2` | Multi-path blocking with LSH support | blocking_paths, lsh_table, excluded_pairs |
| `blocking/lsh_block.sql.j2` | Random hyperplane LSH bucket computation | num_tables, num_functions, seed, dimensions |
| `matching/tier_comparisons.sql.j2` | Comparison scoring + threshold filtering | comparisons, hard_negatives, soft_signals, threshold |
| `reconciliation/cluster_assignment.sql.j2` | Connected components (BQ scripting) | source_table, all_matches_table, max_iterations |
| `reconciliation/gold_output.sql.j2` | Canonical election + gold output | canonical_method, scoring_columns, source_columns |
| `udfs/jaro_winkler.sql.j2` | JavaScript UDF for Jaro-Winkler similarity | udf_dataset |
| `matching/tier_fellegi_sunter.sql.j2` | Fellegi-Sunter probabilistic scoring | comparisons (with levels + log-weights), match_threshold |
| `matching/estimate_from_labels.sql.j2` | Learn m/u from labeled pairs | labeled_pairs_table, comparisons, levels |
| `matching/em_estimation.sql.j2` | EM unsupervised m/u estimation (BQ scripting) | candidates_table, comparisons, max_iterations |
| `matching/active_learning_queue.sql.j2` | Review queue for uncertain pairs | scoring_method, uncertainty_window, queue_size |
| `watermark/create_watermark_table.sql.j2` | Watermark metadata table DDL | table |
| `watermark/read_watermark.sql.j2` | Read current watermark values | table, source_name |
| `watermark/update_watermark.sql.j2` | Update watermark (mark old, insert new) | table, source_name, cursors, run_id |

## Registry Pattern

Feature functions and comparison functions use the same pattern:

```python
FUNCTIONS: dict[str, Callable] = {}

def register(name: str):
    def decorator(func):
        FUNCTIONS[name] = func
        return func
    return decorator

@register("my_function")
def my_function(inputs, **kwargs):
    return "SQL expression"
```

YAML config references functions by name. Engines look them up at SQL generation time. Unknown names produce clear errors listing all available functions.

## Entity UID Generation

Every staged record gets a deterministic `entity_uid`:

```sql
CAST(FARM_FINGERPRINT(
  CONCAT('{source_name}', '||', CAST({unique_key} AS STRING))
) AS STRING)
```

This means:
- Same source + same unique_key = same entity_uid (deterministic)
- Different sources with same key = different entity_uid (source-scoped)
- Renaming a source = new entity_uid (intentional — source identity matters)

## Connected Components Clustering

The clustering algorithm uses iterative BigQuery scripting:

1. **Initialize** — Every entity starts as its own cluster (entity_uid = cluster_id)
2. **Propagate** — For each edge in matches, propagate the minimum cluster_id to both endpoints
3. **Converge** — Repeat until no changes or max_iterations reached
4. **Result** — Each entity has a cluster_id. Entities in the same cluster are considered the same real-world entity.

Unmatched entities remain as singleton clusters (cluster of 1) in the gold output.

## Incremental Processing

The watermark system tracks the high-water mark of cursor columns (typically `updated_at`) per source:

1. **Read** — Before staging, read last watermark from `pipeline_watermarks` table
2. **Filter** — Staging SQL: `WHERE updated_at > watermark - grace_period`
3. **Grace Period** — Subtracts N hours from watermark to catch late-arriving records
4. **Advance** — After successful pipeline completion, write new watermark = MAX(cursor_column) from staged data
5. **Transactional** — Watermarks only advance on success. Failed runs don't corrupt state.

## Error Handling

```
EntityResolutionError (base)
├── ConfigurationError          — YAML/validation errors
├── SQLGenerationError          — Template rendering failures
├── SQLExecutionError           — BigQuery execution failures (has sql, job_id)
├── WatermarkError              — Watermark read/write failures
├── BlockingError               — Blocking SQL generation failures
├── MatchingError               — Matching SQL generation failures
├── ParameterEstimationError    — Fellegi-Sunter parameter estimation failures
├── ReconciliationError         — Clustering/gold output failures
├── EmbeddingError              — Embedding computation failures
└── PipelineAbortError          — Pipeline-level abort
```

BigQuery client retries `ServiceUnavailable` errors up to 3 times with exponential backoff (5s, 10s, 20s).
