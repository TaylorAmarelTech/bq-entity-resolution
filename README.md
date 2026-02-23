# bq-entity-resolution

A configurable, multi-tier entity resolution pipeline for Google BigQuery. Python orchestrates configuration, validation, and SQL generation; BigQuery executes all data processing. Everything is driven by YAML configuration.

## What It Does

Matches and deduplicates entity records (people, companies, insurance claims, healthcare patients, financial transactions) across one or more BigQuery source tables using progressive blocking and probabilistic matching, producing a gold-layer resolved entity table with cluster assignments and canonical record election.

**Key capabilities:**

- Multi-tier matching (exact, fuzzy edit distance, phonetic, token-based, embedding similarity)
- Multi-path blocking with per-path candidate limits to control search space
- LSH (Locality-Sensitive Hashing) blocking for embedding-based matching
- 60+ built-in feature functions (name cleaning, address standardization, phone normalization, nickname resolution, etc.)
- 30+ comparison methods (exact, Levenshtein, Jaro-Winkler, Soundex, cosine similarity, token overlap, etc.)
- Fellegi-Sunter probabilistic matching with EM parameter estimation
- Active learning with review queue generation and label ingestion
- Hard negative disqualification and soft signal scoring
- Connected components clustering with canonical record election (4 strategies)
- Incremental processing with runtime watermarks, grace periods, and checkpoint/resume
- Pluggable backends (BigQuery for production, DuckDB for local testing)
- DAG-based pipeline with compile-time contract validation and runtime quality gates
- Column profiling and weight sensitivity analysis
- Docker-packaged for production deployment

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │               YAML Config                │
                    │  (sources, features, blocking, tiers)    │
                    └───────────────────┬─────────────────────┘
                                        │
                           ┌────────────▼────────────┐
                           │   Pydantic v2 Schema     │
                           │   28 config models       │
                           │   + cross-field checks   │
                           └────────────┬────────────┘
                                        │
              ┌─────────────────────────▼─────────────────────────┐
              │                    Pipeline                        │
              │  (recommended entry point: Pipeline(config).run()) │
              │                                                    │
              │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
              │  │ StageDAG │→ │  Plan    │→ │    Executor      │ │
              │  │ (topo    │  │ (immut.  │  │ (quality gates,  │ │
              │  │  sort)   │  │  SQL)    │  │  checkpoints)    │ │
              │  └──────────┘  └──────────┘  └──────────────────┘ │
              └───────────────────────┬───────────────────────────┘
                                      │
        ┌─────────────────────────────▼─────────────────────────────┐
        │                       Stages (8 types)                     │
        │                                                            │
        │  Staging → Features → Blocking → Matching → Clustering     │
        │                 TermFreq    (per tier)     GoldOutput      │
        │                                            ActiveLearning  │
        │                                                            │
        │  Each stage: inputs/outputs → plan() → list[SQLExpression] │
        └─────────────────────────────┬─────────────────────────────┘
                                      │
        ┌─────────────────────────────▼─────────────────────────────┐
        │               SQL Builders (14 modules, 30 functions)      │
        │                                                            │
        │  Frozen @dataclass params → build_*() → SQLExpression      │
        │  Type-safe, unit-testable, no Jinja2 templates             │
        │                                                            │
        │  staging · features · blocking · comparison · clustering    │
        │  gold_output · golden_record · em · embeddings             │
        │  active_learning · watermark · udf · monitoring            │
        └─────────────────────────────┬─────────────────────────────┘
                                      │ SQL
        ┌─────────────────────────────▼─────────────────────────────┐
        │                   Backend (pluggable)                      │
        │                                                            │
        │  BigQueryBackend  │  DuckDBBackend  │  BQEmulatorBackend   │
        │  (production)     │  (local test)   │  (Docker emulator)   │
        └───────────────────────────────────────────────────────────┘
                                      │
        ┌─────────────────────────────▼─────────────────────────────┐
        │                   BigQuery / DuckDB                        │
        │                                                            │
        │  Bronze: staged_{source} tables (incremental load)         │
        │  Silver: featured, candidates, matches, clusters           │
        │  Gold:   resolved_entities (final output)                  │
        └───────────────────────────────────────────────────────────┘
```

## Quick Start

### Install

```bash
pip install -e .
# or with dev dependencies:
pip install -e ".[dev]"
# or with DuckDB for local testing:
pip install -e ".[local]"
```

### 5-Line Config (Progressive Disclosure)

```python
from bq_entity_resolution.config.presets import quick_config

config = quick_config(
    project="my-gcp-project",
    dataset="entity_resolution",
    sources={"customers": "my-gcp-project.raw.customers"},
    columns={"first_name": "first_name", "last_name": "last_name", "email": "email"},
)
```

### Validate Configuration

```bash
bq-er validate --config config/examples/customer_dedup.yml
```

### Preview Generated SQL

```bash
bq-er preview-sql --config config/examples/customer_dedup.yml --tier fuzzy_name_address
```

### Run the Pipeline

```bash
# Full refresh (reprocess everything)
bq-er run --config my_config.yml --full-refresh

# Incremental (uses watermarks)
bq-er run --config my_config.yml

# Dry run (validate SQL against BigQuery without executing)
bq-er run --config my_config.yml --dry-run

# Run specific tiers only
bq-er run --config my_config.yml --tier exact_identity --tier email_match
```

### Profile Columns for Weight Suggestions

```bash
bq-er profile --config my_config.yml
```

### Analyze Weight Sensitivity

```bash
bq-er analyze --config my_config.yml --tier fuzzy_name --mode contribution
```

### Docker

```bash
docker build -t bq-er .
docker run -v ./config:/app/config/user -v ./secrets:/app/secrets \
  bq-er run --config /app/config/user/my_config.yml
```

## Configuration

Everything is driven by a single YAML file. Here's a minimal example:

```yaml
project:
  name: customer_dedup
  bq_project: my-gcp-project
  bq_dataset_bronze: dedup_bronze
  bq_dataset_silver: dedup_silver
  bq_dataset_gold: dedup_gold

sources:
  - name: customers
    table: my-gcp-project.raw.customers
    unique_key: customer_id
    updated_at: updated_at
    columns:
      - name: first_name
      - name: last_name
      - name: email
      - name: phone

feature_engineering:
  name_features:
    features:
      - name: first_name_clean
        function: name_clean
        input: first_name
      - name: last_name_clean
        function: name_clean
        input: last_name
  contact_features:
    features:
      - name: email_lower
        function: lower_trim
        input: email
  blocking_keys:
    - name: bk_last_first
      function: farm_fingerprint_concat
      inputs: [last_name_clean, first_name_clean]

matching_tiers:
  - name: exact_match
    blocking:
      paths:
        - keys: [bk_last_first]
          candidate_limit: 200
    comparisons:
      - left: first_name_clean
        right: first_name_clean
        method: exact
        weight: 3.0
      - left: last_name_clean
        right: last_name_clean
        method: exact
        weight: 3.0
      - left: email_lower
        right: email_lower
        method: exact
        weight: 5.0
    threshold:
      min_score: 6.0
```

See `config/examples/` for full production configurations:
- `insurance_entity.yml` — 7-tier insurance entity resolution with 80+ features
- `customer_dedup.yml` — 3-tier CRM customer deduplication
- `probabilistic_matching.yml` — Fellegi-Sunter probabilistic with EM training

### Configuration Presets

For common use cases, presets provide out-of-the-box configurations:

```python
from bq_entity_resolution.config.presets import person_dedup_preset

config = person_dedup_preset(
    project="my-project",
    dataset_prefix="er",
    sources={"crm": "proj.raw.customers", "erp": "proj.raw.contacts"},
    column_roles={
        "first_name": "first_name",
        "last_name": "last_name",
        "date_of_birth": "dob",
        "email": "email_address",
        "phone": "phone_number",
    },
)
```

Available presets: `person_dedup_preset`, `person_linkage_preset`, `business_dedup_preset`, `insurance_dedup_preset`, `financial_transaction_preset`, `healthcare_patient_preset`.

## Pipeline Stages

| Stage | Layer | What Happens |
|-------|-------|-------------|
| 1. Watermark Read | Meta | Read cursor positions for incremental load |
| 2. Stage Sources | Bronze | Incremental load from source tables with grace period |
| 3. Feature Engineering | Silver | Compute features, blocking keys, composite keys |
| 4. Term Frequencies | Silver | (Optional) Compute TF statistics for TF-IDF scoring |
| 5. Embeddings + LSH | Silver | (Optional) Generate ML embeddings and LSH buckets |
| 6. Create UDFs | Setup | Create Jaro-Winkler JS UDF if needed |
| 7. Tier Execution | Silver | For each tier: blocking -> comparison -> scoring -> threshold |
| 8. Clustering | Silver | Connected components to assign entity clusters |
| 9. Gold Output | Gold | Elect canonical records, produce resolved_entities |
| 10. Active Learning | Silver | (Optional) Generate review queue for uncertain pairs |
| 11. Watermark Advance | Meta | Update cursor positions on success |

### Matching Tier Flow

Each tier executes independently, in order:

1. **Blocking** — Generate candidate pairs via equi-join on blocking keys (with per-path candidate limits). Pairs already matched in prior tiers are excluded.
2. **Comparison** — Score each candidate pair using weighted comparison functions.
3. **Hard Negatives** — Disqualify pairs that violate constraints (e.g., different entity types).
4. **Soft Signals** — Bonus points for supporting evidence (e.g., matching phone area code).
5. **Threshold** — Keep pairs above minimum score (`sum` or `fellegi_sunter` method).
6. **Accumulate** — Append matches to the all-tiers matches table.

### Scoring Methods

| Method | How It Works | When to Use |
|--------|-------------|-------------|
| `sum` | Weighted sum of binary comparisons. Confidence = score / max_possible. | Simple, interpretable rules |
| `fellegi_sunter` | Log-likelihood ratio scoring with m/u probabilities. Confidence = 2^W / (1 + 2^W). | Probabilistic matching with training data |

### Canonical Record Election

| Method | Strategy |
|--------|----------|
| `completeness` | Record with the most non-null fields (default) |
| `recency` | Most recently updated record |
| `source_priority` | Record from highest-priority source |
| `field_merge` | Golden record assembled per-field from best source |

## Extensibility

### Adding a Feature Function

Add a decorated function in `src/bq_entity_resolution/features/registry.py`:

```python
@register("my_feature")
def my_feature(inputs: list[str], **_: Any) -> str:
    """My custom feature description."""
    col = inputs[0]
    return f"UPPER(TRIM({col}))"
```

Use in YAML:
```yaml
features:
  - name: my_cleaned_col
    function: my_feature
    input: raw_col
```

### Adding a Comparison Function

Add a decorated function in `src/bq_entity_resolution/matching/comparisons.py`:

```python
@register("my_comparison")
def my_comparison(left: str, right: str, threshold: float = 0.8, **_: Any) -> str:
    """My custom comparison."""
    return f"(my_func(l.{left}, r.{right}) >= {threshold} AND l.{left} IS NOT NULL)"
```

Use in YAML:
```yaml
comparisons:
  - left: col_a
    right: col_b
    method: my_comparison
    params: {threshold: 0.9}
    weight: 2.0
```

### Adding a Matching Tier

Pure YAML — no code changes:

```yaml
matching_tiers:
  - name: my_new_tier
    description: "Custom fuzzy matching"
    blocking:
      paths:
        - keys: [bk_soundex_last]
          candidate_limit: 500
    comparisons:
      - {left: name_clean, right: name_clean, method: levenshtein, params: {max_distance: 2}, weight: 3.0}
      - {left: phone_std, right: phone_std, method: exact, weight: 5.0}
    threshold:
      min_score: 5.0
    hard_negatives:
      - {left: entity_type, method: different, action: disqualify}
    soft_signals:
      - {left: email_domain, method: exact, bonus: 1.0}
```

## Built-in Functions

### Feature Functions (60+)

| Category | Functions |
|----------|-----------|
| Name | `name_clean`, `name_clean_strict`, `first_letter`, `first_n_chars`, `char_length`, `soundex`, `extract_salutation`, `strip_salutation`, `extract_suffix`, `strip_suffix`, `word_count`, `first_word`, `last_word`, `initials`, `strip_business_suffix`, `name_fingerprint` |
| Nickname | `nickname_canonical` (70+ pairs: Bob/Robert, Bill/William, etc.), `nickname_match_key` |
| Transposition | `sorted_name_tokens`, `sorted_name_fingerprint` |
| Address | `address_standardize` (40+ USPS abbreviations), `extract_street_number`, `extract_street_name`, `extract_unit_number` |
| Contact | `phone_standardize` (handles country codes), `phone_area_code`, `phone_last_four`, `email_domain`, `email_local_part`, `email_domain_type` |
| Blocking | `farm_fingerprint`, `farm_fingerprint_concat` |
| Zip/Date | `zip5`, `zip3`, `year_of_date`, `date_to_string` |
| Utility | `upper_trim`, `lower_trim`, `left`, `right`, `coalesce`, `concat`, `nullif_empty`, `identity` |

### Comparison Functions (30+)

| Category | Functions |
|----------|-----------|
| Exact | `exact`, `exact_case_insensitive`, `exact_or_null` |
| Edit Distance | `levenshtein`, `levenshtein_normalized`, `levenshtein_score` |
| Jaro-Winkler | `jaro_winkler`, `jaro_winkler_score` (BigQuery JS UDF, auto-created) |
| Phonetic | `soundex_match`, `metaphone_match` |
| Vector | `cosine_similarity`, `cosine_similarity_score` (via `ML.DISTANCE`) |
| Numeric/Date | `numeric_within`, `date_within_days` |
| String | `contains`, `starts_with` |
| Token | `token_set_match`, `token_set_score`, `initials_match`, `abbreviation_match` |
| Hard Negative | `different`, `null_either`, `length_mismatch` |

## Environment Variables

Config values support `${VAR}` and `${VAR:-default}` syntax:

```yaml
project:
  bq_project: "${BQ_PROJECT}"
  bq_dataset_gold: "${GOLD_DATASET:-er_gold}"
```

## Python API

```python
from bq_entity_resolution.pipeline.pipeline import Pipeline
from bq_entity_resolution.config.loader import load_config

config = load_config("my_config.yml")

# Preview SQL without executing
pipeline = Pipeline(config)
plan = pipeline.plan()
for stage_plan in plan.stages:
    print(f"--- {stage_plan.stage_name} ---")
    for expr in stage_plan.sql_expressions:
        print(expr.render())

# Execute against BigQuery
from bq_entity_resolution.backends.bigquery import BigQueryBackend
backend = BigQueryBackend(project="my-project", location="US")
pipeline.run(backend=backend)

# Execute locally with DuckDB (for testing)
from bq_entity_resolution.backends.duckdb import DuckDBBackend
backend = DuckDBBackend()
pipeline.run(backend=backend)
```

## Development

```bash
pip install -e ".[dev,local]"
python -m pytest tests/ -v               # 830+ tests, ~30s
python -m ruff check src/                 # lint
python -m mypy src/                       # type check
```

### Project Structure

```
src/bq_entity_resolution/
  config/        Pydantic v2 schema, YAML loader, presets, role mapping, validators
  sql/builders/  14 Python SQL builder modules (type-safe, testable)
  sql/           SQLExpression wrapper (sqlglot-based), SQL utilities
  features/      Feature function registry (60+ functions via @register)
  matching/      Comparison registry (30+ functions), parameters, active learning
  blocking/      Blocking key validation, LSH bucket logic
  reconciliation/  Clustering strategy descriptions, canonical output logic
  embeddings/    BigQuery ML embedding generation + LSH
  watermark/     Incremental watermark tracking + checkpoint/resume
  stages/        8 Stage classes (composable DAG nodes with inputs/outputs)
  pipeline/      Pipeline, StageDAG, Plan, Executor, Validator, Quality Gates
  backends/      Pluggable backends (BigQuery, DuckDB, BQ Emulator)
  profiling/     Column profiling + weight sensitivity analysis
  monitoring/    Structured logging + metrics
  clients/       BigQuery client wrapper with retries
  naming.py      Centralized table naming (single source of truth)
  constants.py   Shared constants
  exceptions.py  Exception hierarchy
```

## License

MIT
