# bq-entity-resolution

A configurable, multi-tier entity resolution pipeline for Google BigQuery. Python orchestrates the pipeline; BigQuery executes all SQL. Everything is driven by YAML configuration.

## What It Does

Matches and deduplicates entity records (people, companies, etc.) across one or more BigQuery source tables using progressive blocking and fuzzy matching, producing a gold-layer resolved entity table with cluster assignments and canonical record election.

**Key capabilities:**

- Multi-tier matching (exact, fuzzy edit distance, phonetic, token-based, embedding similarity)
- Multi-path blocking with per-path candidate limits to control search space
- LSH (Locality-Sensitive Hashing) blocking for embedding-based matching
- 45+ built-in feature functions (name cleaning, address standardization, phone normalization, nickname resolution, etc.)
- 22+ comparison methods (exact, Levenshtein, Jaro-Winkler, Soundex, cosine similarity, token overlap, etc.)
- Hard negative disqualification and soft signal scoring
- Connected components clustering with canonical record election
- Incremental processing with runtime watermarks and grace periods
- Docker-packaged for production deployment

## Quick Start

### Install

```bash
pip install -e .
# or with dev dependencies:
pip install -e ".[dev]"
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

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    YAML Config                       │
│  (sources, features, blocking, tiers, thresholds)   │
└────────────────────────┬────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│              Python Orchestrator                     │
│  config/schema.py  →  Pydantic validation           │
│  features/engine   →  Feature SQL generation        │
│  blocking/engine   →  Candidate pair SQL            │
│  matching/engine   →  Comparison + scoring SQL      │
│  reconciliation/   →  Clustering + gold output SQL  │
│  watermark/manager →  Incremental processing        │
└────────────────────────┬────────────────────────────┘
                         │ SQL
                         ▼
┌─────────────────────────────────────────────────────┐
│                 BigQuery Engine                       │
│  Bronze: staged_{source} tables (incremental load)  │
│  Silver: featured, candidates, matches, clusters    │
│  Gold:   resolved_entities (final output)           │
└─────────────────────────────────────────────────────┘
```

### Pipeline Stages

| Stage | Layer | What Happens |
|-------|-------|-------------|
| 1. Watermark Read | Meta | Read cursor positions for incremental load |
| 2. Stage Sources | Bronze | Incremental load from source tables with grace period |
| 3. Feature Engineering | Silver | Compute all features, blocking keys, composite keys |
| 4. Embeddings + LSH | Silver | (Optional) Generate embeddings and LSH buckets |
| 5. Create UDFs | Setup | Create Jaro-Winkler JS UDF if needed |
| 6. Tier Execution | Silver | For each tier: blocking → comparison → scoring → threshold |
| 7. Clustering | Silver | Connected components to assign entity clusters |
| 8. Gold Output | Gold | Elect canonical records, produce resolved_entities |
| 9. Watermark Advance | Meta | Update cursor positions on success |

### Matching Tier Flow

Each tier executes independently, in order:

1. **Blocking** — Generate candidate pairs via equi-join on blocking keys (with per-path candidate limits). Pairs already matched in prior tiers are excluded.
2. **Comparison** — Score each candidate pair using weighted comparison functions.
3. **Hard Negatives** — Disqualify pairs that violate constraints (e.g., different first names for individuals).
4. **Soft Signals** — Bonus points for supporting evidence (e.g., matching phone area code).
5. **Threshold** — Keep pairs above minimum score.
6. **Accumulate** — Append matches to the all-tiers matches table.

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

Use it in YAML:
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

Use it in YAML:
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

### Feature Functions (45+)

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

### Comparison Functions (22+)

| Category | Functions |
|----------|-----------|
| Exact | `exact`, `exact_case_insensitive`, `exact_or_null` |
| Edit Distance | `levenshtein`, `levenshtein_normalized`, `levenshtein_score` |
| Jaro-Winkler | `jaro_winkler`, `jaro_winkler_score` (BigQuery JS UDF, auto-created) |
| Phonetic | `soundex_match` |
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

## Development

```bash
pip install -e ".[dev]"
python -m pytest tests/ -v           # run tests
python -m ruff check src/            # lint
python -m mypy src/                  # type check
```

## License

MIT
