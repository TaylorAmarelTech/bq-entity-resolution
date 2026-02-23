# CLAUDE.md — Project Context for AI Assistants

## Project Overview

**bq-entity-resolution** is a Python-orchestrated, YAML-configured entity resolution pipeline for Google BigQuery. It replaces a 37-model dbt project with a cleaner architecture: Python handles configuration, validation, and orchestration while BigQuery executes all SQL for ETL and matching.

## Tech Stack

- **Python 3.11+** with Pydantic v2 (config), Jinja2 (SQL templates), Click (CLI), structlog (logging)
- **BigQuery** as the compute engine (no local data processing)
- **Docker** for deployment (multi-stage build, non-root user)

## Project Structure

```
src/bq_entity_resolution/
  config/       Pydantic schema, YAML loader, cross-field validators
  sql/          Jinja2 SQL generator + templates/ (13 .j2 files)
  features/     Feature function registry (45+ functions) + engine
  blocking/     Blocking engine (equi-join + LSH), standard.py, lsh.py
  matching/     Comparison registry (22+ functions), engine, hard_negatives, soft_signals, parameters (F-S), active_learning
  reconciliation/  Clustering (connected components), gold output, canonical election
  watermark/    Runtime watermark manager backed by BigQuery
  embeddings/   BigQuery ML embedding generation + LSH bucket computation
  pipeline/     Orchestrator (main controller), runner, context
  monitoring/   Structured logging, metrics collection
  clients/      BigQuery client wrapper with retries
  naming.py     Centralized table naming (all table names defined here)
  constants.py  Shared constants, BQ reserved words
  exceptions.py Exception hierarchy
  __main__.py   Click CLI (run, validate, preview-sql)
```

## Key Patterns

1. **Registry pattern** — Feature functions (`features/registry.py`) and comparison functions (`matching/comparisons.py`) are `@register("name")` decorated dicts. Adding a function = 1 decorator + 1 YAML line.
2. **Config-driven** — All behavior from YAML. Schema in `config/schema.py` (Pydantic v2). Loader in `config/loader.py` (env var interpolation: `${VAR}`, `${VAR:-default}`).
3. **Centralized naming** — ALL BigQuery table names flow through `naming.py`. Never construct table names with f-strings elsewhere.
4. **Engine pattern** — Each domain (features, blocking, matching, reconciliation) has an engine class that generates SQL via `SQLGenerator.render()`.
5. **Template SQL** — Jinja2 templates in `sql/templates/` produce BigQuery SQL. Custom filters: `bq_escape`, `farm_fp`, `format_watermark_value`.

## Pipeline Execution Order

```
watermark read → stage sources (bronze) → engineer features (silver) →
embeddings + LSH (if enabled) → create UDFs → estimate F-S parameters (if configured) →
init matches table → tier 1..N (blocking → matching → accumulate) →
clustering → gold output → active learning review queues (if configured) →
watermark advance → metrics
```

## Running Tests

```bash
python -m pytest tests/ -v          # 93 tests, ~0.5s
python -m pytest tests/ -v --tb=short  # shorter tracebacks
```

Tests use Python 3.12 on this machine:
```bash
C:/Users/amare/AppData/Local/Programs/Python/Python312/python.exe -m pytest tests/ -v
```

## CLI Commands

```bash
bq-er validate --config config.yml
bq-er preview-sql --config config.yml --tier fuzzy --stage blocking
bq-er run --config config.yml --full-refresh --dry-run
bq-er run --config config.yml --tier exact_composite --tier fuzzy_name
```

## Important Files to Read First

1. `config/schema.py` — Defines the entire YAML schema (PipelineConfig root model)
2. `naming.py` — Where all table names are defined
3. `features/registry.py` — All 45+ feature functions
4. `matching/comparisons.py` — All 22+ comparison functions
5. `pipeline/orchestrator.py` — Main execution flow
6. `config/examples/insurance_entity.yml` — Full production config example

## Adding New Features

**New feature function** — Add `@register("name")` function in `features/registry.py`. Use in YAML: `function: "name"`.

**New comparison function** — Add `@register("name")` function in `matching/comparisons.py`. Use in YAML: `method: "name"`.

**New matching tier** — Add YAML block under `matching_tiers:`. No code changes needed.

**New feature group** — Add under `feature_engineering.extra_groups` in YAML config. No code changes needed.

**New source schema** — Add under `sources:` in YAML. Define columns, unique_key, updated_at. All SQL is generated from config.

## Code Style

- **ruff** for linting (line length 100, rules: E, F, I, N, W, UP)
- **mypy** strict mode
- All public functions have docstrings
- Type hints everywhere (`from __future__ import annotations`)
- `**_: Any` on registry functions to accept extra kwargs

## Known Limitations

- `threshold.method` implements `sum` and `fellegi_sunter` (not `min_all` or `weighted_sum`)
- Comparison functions hardcode `l.` / `r.` table aliases
- Nickname mapping is hardcoded in Python (not externally configurable)
- BigQuery JS UDF required for Jaro-Winkler (auto-created by pipeline)
- EM estimation runs entirely in BigQuery scripting (no local iteration)
