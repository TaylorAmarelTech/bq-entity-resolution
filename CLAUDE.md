# CLAUDE.md — Project Context for AI Assistants

## Project Overview

**bq-entity-resolution** is a Python-orchestrated, YAML-configured entity resolution pipeline for Google BigQuery. Python handles configuration, validation, SQL generation, and DAG-based orchestration while BigQuery (or DuckDB for local testing) executes all SQL.

## Tech Stack

- **Python 3.11+** with Pydantic v2 (config), Click (CLI), structlog (logging), sqlglot (SQL expression wrapper)
- **BigQuery** as the production compute engine (no local data processing)
- **DuckDB** as the local testing backend (with BQ function shims)
- **Docker** for deployment (multi-stage build, non-root user)

## Project Structure

```
src/bq_entity_resolution/
  config/        Pydantic v2 schema, YAML loader, presets, role mapping, validators
  sql/builders/  14 Python SQL builder modules (type-safe, testable, no Jinja2)
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
  constants.py   Shared constants, BQ reserved words
  exceptions.py  Exception hierarchy
  __main__.py    Click CLI (run, validate, preview-sql, profile, analyze, etc.)
```

## Key Patterns

1. **Registry pattern** — Feature functions (`features/registry.py`) and comparison functions (`matching/comparisons.py`) are `@register("name")` decorated dicts. Adding a function = 1 decorator + 1 YAML line.
2. **Config-driven** — All behavior from YAML. Schema in `config/schema.py` (Pydantic v2). Loader in `config/loader.py` (env var interpolation: `${VAR}`, `${VAR:-default}`).
3. **Centralized naming** — ALL BigQuery table names flow through `naming.py`. Never construct table names with f-strings elsewhere.
4. **Builder pattern** — Each SQL builder module has frozen `@dataclass` params + `build_*()` functions returning `SQLExpression`. No Jinja2 templates.
5. **Stage DAG** — Each pipeline stage declares inputs/outputs. `StageDAG` does topological sort. `PipelineExecutor` runs stages with quality gates.
6. **Plan-Execute split** — `Pipeline.plan()` generates all SQL without executing. `Pipeline.run()` executes against a backend. Enables SQL preview and testing.

## Pipeline Entry Point

The recommended entry point is `Pipeline` in `pipeline/pipeline.py`:
```python
from bq_entity_resolution.pipeline.pipeline import Pipeline
pipeline = Pipeline(config)
plan = pipeline.plan()        # Generate SQL (no execution)
pipeline.run(backend=backend)  # Execute against BigQuery or DuckDB
```

## Running Tests

```bash
python -m pytest tests/ -v               # 830+ tests, ~30s
python -m pytest tests/ -v --tb=short    # shorter tracebacks
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
bq-er profile --config config.yml
bq-er analyze --config config.yml --tier fuzzy --mode contribution
bq-er estimate-params --config config.yml --tier probabilistic
bq-er review-queue --config config.yml --tier fuzzy
bq-er ingest-labels --config config.yml --tier fuzzy
```

## Important Files to Read First

1. `config/schema.py` — Defines the entire YAML schema (PipelineConfig root model, 28 models)
2. `naming.py` — Where all table names are defined
3. `features/registry.py` — All 60+ feature functions
4. `matching/comparisons.py` — All 30+ comparison functions
5. `pipeline/pipeline.py` — Main Pipeline class (recommended entry point)
6. `pipeline/dag.py` — StageDAG construction with `build_pipeline_dag()`
7. `sql/builders/__init__.py` — All 30 builder functions and param classes
8. `stages/base.py` — Stage ABC with `plan()` method
9. `config/examples/insurance_entity.yml` — Full production config example

## Adding New Features

**New feature function** — Add `@register("name")` function in `features/registry.py`. Use in YAML: `function: "name"`.

**New comparison function** — Add `@register("name")` function in `matching/comparisons.py`. Use in YAML: `method: "name"`.

**New matching tier** — Add YAML block under `matching_tiers:`. No code changes needed.

**New SQL builder** — Create frozen `@dataclass` params + `build_*()` function returning `SQLExpression`. Add to `sql/builders/__init__.py` exports.

**New stage** — Subclass `Stage` from `stages/base.py`. Implement `name()`, `inputs()`, `outputs()`, `plan()`. Wire into `build_pipeline_dag()`.

## Code Style

- **ruff** for linting (line length 100, rules: E, F, I, N, W, UP)
- **mypy** strict mode
- All public functions have docstrings
- Type hints everywhere (`from __future__ import annotations`)
- `**_: Any` on registry functions to accept extra kwargs
- All SQL builder dataclasses use `frozen=True`
- All builder functions return `SQLExpression`

## Known Limitations

- `threshold.method` implements `sum` and `fellegi_sunter` (not `min_all` or `weighted_sum`)
- Comparison functions hardcode `l.` / `r.` table aliases
- Nickname mapping is hardcoded in Python (not externally configurable)
- BigQuery JS UDF required for Jaro-Winkler (auto-created by pipeline)
- EM estimation and connected components clustering use BigQuery scripting (DECLARE/LOOP/SET) — DuckDB backend interprets these via Python loop
- DuckDB SQL adaptation (`_adapt_sql()`) uses regex-based rewriting which may not cover all edge cases
