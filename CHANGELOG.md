# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-23

### Added

- DAG-based pipeline architecture: `Pipeline` -> `StageDAG` -> `Stage.plan()` -> `PipelineExecutor`
- 8 composable Stage classes with declared inputs/outputs and compile-time contract validation
- 14 Python SQL builder modules (30 functions, 28 frozen dataclasses) replacing all Jinja2 templates
- `SQLExpression` wrapper (sqlglot-based) for dialect-aware SQL generation
- Pluggable backend system: `BigQueryBackend`, `DuckDBBackend`, `BQEmulatorBackend`
- DuckDB backend with comprehensive BigQuery SQL shims (SOUNDEX, metaphone, FARM_FINGERPRINT, BQ scripting interpreter)
- Progressive disclosure config presets: `quick_config()`, `person_dedup_preset`, `business_dedup_preset`, etc.
- Column role detection and auto-generation of features, blocking keys, and comparisons
- Compile-time schema contract validation (`pipeline/validator.py`)
- Runtime quality gates: `OutputNotEmptyGate`, `ClusterSizeGate`
- Checkpoint/resume for crash-resilient incremental processing
- Incremental clustering with cross-batch canonical index
- Column profiling and weight sensitivity analysis (`profiling/`)
- Config includes with circular dependency detection
- Field-level golden record assembly (`field_merge` canonical method)
- Term frequency (TF-IDF) adjusted scoring
- 830+ tests (unit + integration) with DuckDB-based execution verification
- Comprehensive architectural documentation (`docs/ARCHITECTURE.md`)

### Removed

- All 25 Jinja2 SQL templates (`sql/templates/` directory)
- `SQLGenerator` class and Jinja2 rendering infrastructure (`sql/generator.py`)
- Legacy engine classes: `FeatureEngine`, `BlockingEngine`, `MatchingEngine`, `ReconciliationEngine`
- Legacy `PipelineOrchestrator` (replaced by `Pipeline` + `StageDAG`)
- `jinja2` dependency from `pyproject.toml`

### Changed

- All SQL generation now uses Python builder functions (type-safe, unit-testable)
- `WatermarkManager`, `CheckpointManager`, `EmbeddingManager`, `ActiveLearningEngine`, `ParameterEstimator` no longer depend on `SQLGenerator`
- CLI commands use `Pipeline`/`Stage` classes instead of legacy engines
- Feature function count increased from 45+ to 60+
- Comparison function count increased from 22+ to 30+

## [0.1.0] - 2026-02-22

### Added

- Multi-tier entity resolution pipeline driven by YAML configuration
- Python orchestrator that generates and executes SQL against BigQuery
- 45+ feature functions: name cleaning, address standardization, phone normalization, nickname resolution, phonetic encoding, date/zip extraction, geo hashing, and more
- 22+ comparison functions: exact, Levenshtein, Jaro-Winkler, Soundex, cosine similarity, token overlap, geo distance, and more
- Multi-path blocking with per-path candidate limits
- LSH (Locality-Sensitive Hashing) blocking for embedding-based matching
- Fellegi-Sunter probabilistic matching with EM parameter estimation
- Active learning with review queue generation and label ingestion
- Hard negative disqualification and soft signal scoring
- Connected components clustering with canonical record election
- Incremental processing with runtime watermarks and grace periods
- Full Pydantic v2 configuration validation with cross-field checks
- `bq-er` CLI with commands: `run`, `validate`, `preview-sql`, `estimate-params`, `review-queue`, `ingest-labels`
- Docker packaging with multi-stage build and non-root execution
- Three production-quality example configs (insurance entity, customer dedup, probabilistic matching)
- Environment variable interpolation in YAML configs
- SQL audit logging and pipeline diagnostics

[0.2.0]: https://github.com/bq-entity-resolution/bq-entity-resolution/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/bq-entity-resolution/bq-entity-resolution/releases/tag/v0.1.0
