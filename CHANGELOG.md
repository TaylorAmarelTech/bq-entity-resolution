# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- DuckDB local backend with comprehensive BigQuery SQL shims (macros, Python UDFs, SQL rewrites)
- BigQuery emulator backend for Docker-based BQ-fidelity testing
- 750+ tests (unit + integration) with DuckDB-based execution verification
- Configuration presets for common patterns: person dedup, person linkage, business dedup
- Three production-quality example configs (insurance entity, customer dedup, probabilistic matching)
- Environment variable interpolation in YAML configs
- SQL audit logging and pipeline diagnostics

[0.1.0]: https://github.com/bq-entity-resolution/bq-entity-resolution/releases/tag/v0.1.0
