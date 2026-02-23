"""
CLI entry point for the entity resolution pipeline.

Commands:
  run          Execute the full pipeline
  validate     Validate configuration without running
  preview-sql  Preview generated SQL for a specific tier
"""

from __future__ import annotations

import logging
import os
import sys

import click

from bq_entity_resolution.version import __version__

logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version=__version__)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    help="Logging level",
)
@click.option(
    "--log-format",
    default="json",
    type=click.Choice(["json", "text"], case_sensitive=False),
    help="Log output format",
)
def cli(log_level: str, log_format: str) -> None:
    """BigQuery Entity Resolution Pipeline."""
    from bq_entity_resolution.monitoring.logging import setup_logging

    setup_logging(log_level, log_format)


@cli.command()
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline config YAML",
)
@click.option(
    "--defaults",
    default=None,
    type=click.Path(exists=True),
    help="Path to defaults YAML",
)
@click.option("--full-refresh", is_flag=True, help="Ignore watermarks, reprocess all")
@click.option("--dry-run", is_flag=True, help="Generate SQL but don't execute")
@click.option("--tier", multiple=True, help="Run only specific tier(s) by name")
def run(
    config: str,
    defaults: str | None,
    full_refresh: bool,
    dry_run: bool,
    tier: tuple[str, ...],
) -> None:
    """Execute the entity resolution pipeline."""
    from bq_entity_resolution.config.loader import load_config
    from bq_entity_resolution.pipeline.pipeline import Pipeline

    try:
        cfg = load_config(config, defaults)

        # Filter to specific tiers if requested
        if tier:
            tier_names = set(tier)
            original_count = len(cfg.matching_tiers)
            cfg.matching_tiers = [
                t for t in cfg.matching_tiers if t.name in tier_names
            ]
            if not cfg.matching_tiers:
                click.echo(
                    f"No matching tiers found for: {tier_names}. "
                    f"Available tiers: {[t.name for t in load_config(config, defaults).matching_tiers]}",
                    err=True,
                )
                sys.exit(1)
            click.echo(
                f"Running {len(cfg.matching_tiers)}/{original_count} tier(s): "
                f"{[t.name for t in cfg.matching_tiers]}"
            )

        pipeline = Pipeline(cfg)

        if dry_run:
            plan = pipeline.plan(full_refresh=full_refresh)
            click.echo("DRY RUN — Generated SQL preview:")
            click.echo(plan.preview())
        else:
            from bq_entity_resolution.clients.bigquery import BigQueryClient
            from bq_entity_resolution.backends.bigquery import BigQueryBackend

            bq_client = BigQueryClient(
                project=cfg.project.bq_project,
                location=cfg.project.bq_location,
                max_bytes_billed=cfg.scale.max_bytes_billed,
            )
            backend = BigQueryBackend(bq_client)
            result = pipeline.run(
                backend=backend,
                full_refresh=full_refresh,
            )

            click.echo(f"\nPipeline completed: {result.run_id}")
            click.echo(f"Stages executed: {len(result.completed_stages)}")
            for stage in result.completed_stages:
                click.echo(f"  - {stage}")

    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline config YAML",
)
@click.option("--defaults", default=None, type=click.Path(exists=True))
def validate(config: str, defaults: str | None) -> None:
    """Validate configuration without running the pipeline."""
    from bq_entity_resolution.config.loader import load_config
    from bq_entity_resolution.config.validators import validate_full

    try:
        cfg = load_config(config, defaults)
        validate_full(cfg)

        click.echo("Configuration valid!")
        click.echo(f"  Project: {cfg.project.name}")
        click.echo(f"  Sources: {len(cfg.sources)}")
        for s in cfg.sources:
            click.echo(f"    - {s.name} ({s.table}): {len(s.columns)} columns")
        click.echo(f"  Matching tiers: {len(cfg.matching_tiers)}")
        for t in cfg.matching_tiers:
            status = "enabled" if t.enabled else "disabled"
            click.echo(
                f"    - {t.name} ({status}): "
                f"{len(t.comparisons)} comparisons, "
                f"{len(t.blocking.paths)} blocking paths"
            )
        click.echo(f"  Embeddings: {'enabled' if cfg.embeddings.enabled else 'disabled'}")
        click.echo(f"  Incremental: {'enabled' if cfg.incremental.enabled else 'disabled'}")

    except Exception as e:
        click.echo(f"Configuration INVALID: {e}", err=True)
        sys.exit(1)


@cli.command("preview-sql")
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline config YAML",
)
@click.option("--defaults", default=None, type=click.Path(exists=True))
@click.option("--tier", required=True, help="Tier name to preview SQL for")
@click.option(
    "--stage",
    default="all",
    type=click.Choice(["all", "blocking", "matching"], case_sensitive=False),
    help="Which stage to preview",
)
def preview_sql(config: str, defaults: str | None, tier: str, stage: str) -> None:
    """Preview generated SQL for a specific tier without executing."""
    from bq_entity_resolution.config.loader import load_config
    from bq_entity_resolution.stages.blocking import BlockingStage
    from bq_entity_resolution.stages.matching import MatchingStage

    try:
        cfg = load_config(config, defaults)
        tier_cfg = next((t for t in cfg.matching_tiers if t.name == tier), None)
        if not tier_cfg:
            available = [t.name for t in cfg.matching_tiers]
            click.echo(f"Tier '{tier}' not found. Available: {available}", err=True)
            sys.exit(1)

        tier_index = next(
            i for i, t in enumerate(cfg.matching_tiers) if t.name == tier
        )

        if stage in ("all", "blocking"):
            blocking_stage = BlockingStage(tier_cfg, tier_index, cfg)
            click.echo("-- BLOCKING SQL --")
            for expr in blocking_stage.plan():
                click.echo(expr.render())
            click.echo()

        if stage in ("all", "matching"):
            matching_stage = MatchingStage(tier_cfg, tier_index, cfg)
            click.echo("-- MATCHING SQL --")
            for expr in matching_stage.plan():
                click.echo(expr.render())

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("estimate-params")
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline config YAML",
)
@click.option("--defaults", default=None, type=click.Path(exists=True))
@click.option("--tier", required=True, help="Tier name to estimate parameters for")
def estimate_params(config: str, defaults: str | None, tier: str) -> None:
    """Estimate m/u parameters for a Fellegi-Sunter tier."""
    from bq_entity_resolution.config.loader import load_config
    from bq_entity_resolution.matching.parameters import ParameterEstimator

    try:
        cfg = load_config(config, defaults)
        tier_cfg = next((t for t in cfg.matching_tiers if t.name == tier), None)
        if not tier_cfg:
            available = [t.name for t in cfg.matching_tiers]
            click.echo(f"Tier '{tier}' not found. Available: {available}", err=True)
            sys.exit(1)

        estimator = ParameterEstimator(cfg)
        training = estimator.resolve_training_config(tier_cfg)

        if training.method == "none":
            click.echo(
                f"No training configured for tier '{tier}'. "
                f"Set training.method to 'labeled' or 'em'.",
                err=True,
            )
            sys.exit(1)

        click.echo(f"-- PARAMETER ESTIMATION SQL ({training.method}) --")
        if training.method == "labeled":
            click.echo(estimator.generate_label_estimation_sql(tier_cfg, training))
        else:
            click.echo(estimator.generate_em_estimation_sql(tier_cfg, training))

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("review-queue")
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline config YAML",
)
@click.option("--defaults", default=None, type=click.Path(exists=True))
@click.option("--tier", required=True, help="Tier name to generate review queue for")
def review_queue(config: str, defaults: str | None, tier: str) -> None:
    """Preview active learning review queue SQL for a tier."""
    from bq_entity_resolution.config.loader import load_config
    from bq_entity_resolution.matching.active_learning import ActiveLearningEngine

    try:
        cfg = load_config(config, defaults)
        tier_cfg = next((t for t in cfg.matching_tiers if t.name == tier), None)
        if not tier_cfg:
            available = [t.name for t in cfg.matching_tiers]
            click.echo(f"Tier '{tier}' not found. Available: {available}", err=True)
            sys.exit(1)

        al_engine = ActiveLearningEngine(cfg)
        click.echo("-- ACTIVE LEARNING REVIEW QUEUE SQL --")
        click.echo(al_engine.generate_review_queue_sql(tier_cfg))

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline config YAML",
)
@click.option("--defaults", default=None, type=click.Path(exists=True))
@click.option(
    "--source",
    default=None,
    help="Source name to profile (defaults to first source)",
)
@click.option(
    "--columns",
    default=None,
    help="Comma-separated column names to profile (defaults to all)",
)
def profile(
    config: str,
    defaults: str | None,
    source: str | None,
    columns: str | None,
) -> None:
    """Profile source columns and suggest comparison weights.

    Computes cardinality, null rates, and value distributions for
    source columns, then suggests comparison weights based on
    information content (log2(m/u)). No labeled data required.
    """
    from bq_entity_resolution.config.loader import load_config
    from bq_entity_resolution.profiling.column_profiler import ColumnProfiler

    try:
        cfg = load_config(config, defaults)

        # Resolve source
        src = cfg.sources[0]
        if source:
            src = next((s for s in cfg.sources if s.name == source), None)
            if not src:
                available = [s.name for s in cfg.sources]
                click.echo(f"Source '{source}' not found. Available: {available}", err=True)
                sys.exit(1)

        # Resolve columns
        if columns:
            col_names = [c.strip() for c in columns.split(",")]
        else:
            col_names = [c.name for c in src.columns]

        profiler = ColumnProfiler()
        sql = profiler.generate_profile_sql(src.table, col_names)

        click.echo(f"Source: {src.name} ({src.table})")
        click.echo(f"Columns: {', '.join(col_names)}")
        click.echo()
        click.echo("-- PROFILING SQL --")
        click.echo("-- Run this in BigQuery, then use the results to set weights --")
        click.echo(sql)
        click.echo()

        # Show suggestions from role-based defaults
        suggestions = []
        for comp_col in col_names:
            from bq_entity_resolution.config.roles import detect_role, ROLE_COMPARISONS
            role = detect_role(comp_col)
            if role and role in ROLE_COMPARISONS:
                for spec in ROLE_COMPARISONS[role]:
                    suggestions.append(
                        f"  {comp_col}: method={spec.method}, "
                        f"default_weight={spec.weight}"
                    )

        if suggestions:
            click.echo("Role-based default weights:")
            for s in suggestions:
                click.echo(s)
            click.echo()
            click.echo(
                "To use data-driven weights instead, run the SQL above "
                "in BigQuery and set weight_mode: profile in your config."
            )

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline config YAML",
)
@click.option("--defaults", default=None, type=click.Path(exists=True))
@click.option("--tier", required=True, help="Tier name to analyze")
@click.option(
    "--analysis",
    default="contribution",
    type=click.Choice(["contribution", "threshold", "impact"], case_sensitive=False),
    help="Type of analysis to run",
)
def analyze(config: str, defaults: str | None, tier: str, analysis: str) -> None:
    """Analyze weight sensitivity for a matching tier.

    Three analysis types:
    - contribution: which comparisons drive matches
    - threshold: match counts at different threshold values
    - impact: effect of changing each comparison's weight
    """
    from bq_entity_resolution.config.loader import load_config
    from bq_entity_resolution.profiling.weight_sensitivity import WeightSensitivityAnalyzer

    try:
        cfg = load_config(config, defaults)
        tier_cfg = next((t for t in cfg.matching_tiers if t.name == tier), None)
        if not tier_cfg:
            available = [t.name for t in cfg.matching_tiers]
            click.echo(f"Tier '{tier}' not found. Available: {available}", err=True)
            sys.exit(1)

        analyzer = WeightSensitivityAnalyzer(cfg)

        if analysis == "contribution":
            click.echo(f"-- WEIGHT CONTRIBUTION ANALYSIS: {tier} --")
            click.echo(analyzer.generate_contribution_sql(tier_cfg))
        elif analysis == "threshold":
            click.echo(f"-- THRESHOLD SWEEP: {tier} --")
            click.echo(analyzer.generate_threshold_sweep_sql(tier_cfg))
        elif analysis == "impact":
            click.echo(f"-- WEIGHT IMPACT ANALYSIS: {tier} --")
            click.echo(analyzer.generate_weight_impact_sql(tier_cfg))

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command("ingest-labels")
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline config YAML",
)
@click.option("--defaults", default=None, type=click.Path(exists=True))
@click.option("--tier", required=True, help="Tier name to ingest labels for")
@click.option("--retrain", is_flag=True, help="Re-estimate m/u after ingestion")
@click.option("--dry-run", is_flag=True, help="Preview SQL without executing")
def ingest_labels(
    config: str,
    defaults: str | None,
    tier: str,
    retrain: bool,
    dry_run: bool,
) -> None:
    """Ingest human labels from the review queue and optionally retrain m/u."""
    from bq_entity_resolution.config.loader import load_config
    from bq_entity_resolution.matching.active_learning import ActiveLearningEngine
    from bq_entity_resolution.matching.parameters import ParameterEstimator

    try:
        cfg = load_config(config, defaults)
        tier_cfg = next((t for t in cfg.matching_tiers if t.name == tier), None)
        if not tier_cfg:
            available = [t.name for t in cfg.matching_tiers]
            click.echo(f"Tier '{tier}' not found. Available: {available}", err=True)
            sys.exit(1)

        al_engine = ActiveLearningEngine(cfg)

        # Generate and show/execute ingestion SQL
        ingest_sql = al_engine.generate_label_ingestion_sql(tier_cfg)
        if dry_run:
            click.echo("-- LABEL INGESTION SQL --")
            click.echo(ingest_sql)
        else:
            from bq_entity_resolution.clients.bigquery import BigQueryClient
            from bq_entity_resolution.pipeline.runner import SQLRunner

            bq_client = BigQueryClient(
                project=cfg.project.bq_project,
                location=cfg.project.bq_location,
            )
            runner = SQLRunner(bq_client)
            runner.execute_script(ingest_sql, job_label=f"ingest_labels_{tier}")
            click.echo(f"Labels ingested for tier '{tier}'")

        # Optionally retrain
        if retrain:
            estimator = ParameterEstimator(cfg)
            retrain_sql = estimator.generate_reestimation_sql(tier_cfg)
            if dry_run:
                click.echo("\n-- REESTIMATION SQL --")
                click.echo(retrain_sql)
            else:
                click.echo(f"Re-estimating m/u for tier '{tier}'...")
                result = runner.execute_and_fetch(
                    retrain_sql, job_label=f"retrain_{tier}"
                )
                click.echo(f"Reestimation complete: {len(result)} parameter rows")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--project", prompt="GCP project ID", help="GCP project ID")
@click.option(
    "--table",
    default=None,
    help="Source table (project.dataset.table) — auto-discovers columns if set",
)
@click.option(
    "--preset",
    default="auto",
    type=click.Choice(
        ["auto", "person_dedup", "person_linkage", "business_dedup",
         "insurance", "financial", "healthcare"],
        case_sensitive=False,
    ),
    help="Config preset to use",
)
@click.option(
    "--output",
    default="config.yml",
    type=click.Path(),
    help="Output config file path",
)
def init(project: str, table: str | None, preset: str, output: str) -> None:
    """Scaffold a starter config YAML from prompts or table auto-discovery.

    Examples:
        bq-er init --project my-project --table my-project.raw.customers
        bq-er init --project my-project --preset insurance --output insurance.yml
    """
    import yaml as _yaml

    from bq_entity_resolution.config.roles import detect_role, ROLE_FEATURES, ROLE_COMPARISONS

    columns: list[dict[str, str]] = []
    features: list[dict] = []
    blocking_keys: list[dict] = []
    comparisons: list[dict] = []

    source_name = "source"
    source_table = table or f"{project}.raw.YOUR_TABLE"

    if table:
        # Extract source name from table
        parts = table.rsplit(".", 1)
        source_name = parts[-1] if parts else "source"

        # Try live auto-discovery via BigQuery INFORMATION_SCHEMA
        try:
            from google.cloud import bigquery as _bq  # noqa: F401

            dataset_parts = table.split(".")
            if len(dataset_parts) == 3:
                client = _bq.Client(project=project)
                schema_query = (
                    f"SELECT column_name, data_type "
                    f"FROM `{dataset_parts[0]}.{dataset_parts[1]}.INFORMATION_SCHEMA.COLUMNS` "
                    f"WHERE table_name = '{dataset_parts[2]}' "
                    f"ORDER BY ordinal_position"
                )
                rows = list(client.query(schema_query).result())
                click.echo(f"Discovered {len(rows)} columns from {table}")
                for row in rows:
                    col_name = row["column_name"]
                    col_type = row["data_type"]
                    columns.append({"name": col_name, "type": col_type})
                    role = detect_role(col_name)
                    if role:
                        click.echo(f"  {col_name} ({col_type}) -> role: {role}")
        except Exception as exc:
            click.echo(
                f"Could not auto-discover columns from BigQuery: {exc}\n"
                f"Generating template config with placeholder columns.",
                err=True,
            )

    # If no columns discovered, provide placeholder examples
    if not columns:
        columns = [
            {"name": "id", "type": "STRING"},
            {"name": "first_name", "type": "STRING"},
            {"name": "last_name", "type": "STRING"},
            {"name": "email", "type": "STRING"},
            {"name": "phone", "type": "STRING"},
            {"name": "updated_at", "type": "TIMESTAMP"},
        ]

    # Auto-generate features and comparisons from discovered roles
    for col_def in columns:
        col_name = col_def["name"]
        role = detect_role(col_name)
        if not role:
            continue

        # Features
        for suffix, func in ROLE_FEATURES.get(role, []):
            feat_name = f"{col_name}_{suffix}" if suffix else col_name
            features.append({
                "name": feat_name,
                "function": func,
                "input": col_name,
            })

        # Blocking keys (first feature per role)
        from bq_entity_resolution.config.roles import ROLE_BLOCKING_KEYS
        for bk_suffix, bk_func in ROLE_BLOCKING_KEYS.get(role, []):
            blocking_keys.append({
                "name": f"bk_{bk_suffix}",
                "function": bk_func,
                "inputs": [col_name],
            })

        # Comparisons
        for spec in ROLE_COMPARISONS.get(role, []):
            feature_col = (
                f"{col_name}_{spec.feature_suffix}"
                if spec.feature_suffix
                else col_name
            )
            comp: dict = {
                "left": feature_col,
                "right": feature_col,
                "method": spec.method,
                "weight": spec.weight,
            }
            if spec.params:
                comp["params"] = dict(spec.params)
            comparisons.append(comp)

    # Build config dict
    total_weight = sum(c.get("weight", 1.0) for c in comparisons) if comparisons else 10.0

    config_dict: dict = {
        "version": "1.0",
        "project": {
            "name": source_name,
            "bq_project": f"${{BQ_PROJECT:-{project}}}",
        },
        "sources": [{
            "name": source_name,
            "table": f"${{BQ_PROJECT:-{project}}}.{'.'.join(source_table.split('.')[1:]) if '.' in source_table else 'raw.' + source_name}",
            "unique_key": "id",
            "updated_at": "updated_at",
            "columns": columns,
        }],
    }

    if features:
        config_dict["feature_engineering"] = {
            "auto_features": {"features": features},
            "blocking_keys": blocking_keys,
        }

    if comparisons:
        bk_names = [bk["name"] for bk in blocking_keys] if blocking_keys else ["bk_email_domain"]
        config_dict["matching_tiers"] = [
            {
                "name": "exact",
                "description": "High-confidence exact matches",
                "blocking": {"paths": [{"keys": bk_names[:2] if len(bk_names) >= 2 else bk_names}]},
                "comparisons": comparisons,
                "threshold": {"min_score": round(total_weight * 0.7, 1)},
            },
            {
                "name": "fuzzy",
                "description": "Fuzzy probabilistic matches",
                "blocking": {"paths": [{"keys": bk_names[2:4] if len(bk_names) > 2 else bk_names[:1]}]},
                "comparisons": comparisons,
                "threshold": {"min_score": round(total_weight * 0.4, 1)},
            },
        ]

    # Write YAML
    from pathlib import Path as _Path
    output_path = _Path(output)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# Auto-generated config for {source_name}\n")
        f.write(f"# Generated by: bq-er init --project {project}\n")
        f.write(f"#\n")
        f.write(f"# Next steps:\n")
        f.write(f"#   1. Review and adjust columns, features, and comparisons\n")
        f.write(f"#   2. Validate: bq-er validate --config {output}\n")
        f.write(f"#   3. Preview:  bq-er preview-sql --config {output} --tier exact --stage all\n")
        f.write(f"#   4. Run:      bq-er run --config {output} --dry-run\n")
        f.write(f"#   5. Execute:  bq-er run --config {output}\n")
        f.write(f"#\n\n")
        _yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    click.echo(f"\nConfig written to {output_path}")
    click.echo(f"  Sources: 1 ({source_name})")
    click.echo(f"  Columns: {len(columns)}")
    click.echo(f"  Features: {len(features)}")
    click.echo(f"  Blocking keys: {len(blocking_keys)}")
    click.echo(f"  Comparisons: {len(comparisons)}")
    click.echo(f"  Tiers: {'2 (exact + fuzzy)' if comparisons else '0 — add comparisons manually'}")
    click.echo(f"\nRun 'bq-er validate --config {output}' to check the config.")


@cli.command("check-env")
@click.option(
    "--config",
    default=None,
    type=click.Path(exists=True),
    help="Optional config to check project-specific settings",
)
def check_env(config: str | None) -> None:
    """Verify environment setup for running the pipeline.

    Checks: Python version, required packages, GCP auth,
    BQ_PROJECT env var, and dataset accessibility.
    """
    import importlib

    issues: list[str] = []
    ok: list[str] = []

    # Python version
    py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    if sys.version_info >= (3, 11):
        ok.append(f"Python {py_version}")
    else:
        issues.append(f"Python {py_version} (requires 3.11+)")

    # Required packages
    required_packages = [
        ("pydantic", "pydantic"),
        ("yaml", "pyyaml"),
        ("jinja2", "Jinja2"),
        ("click", "click"),
        ("sqlglot", "sqlglot"),
    ]
    for import_name, pip_name in required_packages:
        try:
            mod = importlib.import_module(import_name)
            try:
                from importlib.metadata import version as _pkg_version
                ver = _pkg_version(pip_name)
            except Exception:
                ver = getattr(mod, "__version__", "?")
            ok.append(f"{pip_name} {ver}")
        except ImportError:
            issues.append(f"{pip_name} not installed — pip install {pip_name}")

    # google-cloud-bigquery (optional but needed for execution)
    try:
        from google.cloud import bigquery as _bq
        ok.append(f"google-cloud-bigquery {_bq.__version__}")
    except ImportError:
        issues.append(
            "google-cloud-bigquery not installed — "
            "pip install google-cloud-bigquery (required for BQ execution)"
        )

    # BQ_PROJECT env var
    bq_project = os.environ.get("BQ_PROJECT")
    if bq_project:
        ok.append(f"BQ_PROJECT={bq_project}")
    else:
        issues.append(
            "BQ_PROJECT not set — export BQ_PROJECT=your-gcp-project"
        )

    # GCP auth
    try:
        import google.auth  # type: ignore[import-untyped]
        credentials, project = google.auth.default()
        ok.append(f"GCP auth OK (project: {project or 'from credentials'})")
    except Exception as exc:
        issues.append(
            f"GCP auth failed: {exc} — "
            f"run 'gcloud auth application-default login' "
            f"or set GOOGLE_APPLICATION_CREDENTIALS"
        )

    # If config provided, check project-specific settings
    if config:
        try:
            from bq_entity_resolution.config.loader import load_config
            cfg = load_config(config)
            ok.append(f"Config valid: {cfg.project.name} ({len(cfg.sources)} source(s))")

            # Try to validate dataset exists
            if bq_project:
                try:
                    from google.cloud import bigquery as _bq2
                    client = _bq2.Client(project=cfg.project.bq_project)
                    for source in cfg.sources:
                        parts = source.table.split(".")
                        if len(parts) >= 2:
                            ds_ref = f"{parts[0]}.{parts[1]}"
                            try:
                                client.get_dataset(ds_ref)
                                ok.append(f"Dataset accessible: {ds_ref}")
                            except Exception:
                                issues.append(f"Dataset not accessible: {ds_ref}")
                except Exception:
                    pass  # BQ client errors already covered
        except Exception as exc:
            issues.append(f"Config invalid: {exc}")

    # Print results
    click.echo("Environment check:")
    click.echo()
    for item in ok:
        click.echo(f"  [OK] {item}")
    for item in issues:
        click.echo(f"  [!!] {item}")

    click.echo()
    if issues:
        click.echo(f"{len(issues)} issue(s) found. Fix these before running the pipeline.")
        sys.exit(1)
    else:
        click.echo("All checks passed. Ready to run.")


@cli.command()
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline config YAML",
)
@click.option("--defaults", default=None, type=click.Path(exists=True))
def describe(config: str, defaults: str | None) -> None:
    """Show a human-readable summary of pipeline configuration.

    Displays sources, features, blocking keys, tiers, comparisons,
    and estimated table outputs — a quick way to understand
    what the pipeline will do before running it.
    """
    from bq_entity_resolution.config.loader import load_config

    try:
        cfg = load_config(config, defaults)

        click.echo(f"Pipeline: {cfg.project.name}")
        click.echo(f"Project:  {cfg.project.bq_project}")
        click.echo(f"Link type: {cfg.link_type}")
        click.echo()

        # Sources
        click.echo(f"Sources ({len(cfg.sources)}):")
        for s in cfg.sources:
            click.echo(f"  {s.name}")
            click.echo(f"    Table: {s.table}")
            click.echo(f"    Key: {s.unique_key}  Updated: {s.updated_at}")
            click.echo(f"    Columns: {len(s.columns)}")
            for c in s.columns[:8]:
                role_str = f" (role: {c.role})" if getattr(c, "role", None) else ""
                click.echo(f"      - {c.name}: {getattr(c, 'type', 'STRING')}{role_str}")
            if len(s.columns) > 8:
                click.echo(f"      ... and {len(s.columns) - 8} more")
        click.echo()

        # Feature engineering
        fe = cfg.feature_engineering
        all_features = []
        all_bk = []
        # Collect features from all groups using model_fields (Pydantic v2)
        field_names = fe.model_fields if hasattr(fe, "model_fields") else {}
        for group_name in field_names:
            group = getattr(fe, group_name, None)
            if hasattr(group, "features"):
                all_features.extend(group.features)
        if hasattr(fe, "blocking_keys"):
            all_bk = fe.blocking_keys

        click.echo(f"Feature Engineering:")
        click.echo(f"  Features: {len(all_features)}")
        for feat in all_features[:6]:
            click.echo(f"    - {feat.name} ({feat.function})")
        if len(all_features) > 6:
            click.echo(f"    ... and {len(all_features) - 6} more")

        click.echo(f"  Blocking keys: {len(all_bk)}")
        for bk in all_bk:
            click.echo(f"    - {bk.name} ({bk.function})")
        click.echo()

        # Comparison pool
        if cfg.comparison_pool:
            click.echo(f"Comparison Pool ({len(cfg.comparison_pool)} methods):")
            for name, comp in list(cfg.comparison_pool.items())[:8]:
                click.echo(f"  - {name}: {comp.method} (weight: {comp.weight})")
            if len(cfg.comparison_pool) > 8:
                click.echo(f"  ... and {len(cfg.comparison_pool) - 8} more")
            click.echo()

        # Matching tiers
        click.echo(f"Matching Tiers ({len(cfg.matching_tiers)}):")
        for t in cfg.matching_tiers:
            status = "enabled" if t.enabled else "DISABLED"
            click.echo(f"  {t.name} [{status}]")
            if t.description:
                click.echo(f"    {t.description}")
            click.echo(f"    Blocking paths: {len(t.blocking.paths)}")
            for i, path in enumerate(t.blocking.paths):
                click.echo(f"      Path {i}: keys={path.keys}")
            click.echo(f"    Comparisons: {len(t.comparisons)}")
            for comp in t.comparisons[:4]:
                if getattr(comp, "ref", None):
                    click.echo(f"      - ref: {comp.ref}")
                else:
                    click.echo(f"      - {comp.left} vs {comp.right}: {comp.method} (w={comp.weight})")
            if len(t.comparisons) > 4:
                click.echo(f"      ... and {len(t.comparisons) - 4} more")
            threshold = t.threshold
            click.echo(f"    Threshold: {threshold.method} >= {threshold.min_score}")
        click.echo()

        # Scale / incremental
        click.echo(f"Incremental: {'enabled' if cfg.incremental.enabled else 'disabled'}")
        click.echo(f"Embeddings:  {'enabled' if cfg.embeddings.enabled else 'disabled'}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
