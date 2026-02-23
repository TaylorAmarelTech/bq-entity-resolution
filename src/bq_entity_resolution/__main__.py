"""
CLI entry point for the entity resolution pipeline.

Commands:
  run          Execute the full pipeline
  validate     Validate configuration without running
  preview-sql  Preview generated SQL for a specific tier
"""

from __future__ import annotations

import logging
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
    from bq_entity_resolution.pipeline.orchestrator import PipelineOrchestrator

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

        orchestrator = PipelineOrchestrator(cfg)
        if dry_run:
            orchestrator.bq_client.dry_run = True
            click.echo("DRY RUN — SQL will be validated but not executed")

        ctx = orchestrator.run(full_refresh=full_refresh)

        # Print summary
        click.echo(f"\nPipeline {ctx.status}: {ctx.run_id}")
        click.echo(f"Duration: {ctx.duration_seconds:.1f}s")
        click.echo(f"Sources staged: {len(ctx.staged_sources)}")
        click.echo(f"Tiers executed: {len(ctx.tier_results)}")
        for name, result in ctx.tier_results.items():
            click.echo(f"  {name}: {result.get('matches_found', 0)} matches")

        if ctx.status == "failed":
            sys.exit(1)

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
    from bq_entity_resolution.blocking.engine import BlockingEngine
    from bq_entity_resolution.config.loader import load_config
    from bq_entity_resolution.matching.engine import MatchingEngine
    from bq_entity_resolution.sql.generator import SQLGenerator

    try:
        cfg = load_config(config, defaults)
        tier_cfg = next((t for t in cfg.matching_tiers if t.name == tier), None)
        if not tier_cfg:
            available = [t.name for t in cfg.matching_tiers]
            click.echo(f"Tier '{tier}' not found. Available: {available}", err=True)
            sys.exit(1)

        sql_gen = SQLGenerator()

        if stage in ("all", "blocking"):
            blocking = BlockingEngine(cfg, sql_gen)
            click.echo("-- BLOCKING SQL --")
            click.echo(blocking.generate_candidates_sql(tier_cfg, tier_index=0))
            click.echo()

        if stage in ("all", "matching"):
            matching = MatchingEngine(cfg, sql_gen)
            click.echo("-- MATCHING SQL --")
            click.echo(matching.generate_tier_sql(tier_cfg, tier_index=0))

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
    from bq_entity_resolution.sql.generator import SQLGenerator

    try:
        cfg = load_config(config, defaults)
        tier_cfg = next((t for t in cfg.matching_tiers if t.name == tier), None)
        if not tier_cfg:
            available = [t.name for t in cfg.matching_tiers]
            click.echo(f"Tier '{tier}' not found. Available: {available}", err=True)
            sys.exit(1)

        sql_gen = SQLGenerator()
        estimator = ParameterEstimator(cfg, sql_gen)
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
    from bq_entity_resolution.sql.generator import SQLGenerator

    try:
        cfg = load_config(config, defaults)
        tier_cfg = next((t for t in cfg.matching_tiers if t.name == tier), None)
        if not tier_cfg:
            available = [t.name for t in cfg.matching_tiers]
            click.echo(f"Tier '{tier}' not found. Available: {available}", err=True)
            sys.exit(1)

        sql_gen = SQLGenerator()
        al_engine = ActiveLearningEngine(cfg, sql_gen)
        click.echo("-- ACTIVE LEARNING REVIEW QUEUE SQL --")
        click.echo(al_engine.generate_review_queue_sql(tier_cfg))

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
    from bq_entity_resolution.sql.generator import SQLGenerator

    try:
        cfg = load_config(config, defaults)
        tier_cfg = next((t for t in cfg.matching_tiers if t.name == tier), None)
        if not tier_cfg:
            available = [t.name for t in cfg.matching_tiers]
            click.echo(f"Tier '{tier}' not found. Available: {available}", err=True)
            sys.exit(1)

        sql_gen = SQLGenerator()
        al_engine = ActiveLearningEngine(cfg, sql_gen)

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
            estimator = ParameterEstimator(cfg, sql_gen)
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


if __name__ == "__main__":
    cli()
