"""CLI command: run — Execute the entity resolution pipeline."""

from __future__ import annotations

import logging
import sys

import click

logger = logging.getLogger(__name__)


@click.command()
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
            from bq_entity_resolution.backends.bigquery import BigQueryBackend
            from bq_entity_resolution.clients.bigquery import BigQueryClient

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
