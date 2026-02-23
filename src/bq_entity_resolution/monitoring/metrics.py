"""
Pipeline metrics collection and reporting.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bq_entity_resolution.config.schema import PipelineConfig
    from bq_entity_resolution.pipeline.context import PipelineContext

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects and reports pipeline execution metrics."""

    def __init__(self, config: PipelineConfig):
        self.config = config

    def record_run(self, ctx: PipelineContext) -> None:
        """Record metrics from a pipeline run."""
        metrics = {
            "run_id": ctx.run_id,
            "status": ctx.status,
            "duration_seconds": ctx.duration_seconds,
            "sources_staged": len(ctx.staged_sources),
            "tiers_executed": len(ctx.tier_results),
            "full_refresh": ctx.full_refresh,
        }

        # Per-tier metrics
        total_matches = 0
        for tier_name, result in ctx.tier_results.items():
            matches = result.get("matches_found", 0)
            total_matches += matches
            metrics[f"tier_{tier_name}_matches"] = matches
            metrics[f"tier_{tier_name}_candidates"] = result.get(
                "candidates_generated", 0
            )

        metrics["total_matches"] = total_matches

        if ctx.error:
            metrics["error"] = ctx.error

        logger.info("Pipeline metrics: %s", metrics)

        # Write to BigQuery if enabled
        if (
            self.config.monitoring.metrics.enabled
            and self.config.monitoring.metrics.destination == "bigquery"
        ):
            self._write_to_bigquery(metrics)

    def _write_to_bigquery(self, metrics: dict) -> None:
        """Write metrics to BigQuery metrics table."""
        # Implementation deferred — requires BigQuery client
        # which would create circular dependency if imported here.
        # The orchestrator can call this with its client.
        logger.debug("Metrics would be written to BigQuery: %s", metrics)
