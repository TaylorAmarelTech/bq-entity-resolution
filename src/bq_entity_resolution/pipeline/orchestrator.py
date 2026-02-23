"""
Pipeline orchestrator: the central controller for the entity resolution pipeline.

Coordinates all stages in order:
1. Initialize context and watermarks
2. Stage incremental data (bronze)
3. Engineer features (silver)
4. Compute embeddings + LSH (if enabled)
5. Create UDFs
6. Estimate Fellegi-Sunter parameters (if configured)
7. Execute matching tiers in order (silver)
8. Reconcile matches across tiers (gold)
9. Generate active learning review queues (if configured)
10. Update watermarks
11. Collect metrics
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from bq_entity_resolution.blocking.engine import BlockingEngine
from bq_entity_resolution.clients.bigquery import BigQueryClient
from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.embeddings.manager import EmbeddingManager
from bq_entity_resolution.exceptions import PipelineAbortError
from bq_entity_resolution.features.engine import FeatureEngine
from bq_entity_resolution.matching.active_learning import ActiveLearningEngine
from bq_entity_resolution.matching.engine import MatchingEngine
from bq_entity_resolution.matching.parameters import ParameterEstimator
from bq_entity_resolution.monitoring.metrics import MetricsCollector
from bq_entity_resolution.naming import (
    checkpoint_table,
    parameters_table,
    sql_audit_table,
    staged_table,
)
from bq_entity_resolution.pipeline.context import PipelineContext
from bq_entity_resolution.pipeline.runner import SQLRunner
from bq_entity_resolution.reconciliation.engine import ReconciliationEngine
from bq_entity_resolution.sql.generator import SQLGenerator
from bq_entity_resolution.watermark.checkpoint import CheckpointManager
from bq_entity_resolution.watermark.manager import WatermarkManager
from bq_entity_resolution.watermark.store import build_watermark_table_name

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pipeline stage constants (used for checkpoint/resume)
# ---------------------------------------------------------------------------
STAGE_WATERMARKS = "init_watermarks"
STAGE_STAGING = "stage_sources"
STAGE_FEATURES = "feature_engineering"
STAGE_TERM_FREQ = "term_frequencies"
STAGE_EMBEDDINGS = "embeddings"
STAGE_UDFS = "create_udfs"
STAGE_PARAMS = "estimate_parameters"
STAGE_MATCHES_INIT = "init_matches_table"
STAGE_TIERS = "execute_tiers"
STAGE_RECONCILE = "reconcile"
STAGE_REVIEW = "review_queues"
STAGE_WATERMARK_ADV = "advance_watermarks"


class PipelineOrchestrator:
    """Main pipeline controller."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.bq_client = BigQueryClient(
            project=config.project.bq_project,
            location=config.project.bq_location,
            max_bytes_billed=config.scale.max_bytes_billed,
        )
        self.sql_gen = SQLGenerator()
        self.runner = SQLRunner(self.bq_client)
        self.watermark_mgr = WatermarkManager(
            bq_client=self.bq_client,
            watermark_table=build_watermark_table_name(
                config.project.bq_project,
                config.project.watermark_dataset,
            ),
            sql_gen=self.sql_gen,
        )
        self.feature_engine = FeatureEngine(config, self.sql_gen)
        self.blocking_engine = BlockingEngine(config, self.sql_gen)
        self.matching_engine = MatchingEngine(config, self.sql_gen)
        self.reconciliation_engine = ReconciliationEngine(config, self.sql_gen)
        self.embedding_mgr = EmbeddingManager(config, self.sql_gen)
        self.param_estimator = ParameterEstimator(config, self.sql_gen)
        self.active_learning = ActiveLearningEngine(config, self.sql_gen)
        self.metrics = MetricsCollector(config)
        self.checkpoint_mgr: CheckpointManager | None = None
        if config.scale.checkpoint_enabled:
            self.checkpoint_mgr = CheckpointManager(
                bq_client=self.bq_client,
                checkpoint_table=checkpoint_table(config),
                sql_gen=self.sql_gen,
            )

    def run(self, full_refresh: bool = False) -> PipelineContext:
        """Execute the complete pipeline."""
        run_id = self._generate_run_id()

        # Checkpoint resume: detect incomplete prior run
        if self.checkpoint_mgr and not full_refresh:
            self.checkpoint_mgr.ensure_table_exists()
            resumable = self.checkpoint_mgr.find_resumable_run()
            if resumable:
                run_id = resumable
                logger.info("Resuming incomplete run: %s", run_id)

        ctx = PipelineContext(
            run_id=run_id,
            started_at=datetime.now(UTC),
            full_refresh=full_refresh,
            config=self.config,
        )

        # Load previously completed stages from BQ checkpoint table
        if self.checkpoint_mgr and not full_refresh:
            try:
                ctx.completed_stages = self.checkpoint_mgr.load_completed_stages(
                    ctx.run_id
                )
                if ctx.completed_stages:
                    logger.info(
                        "Loaded %d completed stage(s) from checkpoint",
                        len(ctx.completed_stages),
                    )
            except Exception:
                logger.warning(
                    "Failed to load checkpoints, starting fresh", exc_info=True
                )
        elif self.checkpoint_mgr and full_refresh:
            self.checkpoint_mgr.ensure_table_exists()

        logger.info("Pipeline run started: %s", ctx.run_id)

        try:
            # 1. Initialize watermarks
            if not self._should_skip_stage(ctx, STAGE_WATERMARKS):
                watermarks = self._init_watermarks(ctx)
                ctx.watermarks = watermarks
                self._mark_stage_complete(ctx, STAGE_WATERMARKS)

            # 2. Stage source data
            if not self._should_skip_stage(ctx, STAGE_STAGING):
                self._stage_sources(ctx)
                self._mark_stage_complete(ctx, STAGE_STAGING)

            # 3. Feature engineering
            if not self._should_skip_stage(ctx, STAGE_FEATURES):
                self._engineer_features(ctx)
                self._mark_stage_complete(ctx, STAGE_FEATURES)

            # 3b. Term frequency computation (if any comparison has TF enabled)
            if not self._should_skip_stage(ctx, STAGE_TERM_FREQ):
                self._compute_term_frequencies(ctx)
                self._mark_stage_complete(ctx, STAGE_TERM_FREQ)

            # 4. Embeddings + LSH (if enabled)
            if not self._should_skip_stage(ctx, STAGE_EMBEDDINGS):
                if self.config.embeddings.enabled:
                    self._compute_embeddings(ctx)
                self._mark_stage_complete(ctx, STAGE_EMBEDDINGS)

            # 5. Create UDFs if needed (e.g., jaro_winkler)
            if not self._should_skip_stage(ctx, STAGE_UDFS):
                self._create_udfs(ctx)
                self._mark_stage_complete(ctx, STAGE_UDFS)

            # 6. Estimate Fellegi-Sunter parameters (if any tier needs them)
            if not self._should_skip_stage(ctx, STAGE_PARAMS):
                self._estimate_parameters(ctx)
                self._mark_stage_complete(ctx, STAGE_PARAMS)

            # 7. Create accumulated matches table
            if not self._should_skip_stage(ctx, STAGE_MATCHES_INIT):
                self._init_matches_table(ctx)
                self._mark_stage_complete(ctx, STAGE_MATCHES_INIT)

            # 8. Execute matching tiers
            if not self._should_skip_stage(ctx, STAGE_TIERS):
                self._execute_tiers(ctx)
                self._mark_stage_complete(ctx, STAGE_TIERS)

            # 9. Reconciliation (clustering + gold output)
            if not self._should_skip_stage(ctx, STAGE_RECONCILE):
                self._reconcile(ctx)
                self._mark_stage_complete(ctx, STAGE_RECONCILE)

            # 10. Generate active learning review queues (if configured)
            if not self._should_skip_stage(ctx, STAGE_REVIEW):
                self._generate_review_queues(ctx)
                self._mark_stage_complete(ctx, STAGE_REVIEW)

            # 11. Advance watermarks
            if not self._should_skip_stage(ctx, STAGE_WATERMARK_ADV):
                self._advance_watermarks(ctx)
                self._mark_stage_complete(ctx, STAGE_WATERMARK_ADV)

            ctx.status = "success"
            if self.checkpoint_mgr:
                try:
                    self.checkpoint_mgr.mark_run_complete(ctx.run_id)
                except Exception:
                    logger.warning(
                        "Failed to mark run complete in checkpoint table",
                        exc_info=True,
                    )
            logger.info(
                "Pipeline run completed: %s (%.1fs, %d bytes billed)",
                ctx.run_id,
                ctx.duration_seconds,
                self.runner.total_bytes_billed,
            )

        except Exception as e:
            ctx.status = "failed"
            ctx.error = str(e)
            logger.exception("Pipeline run failed: %s", ctx.run_id)
            raise

        finally:
            ctx.finished_at = datetime.now(UTC)
            self.metrics.record_run(ctx)
            if self.config.monitoring.persist_sql_log:
                self._persist_sql_log(ctx)

        return ctx

    # ------------------------------------------------------------------
    # Stage implementations
    # ------------------------------------------------------------------

    def _init_watermarks(self, ctx: PipelineContext) -> dict:
        """Read current watermarks (or skip for full refresh)."""
        self.watermark_mgr.ensure_table_exists()
        if ctx.full_refresh:
            logger.info("Full refresh — ignoring watermarks")
            return {s.name: None for s in self.config.sources}
        return {
            s.name: self.watermark_mgr.read(s.name)
            for s in self.config.sources
        }

    def _stage_sources(self, ctx: PipelineContext) -> None:
        """Stage incremental data from each source."""
        logger.info("Staging %d source(s)", len(self.config.sources))
        for source in self.config.sources:
            sql = self.feature_engine.generate_staging_sql(
                source,
                ctx.watermarks.get(source.name),
                full_refresh=ctx.full_refresh,
            )
            ctx.log_sql("staging", source.name, sql)
            self.runner.execute(sql, job_label=f"stage_{source.name}")
            ctx.staged_sources.append(source.name)
            logger.info("Staged source: %s", source.name)

    def _engineer_features(self, ctx: PipelineContext) -> None:
        """Generate all features from staged data."""
        logger.info("Engineering features")
        sql = self.feature_engine.generate_feature_sql()
        ctx.log_sql("features", "all", sql)
        self.runner.execute(sql, job_label="feature_engineering")

    def _compute_term_frequencies(self, ctx: PipelineContext) -> None:
        """Compute term frequency statistics if any comparison uses TF adjustments."""
        tf_sql = self.feature_engine.generate_term_frequency_sql()
        if tf_sql is None:
            return
        logger.info("Computing term frequencies")
        ctx.log_sql("term_frequencies", "compute", tf_sql)
        self.runner.execute(tf_sql, job_label="term_frequencies")

    def _compute_embeddings(self, ctx: PipelineContext) -> None:
        """Compute embeddings and LSH buckets."""
        logger.info("Computing embeddings")
        emb_sql = self.embedding_mgr.generate_embedding_sql()
        ctx.log_sql("embeddings", "compute", emb_sql)
        self.runner.execute(emb_sql, job_label="compute_embeddings")

        logger.info("Computing LSH buckets")
        lsh_sql = self.embedding_mgr.generate_lsh_sql()
        ctx.log_sql("embeddings", "lsh", lsh_sql)
        self.runner.execute(lsh_sql, job_label="compute_lsh")

    def _create_udfs(self, ctx: PipelineContext) -> None:
        """Create required UDFs (e.g., Jaro-Winkler) if any tier needs them."""
        udf_sql = self.matching_engine.generate_create_udfs_sql()
        if udf_sql:
            logger.info("Creating UDFs")
            ctx.log_sql("udfs", "create", udf_sql)
            self.runner.execute(udf_sql, job_label="create_udfs")

    def _init_matches_table(self, ctx: PipelineContext) -> None:
        """Create the accumulated matches table and ensure canonical index exists."""
        sql = self.reconciliation_engine.generate_create_matches_table_sql()
        self.runner.execute(sql, job_label="init_matches_table")

        # Ensure canonical_index_table exists for cross_batch blocking.
        # Uses CTAS from featured schema + cluster_id so blocking keys
        # are available for cross-batch JOIN conditions.
        any_cross_batch = any(
            tier.blocking.cross_batch for tier in self.config.enabled_tiers()
        )
        if any_cross_batch:
            canon_sql = (
                self.reconciliation_engine.generate_create_canonical_index_sql()
            )
            self.runner.execute(canon_sql, job_label="ensure_canonical_index")

    def _execute_tiers(self, ctx: PipelineContext) -> None:
        """Execute each enabled matching tier in order."""
        enabled_tiers = self.config.enabled_tiers()
        logger.info("Executing %d matching tier(s)", len(enabled_tiers))

        for i, tier in enumerate(enabled_tiers):
            logger.info("Tier %d/%d: %s", i + 1, len(enabled_tiers), tier.name)

            # Generate blocking candidates
            excluded = ctx.all_matches_table if i > 0 else None
            blocking_sql = self.blocking_engine.generate_candidates_sql(
                tier, tier_index=i, excluded_pairs_table=excluded,
            )
            ctx.log_sql("blocking", tier.name, blocking_sql)
            block_result = self.runner.execute(
                blocking_sql, job_label=f"block_{tier.name}"
            )

            # Generate comparison + scoring
            matching_sql = self.matching_engine.generate_tier_sql(tier, i)
            ctx.log_sql("matching", tier.name, matching_sql)
            match_result = self.runner.execute(
                matching_sql, job_label=f"match_{tier.name}"
            )

            # Accumulate matches
            accum_sql = self.matching_engine.generate_accumulate_matches_sql(
                tier, ctx.all_matches_table
            )
            self.runner.execute(accum_sql, job_label=f"accum_{tier.name}")

            ctx.tier_results[tier.name] = {
                "candidates_generated": block_result.rows_affected,
                "matches_found": match_result.rows_affected,
                "bytes_billed": block_result.bytes_billed + match_result.bytes_billed,
            }
            logger.info(
                "Tier %s: %d matches found",
                tier.name,
                match_result.rows_affected,
            )

            # Blocking metrics (optional)
            if self.config.monitoring.blocking_metrics.enabled:
                metrics_sql = self.blocking_engine.generate_metrics_sql(tier)
                ctx.log_sql("blocking_metrics", tier.name, metrics_sql)
                metrics_rows = self.runner.execute_and_fetch(
                    metrics_sql, job_label=f"blocking_metrics_{tier.name}"
                )
                if metrics_rows:
                    ctx.tier_results[tier.name]["blocking_metrics"] = metrics_rows[0]
                    logger.info(
                        "Tier %s blocking: %d candidates, %d matches, precision=%.4f",
                        tier.name,
                        metrics_rows[0].get("candidate_pairs", 0),
                        metrics_rows[0].get("matched_pairs", 0),
                        metrics_rows[0].get("precision", 0),
                    )

    def _reconcile(self, ctx: PipelineContext) -> None:
        """Assign clusters and generate gold output."""
        any_cross_batch = any(
            tier.blocking.cross_batch for tier in self.config.enabled_tiers()
        )

        # 1. Cluster assignment (incremental when cross-batch enabled)
        logger.info("Running cluster assignment")
        cluster_sql = self.reconciliation_engine.generate_cluster_sql(
            cross_batch=any_cross_batch and not ctx.full_refresh,
        )
        ctx.log_sql("reconciliation", "clustering", cluster_sql)
        self.runner.execute_script(cluster_sql, job_label="cluster_assignment")

        # 2. Populate canonical index (cross-batch only)
        if any_cross_batch:
            logger.info("Populating canonical index")
            populate_sql = (
                self.reconciliation_engine.generate_populate_canonical_index_sql()
            )
            ctx.log_sql("reconciliation", "populate_canonical", populate_sql)
            self.runner.execute_script(
                populate_sql, job_label="populate_canonical_index"
            )

        # 3. Gold output (reads from canonical_index when cross-batch)
        logger.info("Generating gold output")
        gold_sql = self.reconciliation_engine.generate_gold_output_sql(
            use_canonical=any_cross_batch,
        )
        ctx.log_sql("reconciliation", "gold_output", gold_sql)
        self.runner.execute(gold_sql, job_label="gold_output")

        # Cluster quality metrics (optional)
        if self.config.monitoring.cluster_quality.enabled:
            logger.info("Computing cluster quality metrics")
            quality_sql = self.reconciliation_engine.generate_quality_metrics_sql()
            ctx.log_sql("reconciliation", "cluster_quality", quality_sql)
            quality_rows = self.runner.execute_and_fetch(
                quality_sql, job_label="cluster_quality"
            )
            if quality_rows:
                ctx.cluster_quality = quality_rows[0]
                max_size = quality_rows[0].get("max_cluster_size", 0)
                alert_max = self.config.monitoring.cluster_quality.alert_max_cluster_size
                if max_size > alert_max:
                    logger.warning(
                        "ALERT: Max cluster size %d exceeds threshold %d",
                        max_size,
                        alert_max,
                    )
                    if self.config.monitoring.cluster_quality.abort_on_explosion:
                        raise PipelineAbortError(
                            f"Cluster explosion: max cluster size {max_size} "
                            f"exceeds threshold {alert_max}"
                        )

    def _estimate_parameters(self, ctx: PipelineContext) -> None:
        """Estimate m/u parameters for Fellegi-Sunter tiers that need them."""
        for tier in self.config.enabled_tiers():
            if not self.param_estimator.needs_estimation(tier):
                # Use manual params if F-S tier with manual m/u
                if tier.threshold.method == "fellegi_sunter":
                    params = self.param_estimator.extract_manual_params(tier)
                    self.matching_engine.set_tier_parameters(tier.name, params)
                continue

            training = self.param_estimator.resolve_training_config(tier)
            logger.info(
                "Estimating parameters for tier '%s' via %s",
                tier.name,
                training.method,
            )

            if training.method == "em":
                # EM needs blocking candidates; generate them first
                blocking_sql = self.blocking_engine.generate_candidates_sql(
                    tier, tier_index=0, excluded_pairs_table=None,
                )
                ctx.log_sql("blocking_for_em", tier.name, blocking_sql)
                self.runner.execute(
                    blocking_sql, job_label=f"block_for_em_{tier.name}"
                )
                est_sql = self.param_estimator.generate_em_estimation_sql(
                    tier, training
                )
                ctx.log_sql("parameter_estimation", tier.name, est_sql)
                result = self.runner.execute_script_and_fetch(
                    est_sql, job_label=f"em_estimate_{tier.name}"
                )
            else:
                # Labeled data estimation
                est_sql = self.param_estimator.generate_label_estimation_sql(
                    tier, training
                )
                ctx.log_sql("parameter_estimation", tier.name, est_sql)
                result = self.runner.execute_and_fetch(
                    est_sql, job_label=f"label_estimate_{tier.name}"
                )

            params = self.param_estimator.parse_estimation_results(tier, result)
            self.matching_engine.set_tier_parameters(tier.name, params)

            # Optionally persist parameters
            target = training.parameters_table or parameters_table(
                self.config, tier.name
            )
            persist_sql = self.param_estimator.generate_persist_params_sql(
                params, target
            )
            if persist_sql:
                self.runner.execute(
                    persist_sql, job_label=f"persist_params_{tier.name}"
                )

            logger.info(
                "Parameters estimated for tier '%s': %d comparisons, prior=%.4f",
                tier.name,
                len(params.comparisons),
                params.prior_match_prob,
            )

    def _generate_review_queues(self, ctx: PipelineContext) -> None:
        """Generate active learning review queues for configured tiers."""
        for tier in self.config.enabled_tiers():
            if not tier.active_learning.enabled:
                continue
            logger.info("Generating review queue for tier '%s'", tier.name)
            sql = self.active_learning.generate_review_queue_sql(tier)
            ctx.log_sql("active_learning", tier.name, sql)
            self.runner.execute(sql, job_label=f"review_queue_{tier.name}")

    def _advance_watermarks(self, ctx: PipelineContext) -> None:
        """Advance watermarks to new high-water marks."""
        for source in self.config.sources:
            stg_table = staged_table(self.config, source.name)
            new_wm = self.watermark_mgr.compute_new_watermark(
                stg_table,
                self.config.incremental.cursor_columns,
            )
            if new_wm:
                self.watermark_mgr.write(source.name, new_wm, run_id=ctx.run_id)
                logger.info("Watermark advanced for %s: %s", source.name, new_wm)

    def _should_skip_stage(self, ctx: PipelineContext, stage: str) -> bool:
        """Check if a stage should be skipped (already completed in checkpoint)."""
        if not self.config.scale.checkpoint_enabled:
            return False
        if stage in ctx.completed_stages:
            logger.info("Skipping stage '%s' (checkpoint resume)", stage)
            return True
        return False

    def _mark_stage_complete(self, ctx: PipelineContext, stage: str) -> None:
        """Mark a stage as completed for checkpoint resume."""
        ctx.completed_stages.add(stage)
        if self.checkpoint_mgr:
            try:
                self.checkpoint_mgr.mark_stage_complete(ctx.run_id, stage)
            except Exception:
                logger.warning(
                    "Failed to persist checkpoint for stage '%s'",
                    stage,
                    exc_info=True,
                )

    def _persist_sql_log(self, ctx: PipelineContext) -> None:
        """Persist the SQL audit trail to BigQuery."""
        if not ctx.sql_log:
            return
        try:
            audit_tbl = sql_audit_table(self.config)
            sql = self.sql_gen.render(
                "monitoring/persist_sql_log.sql.j2",
                audit_table=audit_tbl,
                run_id=ctx.run_id,
                entries=ctx.sql_log,
            )
            self.runner.execute(sql, job_label="persist_sql_log")
            logger.info("SQL audit trail persisted: %d entries", len(ctx.sql_log))
        except Exception:
            logger.warning("Failed to persist SQL audit trail", exc_info=True)

    @staticmethod
    def _generate_run_id() -> str:
        """Generate a unique run identifier."""
        ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        return f"er_run_{ts}"
