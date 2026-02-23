"""Tests for SQL audit trail persistence."""

from bq_entity_resolution.config.schema import MonitoringConfig, PipelineConfig, ProjectConfig, SourceConfig, ColumnMapping, MatchingTierConfig, TierBlockingConfig, BlockingPathDef, ComparisonDef, ThresholdConfig, FeatureEngineeringConfig, BlockingKeyDef
from bq_entity_resolution.naming import sql_audit_table
from bq_entity_resolution.pipeline.context import PipelineContext
from bq_entity_resolution.sql.generator import SQLGenerator


def _minimal_config(**overrides):
    return PipelineConfig(
        project=ProjectConfig(name="test", bq_project="proj", watermark_dataset="er_meta"),
        sources=[
            SourceConfig(
                name="src", table="proj.ds.tbl", unique_key="id",
                updated_at="updated_at",
                columns=[ColumnMapping(name="id"), ColumnMapping(name="name")],
            ),
        ],
        feature_engineering=FeatureEngineeringConfig(
            blocking_keys=[BlockingKeyDef(name="bk1", function="farm_fingerprint", inputs=["name"])],
        ),
        matching_tiers=[
            MatchingTierConfig(
                name="t1",
                blocking=TierBlockingConfig(paths=[BlockingPathDef(keys=["bk1"])]),
                comparisons=[ComparisonDef(left="name", right="name", method="exact")],
                threshold=ThresholdConfig(min_score=1.0),
            ),
        ],
        **overrides,
    )


def test_persist_sql_log_default_false():
    """persist_sql_log defaults to False."""
    config = MonitoringConfig()
    assert config.persist_sql_log is False


def test_persist_sql_log_schema_accepts_true():
    """MonitoringConfig accepts persist_sql_log=True."""
    config = MonitoringConfig(persist_sql_log=True)
    assert config.persist_sql_log is True


def test_sql_audit_table_naming():
    """sql_audit_table returns project.watermark_dataset.pipeline_sql_audit."""
    config = _minimal_config()
    result = sql_audit_table(config)
    assert result == "proj.er_meta.pipeline_sql_audit"


def test_sql_audit_template_creates_table():
    """Template SQL contains CREATE TABLE IF NOT EXISTS."""
    gen = SQLGenerator()
    sql = gen.render(
        "monitoring/persist_sql_log.sql.j2",
        audit_table="proj.meta.audit",
        run_id="run_123",
        entries=[{"stage": "test", "step": "s1", "sql": "SELECT 1", "timestamp": "2024-01-01"}],
    )
    assert "CREATE TABLE IF NOT EXISTS" in sql


def test_sql_audit_template_inserts_rows():
    """Template SQL contains INSERT INTO with entries."""
    gen = SQLGenerator()
    sql = gen.render(
        "monitoring/persist_sql_log.sql.j2",
        audit_table="proj.meta.audit",
        run_id="run_123",
        entries=[{"stage": "test", "step": "s1", "sql": "SELECT 1", "timestamp": "2024-01-01"}],
    )
    assert "INSERT INTO" in sql
    assert "run_123" in sql
    assert "SELECT 1" in sql


def test_sql_log_entry_structure():
    """PipelineContext.log_sql creates entries with stage, step, sql, timestamp."""
    from datetime import datetime, timezone
    config = _minimal_config()
    ctx = PipelineContext(
        run_id="test_run",
        started_at=datetime.now(timezone.utc),
        config=config,
    )
    ctx.log_sql("blocking", "tier1", "SELECT * FROM candidates")
    assert len(ctx.sql_log) == 1
    entry = ctx.sql_log[0]
    assert entry["stage"] == "blocking"
    assert entry["step"] == "tier1"
    assert entry["sql"] == "SELECT * FROM candidates"
    assert "timestamp" in entry
