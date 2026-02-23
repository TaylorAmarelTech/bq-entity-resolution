"""Tests for the gold output SQL builder."""

from bq_entity_resolution.sql.builders.gold_output import (
    GoldOutputParams,
    build_gold_output_sql,
)


def test_gold_output_completeness():
    """Completeness canonical method counts non-null columns."""
    params = GoldOutputParams(
        target_table="proj.ds.resolved",
        source_table="proj.ds.featured",
        cluster_table="proj.ds.clusters",
        matches_table="proj.ds.all_matches",
        canonical_method="completeness",
        scoring_columns=["first_name", "last_name", "email"],
        source_columns=["first_name", "last_name", "email"],
    )
    expr = build_gold_output_sql(params)
    sql = expr.render()

    assert "CREATE OR REPLACE TABLE" in sql
    assert "clustered" in sql
    assert "canonical_scores" in sql
    assert "canonicals" in sql
    assert "resolved" in sql
    assert "IS NOT NULL THEN 1" in sql
    assert "canonical_score" in sql
    assert "is_canonical" in sql
    assert "resolved_entity_id" in sql


def test_gold_output_recency():
    """Recency canonical method uses timestamp."""
    params = GoldOutputParams(
        target_table="proj.ds.resolved",
        source_table="proj.ds.featured",
        cluster_table="proj.ds.clusters",
        matches_table="proj.ds.all_matches",
        canonical_method="recency",
        source_columns=["name"],
    )
    expr = build_gold_output_sql(params)
    sql = expr.render()

    assert "UNIX_MICROS(_source_updated_at)" in sql


def test_gold_output_source_priority():
    """Source priority canonical method uses CASE on source_name."""
    params = GoldOutputParams(
        target_table="proj.ds.resolved",
        source_table="proj.ds.featured",
        cluster_table="proj.ds.clusters",
        matches_table="proj.ds.all_matches",
        canonical_method="source_priority",
        source_columns=["name"],
        source_priority=["gold_source", "silver_source"],
    )
    expr = build_gold_output_sql(params)
    sql = expr.render()

    assert "CASE source_name" in sql
    assert "gold_source" in sql
    assert "silver_source" in sql


def test_gold_output_match_metadata():
    """Match metadata includes tier, score, confidence."""
    params = GoldOutputParams(
        target_table="proj.ds.resolved",
        source_table="proj.ds.featured",
        cluster_table="proj.ds.clusters",
        matches_table="proj.ds.all_matches",
        include_match_metadata=True,
        source_columns=["name"],
    )
    expr = build_gold_output_sql(params)
    sql = expr.render()

    assert "matched_by_tier" in sql
    assert "match_score" in sql
    assert "match_confidence" in sql
    assert "ROW_NUMBER()" in sql


def test_gold_output_no_match_metadata():
    """Without match metadata, no LEFT JOIN."""
    params = GoldOutputParams(
        target_table="proj.ds.resolved",
        source_table="proj.ds.featured",
        cluster_table="proj.ds.clusters",
        matches_table="proj.ds.all_matches",
        include_match_metadata=False,
        source_columns=["name"],
    )
    expr = build_gold_output_sql(params)
    sql = expr.render()

    assert "matched_by_tier" not in sql
    assert "LEFT JOIN" not in sql


def test_gold_output_partition_and_cluster():
    """Partitioning and clustering options."""
    params = GoldOutputParams(
        target_table="proj.ds.resolved",
        source_table="proj.ds.featured",
        cluster_table="proj.ds.clusters",
        matches_table="proj.ds.all_matches",
        source_columns=["name"],
        partition_column="_pipeline_loaded_at",
        cluster_columns=["source_name", "cluster_id"],
    )
    expr = build_gold_output_sql(params)
    sql = expr.render()

    assert "PARTITION BY _pipeline_loaded_at" in sql
    assert "CLUSTER BY source_name, cluster_id" in sql


def test_gold_output_entity_id_prefix():
    """Custom entity ID prefix."""
    params = GoldOutputParams(
        target_table="proj.ds.resolved",
        source_table="proj.ds.featured",
        cluster_table="proj.ds.clusters",
        matches_table="proj.ds.all_matches",
        entity_id_prefix="cust",
        source_columns=["name"],
    )
    expr = build_gold_output_sql(params)
    sql = expr.render()

    assert "'cust_'" in sql


def test_gold_output_passthrough_columns():
    """Passthrough columns are included in output."""
    params = GoldOutputParams(
        target_table="proj.ds.resolved",
        source_table="proj.ds.featured",
        cluster_table="proj.ds.clusters",
        matches_table="proj.ds.all_matches",
        source_columns=["name"],
        passthrough_columns=["raw_id", "external_ref"],
    )
    expr = build_gold_output_sql(params)
    sql = expr.render()

    assert "f.raw_id" in sql
    assert "f.external_ref" in sql
