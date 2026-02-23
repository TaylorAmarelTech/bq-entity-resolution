"""Tests for the features SQL builder."""

from bq_entity_resolution.sql.builders.features import (
    FeatureParams,
    FeatureExpr,
    CustomJoin,
    TFColumn,
    build_features_sql,
    build_term_frequencies_sql,
)


def test_basic_features():
    """Feature engineering generates multi-pass CTE."""
    params = FeatureParams(
        target_table="proj.ds.featured",
        source_tables=["proj.ds.staged_crm"],
        source_columns=["first_name", "last_name"],
        feature_expressions=[
            FeatureExpr("name_clean", "TRIM(UPPER(first_name))"),
            FeatureExpr("name_soundex", "SOUNDEX(last_name)"),
        ],
        blocking_keys=[
            FeatureExpr("bk_soundex", "SOUNDEX(last_name)"),
        ],
    )
    expr = build_features_sql(params)
    sql = expr.render()

    assert "CREATE OR REPLACE TABLE" in sql
    assert "WITH base AS" in sql
    assert "features_pass1 AS" in sql
    assert "featured AS" in sql
    assert "TRIM(UPPER(first_name)) AS name_clean" in sql
    assert "SOUNDEX(last_name) AS name_soundex" in sql
    assert "SOUNDEX(last_name) AS bk_soundex" in sql


def test_features_multiple_sources():
    """Multiple source tables are UNIONed."""
    params = FeatureParams(
        target_table="proj.ds.featured",
        source_tables=["proj.ds.staged_a", "proj.ds.staged_b"],
        source_columns=["name"],
        feature_expressions=[FeatureExpr("upper_name", "UPPER(name)")],
    )
    expr = build_features_sql(params)
    sql = expr.render()

    assert "UNION ALL" in sql
    assert "staged_a" in sql
    assert "staged_b" in sql


def test_features_dependent_features():
    """Dependent features appear in pass 2."""
    params = FeatureParams(
        target_table="proj.ds.featured",
        source_tables=["proj.ds.staged"],
        source_columns=["first_name"],
        feature_expressions=[
            FeatureExpr("name_clean", "TRIM(UPPER(first_name))"),
        ],
        dependent_features=[
            FeatureExpr("name_soundex", "SOUNDEX(name_clean)"),
        ],
    )
    expr = build_features_sql(params)
    sql = expr.render()

    assert "features_pass1 AS" in sql
    assert "featured AS" in sql
    assert "SOUNDEX(name_clean) AS name_soundex" in sql


def test_features_composite_keys():
    """Composite keys are generated in pass 3."""
    params = FeatureParams(
        target_table="proj.ds.featured",
        source_tables=["proj.ds.staged"],
        source_columns=["first_name", "last_name", "dob"],
        feature_expressions=[],
        composite_keys=[
            FeatureExpr(
                "bk_name_dob",
                "CONCAT(UPPER(first_name), '_', UPPER(last_name), '_', CAST(dob AS STRING))",
            ),
        ],
    )
    expr = build_features_sql(params)
    sql = expr.render()

    assert "bk_name_dob" in sql
    assert "CONCAT(" in sql


def test_features_custom_joins():
    """Custom joins are included in pass 1."""
    params = FeatureParams(
        target_table="proj.ds.featured",
        source_tables=["proj.ds.staged"],
        source_columns=["name"],
        feature_expressions=[FeatureExpr("upper_name", "UPPER(name)")],
        custom_joins=[
            CustomJoin(
                table="proj.ref.lookup",
                alias="lkp",
                on="b.type_id = lkp.id",
            ),
        ],
    )
    expr = build_features_sql(params)
    sql = expr.render()

    assert "LEFT JOIN" in sql
    assert "proj.ref.lookup" in sql
    assert "lkp" in sql


def test_term_frequencies_single_column():
    """Term frequency for a single column."""
    expr = build_term_frequencies_sql(
        target_table="proj.ds.tf_stats",
        source_table="proj.ds.featured",
        tf_columns=[TFColumn("last_name")],
    )
    sql = expr.render()

    assert "CREATE OR REPLACE TABLE" in sql
    assert "'last_name' AS term_frequency_column" in sql
    assert "term_frequency_ratio" in sql
    assert "COUNT(*)" in sql


def test_term_frequencies_multiple_columns():
    """Term frequency for multiple columns uses UNION ALL."""
    expr = build_term_frequencies_sql(
        target_table="proj.ds.tf_stats",
        source_table="proj.ds.featured",
        tf_columns=[TFColumn("last_name"), TFColumn("city")],
    )
    sql = expr.render()

    assert "UNION ALL" in sql
    assert "'last_name' AS term_frequency_column" in sql
    assert "'city' AS term_frequency_column" in sql


def test_features_returns_sql_expression():
    """Builder returns SQLExpression."""
    params = FeatureParams(
        target_table="t",
        source_tables=["s"],
        source_columns=[],
        feature_expressions=[FeatureExpr("x", "1")],
    )
    expr = build_features_sql(params)
    assert expr.is_raw is True
    assert isinstance(expr.render(), str)
