"""Tests for the EM estimation SQL builder."""

from bq_entity_resolution.sql.builders.em import (
    EMParams,
    EMComparison,
    EMLevel,
    build_em_estimation_sql,
    build_em_mstep_sql,
)


def test_em_estimation_basic():
    """EM estimation generates full BQ script."""
    params = EMParams(
        candidates_table="proj.ds.candidates",
        source_table="proj.ds.featured",
        comparisons=[
            EMComparison(
                name="name",
                left="name_clean",
                right="name_clean",
                levels=[
                    EMLevel(
                        label="exact",
                        sql_expr="l.name_clean = r.name_clean",
                    ),
                ],
            ),
        ],
        max_iterations=25,
        convergence_threshold=0.001,
        sample_size=10000,
    )
    expr = build_em_estimation_sql(params)
    sql = expr.render()

    assert "DECLARE iteration INT64" in sql
    assert "DECLARE match_prior FLOAT64" in sql
    assert "CREATE TEMP TABLE _em_pairs" in sql
    assert "CREATE TEMP TABLE _em_params" in sql
    assert "LOOP" in sql
    assert "_em_scored" in sql
    assert "LN(match_prior)" in sql
    assert "LN(1.0 - match_prior)" in sql
    assert "SAFE_DIVIDE" in sql
    assert "m_prob" in sql
    assert "u_prob" in sql
    assert "LEAVE" in sql
    assert "END LOOP" in sql
    assert "m_probability" in sql
    assert "u_probability" in sql


def test_em_estimation_sample_size():
    """Sample size limits candidate pairs."""
    params = EMParams(
        candidates_table="c",
        source_table="s",
        comparisons=[
            EMComparison(
                name="x",
                left="x",
                right="x",
                levels=[EMLevel(label="m", sql_expr="l.x = r.x")],
            ),
        ],
        sample_size=5000,
    )
    expr = build_em_estimation_sql(params)
    sql = expr.render()

    assert "LIMIT 5000" in sql


def test_em_estimation_convergence():
    """Convergence threshold appears in IF condition."""
    params = EMParams(
        candidates_table="c",
        source_table="s",
        comparisons=[
            EMComparison(
                name="x",
                left="x",
                right="x",
                levels=[EMLevel(label="m", sql_expr="l.x = r.x")],
            ),
        ],
        convergence_threshold=0.0001,
    )
    expr = build_em_estimation_sql(params)
    sql = expr.render()

    assert "0.0001" in sql


def test_em_estimation_multiple_comparisons():
    """Multiple comparisons generate multiple level columns."""
    params = EMParams(
        candidates_table="c",
        source_table="s",
        comparisons=[
            EMComparison(
                name="name",
                left="name",
                right="name",
                levels=[EMLevel(label="exact", sql_expr="l.name = r.name")],
            ),
            EMComparison(
                name="dob",
                left="dob",
                right="dob",
                levels=[EMLevel(label="match", sql_expr="l.dob = r.dob")],
            ),
        ],
    )
    expr = build_em_estimation_sql(params)
    sql = expr.render()

    assert "name__exact" in sql
    assert "dob__match" in sql


def test_em_estimation_init_values():
    """Initial m/u values are 0.9/0.1."""
    params = EMParams(
        candidates_table="c",
        source_table="s",
        comparisons=[
            EMComparison(
                name="x",
                left="x",
                right="x",
                levels=[EMLevel(label="m", sql_expr="l.x = r.x")],
            ),
        ],
    )
    expr = build_em_estimation_sql(params)
    sql = expr.render()

    assert "0.9, 0.1" in sql


def test_em_estimation_zero_guard():
    """M-step uses COALESCE to guard against zero-count division."""
    params = EMParams(
        candidates_table="c",
        source_table="s",
        comparisons=[
            EMComparison(
                name="x",
                left="x",
                right="x",
                levels=[EMLevel(label="m", sql_expr="l.x = r.x")],
            ),
        ],
    )
    expr = build_em_estimation_sql(params)
    sql = expr.render()

    assert "COALESCE(SAFE_DIVIDE" in sql
    assert "NULLIF(SUM(match_weight), 0)" in sql
    assert "GREATEST(0.001" in sql
    assert "LEAST(0.999" in sql


def test_em_mstep_standalone():
    """Standalone M-step SQL for local execution."""
    expr = build_em_mstep_sql([("name", "exact"), ("dob", "match")])
    sql = expr.render()

    assert "name__exact" in sql
    assert "dob__match" in sql
    assert "m_prob" in sql
    assert "u_prob" in sql
    assert "UNION ALL" in sql
