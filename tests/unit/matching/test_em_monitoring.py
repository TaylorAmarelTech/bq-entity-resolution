"""Tests for EM log-likelihood monitoring in the estimation template."""

from bq_entity_resolution.sql.generator import SQLGenerator


def _render_em_template() -> str:
    """Render the EM estimation template with minimal parameters."""
    gen = SQLGenerator()
    return gen.render(
        "matching/em_estimation.sql.j2",
        candidates_table="proj.silver.candidates",
        source_table="proj.silver.featured",
        comparisons=[
            {
                "name": "name",
                "left": "first_name",
                "right": "first_name",
                "levels": [
                    {"label": "exact", "sql_expr": "l.first_name = r.first_name", "has_expr": True},
                    {"label": "else", "sql_expr": None, "has_expr": False},
                ],
            }
        ],
        max_iterations=10,
        convergence_threshold=0.001,
        sample_size=10000,
        initial_match_proportion=0.1,
    )


def test_em_template_declares_log_likelihood():
    """EM template declares log_likelihood variable."""
    sql = _render_em_template()
    assert "DECLARE log_likelihood FLOAT64" in sql


def test_em_template_tracks_prev_log_likelihood():
    """EM template declares prev_log_likelihood for tracking changes."""
    sql = _render_em_template()
    assert "DECLARE prev_log_likelihood FLOAT64" in sql


def test_em_template_computes_log_likelihood():
    """EM template computes log-likelihood using LOG()."""
    sql = _render_em_template()
    assert "SET log_likelihood" in sql
    assert "LOG(" in sql


def test_em_template_outputs_log_likelihood():
    """Final SELECT includes log_likelihood for callers to inspect."""
    sql = _render_em_template()
    assert "final_log_likelihood" in sql


def test_em_convergence_uses_log_likelihood():
    """Convergence check references log_likelihood."""
    sql = _render_em_template()
    assert "ABS(log_likelihood - prev_log_likelihood)" in sql


def test_em_mstep_zero_count_protection():
    """M-step COALESCE prevents NULL when SAFE_DIVIDE gets zero denominator."""
    sql = _render_em_template()
    # COALESCE wraps SAFE_DIVIDE to fall back to 0.5 (uninformative prior)
    assert "COALESCE(SAFE_DIVIDE(" in sql
    assert ", 0.5)" in sql
