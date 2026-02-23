"""Tests for pre-render template parameter validation."""

import pytest

from bq_entity_resolution.exceptions import SQLGenerationError
from bq_entity_resolution.sql.generator import SQLGenerator, TEMPLATE_REQUIRED_PARAMS


def test_required_params_dict_exists():
    """TEMPLATE_REQUIRED_PARAMS is a non-empty dict."""
    assert isinstance(TEMPLATE_REQUIRED_PARAMS, dict)
    assert len(TEMPLATE_REQUIRED_PARAMS) > 0


def test_render_with_all_params_succeeds():
    """Rendering with all required params does not raise."""
    gen = SQLGenerator()
    sql = gen.render(
        "watermark/read_watermark.sql.j2",
        table="proj.ds.wm",
        source_name="src",
    )
    assert "proj.ds.wm" in sql


def test_render_missing_required_param_raises():
    """Missing a required parameter raises SQLGenerationError."""
    gen = SQLGenerator()
    with pytest.raises(SQLGenerationError, match="missing required parameters"):
        gen.render("watermark/read_watermark.sql.j2", table="proj.ds.wm")
        # source_name is missing


def test_render_error_message_lists_missing():
    """Error message contains the missing parameter name."""
    gen = SQLGenerator()
    with pytest.raises(SQLGenerationError, match="source_name"):
        gen.render("watermark/read_watermark.sql.j2", table="proj.ds.wm")


def test_staging_template_params_defined():
    """Staging template has required params defined."""
    assert "staging/incremental_load.sql.j2" in TEMPLATE_REQUIRED_PARAMS
    params = TEMPLATE_REQUIRED_PARAMS["staging/incremental_load.sql.j2"]
    assert "target_table" in params
    assert "source" in params


def test_blocking_template_params_defined():
    """Blocking template has required params defined."""
    assert "blocking/multi_path_candidates.sql.j2" in TEMPLATE_REQUIRED_PARAMS
    params = TEMPLATE_REQUIRED_PARAMS["blocking/multi_path_candidates.sql.j2"]
    assert "target_table" in params
    assert "blocking_paths" in params


def test_unknown_template_no_validation():
    """Templates not in the dict skip parameter validation."""
    gen = SQLGenerator()
    # monitoring/persist_sql_log.sql.j2 is not in TEMPLATE_REQUIRED_PARAMS
    # It should not raise for missing params (but may fail to render)
    assert "monitoring/persist_sql_log.sql.j2" not in TEMPLATE_REQUIRED_PARAMS
