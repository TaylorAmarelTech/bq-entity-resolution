"""Tests for SQL generator."""

from datetime import datetime, timezone

from bq_entity_resolution.sql.generator import (
    SQLGenerator,
    bq_escape,
    farm_fingerprint_expr,
    format_watermark_value,
)


def test_bq_escape_reserved_word():
    assert bq_escape("SELECT") == "`SELECT`"
    assert bq_escape("FROM") == "`FROM`"


def test_bq_escape_normal():
    assert bq_escape("my_column") == "my_column"


def test_bq_escape_with_dot():
    assert bq_escape("project.dataset") == "`project.dataset`"


def test_farm_fingerprint_single():
    result = farm_fingerprint_expr(["col"])
    assert result == "FARM_FINGERPRINT(CAST(col AS STRING))"


def test_farm_fingerprint_multiple():
    result = farm_fingerprint_expr(["a", "b"])
    assert "FARM_FINGERPRINT" in result
    assert "CONCAT" in result
    assert "||" in result


def test_farm_fingerprint_string_input():
    result = farm_fingerprint_expr("col")
    assert "FARM_FINGERPRINT" in result


def test_generator_render_watermark_template():
    gen = SQLGenerator()
    sql = gen.render(
        "watermark/create_watermark_table.sql.j2",
        table="proj.dataset.watermarks",
    )
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "proj.dataset.watermarks" in sql
    assert "source_name" in sql


def test_generator_render_read_watermark():
    gen = SQLGenerator()
    sql = gen.render(
        "watermark/read_watermark.sql.j2",
        table="proj.meta.wm",
        source_name="my_source",
    )
    assert "my_source" in sql
    assert "is_current = TRUE" in sql


# ---------------------------------------------------------------------------
# format_watermark_value filter tests
# ---------------------------------------------------------------------------


def test_format_watermark_value_none():
    assert format_watermark_value(None) == "NULL"


def test_format_watermark_value_integer():
    assert format_watermark_value(42) == "42"
    assert format_watermark_value(0) == "0"


def test_format_watermark_value_float():
    assert format_watermark_value(3.14) == "3.14"


def test_format_watermark_value_timestamp_string():
    result = format_watermark_value("2024-01-15T10:30:00")
    assert result == "TIMESTAMP('2024-01-15T10:30:00')"


def test_format_watermark_value_timestamp_with_space():
    result = format_watermark_value("2024-01-15 10:30:00")
    assert result == "TIMESTAMP('2024-01-15 10:30:00')"


def test_format_watermark_value_datetime_object():
    dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    result = format_watermark_value(dt)
    assert "TIMESTAMP(" in result
    assert "2024-06-15" in result


def test_format_watermark_value_plain_string():
    result = format_watermark_value("hello")
    assert result == "'hello'"


def test_format_watermark_value_escapes_quotes():
    result = format_watermark_value("it's")
    assert "''" in result  # Standard SQL: doubled single quotes


def test_format_watermark_value_used_in_template():
    """Verify the filter works end-to-end in a Jinja2 template."""
    gen = SQLGenerator()
    sql = gen.render(
        "staging/incremental_load.sql.j2",
        target_table="proj.bronze.staged_src",
        source=_make_minimal_source(),
        watermark={"updated_at": "2024-01-01T00:00:00"},
        grace_period_hours=48,
        full_refresh=False,
    )
    assert "TIMESTAMP('2024-01-01T00:00:00')" in sql
    assert "INTERVAL 48 HOUR" in sql


def _make_minimal_source():
    """Create a minimal source-like object for template rendering."""
    from types import SimpleNamespace
    return SimpleNamespace(
        name="test_src",
        table="proj.raw.data",
        unique_key="id",
        updated_at="updated_at",
        columns=[SimpleNamespace(name="col_a")],
        passthrough_columns=[],
        joins=[],
        filter=None,
        partition_column=None,
        batch_size=None,
    )
