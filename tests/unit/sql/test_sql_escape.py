"""Tests for SQL string escaping and the sql_escape Jinja2 filter."""

from bq_entity_resolution.sql.generator import (
    SQLGenerator,
    format_watermark_value,
    sql_escape,
)


def test_format_watermark_value_single_quote():
    """Single quotes are doubled per standard SQL quoting."""
    result = format_watermark_value("O'Brien")
    assert result == "'O''Brien'"
    assert "\\'" not in result


def test_format_watermark_value_timestamp():
    """Timestamps are wrapped in TIMESTAMP() with no extra escaping."""
    result = format_watermark_value("2024-01-15T10:30:00")
    assert result == "TIMESTAMP('2024-01-15T10:30:00')"


def test_format_watermark_value_numeric():
    """Numeric values are returned as-is (no quotes)."""
    assert format_watermark_value(42) == "42"
    assert format_watermark_value(3.14) == "3.14"


def test_format_watermark_value_null():
    """None returns the SQL NULL keyword."""
    assert format_watermark_value(None) == "NULL"


def test_sql_escape_filter_registered():
    """sql_escape filter is registered on the SQLGenerator environment."""
    gen = SQLGenerator()
    assert "sql_escape" in gen.env.filters


def test_sql_escape_filter_doubles_quotes():
    """sql_escape doubles single quotes."""
    assert sql_escape("O'Brien") == "O''Brien"
    assert sql_escape("it's a 'test'") == "it''s a ''test''"


def test_sql_escape_filter_no_change_for_safe_strings():
    """sql_escape returns safe strings unchanged."""
    assert sql_escape("hello") == "hello"
    assert sql_escape("source_alpha") == "source_alpha"


def test_sql_escape_in_watermark_template():
    """Rendered watermark SQL uses '' not \\' for escaped quotes."""
    gen = SQLGenerator()
    sql = gen.render(
        "watermark/read_watermark.sql.j2",
        table="proj.dataset.watermarks",
        source_name="O'Malley's",
    )
    assert "O''Malley''s" in sql
    assert "\\'" not in sql
