"""Tests for CheckpointManager."""

from unittest.mock import MagicMock

from bq_entity_resolution.watermark.checkpoint import CheckpointManager


def _make_mgr():
    client = MagicMock()
    mgr = CheckpointManager(
        bq_client=client,
        checkpoint_table="proj.meta.pipeline_checkpoints",
    )
    return mgr, client


def test_ensure_table_exists():
    mgr, client = _make_mgr()
    mgr.ensure_table_exists()
    client.execute.assert_called_once()
    sql = client.execute.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "pipeline_checkpoints" in sql


def test_load_completed_stages_returns_set():
    mgr, client = _make_mgr()
    client.execute_and_fetch.return_value = [
        {"stage_name": "init_watermarks"},
        {"stage_name": "stage_sources"},
    ]
    result = mgr.load_completed_stages("run_123")
    assert result == {"init_watermarks", "stage_sources"}
    sql = client.execute_and_fetch.call_args[0][0]
    assert "run_123" in sql
    assert "status = 'completed'" in sql


def test_load_completed_stages_empty():
    mgr, client = _make_mgr()
    client.execute_and_fetch.return_value = []
    result = mgr.load_completed_stages("run_123")
    assert result == set()


def test_find_resumable_run_found():
    mgr, client = _make_mgr()
    client.execute_and_fetch.return_value = [{"run_id": "er_run_20250101_120000"}]
    result = mgr.find_resumable_run()
    assert result == "er_run_20250101_120000"
    sql = client.execute_and_fetch.call_args[0][0]
    assert "__run_complete__" in sql


def test_find_resumable_run_none():
    mgr, client = _make_mgr()
    client.execute_and_fetch.return_value = []
    result = mgr.find_resumable_run()
    assert result is None


def test_mark_stage_complete():
    mgr, client = _make_mgr()
    mgr.mark_stage_complete("run_123", "stage_sources")
    client.execute.assert_called_once()
    sql = client.execute.call_args[0][0]
    assert "INSERT INTO" in sql
    assert "run_123" in sql
    assert "stage_sources" in sql


def test_mark_run_complete():
    mgr, client = _make_mgr()
    mgr.mark_run_complete("run_123")
    client.execute.assert_called_once()
    sql = client.execute.call_args[0][0]
    assert "__run_complete__" in sql
    assert "run_123" in sql


def test_sql_escape_in_run_id():
    """Ensure single quotes in run_id are escaped."""
    mgr, client = _make_mgr()
    client.execute_and_fetch.return_value = []
    mgr.load_completed_stages("it's_a_test")
    sql = client.execute_and_fetch.call_args[0][0]
    assert "it''s_a_test" in sql
