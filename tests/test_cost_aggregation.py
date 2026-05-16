"""Tests for SessionDB.get_cost_summary() — cross-session cost aggregation."""
import pytest
from pathlib import Path
from hermes_state import SessionDB


def test_get_cost_summary_empty_db(tmp_path):
    db_path = tmp_path / "test_cost_empty.db"
    db = SessionDB(db_path=db_path)
    try:
        summary = db.get_cost_summary(since_hours=24)
        assert summary["session_count"] == 0
        assert summary["total_input_tokens"] == 0
        assert summary["total_output_tokens"] == 0
        assert summary["total_estimated_cost_usd"] == 0.0
        assert "total_cache_read_tokens" in summary
    finally:
        db.close()


def test_get_cost_summary_with_sessions(tmp_path):
    db_path = tmp_path / "test_cost_sessions.db"
    db = SessionDB(db_path=db_path)
    try:
        sid1 = db.create_session(session_id="s1", source="test1")
        db.update_token_counts(sid1, input_tokens=1000, output_tokens=500,
                               estimated_cost_usd=0.001, api_call_count=1)
        sid2 = db.create_session(session_id="s2", source="test2")
        db.update_token_counts(sid2, input_tokens=2000, output_tokens=800,
                               estimated_cost_usd=0.002, api_call_count=2)

        summary = db.get_cost_summary(since_hours=24)
        assert summary["session_count"] == 2
        assert summary["total_input_tokens"] == 3000
        assert summary["total_output_tokens"] == 1300
        assert summary["total_estimated_cost_usd"] == 0.003
    finally:
        db.close()


def test_get_cost_summary_excludes_zero_api_calls(tmp_path):
    db_path = tmp_path / "test_cost_idle.db"
    db = SessionDB(db_path=db_path)
    try:
        sid = db.create_session(session_id="idle1", source="idle")
        # No update_token_counts → api_call_count stays 0

        summary = db.get_cost_summary(since_hours=24)
        assert summary["session_count"] == 0
    finally:
        db.close()


def test_get_cost_summary_all_time(tmp_path):
    """since_hours=None aggregates ALL sessions regardless of time window."""
    db_path = tmp_path / "test_cost_alltime.db"
    db = SessionDB(db_path=db_path)
    try:
        sid1 = db.create_session(session_id="s1", source="test1")
        db.update_token_counts(sid1, input_tokens=500, output_tokens=200,
                               estimated_cost_usd=0.005, api_call_count=1)
        sid2 = db.create_session(session_id="s2", source="test2")
        db.update_token_counts(sid2, input_tokens=800, output_tokens=300,
                               estimated_cost_usd=0.008, api_call_count=1)

        summary = db.get_cost_summary(since_hours=None)
        assert summary["session_count"] == 2
        assert summary["total_input_tokens"] == 1300
        assert summary["total_output_tokens"] == 500
        assert summary["total_estimated_cost_usd"] == 0.013
    finally:
        db.close()
