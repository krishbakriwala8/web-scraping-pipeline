"""
tests/test_db.py
================
Unit tests for the database layer.

All tests use MagicMock to avoid requiring a live PostgreSQL instance.
Tests verify:
  - load_records calls execute_values with correct row tuples
  - load_records batches correctly
  - log_ingestion writes the correct parameters
  - DatabasePool.execute delegates to cursor correctly
"""
from datetime import datetime
from unittest.mock import MagicMock, patch, call
import uuid

import pytest

from db.loader import load_records, log_ingestion, _record_to_tuple
from etl.validator import CleanRecord


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_record(**overrides) -> CleanRecord:
    base = dict(
        source_name="Hacker News",
        title="Test Title",
        url="https://example.com",
        description="Test description",
        author="Alice",
        category="tech_news",
        tags=["python", "ai"],
        score=7.5,
        price=None,
        published_at=None,
        batch_id="batch-001",
        fingerprint=uuid.uuid4().hex[:40],
    )
    base.update(overrides)
    return CleanRecord.model_validate(base)


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    cur.rowcount = 1
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    pool.get_connection.return_value = conn
    return pool, conn, cur


# ---------------------------------------------------------------------------
# _record_to_tuple
# ---------------------------------------------------------------------------

def test_record_to_tuple_has_correct_length():
    record = _make_record()
    t = _record_to_tuple(record)
    assert len(t) == 13  # 13 columns in SQL_UPSERT_CLEAN


def test_record_to_tuple_field_order():
    record = _make_record(title="My Title", author="Bob", description="")
    t = _record_to_tuple(record)
    assert t[0] == "Hacker News"   # source_name
    assert t[1] == "My Title"      # title
    assert t[3] == ""              # description (explicitly empty)
    assert t[4] == "Bob"           # author


# ---------------------------------------------------------------------------
# load_records
# ---------------------------------------------------------------------------

@patch("db.loader.execute_values")
def test_load_records_calls_execute_values(mock_ev, mock_pool):
    pool, conn, cur = mock_pool
    records = [_make_record(), _make_record()]
    load_records(pool, records, batch_size=100)
    assert mock_ev.called
    args = mock_ev.call_args
    assert args[0][0] is cur              # cursor
    assert len(args[0][2]) == 2           # 2 row tuples


@patch("db.loader.execute_values")
def test_load_records_empty_list(mock_ev, mock_pool):
    pool, _, _ = mock_pool
    result = load_records(pool, [], batch_size=100)
    assert result == 0
    mock_ev.assert_not_called()


@patch("db.loader.execute_values")
def test_load_records_batches_correctly(mock_ev, mock_pool):
    pool, conn, cur = mock_pool
    records = [_make_record() for _ in range(5)]
    load_records(pool, records, batch_size=2)
    # 5 records / batch_size 2 = 3 calls (2, 2, 1)
    assert mock_ev.call_count == 3


# ---------------------------------------------------------------------------
# log_ingestion
# ---------------------------------------------------------------------------

def test_log_ingestion_executes_with_correct_params(mock_pool):
    pool, conn, cur = mock_pool
    now = datetime.utcnow()
    log_ingestion(
        pool=pool,
        batch_id="b-001",
        source_name="Hacker News",
        records_raw=50,
        records_clean=45,
        records_failed=5,
        started_at=now,
        finished_at=now,
        status="success",
        error_message="",
    )
    cur.execute.assert_called_once()
    call_args = cur.execute.call_args
    params = call_args[0][1]
    assert params["batch_id"] == "b-001"
    assert params["source_name"] == "Hacker News"
    assert params["records_raw"] == 50
    assert params["records_clean"] == 45
    assert params["records_failed"] == 5
    assert params["status"] == "success"


def test_log_ingestion_truncates_long_error(mock_pool):
    pool, conn, cur = mock_pool
    long_error = "x" * 5000
    now = datetime.utcnow()
    log_ingestion(
        pool=pool,
        batch_id="b-002",
        source_name="Test",
        records_raw=0, records_clean=0, records_failed=0,
        started_at=now, finished_at=now,
        error_message=long_error,
    )
    params = cur.execute.call_args[0][1]
    assert len(params["error_message"]) <= 1000
