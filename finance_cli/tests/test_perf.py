from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.logging_config import CorrelationFilter, StructuredFormatter
from finance_cli.perf import (
    TimedConnection,
    _normalize_sql,
    _record_perf_sample,
    _request_id_var,
    prune_perf_samples,
    set_request_id,
)


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def test_record_perf_sample_writes_to_perf_samples_table(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    token = set_request_id("req-123")
    try:
        _record_perf_sample(
            db_path,
            "tool",
            "tool.demo",
            123,
            tags={"route": "/api/demo"},
            is_error=True,
        )
    finally:
        _request_id_var.reset(token)

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT source, metric, value_ms, is_error, request_id, tags
            FROM perf_samples
            """
        ).fetchone()

    assert row is not None
    assert row["source"] == "tool"
    assert row["metric"] == "tool.demo"
    assert row["value_ms"] == 123
    assert row["is_error"] == 1
    assert row["request_id"] == "req-123"
    assert json.loads(row["tags"]) == {"route": "/api/demo"}


def test_record_perf_sample_never_raises_on_db_errors(tmp_path: Path) -> None:
    broken_db_path = tmp_path / "missing" / "finance.db"
    _record_perf_sample(broken_db_path, "tool", "tool.demo", 50)
    assert not broken_db_path.parent.exists()


def test_timed_cursor_records_slow_queries_when_threshold_exceeded(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_SLOW_QUERY_MS", "5")

    perf_counter_values = iter((10.0, 10.025))
    monkeypatch.setattr(
        "finance_cli.perf.time.perf_counter",
        lambda: next(perf_counter_values),
    )

    token = set_request_id("req-slow")
    try:
        with sqlite3.connect(str(db_path), factory=TimedConnection) as conn:
            conn.execute(
                "SELECT * FROM perf_samples WHERE metric='tool.demo' AND value_ms=123"
            ).fetchall()
    finally:
        _request_id_var.reset(token)

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT source, metric, value_ms, request_id, tags
            FROM perf_samples
            WHERE source = 'query'
            """
        ).fetchone()

    assert row is not None
    assert row["source"] == "query"
    assert row["metric"].startswith("query.SELECT * FROM perf_samples WHERE metric=?")
    assert row["value_ms"] == 25
    assert row["request_id"] == "req-slow"
    assert json.loads(row["tags"]) == {
        "sql_fingerprint": "SELECT * FROM perf_samples WHERE metric=? AND value_ms=?"
    }


def test_timed_cursor_does_nothing_when_threshold_is_zero(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_SLOW_QUERY_MS", "0")
    called: list[tuple[str, int]] = []

    def fake_record_query_sample(fingerprint: str, elapsed_ms: int) -> None:
        called.append((fingerprint, elapsed_ms))

    monkeypatch.setattr("finance_cli.perf._record_query_sample", fake_record_query_sample)

    with sqlite3.connect(str(db_path), factory=TimedConnection) as conn:
        conn.execute("SELECT * FROM perf_samples").fetchall()

    with connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM perf_samples").fetchone()[0]

    assert called == []
    assert count == 0


def test_normalize_sql_replaces_literals_correctly() -> None:
    normalized = _normalize_sql(
        "SELECT * FROM txns WHERE note='Coffee Shop' AND amount=12.34 "
        "AND retries=-2 AND memo='O''Reilly'"
    )

    assert normalized == (
        "SELECT * FROM txns WHERE note=? AND amount=? AND retries=? AND memo=?"
    )


def test_prune_perf_samples_deletes_old_records(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO perf_samples (source, metric, value_ms, created_at)
            VALUES ('tool', 'tool.old', 50, datetime('now', '-45 days'))
            """
        )
        conn.execute(
            """
            INSERT INTO perf_samples (source, metric, value_ms, created_at)
            VALUES ('tool', 'tool.new', 10, datetime('now'))
            """
        )
        prune_perf_samples(conn, retention_days=30)
        rows = conn.execute(
            "SELECT metric FROM perf_samples ORDER BY metric"
        ).fetchall()

    assert [row["metric"] for row in rows] == ["tool.new"]


def test_correlation_filter_injects_request_id() -> None:
    token = set_request_id("req-filter")
    try:
        record = logging.getLogger("finance_cli.test").makeRecord(
            "finance_cli.test",
            logging.INFO,
            __file__,
            10,
            "hello",
            args=(),
            exc_info=None,
        )
        assert CorrelationFilter().filter(record) is True
    finally:
        _request_id_var.reset(token)

    assert record.request_id == "req-filter"


def test_structured_formatter_produces_valid_json_with_extra_fields() -> None:
    token = set_request_id("req-json")
    try:
        logger = logging.getLogger("finance_cli.test")
        record = logger.makeRecord(
            "finance_cli.test",
            logging.WARNING,
            __file__,
            20,
            "slow %s",
            args=("query",),
            exc_info=None,
            extra={"duration_ms": 42, "sql_fingerprint": "SELECT * FROM perf_samples"},
        )
        CorrelationFilter().filter(record)
        payload = json.loads(StructuredFormatter().format(record))
    finally:
        _request_id_var.reset(token)

    assert payload["level"] == "WARNING"
    assert payload["logger"] == "finance_cli.test"
    assert payload["msg"] == "slow query"
    assert payload["duration_ms"] == 42
    assert payload["sql_fingerprint"] == "SELECT * FROM perf_samples"
    assert payload["request_id"] == "req-json"
    assert "ts" in payload
