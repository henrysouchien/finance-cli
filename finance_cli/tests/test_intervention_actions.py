from __future__ import annotations

from pathlib import Path
import threading

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import ConflictError, NotFoundError, ValidationError
from finance_cli.intervention_engine import record_action
from finance_cli.tests.test_intervention_engine import NOW, _seed_intervention_log


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def test_record_action_marks_pending_row_acted(db_path: Path) -> None:
    with connect(db_path) as conn:
        log_id = _seed_intervention_log(conn, pattern_id="D-1", fired_at="2026-04-01 12:00:00")
        row = record_action(conn, log_id, "acted", now=NOW)

    assert row["id"] == log_id
    assert row["user_action"] == "acted"
    assert row["acted_at"] == NOW.strftime("%Y-%m-%d %H:%M:%S")


def test_record_action_marks_pending_row_dismissed(db_path: Path) -> None:
    with connect(db_path) as conn:
        log_id = _seed_intervention_log(conn, pattern_id="D-1", fired_at="2026-04-01 12:00:00")
        row = record_action(conn, log_id, "dismissed", now=NOW)

    assert row["id"] == log_id
    assert row["user_action"] == "dismissed"
    assert row["acted_at"] == NOW.strftime("%Y-%m-%d %H:%M:%S")


def test_record_action_is_idempotent_on_same_target_state(db_path: Path) -> None:
    with connect(db_path) as conn:
        log_id = _seed_intervention_log(
            conn,
            pattern_id="D-1",
            fired_at="2026-04-01 12:00:00",
            user_action="acted",
            acted_at="2026-04-02 12:00:00",
        )
        row = record_action(conn, log_id, "acted", now=NOW)

    assert row["id"] == log_id
    assert row["user_action"] == "acted"
    assert row["acted_at"] == "2026-04-02 12:00:00"


def test_record_action_raises_conflict_on_terminal_mismatch(db_path: Path) -> None:
    with connect(db_path) as conn:
        log_id = _seed_intervention_log(
            conn,
            pattern_id="D-1",
            fired_at="2026-04-01 12:00:00",
            user_action="dismissed",
            acted_at="2026-04-02 12:00:00",
        )
        with pytest.raises(ConflictError, match="already in state 'dismissed'"):
            record_action(conn, log_id, "acted", now=NOW)


def test_record_action_raises_not_found_for_missing_log_id(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(NotFoundError, match="999 not found"):
            record_action(conn, 999, "acted", now=NOW)


def test_record_action_validates_action_name(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValidationError, match="Invalid action"):
            record_action(conn, 1, "ignored", now=NOW)


def test_record_action_serializes_concurrent_requests_with_begin_immediate(db_path: Path) -> None:
    with connect(db_path) as conn:
        log_id = _seed_intervention_log(conn, pattern_id="D-1", fired_at="2026-04-01 12:00:00")

    barrier = threading.Barrier(5)
    errors: list[BaseException] = []
    results: list[dict[str, object]] = []

    def worker() -> None:
        try:
            with connect(db_path, busy_timeout=5000) as conn:
                barrier.wait()
                results.append(record_action(conn, log_id, "acted", now=NOW))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with connect(db_path) as conn:
        row = conn.execute("SELECT user_action, acted_at FROM intervention_log WHERE id = ?", (log_id,)).fetchone()
        busy_timeout = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])

    assert errors == []
    assert len(results) == 5
    assert {int(result["id"]) for result in results} == {log_id}
    assert {str(result["user_action"]) for result in results} == {"acted"}
    assert row["user_action"] == "acted"
    assert row["acted_at"] == NOW.strftime("%Y-%m-%d %H:%M:%S")
    assert busy_timeout >= 0
