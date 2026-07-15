from __future__ import annotations

import json
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.frontend_logs import prune_frontend_logs, record_frontend_log


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def test_record_frontend_log_writes_row(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_frontend_log(
        db_path,
        "warn",
        "ChatPanel",
        "Stream reconnecting",
        "/dashboard",
        {"attempt": 3},
    )

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT level, namespace, message, page, metadata
            FROM frontend_logs
            """
        ).fetchone()

    assert row is not None
    assert row["level"] == "warn"
    assert row["namespace"] == "ChatPanel"
    assert row["message"] == "Stream reconnecting"
    assert row["page"] == "/dashboard"
    assert json.loads(row["metadata"]) == {"attempt": 3}


def test_record_frontend_log_rejects_invalid_levels(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_frontend_log(db_path, "debug", "ChatPanel", "Ignored")
    record_frontend_log(db_path, "info", "ChatPanel", "Ignored")

    with connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM frontend_logs").fetchone()[0]

    assert count == 0


def test_record_frontend_log_truncates_message(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_frontend_log(db_path, "error", "ChatPanel", "x" * 2500)

    with connect(db_path) as conn:
        row = conn.execute("SELECT message FROM frontend_logs").fetchone()

    assert row is not None
    assert len(row["message"]) == 2000


def test_record_frontend_log_redacts_pii_before_truncating_message(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_frontend_log(db_path, "error", "ChatPanel", ("x" * 1994) + " sk-live-secret")

    with connect(db_path) as conn:
        row = conn.execute("SELECT message FROM frontend_logs").fetchone()

    assert row is not None
    assert len(row["message"]) == 2000
    assert "sk-live-secret" not in row["message"]
    assert "[KEY]" in row["message"]


def test_record_frontend_log_redacts_namespace_page_and_preserves_nulls(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_frontend_log(
        db_path,
        "warn",
        "Auth sk-live-secret",
        "message",
        "/callback?code=abc",
    )
    record_frontend_log(db_path, "warn", None, "message", None)

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT namespace, page
            FROM frontend_logs
            ORDER BY id
            """
        ).fetchall()

    assert rows[0]["namespace"] == "Auth [KEY]"
    assert rows[0]["page"] == "/callback?code=[REDACTED]"
    assert rows[1]["namespace"] is None
    assert rows[1]["page"] is None


def test_record_frontend_log_sanitizes_metadata(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_frontend_log(
        db_path,
        "warn",
        "ChatPanel",
        "sanitize",
        metadata={
            "long": "x" * 250,
            "details": {"attempt": 3, "route": "/api/chat"},
            "error": {"name": "TypeError", "message": "boom"},
            **{f"extra_{index}": index for index in range(25)},
        },
    )

    with connect(db_path) as conn:
        row = conn.execute("SELECT metadata FROM frontend_logs").fetchone()

    metadata = json.loads(row["metadata"])
    assert metadata["long"] == "x" * 200
    assert metadata["details"] == '{"attempt":3,"route":"/api/chat"}'
    assert metadata["error"] == {"name": "TypeError", "message": "boom"}
    assert len(metadata) == 20


def test_record_frontend_log_redacts_metadata_before_truncating(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_frontend_log(
        db_path,
        "warn",
        "ChatPanel",
        "metadata redaction",
        metadata={
            "long": ("x" * 194) + " sk-live-secret",
            "details": {"url": "/callback?code=abc"},
            "error": {"name": "Error", "message": "card 4111-1111-1111-1111"},
        },
    )

    with connect(db_path) as conn:
        row = conn.execute("SELECT metadata FROM frontend_logs").fetchone()

    metadata = json.loads(row["metadata"])
    assert len(metadata["long"]) == 200
    assert "sk-live-secret" not in metadata["long"]
    assert "[KEY]" in metadata["long"]
    assert metadata["details"] == '{"url":"/callback?code=[REDACTED]"}'
    assert metadata["error"] == {"name": "Error", "message": "card [CARD]"}


def test_record_frontend_log_keeps_metadata_json_valid(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_frontend_log(
        db_path,
        "error",
        "ChatPanel",
        "json valid",
        metadata={
            "bad_set": {1, 2, 3},
            "nan": float("nan"),
        },
    )

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT metadata, json_valid(metadata) AS is_valid
            FROM frontend_logs
            """
        ).fetchone()

    assert row is not None
    assert row["is_valid"] == 1
    assert json.loads(row["metadata"]) == {
        "bad_set": "[unserializable]",
        "nan": None,
    }


def test_record_frontend_log_uses_truncation_marker_for_oversized_metadata(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    oversized = {
        f"wide_key_{index}_wide_key_{index}": "x" * 200
        for index in range(20)
    }

    record_frontend_log(
        db_path,
        "warn",
        "ChatPanel",
        "oversized",
        metadata=oversized,
    )

    with connect(db_path) as conn:
        row = conn.execute("SELECT metadata FROM frontend_logs").fetchone()

    assert json.loads(row["metadata"]) == {"_keys": 20, "_truncated": True}


def test_record_frontend_log_succeeds_after_malformed_metadata_row(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute("PRAGMA ignore_check_constraints = 1")
        conn.execute(
            """
            INSERT INTO frontend_logs (level, namespace, message, metadata)
            VALUES ('warn', 'seed', 'bad metadata', '{not json')
            """
        )

    record_frontend_log(db_path, "warn", "ChatPanel", "after malformed", metadata={"ok": True})

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT message, metadata
            FROM frontend_logs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

    assert row["message"] == "after malformed"
    assert json.loads(row["metadata"]) == {"ok": True}


def test_prune_frontend_logs_deletes_old_records(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO frontend_logs (level, message, created_at)
            VALUES ('warn', 'old', datetime('now', '-45 days'))
            """
        )
        conn.execute(
            """
            INSERT INTO frontend_logs (level, message, created_at)
            VALUES ('error', 'new', datetime('now'))
            """
        )
        prune_frontend_logs(conn, retention_days=30)
        rows = conn.execute(
            "SELECT message FROM frontend_logs ORDER BY message"
        ).fetchall()

    assert [row["message"] for row in rows] == ["new"]
