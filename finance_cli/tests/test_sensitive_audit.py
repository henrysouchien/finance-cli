from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.sensitive_audit import audit_hash, record_sqlite_sensitive_audit_event


def test_sensitive_audit_sqlite_redacts_hashes_and_chains_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        first_hash = record_sqlite_sensitive_audit_event(
            conn,
            user_id="user-1",
            actor_type="agent",
            event_type="data_export.csv",
            target_type="export",
            target_id="/tmp/private/export.csv",
            surface="mcp",
            request_id="request-1",
            session_id="session-secret",
            ip="203.0.113.10",
            user_agent="TestAgent/1.0",
            details={
                "api_key": "sk-ant-api03-secret",
                "bundle_path": "/tmp/private/export.csv",
                "mode": "full",
            },
            raise_errors=True,
        )
        second_hash = record_sqlite_sensitive_audit_event(
            conn,
            user_id="user-1",
            event_type="settings.ai_egress.updated",
            target_type="user_setting",
            target_id="ai_egress_mode",
            surface="web",
            details={"mode": "redacted"},
            raise_errors=True,
        )

        rows = conn.execute(
            """
            SELECT event_type, target_id_hash, session_id_hash, details, prev_hash, row_hash
              FROM sensitive_audit_events
             ORDER BY id
            """
        ).fetchall()

    assert len(rows) == 2
    assert rows[0]["event_type"] == "data_export.csv"
    assert rows[0]["target_id_hash"] == audit_hash("/tmp/private/export.csv")
    assert rows[0]["session_id_hash"] == audit_hash("session-secret")
    assert rows[0]["session_id_hash"] != "session-secret"
    details = json.loads(rows[0]["details"])
    assert details["api_key"] == "[redacted]"
    assert details["bundle_path"] == {"sha256": audit_hash("/tmp/private/export.csv")}
    assert details["mode"] == "full"
    assert rows[0]["prev_hash"] is None
    assert rows[0]["row_hash"] == first_hash
    assert rows[1]["prev_hash"] == first_hash
    assert rows[1]["row_hash"] == second_hash


def test_sensitive_audit_sqlite_is_append_only(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        record_sqlite_sensitive_audit_event(
            conn,
            user_id="user-1",
            event_type="data_export.csv",
            raise_errors=True,
        )

        try:
            conn.execute("UPDATE sensitive_audit_events SET event_type = 'changed'")
        except sqlite3.IntegrityError as exc:
            assert "append-only" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected append-only update trigger to abort")

        try:
            conn.execute("DELETE FROM sensitive_audit_events")
        except sqlite3.IntegrityError as exc:
            assert "append-only" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected append-only delete trigger to abort")
