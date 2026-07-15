from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.error_capture import (
    capture_error,
    prune_errors,
    _send_alert,
    _error_fingerprint,
    _extract_first_app_frame,
    _redact,
    _try_record_alert,
)


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _build_exception(message: str = "boom") -> Exception:
    try:
        _raise_sample_error(message)
    except Exception as exc:  # noqa: BLE001
        return exc
    raise AssertionError("expected helper to raise")


def _raise_sample_error(message: str) -> None:
    raise ValueError(message)


def _create_notification_channels_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE notification_channels (
            channel TEXT PRIMARY KEY,
            config TEXT NOT NULL,
            label TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )


def _insert_notification_channel(
    conn: sqlite3.Connection,
    channel: str,
    config: dict[str, object],
    label: str = "",
) -> None:
    conn.execute(
        "INSERT INTO notification_channels (channel, config, label) VALUES (?, ?, ?)",
        (channel, json.dumps(config), label),
    )
    conn.commit()


def test_capture_error_writes_to_errors_table_and_returns_id(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    exc = _build_exception("import failed")

    error_id = capture_error(
        exc,
        source="cli",
        endpoint="demo",
        context={"request_id": "req-123", "tool_name": "demo"},
        db_path=db_path,
    )

    assert error_id is not None

    with connect(db_path) as conn:
        error_row = conn.execute(
            """
            SELECT id, source, endpoint, error_type, request_id, occurrence_count
            FROM errors
            WHERE id = ?
            """,
            (error_id,),
        ).fetchone()
        occurrence_row = conn.execute(
            """
            SELECT error_id, request_id
            FROM error_occurrences
            WHERE error_id = ?
            """,
            (error_id,),
        ).fetchone()

    assert error_row is not None
    assert error_row["source"] == "cli"
    assert error_row["endpoint"] == "demo"
    assert error_row["error_type"] == "ValueError"
    assert error_row["request_id"] == "req-123"
    assert error_row["occurrence_count"] == 1
    assert occurrence_row is not None
    assert occurrence_row["error_id"] == error_id
    assert occurrence_row["request_id"] == "req-123"


def test_fingerprint_stability_ignores_line_numbers() -> None:
    traceback_one = (
        'Traceback (most recent call last):\n'
        '  File "/tmp/project/finance_cli/mcp_server.py", line 10, in _call\n'
        "    raise ValueError('boom')\n"
    )
    traceback_two = (
        'Traceback (most recent call last):\n'
        '  File "/tmp/project/finance_cli/mcp_server.py", line 99, in _call\n'
        "    raise ValueError('boom')\n"
    )

    first = _error_fingerprint("mcp", "txn_list", "ValueError", traceback_one)
    second = _error_fingerprint("mcp", "txn_list", "ValueError", traceback_two)

    assert first == second


def test_b3_captured_prevents_double_capture(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    exc = _build_exception("duplicate")

    first = capture_error(exc, source="cli", endpoint="dup", db_path=db_path)
    second = capture_error(exc, source="cli", endpoint="dup", db_path=db_path)

    assert first is not None
    assert second is None

    with connect(db_path) as conn:
        error_count = conn.execute("SELECT COUNT(*) FROM errors").fetchone()[0]
        occurrence_count = conn.execute("SELECT COUNT(*) FROM error_occurrences").fetchone()[0]

    assert error_count == 1
    assert occurrence_count == 1


def test_redact_strips_sensitive_values() -> None:
    text = (
        "card 4111 1111 1111 1111 acct 123456789 email user@example.com "
        "token sk-live-secret ip 10.0.0.1 path /Users/tester/private/file.txt"
    )

    redacted = _redact(text)

    assert "[CARD]" in redacted
    assert "[ACCT]" in redacted
    assert "[EMAIL]" in redacted
    assert "[KEY]" in redacted
    assert "[IP]" in redacted
    assert "[USER_PATH]" in redacted
    assert "4111 1111 1111 1111" not in redacted
    assert "user@example.com" not in redacted
    assert "10.0.0.1" not in redacted


def test_redact_scrubs_data_server_paths() -> None:
    text = "see /data/finance/users/abc/file.csv for details"

    redacted = _redact(text)

    assert redacted == "see [SERVER_PATH] for details"


def test_redact_scrubs_var_server_paths() -> None:
    text = "traceback from /var/www/finance_web/app.py"

    redacted = _redact(text)

    assert redacted == "traceback from [SERVER_PATH]"


def test_redact_preserves_api_routes_and_urls() -> None:
    text = "GET /api/v1/sessions via https://example.com/api/v1/sessions"

    assert _redact(text) == text


def test_extract_first_app_frame_returns_module_and_function() -> None:
    traceback_str = (
        'Traceback (most recent call last):\n'
        '  File "/tmp/project/lib/python3.12/site-packages/pkg.py", line 1, in wrapper\n'
        "    run()\n"
        '  File "/tmp/project/finance_cli/importers/csv_normalizers.py", line 206, in normalize_csv\n'
        "    raise ValueError('boom')\n"
    )

    assert _extract_first_app_frame(traceback_str) == "importers.csv_normalizers:normalize_csv"


def test_resolved_error_reopens_on_recurrence(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    first_exc = _build_exception("reopen me")

    error_id = capture_error(first_exc, source="cli", endpoint="demo", db_path=db_path)
    assert error_id is not None

    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE errors
            SET status = 'resolved',
                resolved_at = datetime('now'),
                resolution = 'fixed once'
            WHERE id = ?
            """,
            (error_id,),
        )

    second_exc = _build_exception("reopen me")
    second_id = capture_error(second_exc, source="cli", endpoint="demo", db_path=db_path)

    assert second_id == error_id

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT status, resolved_at, resolution, occurrence_count
            FROM errors
            WHERE id = ?
            """,
            (error_id,),
        ).fetchone()

    assert row is not None
    assert row["status"] == "open"
    assert row["resolved_at"] is None
    assert row["resolution"] is None
    assert row["occurrence_count"] == 2


def test_alert_idempotency_respects_window_key(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        assert _try_record_alert(conn, "fp-1", "new_error", "new_fp:fp-1:1") is True
        assert _try_record_alert(conn, "fp-1", "new_error", "new_fp:fp-1:1") is False
        count = conn.execute("SELECT COUNT(*) FROM error_alerts").fetchone()[0]

    assert count == 1


def test_send_alert_skips_notifications_when_alerts_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "finance_cli.error_capture.alerts",
        SimpleNamespace(send=lambda body, channel, **kw: calls.append((body, channel))),
    )

    _send_alert(
        {
            "severity": "error",
            "endpoint": "setup.connect",
            "source": "cli",
            "error_type": "ValueError",
            "message": "boom",
        },
        "new_error",
    )

    assert calls == []


def test_send_alert_sends_notification_when_alerts_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr("finance_cli.error_capture._HAS_ALERTS", True)
    monkeypatch.setattr(
        "finance_cli.error_capture.alerts",
        SimpleNamespace(send=lambda body, channel, **kw: calls.append((body, channel))),
    )

    _send_alert(
        {
            "severity": "error",
            "endpoint": "setup.connect",
            "source": "cli",
            "error_type": "ValueError",
            "message": "boom",
        },
        "new_error",
    )

    assert calls == [("ERROR: ValueError in setup.connect [new_error]", "telegram")]


def test_send_alert_resolves_per_user_creds_for_sqlite_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = sqlite3.connect(":memory:")
    calls: list[tuple[str, str, dict[str, str]]] = []
    monkeypatch.setattr("finance_cli.error_capture._HAS_ALERTS", True)
    monkeypatch.setattr(
        "finance_cli.error_capture.alerts",
        SimpleNamespace(send=lambda body, channel, **kw: calls.append((body, channel, kw))),
    )
    _create_notification_channels_table(conn)
    _insert_notification_channel(conn, "telegram", {"chat_id": "user-chat"})

    try:
        _send_alert(
            {
                "severity": "error",
                "endpoint": "setup.connect",
                "source": "cli",
                "error_type": "ValueError",
                "message": "boom",
            },
            "new_error",
            conn,
        )
    finally:
        conn.close()

    assert calls == [
        ("ERROR: ValueError in setup.connect [new_error]", "telegram", {"chat_id": "user-chat"})
    ]


def test_send_alert_skips_cred_resolution_for_non_sqlite_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, dict[str, str]]] = []
    monkeypatch.setattr("finance_cli.error_capture._HAS_ALERTS", True)
    monkeypatch.setattr(
        "finance_cli.error_capture.alerts",
        SimpleNamespace(send=lambda body, channel, **kw: calls.append((body, channel, kw))),
    )

    _send_alert(
        {
            "severity": "error",
            "endpoint": "setup.connect",
            "source": "cli",
            "error_type": "ValueError",
            "message": "boom",
        },
        "new_error",
        object(),
    )

    assert calls == [("ERROR: ValueError in setup.connect [new_error]", "telegram", {})]


def test_capture_error_sends_alert_with_per_user_sqlite_creds(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = _init_db(tmp_path)
    exc = _build_exception("send routed alert")
    calls: list[tuple[str, str, dict[str, str]]] = []
    monkeypatch.setattr("finance_cli.error_capture._HAS_ALERTS", True)
    monkeypatch.setattr(
        "finance_cli.error_capture.alerts",
        SimpleNamespace(send=lambda body, channel, **kw: calls.append((body, channel, kw))),
    )

    with connect(db_path) as conn:
        _insert_notification_channel(conn, "telegram", {"chat_id": "user-chat"})

    error_id = capture_error(exc, source="mcp", endpoint="test", db_path=db_path)

    assert error_id is not None
    assert calls == [("ERROR: ValueError in test [new_error]", "telegram", {"chat_id": "user-chat"})]


def test_structured_log_fallback_without_storage_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exc = _build_exception("missing backend")
    captured: dict[str, object] = {}

    def fake_log_error(message: str, *, extra: dict[str, object]) -> None:
        captured["message"] = message
        captured["extra"] = extra

    monkeypatch.setattr("finance_cli.error_capture.log.error", fake_log_error)
    result = capture_error(
        exc,
        source="cli",
        endpoint="demo",
        context={"request_id": "req-fallback"},
    )

    assert result is None
    assert captured["message"] == "error_capture_fallback"
    extra = captured["extra"]
    assert isinstance(extra, dict)
    assert extra["original_error_type"] == "ValueError"
    assert extra["original_message"] == "missing backend"
    assert extra["source"] == "cli"
    assert extra["endpoint"] == "demo"
    assert extra["severity"] == "error"
    assert extra["request_id"] == "req-fallback"
    assert extra["fingerprint"]


def test_allowed_context_keys_filter_context_dict(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    exc = _build_exception("context check")

    error_id = capture_error(
        exc,
        source="mcp",
        endpoint="tool.demo",
        context={
            "request_id": "req-ctx",
            "tool_name": "tool.demo",
            "tool_input_keys": ["amount", "note"],
            "status_code": 500,
            "prompt": "should not persist",
            "db_path": "/Users/tester/secret.db",
        },
        db_path=db_path,
    )

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT context FROM errors WHERE id = ?",
            (error_id,),
        ).fetchone()

    assert row is not None
    context = json.loads(row["context"])
    assert context == {
        "request_id": "req-ctx",
        "status_code": 500,
        "tool_input_keys": ["amount", "note"],
        "tool_name": "tool.demo",
    }


def test_prune_errors_deletes_old_resolved_rows_and_children(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO errors (
                id,
                fingerprint,
                severity,
                source,
                endpoint,
                error_type,
                message,
                status,
                occurrence_count,
                first_seen,
                last_seen
            )
            VALUES (
                'old-error',
                'fp-old',
                'error',
                'cli',
                'old',
                'ValueError',
                'old message',
                'resolved',
                1,
                datetime('now', '-120 days'),
                datetime('now', '-120 days')
            )
            """
        )
        conn.execute(
            """
            INSERT INTO errors (
                id,
                fingerprint,
                severity,
                source,
                endpoint,
                error_type,
                message,
                status,
                occurrence_count,
                first_seen,
                last_seen
            )
            VALUES (
                'new-error',
                'fp-new',
                'error',
                'cli',
                'new',
                'ValueError',
                'new message',
                'open',
                1,
                datetime('now'),
                datetime('now')
            )
            """
        )
        conn.execute(
            """
            INSERT INTO error_occurrences (error_id, created_at)
            VALUES ('old-error', datetime('now', '-45 days'))
            """
        )
        conn.execute(
            """
            INSERT INTO error_occurrences (error_id, created_at)
            VALUES ('new-error', datetime('now'))
            """
        )
        conn.execute(
            """
            INSERT INTO error_alerts (fingerprint, alert_reason, window_key, created_at)
            VALUES ('fp-old', 'new_error', 'new_fp:fp-old:1', datetime('now', '-45 days'))
            """
        )

        prune_errors(conn)

        error_ids = [row["id"] for row in conn.execute("SELECT id FROM errors ORDER BY id").fetchall()]
        occurrence_ids = [
            row["error_id"]
            for row in conn.execute("SELECT error_id FROM error_occurrences ORDER BY error_id").fetchall()
        ]
        alert_count = conn.execute("SELECT COUNT(*) FROM error_alerts").fetchone()[0]

    assert error_ids == ["new-error"]
    assert occurrence_ids == ["new-error"]
    assert alert_count == 0
