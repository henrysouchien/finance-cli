from __future__ import annotations

import json
import sqlite3
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from finance_cli.__main__ import build_parser
from finance_cli.commands import notify_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.notification_utils import resolve_notification_creds


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _make_args(**kwargs) -> Namespace:
    defaults = {
        "channel": "telegram",
        "view": "all",
        "month": None,
        "dry_run": False,
        "config": "",
        "label": "",
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


def _raw_conn() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")


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


def _create_telegram_config_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE telegram_config (
            id INTEGER PRIMARY KEY,
            chat_id TEXT
        )
        """
    )


def _insert_channel(conn: sqlite3.Connection, channel: str, config: dict[str, object], label: str = "") -> None:
    conn.execute(
        "INSERT INTO notification_channels (channel, config, label) VALUES (?, ?, ?)",
        (channel, json.dumps(config), label),
    )
    conn.commit()


def test_resolve_no_table_require_false_returns_empty() -> None:
    conn = _raw_conn()

    try:
        assert resolve_notification_creds(conn, "telegram", require=False) == {}
    finally:
        conn.close()


def test_resolve_no_table_require_true_raises() -> None:
    conn = _raw_conn()

    try:
        with pytest.raises(ValueError, match="notify_channel_set"):
            resolve_notification_creds(conn, "telegram", require=True)
    finally:
        conn.close()


def test_resolve_telegram_row_returns_chat_id() -> None:
    conn = _raw_conn()
    _create_notification_channels_table(conn)
    _insert_channel(conn, "telegram", {"chat_id": "12345"})

    try:
        assert resolve_notification_creds(conn, "telegram") == {"chat_id": "12345"}
    finally:
        conn.close()


def test_resolve_telegram_falls_back_to_telegram_config() -> None:
    conn = _raw_conn()
    _create_notification_channels_table(conn)
    _create_telegram_config_table(conn)
    conn.execute("INSERT INTO telegram_config (id, chat_id) VALUES (1, 'tg-fallback')")
    conn.commit()

    try:
        assert resolve_notification_creds(conn, "telegram") == {"chat_id": "tg-fallback"}
    finally:
        conn.close()


def test_resolve_channels_row_wins_over_telegram_config() -> None:
    conn = _raw_conn()
    _create_notification_channels_table(conn)
    _create_telegram_config_table(conn)
    _insert_channel(conn, "telegram", {"chat_id": "primary"})
    conn.execute("INSERT INTO telegram_config (id, chat_id) VALUES (1, 'fallback')")
    conn.commit()

    try:
        assert resolve_notification_creds(conn, "telegram") == {"chat_id": "primary"}
    finally:
        conn.close()


def test_resolve_imessage_row_returns_target_and_service() -> None:
    conn = _raw_conn()
    _create_notification_channels_table(conn)
    _insert_channel(conn, "imessage", {"target": "person@example.com", "service": "sms"})

    try:
        assert resolve_notification_creds(conn, "imessage") == {
            "target": "person@example.com",
            "service": "sms",
        }
    finally:
        conn.close()


def test_resolve_imessage_does_not_use_telegram_fallback() -> None:
    conn = _raw_conn()
    _create_notification_channels_table(conn)
    _create_telegram_config_table(conn)
    conn.execute("INSERT INTO telegram_config (id, chat_id) VALUES (1, 'tg-fallback')")
    conn.commit()

    try:
        assert resolve_notification_creds(conn, "imessage", require=False) == {}
    finally:
        conn.close()


def test_resolve_malformed_json_propagates() -> None:
    conn = _raw_conn()
    _create_notification_channels_table(conn)
    conn.execute(
        "INSERT INTO notification_channels (channel, config, label) VALUES (?, ?, ?)",
        ("telegram", "{bad json", ""),
    )
    conn.commit()

    try:
        with pytest.raises(json.JSONDecodeError):
            resolve_notification_creds(conn, "telegram")
    finally:
        conn.close()


def test_resolve_conn_none_returns_empty() -> None:
    assert resolve_notification_creds(None, "telegram") == {}


def test_resolve_whitelists_keys() -> None:
    conn = _raw_conn()
    _create_notification_channels_table(conn)
    _insert_channel(conn, "telegram", {"chat_id": "123", "token": "evil"})

    try:
        assert resolve_notification_creds(conn, "telegram") == {"chat_id": "123"}
    finally:
        conn.close()


def test_resolve_missing_required_key_falls_back_to_telegram_config() -> None:
    conn = _raw_conn()
    _create_notification_channels_table(conn)
    _create_telegram_config_table(conn)
    _insert_channel(conn, "telegram", {"label": "broken"})
    conn.execute("INSERT INTO telegram_config (id, chat_id) VALUES (1, 'tg-fallback')")
    conn.commit()

    try:
        assert resolve_notification_creds(conn, "telegram") == {"chat_id": "tg-fallback"}
    finally:
        conn.close()


def test_handle_budget_alerts_uses_resolved_channel_config(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    sent: dict[str, object] = {}

    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO notification_channels (channel, config, label) VALUES (?, ?, ?)",
            ("telegram", json.dumps({"chat_id": "user-chat"}), "primary"),
        )
        conn.commit()

        monkeypatch.setattr(
            notify_cmd,
            "budget_alerts",
            lambda _conn, month=None, view="all": {
                "month": month or "2026-03",
                "days_elapsed": 5,
                "days_in_month": 31,
                "alerts": [],
                "ok_count": 2,
                "over_count": 0,
                "alert_count": 0,
                "warn_count": 0,
            },
        )
        monkeypatch.setattr(notify_cmd, "_HAS_ALERTS", True)

        def fake_send(message: str, channel: str = "telegram", **kwargs) -> dict[str, object]:
            sent["message"] = message
            sent["channel"] = channel
            sent["kwargs"] = kwargs
            return {"ok": True}

        monkeypatch.setattr(notify_cmd, "alerts", SimpleNamespace(send=fake_send))

        result = notify_cmd.handle_budget_alerts(
            _make_args(channel="telegram", dry_run=False),
            conn,
            data_dir=tmp_path,
        )

    assert sent["channel"] == "telegram"
    assert sent["kwargs"] == {"chat_id": "user-chat"}
    assert result["data"]["delivery"]["ok"] is True


def test_handle_test_uses_resolved_channel_config(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    sent: dict[str, object] = {}

    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO notification_channels (channel, config, label) VALUES (?, ?, ?)",
            ("imessage", json.dumps({"target": "person@example.com", "service": "sms"}), "phone"),
        )
        conn.commit()

        monkeypatch.setattr(notify_cmd, "_HAS_ALERTS", True)

        def fake_send(message: str, channel: str = "telegram", **kwargs) -> dict[str, object]:
            sent["message"] = message
            sent["channel"] = channel
            sent["kwargs"] = kwargs
            return {"ok": True}

        monkeypatch.setattr(notify_cmd, "alerts", SimpleNamespace(send=fake_send))

        result = notify_cmd.handle_test(
            _make_args(channel="imessage", dry_run=False),
            conn,
            data_dir=tmp_path,
        )

    assert sent["channel"] == "imessage"
    assert sent["kwargs"] == {"target": "person@example.com", "service": "sms"}
    assert result["data"]["delivery"]["ok"] is True


def test_handle_test_cli_mode_allows_env_fallback(monkeypatch) -> None:
    conn = _raw_conn()
    sent: dict[str, object] = {}

    try:
        monkeypatch.setattr(notify_cmd, "_HAS_ALERTS", True)

        def fake_send(message: str, channel: str = "telegram", **kwargs) -> dict[str, object]:
            sent["message"] = message
            sent["channel"] = channel
            sent["kwargs"] = kwargs
            return {"ok": True}

        monkeypatch.setattr(notify_cmd, "alerts", SimpleNamespace(send=fake_send))

        result = notify_cmd.handle_test(_make_args(channel="telegram", dry_run=False), conn, data_dir=None)

        assert sent["channel"] == "telegram"
        assert sent["kwargs"] == {}
        assert result["data"]["delivery"]["ok"] is True
    finally:
        conn.close()


def test_handle_test_multi_user_requires_channel_config(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="notify_channel_set"):
            notify_cmd.handle_test(_make_args(channel="telegram"), conn, data_dir=tmp_path)


def test_handle_test_dry_run_includes_resolved_creds(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO notification_channels (channel, config, label) VALUES (?, ?, ?)",
            ("telegram", json.dumps({"chat_id": "dry-run-chat"}), "primary"),
        )
        conn.commit()

        result = notify_cmd.handle_test(
            _make_args(channel="telegram", dry_run=True),
            conn,
            data_dir=tmp_path,
        )

    assert result["data"]["delivery"]["resolved_creds"] == {"chat_id": "dry-run-chat"}
    assert result["data"]["delivery"]["dry_run"] is True


def test_handle_channel_set_and_list_returns_row(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        notify_cmd.handle_channel_set(
            _make_args(channel="telegram", config='{"chat_id":"123"}', label="primary"),
            conn,
        )
        result = notify_cmd.handle_channel_list(_make_args(), conn)

    assert result["summary"]["count"] == 1
    assert result["data"]["channels"][0]["channel"] == "telegram"
    assert result["data"]["channels"][0]["config"] == {"chat_id": "123"}
    assert result["data"]["channels"][0]["label"] == "primary"


def test_handle_channel_list_reports_telegram_fallback(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO telegram_config (
                id, bot_token_ref, bot_username, bot_first_name, bot_id, chat_id
            ) VALUES (1, 'env:telegram', 'cashnerd_bot', 'CashNerd', 1, 'tg-fallback')
            """
        )
        conn.commit()

        result = notify_cmd.handle_channel_list(_make_args(), conn)

    assert result["data"]["telegram_fallback_configured"] is True
    assert result["data"]["telegram_fallback_chat_id"] == "tg-fallback"


def test_handle_channel_set_rejects_invalid_channel(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="Unsupported notification channel"):
            notify_cmd.handle_channel_set(
                _make_args(channel="email", config='{"address":"x@example.com"}'),
                conn,
            )


def test_handle_channel_set_rejects_missing_required_key(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="must include 'chat_id'"):
            notify_cmd.handle_channel_set(
                _make_args(channel="telegram", config='{"label":"broken"}'),
                conn,
            )


def test_handle_channel_remove_deletes_row(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        notify_cmd.handle_channel_set(
            _make_args(channel="telegram", config='{"chat_id":"123"}', label="primary"),
            conn,
        )
        removed = notify_cmd.handle_channel_remove(_make_args(channel="telegram"), conn)
        listed = notify_cmd.handle_channel_list(_make_args(), conn)

    assert removed["data"]["deleted"] is True
    assert listed["data"]["channels"] == []


def test_handle_channel_remove_dry_run_keeps_row(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        notify_cmd.handle_channel_set(
            _make_args(channel="telegram", config='{"chat_id":"123"}', label="primary"),
            conn,
        )
        preview = notify_cmd.handle_channel_remove(_make_args(channel="telegram", dry_run=True), conn)
        listed = notify_cmd.handle_channel_list(_make_args(), conn)

    assert preview["data"]["dry_run"] is True
    assert preview["data"]["would_delete"] is True
    assert listed["data"]["channels"][0]["channel"] == "telegram"


def test_handle_channel_set_upsert_preserves_created_at(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        notify_cmd.handle_channel_set(
            _make_args(channel="telegram", config='{"chat_id":"first"}', label="primary"),
            conn,
        )
        first_row = conn.execute(
            """
            SELECT created_at, config
            FROM notification_channels
            WHERE channel = 'telegram'
            """
        ).fetchone()

        notify_cmd.handle_channel_set(
            _make_args(channel="telegram", config='{"chat_id":"second"}', label="backup"),
            conn,
        )
        second_row = conn.execute(
            """
            SELECT created_at, config, label
            FROM notification_channels
            WHERE channel = 'telegram'
            """
        ).fetchone()

    assert first_row is not None
    assert second_row is not None
    assert first_row["created_at"] == second_row["created_at"]
    assert json.loads(second_row["config"]) == {"chat_id": "second"}
    assert second_row["label"] == "backup"


def test_notify_cli_parser_registers_channel_subcommands() -> None:
    parser = build_parser()

    parsed_set = parser.parse_args(
        ["notify", "channel-set", "telegram", '{"chat_id":"123"}', "--label", "primary"]
    )
    parsed_list = parser.parse_args(["notify", "channel-list"])
    parsed_remove = parser.parse_args(["notify", "channel-remove", "telegram"])

    assert parsed_set.notify_command == "channel-set"
    assert parsed_set.channel == "telegram"
    assert parsed_set.config == '{"chat_id":"123"}'
    assert parsed_set.label == "primary"
    assert parsed_list.notify_command == "channel-list"
    assert parsed_remove.notify_command == "channel-remove"
    assert parsed_remove.channel == "telegram"
