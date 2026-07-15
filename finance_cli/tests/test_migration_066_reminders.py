from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_066_creates_synced_reminders_table(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(reminders)").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO reminders (
                id, kind, title, body, due_at, channel, status, payload_json, idempotency_key
            )
            VALUES (
                'reminder-1', 'card_rotation', 'Title', 'Body',
                '2026-05-09 09:00:00', 'telegram', 'pending',
                '{"account_id":"card-1"}', 'idem-1'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'reminders'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "kind",
        "title",
        "body",
        "due_at",
        "channel",
        "status",
        "payload_json",
        "idempotency_key",
        "sent_at",
        "cancelled_at",
        "last_error",
        "created_at",
        "updated_at",
    } <= columns
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_reminders_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": "reminder-1"}
    assert json.loads(changelog["new_json"])["payload_json"] == '{"account_id":"card-1"}'
    assert changelog["origin_session_id"] == "local-test"
