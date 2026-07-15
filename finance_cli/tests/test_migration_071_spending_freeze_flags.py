from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_071_creates_synced_spending_freeze_flags_table(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(spending_freeze_flags)").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO spending_freeze_flags (
                id, scope, status, reason, bill_name, bill_amount_cents,
                due_date, hold_until, target_balance_after_cents, source,
                payload_json, idempotency_key
            ) VALUES (
                'freeze-1', 'discretionary', 'active', 'Hold until rent clears',
                'Rent', 250000, '2099-06-01', '2099-06-01', 45000,
                'agent', '{}', 'spending_freeze:discretionary:-:-:2099-06-01:rent'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'spending_freeze_flags'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "scope",
        "status",
        "account_id",
        "category_id",
        "reason",
        "bill_name",
        "bill_amount_cents",
        "due_date",
        "hold_until",
        "target_balance_after_cents",
        "source",
        "payload_json",
        "idempotency_key",
        "resolved_at",
        "created_at",
        "updated_at",
    } <= columns
    assert "spending_freeze_flags_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_spending_freeze_flags_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": "freeze-1"}
    assert json.loads(changelog["new_json"])["hold_until"] == "2099-06-01"
    assert changelog["origin_session_id"] == "local-test"
