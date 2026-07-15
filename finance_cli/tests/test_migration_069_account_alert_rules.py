from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_069_creates_synced_account_alert_rules_table(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(account_alert_rules)").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, balance_current_cents, is_active
            ) VALUES ('checking-1', 'Test Bank', 'Checking', 'checking', 40000, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO account_alert_rules (
                id, rule_type, account_id, threshold_cents, channel, status, payload_json, idempotency_key
            ) VALUES (
                'rule-1', 'low_balance', 'checking-1', 50000, 'telegram', 'active', '{}',
                'low_balance:checking-1:telegram'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'account_alert_rules'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "rule_type",
        "account_id",
        "threshold_cents",
        "channel",
        "status",
        "cooldown_hours",
        "last_triggered_at",
        "last_error",
        "payload_json",
        "idempotency_key",
        "created_at",
        "updated_at",
    } <= columns
    assert "account_alert_rules_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_account_alert_rules_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": "rule-1"}
    assert json.loads(changelog["new_json"])["threshold_cents"] == 50000
    assert changelog["origin_session_id"] == "local-test"
