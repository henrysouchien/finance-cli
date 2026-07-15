from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_075_creates_synced_savings_automations_table(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(savings_automations)").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO goals (
                id, name, metric, target_cents, starting_cents, direction, deadline, is_active
            ) VALUES ('goal-1', 'House Fund', 'liquid_cash', 2000000, 500000, 'up', '2030-01-01', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO savings_automations (
                id, goal_id, funding_method, cadence, amount_cents, start_date,
                day_of_month, target_amount_cents, projected_end_balance_cents,
                goal_date, reason, source, snapshot_json, idempotency_key
            ) VALUES (
                'auto-1', 'goal-1', 'auto_transfer', 'monthly', 50000,
                '2026-06-01', 1, 2000000, 2100000, '2030-01-01',
                'Lock in the savings pace', 'agent', '{"goal_name":"House Fund"}',
                'savings_automation:goal-1'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'savings_automations'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "goal_id",
        "status",
        "funding_method",
        "cadence",
        "amount_cents",
        "start_date",
        "day_of_month",
        "source_account_id",
        "destination_account_id",
        "target_amount_cents",
        "projected_end_balance_cents",
        "goal_date",
        "reason",
        "source",
        "snapshot_json",
        "idempotency_key",
        "created_at",
        "updated_at",
    } <= columns
    assert "savings_automations_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_savings_automations_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": "auto-1"}
    assert json.loads(changelog["new_json"])["amount_cents"] == 50000
    assert changelog["origin_session_id"] == "local-test"
