from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_073_creates_synced_retirement_contribution_targets_table(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(retirement_contribution_targets)"
            ).fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO retirement_contribution_targets (
                id, tax_year, account_type, monthly_target_cents, start_month,
                end_month, room_remaining_cents, estimated_tax_savings_cents,
                deadline, reason, source, payload_json, idempotency_key
            ) VALUES (
                'target-1', 2026, 'sep_ira', 100000, '2026-10',
                '2026-12', 300000, 75000, '2026-12-31',
                'Year-end contribution target', 'agent',
                '{"months_count":3}', 'retirement_target:2026:sep_ira:2026-10:2026-12'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'retirement_contribution_targets'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "tax_year",
        "account_type",
        "status",
        "monthly_target_cents",
        "start_month",
        "end_month",
        "room_remaining_cents",
        "annual_limit_cents",
        "contributed_ytd_cents",
        "estimated_tax_savings_cents",
        "deadline",
        "reason",
        "source",
        "payload_json",
        "idempotency_key",
        "resolved_at",
        "created_at",
        "updated_at",
    } <= columns
    assert "retirement_contribution_targets_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_retirement_contribution_targets_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": "target-1"}
    assert json.loads(changelog["new_json"])["monthly_target_cents"] == 100000
    assert changelog["origin_session_id"] == "local-test"
