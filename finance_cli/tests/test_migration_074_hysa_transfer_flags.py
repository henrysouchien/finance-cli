from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_074_creates_synced_hysa_transfer_flags_table(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(hysa_transfer_flags)").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, balance_current_cents, is_active
            ) VALUES ('checking-1', 'Cash Bank', 'Checking', 'checking', 800000, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO hysa_transfer_flags (
                id, account_id, current_balance_cents, suggested_transfer_cents,
                retained_buffer_cents, minimum_balance_cents, current_apy_bps,
                hysa_apy_bps, estimated_annual_yield_cents, observed_since,
                lookback_days, reason, source, snapshot_json, idempotency_key
            ) VALUES (
                'flag-1', 'checking-1', 800000, 600000, 200000, 200000,
                1, 450, 26940, '2026-02-20', 90,
                'Surplus checking cash', 'agent', '{"evidence_points":3}',
                'hysa_transfer:checking-1'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'hysa_transfer_flags'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "account_id",
        "status",
        "current_balance_cents",
        "suggested_transfer_cents",
        "retained_buffer_cents",
        "minimum_balance_cents",
        "current_apy_bps",
        "hysa_apy_bps",
        "estimated_annual_yield_cents",
        "observed_since",
        "lookback_days",
        "reason",
        "source",
        "snapshot_json",
        "idempotency_key",
        "resolved_at",
        "created_at",
        "updated_at",
    } <= columns
    assert "hysa_transfer_flags_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_hysa_transfer_flags_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": "flag-1"}
    assert json.loads(changelog["new_json"])["suggested_transfer_cents"] == 600000
    assert changelog["origin_session_id"] == "local-test"
