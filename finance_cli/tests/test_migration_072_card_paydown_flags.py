from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_072_creates_synced_card_paydown_flags_table(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(card_paydown_flags)").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, balance_current_cents, is_active
            ) VALUES ('card-1', 'High Bank', 'Rewards', 'credit_card', -250000, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO card_paydown_flags (
                id, account_id, status, reason, suggested_payment_cents, source,
                snapshot_json, idempotency_key
            ) VALUES (
                'flag-1', 'card-1', 'active', 'Target next paydown', 50000,
                'agent', '{"balance_cents":250000}', 'card_paydown:card-1'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'card_paydown_flags'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "account_id",
        "liability_id",
        "status",
        "reason",
        "suggested_payment_cents",
        "cash_source_account_id",
        "interest_saved_annual_cents",
        "source",
        "snapshot_json",
        "idempotency_key",
        "resolved_at",
        "created_at",
        "updated_at",
    } <= columns
    assert "card_paydown_flags_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_card_paydown_flags_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": "flag-1"}
    assert json.loads(changelog["new_json"])["account_id"] == "card-1"
    assert changelog["origin_session_id"] == "local-test"
