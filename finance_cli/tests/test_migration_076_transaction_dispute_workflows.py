from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_076_creates_synced_transaction_dispute_workflows_table(
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
                "PRAGMA table_info(transaction_dispute_workflows)"
            ).fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, balance_current_cents, is_active
            ) VALUES ('card-1', 'Card Bank', 'Rewards', 'credit_card', -50000, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents, is_active, source
            ) VALUES ('txn-1', 'card-1', '2026-05-01', 'ACME', -5000, 1, 'manual')
            """
        )
        conn.execute(
            """
            INSERT INTO transaction_dispute_workflows (
                id, transaction_id, account_id, dispute_reason, amount_cents,
                merchant_name, transaction_date, note, source, snapshot_json,
                idempotency_key
            ) VALUES (
                'workflow-1', 'txn-1', 'card-1', 'unrecognized_merchant', 5000,
                'ACME', '2026-05-01', 'User wants to verify', 'agent',
                '{"transaction":{"id":"txn-1"}}',
                'txn_dispute:txn-1:-:unrecognized_merchant'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'transaction_dispute_workflows'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "transaction_id",
        "duplicate_transaction_id",
        "account_id",
        "status",
        "dispute_reason",
        "amount_cents",
        "merchant_name",
        "transaction_date",
        "duplicate_date",
        "note",
        "source",
        "snapshot_json",
        "idempotency_key",
        "created_at",
        "updated_at",
    } <= columns
    assert "transaction_dispute_workflows_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_transaction_dispute_workflows_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": "workflow-1"}
    assert json.loads(changelog["new_json"])["amount_cents"] == 5000
    assert changelog["origin_session_id"] == "local-test"
