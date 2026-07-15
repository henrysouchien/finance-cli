from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


TRACKED_TABLES = (
    "transactions",
    "categories",
    "vendor_memory",
    "budgets",
    "subscriptions",
    "goals",
    "manual_loans",
    "accounts",
    "balance_snapshots",
    "liabilities",
    "import_batches",
    "category_mappings",
    "notification_channels",
    "mileage_log",
    "contractors",
    "contractor_payments",
)


def test_migration_056_creates_sync_changelog_and_triggers(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    expected_triggers = {
        f"_sync_log_{table}_{op}"
        for table in TRACKED_TABLES
        for op in ("insert", "update", "delete")
    }

    with connect(db_path) as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        assert max(versions) == db_module.SCHEMA_VERSION

        changelog_columns = {
            row["name"]: row["type"]
            for row in conn.execute("PRAGMA table_info(_sync_changelog)").fetchall()
        }
        assert changelog_columns == {
            "id": "INTEGER",
            "table_name": "TEXT",
            "op": "TEXT",
            "pk_json": "TEXT",
            "old_json": "TEXT",
            "new_json": "TEXT",
            "created_at": "TEXT",
            "origin_session_id": "TEXT",
        }

        trigger_names = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
        }
        assert expected_triggers <= trigger_names


def test_migration_056_logs_transaction_and_notification_channel_changes(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-sync-1', '2026-04-16', 'Coffee', -550, 'manual', 1)
            """
        )
        conn.execute(
            "UPDATE transactions SET notes = 'morning run' WHERE id = 'txn-sync-1'"
        )
        conn.execute("DELETE FROM transactions WHERE id = 'txn-sync-1'")

        txn_rows = conn.execute(
            """
            SELECT op, pk_json, old_json, new_json
              FROM _sync_changelog
             WHERE table_name = 'transactions'
             ORDER BY id
            """
        ).fetchall()
        assert [row["op"] for row in txn_rows] == ["INSERT", "UPDATE", "DELETE"]

        insert_pk = json.loads(txn_rows[0]["pk_json"])
        insert_new = json.loads(txn_rows[0]["new_json"])
        assert insert_pk == {"id": "txn-sync-1"}
        assert txn_rows[0]["old_json"] is None
        assert insert_new["description"] == "Coffee"

        update_old = json.loads(txn_rows[1]["old_json"])
        update_new = json.loads(txn_rows[1]["new_json"])
        assert update_old["notes"] is None
        assert update_new["notes"] == "morning run"

        delete_old = json.loads(txn_rows[2]["old_json"])
        assert txn_rows[2]["new_json"] is None
        assert delete_old["id"] == "txn-sync-1"

        conn.execute(
            """
            INSERT INTO notification_channels (channel, config, label)
            VALUES ('telegram', '{"chat_id":"123"}', 'Primary')
            """
        )
        conn.execute(
            """
            UPDATE notification_channels
               SET label = 'Updated'
             WHERE channel = 'telegram'
            """
        )
        conn.execute("DELETE FROM notification_channels WHERE channel = 'telegram'")

        channel_rows = conn.execute(
            """
            SELECT op, pk_json, old_json, new_json
              FROM _sync_changelog
             WHERE table_name = 'notification_channels'
             ORDER BY id
            """
        ).fetchall()
        assert [row["op"] for row in channel_rows] == ["INSERT", "UPDATE", "DELETE"]

        channel_insert = json.loads(channel_rows[0]["pk_json"])
        channel_update_old = json.loads(channel_rows[1]["old_json"])
        channel_update_new = json.loads(channel_rows[1]["new_json"])
        channel_delete_old = json.loads(channel_rows[2]["old_json"])

        assert channel_insert == {"channel": "telegram"}
        assert channel_update_old["label"] == "Primary"
        assert channel_update_new["label"] == "Updated"
        assert channel_delete_old["channel"] == "telegram"
