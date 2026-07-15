from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_061_logs_cost_ledger_changes_with_phase2_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="install-123") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        for op in ("insert", "update", "delete"):
            assert f"_sync_log_cost_ledger_{op}" in trigger_names

        cursor = conn.execute(
            """
            INSERT INTO cost_ledger (
                provider,
                operation,
                cost_usd6,
                input_tokens,
                output_tokens,
                request_id,
                is_byok,
                allowance_debit_usd6,
                credits_debit_usd6,
                overflow_unattributed_usd6
            )
            VALUES ('plaid', 'transactions_sync', 123, 0, 0, 'req-cost-1', 0, 100, 20, 3)
            """
        )
        row_id = int(cursor.lastrowid)
        changelog = conn.execute(
            """
            SELECT op, pk_json, old_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'cost_ledger'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": row_id}
    assert changelog["old_json"] is None
    assert changelog["origin_session_id"] == "install-123"
    payload = json.loads(changelog["new_json"])
    assert payload["provider"] == "plaid"
    assert payload["operation"] == "transactions_sync"
    assert payload["cost_usd6"] == 123
    assert payload["allowance_debit_usd6"] == 100
    assert payload["credits_debit_usd6"] == 20
    assert payload["overflow_unattributed_usd6"] == 3


def test_cost_ledger_stream_apply_does_not_relog(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="__STREAM__") as conn:
        conn.execute(
            """
            INSERT INTO cost_ledger (id, provider, operation, cost_usd6, request_id)
            VALUES (42, 'plaid', 'transactions_sync', 456, 'req-stream')
            """
        )
        count = conn.execute(
            """
            SELECT COUNT(*) AS count
              FROM _sync_changelog
             WHERE table_name = 'cost_ledger'
            """
        ).fetchone()["count"]

    assert count == 0
