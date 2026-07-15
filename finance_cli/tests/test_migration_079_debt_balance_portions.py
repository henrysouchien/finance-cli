from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_079_creates_synced_debt_balance_portions_table(
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
            for row in conn.execute("PRAGMA table_info(debt_balance_portions)").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type,
                balance_current_cents, is_active
            ) VALUES ('card-1', 'Card Bank', 'Rewards', 'credit_card', -100000, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO debt_balance_portions (
                id, account_id, label, portion_type, principal_cents, apr_pct,
                monthly_payment_cents, source
            ) VALUES (
                'portion-1', 'card-1', 'Plan It', 'installment', 100000, 10.0,
                8960, 'manual'
            )
            """
        )
        conn.execute(
            """
            UPDATE debt_balance_portions
               SET principal_cents = 90000
             WHERE id = 'portion-1'
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'debt_balance_portions'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "account_id",
        "label",
        "portion_type",
        "principal_cents",
        "apr_pct",
        "monthly_payment_cents",
        "start_date",
        "promo_end_date",
        "source",
        "is_active",
        "notes",
        "created_at",
        "updated_at",
    } <= columns
    assert "debt_balance_portions_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_debt_balance_portions_{op}" in trigger_names
    assert changelog["op"] == "UPDATE"
    assert json.loads(changelog["pk_json"]) == {"id": "portion-1"}
    assert json.loads(changelog["new_json"])["principal_cents"] == 90000
    assert changelog["origin_session_id"] == "local-test"
