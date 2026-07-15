from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_065_adds_intro_apr_end_date_and_syncs_change_feed(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(liabilities)").fetchall()
        }
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, balance_current_cents, is_active
            ) VALUES ('card-1', 'Test Bank', 'Intro Card', 'credit_card', -10000, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO liabilities (
                id, account_id, liability_type, is_active, apr_purchase, intro_apr_end_date
            ) VALUES ('liability-1', 'card-1', 'credit', 1, 0.0, '2026-12-31')
            """
        )
        conn.execute(
            """
            UPDATE liabilities
               SET intro_apr_end_date = '2027-01-31'
             WHERE id = 'liability-1'
            """
        )
        conn.commit()

        row = conn.execute(
            """
            SELECT op, new_json
              FROM _sync_changelog
             WHERE table_name = 'liabilities'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert "intro_apr_end_date" in columns
    assert row["op"] == "UPDATE"
    assert json.loads(row["new_json"])["intro_apr_end_date"] == "2027-01-31"
