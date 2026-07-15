from __future__ import annotations

import os
import json
from pathlib import Path
import subprocess
import sys
import textwrap

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_078_adds_plaid_consent_expiration_and_syncs_change_feed(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(plaid_items)").fetchall()
        }
        conn.execute(
            """
            INSERT INTO plaid_items (
                id, plaid_item_id, institution_name, status, consent_expiration_time
            ) VALUES ('plaid-1', 'item-1', 'Test Bank', 'active', '2027-06-01T00:00:00Z')
            """
        )
        conn.execute(
            """
            UPDATE plaid_items
               SET consent_expiration_time = '2027-07-01T00:00:00Z'
             WHERE id = 'plaid-1'
            """
        )
        conn.commit()

        row = conn.execute(
            """
            SELECT op, new_json
              FROM _sync_changelog
             WHERE table_name = 'plaid_items'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert "consent_expiration_time" in columns
    assert row["op"] == "UPDATE"
    assert json.loads(row["new_json"])["consent_expiration_time"] == "2027-07-01T00:00:00Z"


def test_pending_special_migrations_are_serialized_across_processes(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        conn.execute("DELETE FROM schema_version WHERE version IN (78, 79)")
        conn.commit()

    script = textwrap.dedent(
        f"""
        import time
        from pathlib import Path
        from finance_cli import db as db_module

        original = db_module._apply_plaid_consent_expiration_migration

        def slow_plaid_consent_migration(conn):
            time.sleep(0.75)
            original(conn)

        db_module._apply_plaid_consent_expiration_migration = slow_plaid_consent_migration
        db_module.initialize_database(Path({str(db_path)!r}))
        """
    )
    env = os.environ.copy()
    env["FINANCE_CLI_REQUIRE_DB_ENCRYPTION"] = "off"

    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[2],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]

    results = [process.communicate(timeout=15) for process in processes]
    failures = [
        (process.returncode, stdout, stderr)
        for process, (stdout, stderr) in zip(processes, results, strict=True)
        if process.returncode != 0
    ]

    assert failures == []
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT version, COUNT(*) AS n FROM schema_version WHERE version IN (78, 79) GROUP BY version"
        ).fetchall()

    assert {int(row["version"]): int(row["n"]) for row in rows} == {78: 1, 79: 1}
