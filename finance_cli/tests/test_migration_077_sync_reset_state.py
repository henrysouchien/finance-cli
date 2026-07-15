from __future__ import annotations

from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_077_creates_sync_reset_state_seed(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        row = conn.execute(
            """
            SELECT id, reset_epoch, reset_reason, reset_at, origin_session_id
              FROM sync_reset_state
             WHERE id = 0
            """
        ).fetchone()
        trigger = conn.execute(
            """
            SELECT name
              FROM sqlite_master
             WHERE type = 'trigger'
               AND name LIKE '_sync_log_sync_reset_state_%'
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert row["id"] == 0
    assert len(row["reset_epoch"]) == 32
    assert row["reset_reason"] == "initial"
    assert row["reset_at"]
    assert row["origin_session_id"] == ""
    assert trigger is None
