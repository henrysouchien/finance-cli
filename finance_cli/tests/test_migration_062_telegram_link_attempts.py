from __future__ import annotations

from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_062_creates_telegram_link_attempts(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(telegram_link_attempts)").fetchall()
        }
        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }

    assert max(versions) == db_module.SCHEMA_VERSION
    assert columns == {
        "chat_id",
        "failed_count",
        "first_failed_at",
        "last_failed_at",
        "locked_until",
    }
    assert "idx_tg_link_attempts_locked_until" in indexes
