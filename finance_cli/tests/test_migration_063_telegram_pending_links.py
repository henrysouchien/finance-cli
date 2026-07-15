from __future__ import annotations

from pathlib import Path

from finance_cli.db import initialize_database


def test_migration_063_creates_telegram_pending_links(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    import sqlite3

    with sqlite3.connect(str(db_path)) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(telegram_pending_links)").fetchall()
        }
        indexes = {
            row[1]
            for row in conn.execute("PRAGMA index_list(telegram_pending_links)").fetchall()
        }

    assert {
        "id",
        "chat_id",
        "telegram_user_id",
        "telegram_username",
        "telegram_first_name",
        "telegram_last_name",
        "requested_at",
        "expires_at",
    } <= columns
    assert "idx_tg_pending_links_expires_at" in indexes
