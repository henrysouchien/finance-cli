from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def _apply_migrations_up_to(db_path: Path, max_version: int) -> None:
    migration_dir = Path(__file__).resolve().parents[1] / "migrations"
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version     INTEGER PRIMARY KEY,
                applied_at  TEXT DEFAULT (datetime('now')),
                description TEXT
            )
            """
        )
        for path in sorted(migration_dir.glob("*.sql")):
            version = int(path.name.split("_", 1)[0])
            if version > max_version:
                continue
            conn.executescript(path.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO schema_version (version, description) VALUES (?, ?)",
                (version, path.name),
            )
        conn.commit()


def test_migration_015_rebuilds_sources_and_schema_objects(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _apply_migrations_up_to(db_path, max_version=14)

    ids = {
        "category": uuid.uuid4().hex,
        "rule": uuid.uuid4().hex,
        "csv_reset": uuid.uuid4().hex,
        "csv_keep_rule": uuid.uuid4().hex,
        "pdf_keep": uuid.uuid4().hex,
        "plaid_keep": uuid.uuid4().hex,
        "manual_keep": uuid.uuid4().hex,
        "fts": uuid.uuid4().hex,
        "ai_log": uuid.uuid4().hex,
    }

    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO categories (id, name, is_system) VALUES (?, 'Dining', 1)",
            (ids["category"],),
        )
        conn.execute(
            """
            INSERT INTO vendor_memory (
                id, description_pattern, category_id, use_type, confidence, priority, is_enabled, is_confirmed, match_count
            ) VALUES (?, 'merchant', ?, 'Any', 1.0, 0, 1, 1, 0)
            """,
            (ids["rule"], ids["category"]),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, category_source, category_confidence, source
            ) VALUES (?, '2026-02-01', 'CSV RESET', -1000, ?, 'user', 1.0, 'csv_import')
            """,
            (ids["csv_reset"], ids["category"]),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, category_source, category_confidence, category_rule_id, source
            ) VALUES (?, '2026-02-01', 'CSV KEEP RULE', -1000, ?, 'user', 1.0, ?, 'csv_import')
            """,
            (ids["csv_keep_rule"], ids["category"], ids["rule"]),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, category_source, category_confidence, source
            ) VALUES (?, '2026-02-01', 'PDF KEEP', -1000, ?, 'user', 1.0, 'pdf_import')
            """,
            (ids["pdf_keep"], ids["category"]),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, category_source, category_confidence, source
            ) VALUES (?, '2026-02-01', 'PLAID KEEP', -1000, ?, 'user', 1.0, 'plaid')
            """,
            (ids["plaid_keep"], ids["category"]),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, category_source, category_confidence, source
            ) VALUES (?, '2026-02-01', 'MANUAL KEEP', -1000, ?, 'user', 1.0, 'manual')
            """,
            (ids["manual_keep"], ids["category"]),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, category_source, category_confidence, source
            ) VALUES (?, '2026-02-01', 'FTS NEEDLE', -1000, ?, 'user', 1.0, 'manual')
            """,
            (ids["fts"], ids["category"]),
        )
        conn.execute(
            """
            INSERT INTO ai_categorization_log (
                id, batch_id, transaction_id, provider, model, category_name, use_type, confidence, reasoning, prompt_hash
            ) VALUES (?, 'batch_1', ?, 'openai', 'gpt-4.1', 'Dining', 'Personal', 0.7, 'seed', 'abc')
            """,
            (ids["ai_log"], ids["fts"]),
        )
        conn.commit()

    initialize_database(db_path)

    with connect(db_path) as conn:
        category_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(categories)").fetchall()
        }
        assert "level" in category_columns
        level = conn.execute(
            "SELECT level FROM categories WHERE id = ?",
            (ids["category"],),
        ).fetchone()
        assert level["level"] == 0

        csv_reset = conn.execute(
            """
            SELECT category_id, category_source, category_confidence, category_rule_id
              FROM transactions
             WHERE id = ?
            """,
            (ids["csv_reset"],),
        ).fetchone()
        assert csv_reset["category_source"] == "institution"
        assert csv_reset["category_id"] is None
        assert csv_reset["category_confidence"] is None
        assert csv_reset["category_rule_id"] is None

        csv_keep_rule = conn.execute(
            """
            SELECT category_id, category_source, category_confidence, category_rule_id
              FROM transactions
             WHERE id = ?
            """,
            (ids["csv_keep_rule"],),
        ).fetchone()
        assert csv_keep_rule["category_source"] == "user"
        assert csv_keep_rule["category_id"] == ids["category"]
        assert csv_keep_rule["category_rule_id"] == ids["rule"]

        preserved_sources = {
            row["description"]: row["category_source"]
            for row in conn.execute(
                """
                SELECT description, category_source
                  FROM transactions
                 WHERE id IN (?, ?, ?)
                """,
                (ids["pdf_keep"], ids["plaid_keep"], ids["manual_keep"]),
            ).fetchall()
        }
        assert preserved_sources == {
            "PDF KEEP": "user",
            "PLAID KEEP": "user",
            "MANUAL KEEP": "user",
        }

        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, category_source, source)
            VALUES (?, '2026-02-02', 'INSTITUTION CHECK', -500, 'institution', 'manual')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

        indexes = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
        }
        assert "idx_txn_recurring" in indexes
        assert "idx_ai_log_batch" in indexes
        assert "idx_ai_log_txn" in indexes

        triggers = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        assert {"txn_ai", "txn_ad", "txn_au"} <= triggers

        fts_rows = conn.execute(
            """
            SELECT t.id
              FROM txn_fts f
              JOIN transactions t ON t.rowid = f.rowid
             WHERE txn_fts MATCH 'needle'
            """
        ).fetchall()
        assert [row["id"] for row in fts_rows] == [ids["fts"]]

        ai_log_count = conn.execute("SELECT COUNT(*) AS n FROM ai_categorization_log").fetchone()["n"]
        assert ai_log_count == 1


def test_run_pending_migrations_creates_backup_before_015(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    _apply_migrations_up_to(db_path, max_version=14)

    calls: list[Path | None] = []

    def _fake_backup(*, conn=None, db_path=None, destination=None):
        del conn, destination
        calls.append(db_path)
        return (db_path or Path("backup.db")).with_name("backup.db")

    monkeypatch.setattr(db_module, "backup_database", _fake_backup)

    initialize_database(db_path)
    assert calls == [db_path.resolve()]

    initialize_database(db_path)
    assert calls == [db_path.resolve()]
