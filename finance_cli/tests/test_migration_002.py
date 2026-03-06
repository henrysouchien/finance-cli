from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database


def _apply_001_only(db_path: Path) -> None:
    migration_sql = (Path(__file__).resolve().parents[1] / "migrations" / "001_initial.sql").read_text(encoding="utf-8")

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(migration_sql)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version     INTEGER PRIMARY KEY,
                applied_at  TEXT DEFAULT (datetime('now')),
                description TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO schema_version (version, description) VALUES (1, '001_initial.sql')"
        )

        conn.execute(
            """
            INSERT INTO transactions (
                id,
                date,
                description,
                amount_cents,
                category_source,
                source
            ) VALUES (?, '2025-01-01', 'Legacy Migration Coffee', -1200, 'plaid', 'manual')
            """,
            ("legacy_txn",),
        )
        conn.commit()


def test_migration_002_applies_and_rebuilds_fts(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _apply_001_only(db_path)
    migration_dir = Path(__file__).resolve().parents[1] / "migrations"
    expected_versions = sorted(int(path.name.split("_", 1)[0]) for path in migration_dir.glob("*.sql"))

    initialize_database(db_path)

    with connect(db_path) as conn:
        version_rows = conn.execute("SELECT version FROM schema_version ORDER BY version").fetchall()
        versions = [int(row["version"]) for row in version_rows]
        assert versions == expected_versions

        columns = {row["name"] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()}
        assert "split_group_id" in columns
        assert "parent_transaction_id" in columns
        assert "split_pct" in columns
        assert "split_note" in columns

        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
        assert "idx_txn_split_group" in indexes
        assert "idx_txn_parent" in indexes

        migrated = conn.execute(
            "SELECT id, description FROM transactions WHERE id = 'legacy_txn'"
        ).fetchone()
        assert migrated is not None
        assert migrated["description"] == "Legacy Migration Coffee"

        fts_rows = conn.execute(
            """
            SELECT t.id
              FROM txn_fts f
              JOIN transactions t ON t.rowid = f.rowid
             WHERE txn_fts MATCH 'legacy'
            """
        ).fetchall()
        assert [row["id"] for row in fts_rows] == ["legacy_txn"]

        new_txn_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO transactions (
                id,
                date,
                description,
                amount_cents,
                category_source,
                source
            ) VALUES (?, '2025-01-02', 'Post Migration Searchable', -500, 'keyword_rule', 'pdf_import')
            """,
            (new_txn_id,),
        )
        conn.commit()

        fts_new_rows = conn.execute(
            """
            SELECT t.id
              FROM txn_fts f
              JOIN transactions t ON t.rowid = f.rowid
             WHERE txn_fts MATCH 'searchable'
            """
        ).fetchall()
        assert [row["id"] for row in fts_new_rows] == [new_txn_id]

        conn.execute(
            """
            INSERT INTO import_batches (
                id,
                source_type,
                file_path,
                file_hash_sha256,
                bank_parser
            ) VALUES (?, 'csv', '/tmp/a.csv', 'hash-abc', 'ai:gpt-test')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

        conn.execute(
            """
            INSERT INTO import_batches (
                id,
                source_type,
                file_path,
                file_hash_sha256,
                bank_parser
            ) VALUES (?, 'csv', '/tmp/b.csv', 'hash-abc', 'azure:prebuilt-bankStatement.us')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO import_batches (
                    id,
                    source_type,
                    file_path,
                    file_hash_sha256,
                    bank_parser
                ) VALUES (?, 'csv', '/tmp/c.csv', 'hash-abc', 'ai:gpt-test')
                """,
                (uuid.uuid4().hex,),
            )
            conn.commit()

        import_batch_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(import_batches)").fetchall()
        }
        assert "ai_raw_output_json" in import_batch_columns
        assert "ai_validation_json" in import_batch_columns
        assert "ai_model" in import_batch_columns
        assert "ai_prompt_version" in import_batch_columns
        assert "ai_prompt_hash" in import_batch_columns
        assert "content_hash_sha256" in import_batch_columns
        assert "total_charges_cents" in import_batch_columns
        assert "total_payments_cents" in import_batch_columns
        assert "new_balance_cents" in import_batch_columns
        assert "expected_transaction_count" in import_batch_columns

        ai_log_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(ai_categorization_log)").fetchall()
        }
        assert "transaction_id" in ai_log_columns
        assert "batch_id" in ai_log_columns

        account_columns = {row["name"] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()}
        assert "balance_current_cents" in account_columns
        assert "balance_available_cents" in account_columns
        assert "balance_limit_cents" in account_columns
        assert "iso_currency_code" in account_columns
        assert "unofficial_currency_code" in account_columns
        assert "balance_updated_at" in account_columns

        snapshot_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(balance_snapshots)").fetchall()
        }
        assert "account_id" in snapshot_columns
        assert "source" in snapshot_columns

        liability_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(liabilities)").fetchall()
        }
        assert "account_id" in liability_columns
        assert "liability_type" in liability_columns
        assert "is_active" in liability_columns

        plaid_item_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(plaid_items)").fetchall()
        }
        assert "last_sync_at" in plaid_item_columns
        assert "last_balance_refresh_at" in plaid_item_columns
        assert "last_liabilities_fetch_at" in plaid_item_columns
        assert "institution_id" in plaid_item_columns
