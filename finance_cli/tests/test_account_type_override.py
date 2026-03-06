from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.plaid_client import _ensure_account


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


def test_migration_019_adds_account_type_override_and_enforces_check(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        account_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()
        }
        assert "account_type_override" in account_columns

        account_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (id, institution_name, account_type, account_type_override, is_active)
            VALUES (?, 'Test Bank', 'checking', 'checking', 1)
            """,
            (account_id,),
        )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE accounts SET account_type_override = 'foo' WHERE id = ?",
                (account_id,),
            )


def test_migration_019_backfills_merrill_cma_edge(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _apply_migrations_up_to(db_path, max_version=18)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (
                id,
                plaid_account_id,
                plaid_item_id,
                institution_name,
                account_name,
                account_type,
                source,
                is_active
            ) VALUES (?, 'plaid_cma_edge', 'item_merrill', 'Merrill', 'CMA-Edge', 'checking', 'plaid', 1)
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO accounts (
                id,
                institution_name,
                account_name,
                account_type,
                source,
                is_active
            ) VALUES (?, 'Merrill', 'CMA-Edge', 'checking', 'manual', 1)
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    initialize_database(db_path)

    with connect(db_path) as conn:
        plaid_row = conn.execute(
            """
            SELECT account_type, account_type_override
              FROM accounts
             WHERE plaid_account_id = 'plaid_cma_edge'
            """
        ).fetchone()
        assert plaid_row["account_type"] == "investment"
        assert plaid_row["account_type_override"] == "investment"

        manual_row = conn.execute(
            """
            SELECT account_type, account_type_override
              FROM accounts
             WHERE institution_name = 'Merrill'
               AND account_name = 'CMA-Edge'
               AND source = 'manual'
            """
        ).fetchone()
        assert manual_row["account_type"] == "checking"
        assert manual_row["account_type_override"] is None


def test_ensure_account_preserves_type_when_override_is_set(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    account_id = uuid.uuid4().hex
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (
                id,
                plaid_account_id,
                plaid_item_id,
                institution_name,
                account_name,
                account_type,
                account_type_override,
                source,
                is_active
            ) VALUES (?, 'acct_override', 'item_old', 'Merrill', 'CMA-Edge', 'investment', 'investment', 'plaid', 1)
            """,
            (account_id,),
        )

        updated_id = _ensure_account(
            conn,
            plaid_item_id="item_new",
            institution_name="Merrill",
            plaid_account_id="acct_override",
            account_payload={"name": "CMA-Edge Updated", "type": "depository", "subtype": "checking"},
        )

        assert updated_id == account_id
        row = conn.execute(
            """
            SELECT account_name, account_type, account_type_override
              FROM accounts
             WHERE id = ?
            """,
            (account_id,),
        ).fetchone()
        assert row["account_name"] == "CMA-Edge Updated"
        assert row["account_type"] == "investment"
        assert row["account_type_override"] == "investment"


def test_ensure_account_updates_type_when_no_override(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    account_id = uuid.uuid4().hex
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (
                id,
                plaid_account_id,
                plaid_item_id,
                institution_name,
                account_name,
                account_type,
                source,
                is_active
            ) VALUES (?, 'acct_no_override', 'item_old', 'Merrill', 'CMA-Edge', 'checking', 'plaid', 1)
            """,
            (account_id,),
        )

        updated_id = _ensure_account(
            conn,
            plaid_item_id="item_new",
            institution_name="Merrill",
            plaid_account_id="acct_no_override",
            account_payload={"name": "CMA-Edge Updated", "type": "investment", "subtype": "brokerage"},
        )

        assert updated_id == account_id
        row = conn.execute(
            """
            SELECT account_type, account_type_override
              FROM accounts
             WHERE id = ?
            """,
            (account_id,),
        ).fetchone()
        assert row["account_type"] == "investment"
        assert row["account_type_override"] is None


def test_ensure_account_preserves_override_when_payload_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    account_id = uuid.uuid4().hex
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (
                id,
                plaid_account_id,
                plaid_item_id,
                institution_name,
                account_name,
                account_type,
                account_type_override,
                source,
                is_active
            ) VALUES (?, 'acct_fallback', 'item_old', 'Merrill', 'CMA-Edge', 'investment', 'investment', 'plaid', 1)
            """,
            (account_id,),
        )

        updated_id = _ensure_account(
            conn,
            plaid_item_id="item_new",
            institution_name="Merrill",
            plaid_account_id="acct_fallback",
            account_payload=None,
        )

        assert updated_id == account_id
        row = conn.execute(
            """
            SELECT account_type, account_type_override
              FROM accounts
             WHERE id = ?
            """,
            (account_id,),
        ).fetchone()
        assert row["account_type"] == "investment"
        assert row["account_type_override"] == "investment"
