from __future__ import annotations

import sqlite3
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.importers import _account_id_for_source, import_normalized_rows, upsert_account_alias
from finance_cli.importers.pdf import ExtractResult, import_extracted_statement, import_pdf_statement
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


def _apply_migration(db_path: Path, version: int) -> None:
    migration_dir = Path(__file__).resolve().parents[1] / "migrations"
    path = next(path for path in sorted(migration_dir.glob("*.sql")) if int(path.name.split("_", 1)[0]) == version)
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(path.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO schema_version (version, description) VALUES (?, ?)",
            (version, path.name),
        )
        conn.commit()


def _insert_account(
    conn,
    *,
    account_id: str,
    institution_name: str,
    account_type: str,
    card_ending: str | None,
    source: str,
    account_name: str | None = None,
    plaid_account_id: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id,
            plaid_account_id,
            institution_name,
            account_name,
            account_type,
            card_ending,
            source,
            is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            account_id,
            plaid_account_id,
            institution_name,
            account_name or f"{institution_name} {card_ending or ''}".strip(),
            account_type,
            card_ending,
            source,
        ),
    )


def test_storage_guard_scrubs_non_numeric_card_ending_on_account_insert(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rows = [
        {
            "Date": "2026-02-10",
            "Description": "APPLE PURCHASE",
            "Amount": "-2.00",
            "Card Ending": "Apple",
            "Account Type": "credit_card",
            "Source": "Apple Card",
            "Is Payment": "false",
        }
    ]

    with connect(db_path) as conn:
        import_normalized_rows(conn, rows, "Apple Card", validate_name=False)
        account = conn.execute(
            "SELECT id, account_name, card_ending FROM accounts"
        ).fetchone()

    assert account["id"] == _account_id_for_source("Apple Card", "Apple")
    assert account["account_name"] == "Apple Card"
    assert account["card_ending"] is None


def test_migration_bridges_clean_hash_to_existing_canonical_account(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _apply_migrations_up_to(db_path, max_version=50)

    old_hash = _account_id_for_source("Apple Card", "Apple")
    new_hash = _account_id_for_source("Apple Card", "")
    plaid_id = "plaid_apple_card"

    with connect(db_path) as conn:
        _insert_account(
            conn,
            account_id=old_hash,
            institution_name="Apple Card",
            account_type="credit_card",
            card_ending="Apple",
            source="csv_import",
            account_name="Apple Card",
        )
        _insert_account(
            conn,
            account_id=plaid_id,
            institution_name="Apple Card",
            account_type="credit_card",
            card_ending=None,
            source="plaid",
            plaid_account_id="plaid_ext_apple_card",
            account_name="Apple Card",
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (old_hash, plaid_id),
        )
        conn.commit()

    _apply_migration(db_path, version=51)

    rows = [
        {
            "Date": "2026-02-10",
            "Description": "APPLE PURCHASE",
            "Amount": "-2.00",
            "Card Ending": "",
            "Account Type": "credit_card",
            "Source": "Apple Card",
            "Is Payment": "false",
        }
    ]

    with connect(db_path) as conn:
        migrated_alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (new_hash,),
        ).fetchone()
        old_row = conn.execute(
            "SELECT card_ending FROM accounts WHERE id = ?",
            (old_hash,),
        ).fetchone()
        stub_row = conn.execute(
            "SELECT account_name, card_ending FROM accounts WHERE id = ?",
            (new_hash,),
        ).fetchone()

        import_normalized_rows(conn, rows, "Apple Card", validate_name=False)
        txn = conn.execute(
            "SELECT account_id FROM transactions"
        ).fetchone()
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]

    assert migrated_alias is not None
    assert migrated_alias["canonical_id"] == plaid_id
    assert old_row["card_ending"] is None
    assert stub_row["account_name"] == "Apple Card"
    assert stub_row["card_ending"] is None
    assert txn["account_id"] == plaid_id
    assert account_count == 3


def test_upsert_account_alias_flattens_existing_alias_chains(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    old_hash = "hash_old"
    new_hash = "hash_new"
    plaid_id = "plaid_card"

    with connect(db_path) as conn:
        _insert_account(
            conn,
            account_id=old_hash,
            institution_name="Apple Card",
            account_type="credit_card",
            card_ending=None,
            source="csv_import",
            account_name="Apple Card",
        )
        _insert_account(
            conn,
            account_id=new_hash,
            institution_name="Apple Card",
            account_type="credit_card",
            card_ending=None,
            source="csv_import",
            account_name="Apple Card",
        )
        _insert_account(
            conn,
            account_id=plaid_id,
            institution_name="Apple Card",
            account_type="credit_card",
            card_ending=None,
            source="plaid",
            plaid_account_id="plaid_ext_card",
            account_name="Apple Card",
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (new_hash, old_hash),
        )

        upsert_account_alias(conn, hash_account_id=old_hash, canonical_id=plaid_id)
        aliases = conn.execute(
            "SELECT hash_account_id, canonical_id FROM account_aliases ORDER BY hash_account_id"
        ).fetchall()

    assert [tuple(row) for row in aliases] == [
        (new_hash, plaid_id),
        (old_hash, plaid_id),
    ]


def test_legacy_apple_pdf_import_preserves_credit_card_type_without_card_ending(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "apple.pdf"
    pdf_path.write_bytes(b"dummy")

    sample_text = "01/30/2026 ELECTRONICS STORE SAN FRANCISCO CA USA $750.00"
    monkeypatch.setattr("finance_cli.importers.pdf._extract_pdf_text", lambda _: sample_text)

    with connect(db_path) as conn:
        result = import_pdf_statement(conn, pdf_path=pdf_path, bank="apple", dry_run=False)
        account = conn.execute(
            "SELECT account_type, card_ending FROM accounts"
        ).fetchone()

    assert result["inserted"] == 1
    assert account["account_type"] == "credit_card"
    assert account["card_ending"] is None


def test_pdf_import_scrubs_non_numeric_card_ending_before_hashing(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "scrubbed.pdf"
    pdf_path.write_bytes(b"dummy")

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2026-02-10",
                "description": "APPLE PURCHASE",
                "amount_cents": -200,
                "source": "Apple Card",
                "card_ending": "Apple",
            }
        ],
        extracted_total_cents=-200,
        reconciled=True,
        warnings=[],
        statement_card_ending="Apple",
        statement_account_type="credit_card",
    )

    with connect(db_path) as conn:
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude", validate_name=False)
        txn = conn.execute(
            "SELECT account_id FROM transactions"
        ).fetchone()
        account = conn.execute(
            "SELECT id, card_ending, account_type FROM accounts"
        ).fetchone()

    assert txn["account_id"] == _account_id_for_source("Apple Card", "")
    assert account["id"] == _account_id_for_source("Apple Card", "")
    assert account["card_ending"] is None
    assert account["account_type"] == "credit_card"


def test_plaid_mask_scrubbed_before_storage(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        account_id = _ensure_account(
            conn,
            plaid_item_id="item_test",
            institution_name="Test Bank",
            plaid_account_id="acct_test",
            account_payload={"name": "Rewards Card", "type": "credit", "subtype": "credit card", "mask": "Amex"},
        )
        account = conn.execute(
            "SELECT card_ending, account_type FROM accounts WHERE id = ?",
            (account_id,),
        ).fetchone()

    assert account["card_ending"] is None
    assert account["account_type"] == "credit_card"
