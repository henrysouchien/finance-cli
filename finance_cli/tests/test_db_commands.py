from __future__ import annotations

import json
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def _setup_db(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    return db_path


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def test_db_backup_creates_timestamped_copy(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)

    code, payload = _run_cli(["db", "backup"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.backup"

    backup_path = Path(payload["data"]["backup_path"])
    assert backup_path.exists()
    assert backup_path != db_path
    assert backup_path.name.startswith("finance_backup_")


def test_db_reset_requires_yes_flag(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    code, payload = _run_cli(["db", "reset"], capsys)
    assert code == 1
    assert payload["status"] == "error"
    assert payload["command"] == "db.reset"
    assert "--yes" in payload["error"]


def test_db_reset_preserves_plaid_items_and_resets_sync_metadata(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (
                id, plaid_item_id, institution_name, access_token_ref, status,
                sync_cursor, last_sync_at, last_balance_refresh_at, last_liabilities_fetch_at
            ) VALUES (?, 'item_keep', 'Test Bank', 'secret/ref', 'active', ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                "cursor_123",
                "2026-02-20 10:00:00",
                "2026-02-20 10:00:00",
                "2026-02-20 10:00:00",
            ),
        )
        account_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO accounts (id, plaid_item_id, institution_name, account_name, account_type, is_active)
            VALUES (?, 'item_keep', 'Test Bank', 'Checking', 'checking', 1)
            """,
            (account_id,),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, account_id, date, description, amount_cents, source, is_active)
            VALUES (?, ?, '2026-02-20', 'Coffee', -500, 'plaid', 1)
            """,
            (uuid.uuid4().hex, account_id),
        )
        conn.execute(
            """
            INSERT INTO import_batches (
                id, source_type, file_path, file_hash_sha256, bank_parser,
                extracted_count, imported_count, skipped_count, reconcile_status
            ) VALUES (?, 'pdf', '/tmp/stmt.pdf', ?, 'ai:openai', 1, 1, 0, 'no_totals')
            """,
            (uuid.uuid4().hex, uuid.uuid4().hex),
        )
        conn.commit()

    code, payload = _run_cli(["db", "reset", "--yes"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["preserve_plaid_items"] is True
    assert Path(payload["data"]["backup_path"]).exists()

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
        item = conn.execute(
            """
            SELECT sync_cursor, last_sync_at, last_balance_refresh_at, last_liabilities_fetch_at
              FROM plaid_items
             WHERE plaid_item_id = 'item_keep'
            """
        ).fetchone()

    assert txn_count == 0
    assert account_count == 0
    assert batch_count == 0
    assert item is not None
    assert item["sync_cursor"] is None
    assert item["last_sync_at"] is None
    assert item["last_balance_refresh_at"] is None
    assert item["last_liabilities_fetch_at"] is None


def test_db_reset_drop_plaid_items_removes_all_items(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, status)
            VALUES (?, 'item_drop', 'Test Bank', 'secret/ref', 'active')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    code, payload = _run_cli(["db", "reset", "--yes", "--drop-plaid-items"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["preserve_plaid_items"] is False

    with connect(db_path) as conn:
        item_count = conn.execute("SELECT COUNT(*) AS n FROM plaid_items").fetchone()["n"]
    assert item_count == 0


def test_db_status_returns_snapshot(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        dining_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Dining', 0)", (dining_id,))

        active_account = uuid.uuid4().hex
        inactive_account = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO accounts (id, institution_name, account_type, is_active) VALUES (?, 'Test Bank', 'checking', 1)",
            (active_account,),
        )
        conn.execute(
            "INSERT INTO accounts (id, institution_name, account_type, is_active) VALUES (?, 'Old Bank', 'checking', 0)",
            (inactive_account,),
        )

        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents, category_id, category_source, source, is_active, is_payment
            ) VALUES (?, ?, '2026-02-01', 'Lunch', -1500, ?, 'keyword_rule', 'manual', 1, 0)
            """,
            (uuid.uuid4().hex, active_account, dining_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents, category_source, source, is_active, is_payment
            ) VALUES (?, ?, '2026-02-10', 'Transfer', -2500, NULL, 'manual', 1, 1)
            """,
            (uuid.uuid4().hex, active_account),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents, category_id, category_source, source, is_active, is_payment
            ) VALUES (?, ?, '2026-01-15', 'Old Txn', -300, ?, 'user', 'manual', 0, 0)
            """,
            (uuid.uuid4().hex, inactive_account, dining_id),
        )

        conn.execute(
            """
            INSERT INTO import_batches (id, source_type, file_path, created_at)
            VALUES (?, 'csv', '/tmp/older.csv', '2026-01-01 09:00:00')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO import_batches (id, source_type, file_path, created_at)
            VALUES (?, 'pdf', '/tmp/latest.pdf', '2026-02-20 08:30:00')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    code, payload = _run_cli(["db", "status"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.status"

    data = payload["data"]
    assert data["transaction_counts"] == {"total": 3, "active": 2, "inactive": 1}
    assert data["date_range"] == {"earliest": "2026-02-01", "latest": "2026-02-10"}
    assert data["active_account_count"] == 1
    assert data["uncategorized_count"] == 1
    assert data["payment_count"] == 1
    assert data["last_import_at"] == "2026-02-20 08:30:00"

    source_dist = {row["category_source"]: row["count"] for row in data["category_source_distribution"]}
    assert source_dist["keyword_rule"] == 1
    assert source_dist["uncategorized"] == 1

    top_categories = {row["category_name"]: row["count"] for row in data["top_categories"]}
    assert top_categories["Dining"] == 1
    assert top_categories["Uncategorized"] == 1
