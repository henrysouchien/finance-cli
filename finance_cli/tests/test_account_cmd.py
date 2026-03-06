from __future__ import annotations

import json
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def _seed_account(
    conn,
    *,
    institution: str,
    name: str,
    account_type: str = "checking",
    source: str = "manual",
    is_active: int = 1,
    balance_current_cents: int | None = None,
    account_type_override: str | None = None,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            account_type_override, source, is_active, balance_current_cents
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            account_id,
            institution,
            name,
            account_type,
            account_type_override,
            source,
            is_active,
            balance_current_cents,
        ),
    )
    return account_id


def _seed_txn(
    conn,
    *,
    account_id: str,
    date: str,
    description: str,
    amount_cents: int,
    is_active: int = 1,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, source, is_active
        ) VALUES (?, ?, ?, ?, ?, 'manual', ?)
        """,
        (txn_id, account_id, date, description, amount_cents, is_active),
    )
    return txn_id


def _seed_subscription(
    conn,
    *,
    account_id: str,
    vendor_name: str,
    is_auto_detected: int = 1,
    is_active: int = 1,
) -> str:
    sub_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO subscriptions (
            id, vendor_name, amount_cents, frequency, account_id,
            is_active, is_auto_detected
        ) VALUES (?, ?, 1500, 'monthly', ?, ?, ?)
        """,
        (sub_id, vendor_name, account_id, is_active, is_auto_detected),
    )
    return sub_id


def _seed_alias(conn, *, hash_account_id: str, canonical_id: str) -> None:
    conn.execute(
        """
        INSERT INTO account_aliases (hash_account_id, canonical_id)
        VALUES (?, ?)
        """,
        (hash_account_id, canonical_id),
    )


def _setup_db(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    return db_path


def test_account_list_filters(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        active_bofa = _seed_account(
            conn,
            institution="Bank of America",
            name="Everyday Checking",
            account_type="checking",
            source="plaid",
            is_active=1,
            balance_current_cents=123_45,
        )
        inactive_bofa = _seed_account(
            conn,
            institution="Bank of America",
            name="Old Savings",
            account_type="savings",
            source="plaid",
            is_active=0,
        )
        schwab = _seed_account(
            conn,
            institution="Charles Schwab",
            name="Brokerage",
            account_type="investment",
            source="schwab",
            is_active=1,
            balance_current_cents=5_000_00,
        )
        _seed_account(
            conn,
            institution="Credit Union",
            name="Rewards Card",
            account_type="credit_card",
            source="csv_import",
            is_active=1,
        )
        conn.commit()

    code, payload = _run_cli(["account", "list"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "account.list"
    listed_ids = {row["id"] for row in payload["data"]["accounts"]}
    assert active_bofa in listed_ids
    assert schwab in listed_ids
    assert inactive_bofa not in listed_ids
    assert payload["summary"]["total_accounts"] == 3

    code, payload = _run_cli(["account", "list", "--status", "all"], capsys)
    assert code == 0
    listed_ids = {row["id"] for row in payload["data"]["accounts"]}
    assert inactive_bofa in listed_ids
    assert payload["summary"]["total_accounts"] == 4

    code, payload = _run_cli(["account", "list", "--type", "investment"], capsys)
    assert code == 0
    assert [row["id"] for row in payload["data"]["accounts"]] == [schwab]

    code, payload = _run_cli(["account", "list", "--institution", "bank of america", "--status", "all"], capsys)
    assert code == 0
    assert {row["id"] for row in payload["data"]["accounts"]} == {active_bofa, inactive_bofa}

    code, payload = _run_cli(["account", "list", "--source", "schwab"], capsys)
    assert code == 0
    assert [row["id"] for row in payload["data"]["accounts"]] == [schwab]


def test_account_show_and_invalid_id(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            institution="Test Bank",
            name="Primary",
            account_type="checking",
            source="manual",
            is_active=1,
            balance_current_cents=99_00,
        )
        _seed_txn(
            conn,
            account_id=account_id,
            date="2026-01-10",
            description="Deposit",
            amount_cents=50_00,
            is_active=1,
        )
        _seed_txn(
            conn,
            account_id=account_id,
            date="2026-02-10",
            description="Coffee",
            amount_cents=-4_50,
            is_active=0,
        )
        conn.commit()

    code, payload = _run_cli(["account", "show", account_id], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["account"]["id"] == account_id
    stats = payload["data"]["transaction_stats"]
    assert stats["total_transactions"] == 2
    assert stats["active_transactions"] == 1
    assert stats["inactive_transactions"] == 1
    assert stats["first_transaction_date"] == "2026-01-10"
    assert stats["last_transaction_date"] == "2026-02-10"

    code, payload = _run_cli(["account", "show", "missing-account"], capsys)
    assert code == 1
    assert payload["status"] == "error"
    assert payload["command"] == "account.show"
    assert "not found" in payload["error"].lower()


def test_account_set_type_updates_type_and_override(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            institution="Merrill",
            name="CMA-Edge",
            account_type="checking",
            account_type_override=None,
            source="plaid",
        )
        conn.commit()

    code, payload = _run_cli(["account", "set-type", account_id, "--type", "investment"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    data = payload["data"]
    assert data["account_id"] == account_id
    assert data["old_type"] == "checking"
    assert data["new_type"] == "investment"
    assert data["account_type_override"] == "investment"
    assert data["override_set"] is True

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT account_type, account_type_override FROM accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        assert row["account_type"] == "investment"
        assert row["account_type_override"] == "investment"


def test_account_set_type_invalid_type_rejected(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, institution="Test", name="Checking")
        conn.commit()

    code, payload = _run_cli(["account", "set-type", account_id, "--type", "not-a-real-type"], capsys)
    assert code == 2
    assert payload["status"] == "error"
    assert payload["command"] == "account"


def test_account_deactivate_and_activate_idempotent(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, institution="Test", name="Checking", is_active=1)
        conn.commit()

    code, payload = _run_cli(["account", "deactivate", account_id], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["deactivated"] is True
    assert payload["data"]["already_inactive"] is False

    with connect(db_path) as conn:
        row = conn.execute("SELECT is_active FROM accounts WHERE id = ?", (account_id,)).fetchone()
        assert row["is_active"] == 0

    code, payload = _run_cli(["account", "deactivate", account_id], capsys)
    assert code == 0
    assert payload["data"]["deactivated"] is False
    assert payload["data"]["already_inactive"] is True

    code, payload = _run_cli(["account", "activate", account_id], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["activated"] is True
    assert payload["data"]["already_active"] is False

    with connect(db_path) as conn:
        row = conn.execute("SELECT is_active FROM accounts WHERE id = ?", (account_id,)).fetchone()
        assert row["is_active"] == 1

    code, payload = _run_cli(["account", "activate", account_id], capsys)
    assert code == 0
    assert payload["data"]["activated"] is False
    assert payload["data"]["already_active"] is True


def test_account_deactivate_cascade_deactivates_txns_and_auto_subscriptions(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, institution="Test", name="Checking", is_active=1)
        _seed_txn(
            conn,
            account_id=account_id,
            date="2026-01-01",
            description="Active txn",
            amount_cents=-1000,
            is_active=1,
        )
        _seed_txn(
            conn,
            account_id=account_id,
            date="2026-01-02",
            description="Already inactive txn",
            amount_cents=-500,
            is_active=0,
        )
        auto_sub_id = _seed_subscription(
            conn,
            account_id=account_id,
            vendor_name="Auto Service",
            is_auto_detected=1,
            is_active=1,
        )
        manual_sub_id = _seed_subscription(
            conn,
            account_id=account_id,
            vendor_name="Manual Service",
            is_auto_detected=0,
            is_active=1,
        )
        conn.commit()

    code, payload = _run_cli(["account", "deactivate", account_id, "--cascade"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    cascade = payload["data"]["cascade"]
    assert cascade["enabled"] is True
    assert cascade["deactivated_transactions"] == 1
    assert cascade["deactivated_subscriptions"] == 1

    with connect(db_path) as conn:
        account_row = conn.execute("SELECT is_active FROM accounts WHERE id = ?", (account_id,)).fetchone()
        assert account_row["is_active"] == 0

        txn_counts = conn.execute(
            """
            SELECT
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_count,
                COUNT(*) AS total_count
            FROM transactions
            WHERE account_id = ?
            """,
            (account_id,),
        ).fetchone()
        assert txn_counts["active_count"] == 0
        assert txn_counts["total_count"] == 2

        auto_sub = conn.execute("SELECT is_active FROM subscriptions WHERE id = ?", (auto_sub_id,)).fetchone()
        manual_sub = conn.execute("SELECT is_active FROM subscriptions WHERE id = ?", (manual_sub_id,)).fetchone()
        assert auto_sub["is_active"] == 0
        assert manual_sub["is_active"] == 1


def test_account_deactivate_refuses_canonical_alias_target_without_force(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        canonical_id = _seed_account(
            conn,
            institution="Bank",
            name="Canonical",
            account_type="checking",
            is_active=1,
        )
        alias_id = _seed_account(
            conn,
            institution="Bank",
            name="Alias",
            account_type="checking",
            is_active=1,
        )
        _seed_alias(conn, hash_account_id=alias_id, canonical_id=canonical_id)
        conn.commit()

    code, payload = _run_cli(["account", "deactivate", canonical_id], capsys)
    assert code == 1
    assert payload["status"] == "error"
    assert payload["command"] == "account.deactivate"
    assert "use --force" in payload["error"].lower()

    with connect(db_path) as conn:
        row = conn.execute("SELECT is_active FROM accounts WHERE id = ?", (canonical_id,)).fetchone()
        assert row["is_active"] == 1

    code, payload = _run_cli(["account", "deactivate", canonical_id, "--force"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["deactivated"] is True
    assert payload["data"]["alias_guard"]["forced"] is True


def test_mcp_account_list_and_show(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            institution="Charles Schwab",
            name="Brokerage",
            account_type="investment",
            source="schwab",
            is_active=1,
        )
        _seed_txn(
            conn,
            account_id=account_id,
            date="2026-02-01",
            description="Dividend",
            amount_cents=3200,
            is_active=1,
        )
        conn.commit()

    from finance_cli.mcp_server import account_list, account_show

    listed = account_list(source="schwab")
    assert "data" in listed
    assert "summary" in listed
    assert len(listed["data"]["accounts"]) == 1
    assert listed["data"]["accounts"][0]["id"] == account_id

    shown = account_show(id=account_id)
    assert "data" in shown
    assert "summary" in shown
    assert shown["data"]["account"]["id"] == account_id
    assert shown["data"]["transaction_stats"]["total_transactions"] == 1


def test_mcp_account_set_type_persists(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(
            conn,
            institution="Merrill",
            name="CMA-Edge",
            account_type="checking",
            account_type_override=None,
            source="plaid",
            is_active=1,
        )
        conn.commit()

    from finance_cli.mcp_server import account_set_type

    result = account_set_type(id=account_id, account_type="investment")
    assert result["data"]["account_id"] == account_id
    assert result["data"]["new_type"] == "investment"

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT account_type, account_type_override FROM accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        assert row["account_type"] == "investment"
        assert row["account_type_override"] == "investment"


def test_mcp_account_deactivate_persists(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, institution="Test", name="Checking", is_active=1)
        _seed_txn(
            conn,
            account_id=account_id,
            date="2026-02-01",
            description="Rent",
            amount_cents=-120000,
            is_active=1,
        )
        _seed_subscription(
            conn,
            account_id=account_id,
            vendor_name="Gym",
            is_auto_detected=1,
            is_active=1,
        )
        conn.commit()

    from finance_cli.mcp_server import account_deactivate

    result = account_deactivate(id=account_id, cascade=True, force=False)
    assert result["data"]["deactivated"] is True
    assert result["data"]["cascade"]["deactivated_transactions"] == 1
    assert result["data"]["cascade"]["deactivated_subscriptions"] == 1

    with connect(db_path) as conn:
        account_row = conn.execute("SELECT is_active FROM accounts WHERE id = ?", (account_id,)).fetchone()
        txn_row = conn.execute(
            "SELECT is_active FROM transactions WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        sub_row = conn.execute(
            "SELECT is_active FROM subscriptions WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        assert account_row["is_active"] == 0
        assert txn_row["is_active"] == 0
        assert sub_row["is_active"] == 0


def test_mcp_account_activate_persists(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        account_id = _seed_account(conn, institution="Test", name="Old Account", is_active=0)
        conn.commit()

    from finance_cli.mcp_server import account_activate

    result = account_activate(id=account_id)
    assert result["data"]["activated"] is True

    with connect(db_path) as conn:
        row = conn.execute("SELECT is_active FROM accounts WHERE id = ?", (account_id,)).fetchone()
        assert row["is_active"] == 1
