from __future__ import annotations

import json
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def _run_cli(args: list[str], capsys) -> dict:
    code = main(args)
    assert code == 0
    return json.loads(capsys.readouterr().out)


def test_balance_show_net_worth_and_history(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    checking_id = uuid.uuid4().hex
    credit_id = uuid.uuid4().hex
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (
                id, plaid_account_id, institution_name, account_name, account_type, is_active,
                balance_current_cents, balance_available_cents, iso_currency_code
            ) VALUES (?, 'acct_check', 'Test Bank', 'Checking', 'checking', 1, 250000, 240000, 'USD')
            """,
            (checking_id,),
        )
        conn.execute(
            """
            INSERT INTO accounts (
                id, plaid_account_id, institution_name, account_name, account_type, is_active,
                balance_current_cents, balance_limit_cents, iso_currency_code
            ) VALUES (?, 'acct_cc', 'Test Bank', 'Card', 'credit_card', 1, 85000, 300000, 'USD')
            """,
            (credit_id,),
        )
        conn.execute(
            """
            INSERT INTO balance_snapshots (
                id, account_id, balance_current_cents, balance_available_cents, source, snapshot_date
            ) VALUES (?, ?, 245000, 235000, 'sync', date('now', '-1 day'))
            """,
            (uuid.uuid4().hex, checking_id),
        )
        conn.execute(
            """
            INSERT INTO balance_snapshots (
                id, account_id, balance_current_cents, balance_available_cents, source, snapshot_date
            ) VALUES (?, ?, 250000, 240000, 'refresh', date('now'))
            """,
            (uuid.uuid4().hex, checking_id),
        )
        conn.commit()

    show_payload = _run_cli(["balance", "show"], capsys)
    assert show_payload["command"] == "balance.show"
    assert show_payload["summary"]["total_accounts"] == 2
    assert show_payload["data"]["total_assets_cents"] == 250000
    assert show_payload["data"]["total_liabilities_cents"] == 85000

    worth_payload = _run_cli(["balance", "net-worth"], capsys)
    assert worth_payload["command"] == "balance.net_worth"
    assert worth_payload["data"]["assets_cents"] == 250000
    assert worth_payload["data"]["liabilities_cents"] == 85000
    assert worth_payload["data"]["net_worth_cents"] == 165000

    hist_payload = _run_cli(["balance", "history", "--account", checking_id, "--days", "7"], capsys)
    assert hist_payload["command"] == "balance.history"
    assert hist_payload["summary"]["total_points"] == 2


def test_liability_show_and_upcoming(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    account_id = uuid.uuid4().hex
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (
                id, plaid_account_id, institution_name, account_name, account_type, is_active
            ) VALUES (?, 'acct_liab', 'Loan Bank', 'Mortgage', 'loan', 1)
            """,
            (account_id,),
        )
        conn.execute(
            """
            INSERT INTO liabilities (
                id, account_id, liability_type, is_active, minimum_payment_cents, next_payment_due_date,
                next_monthly_payment_cents, current_late_fee_cents
            ) VALUES (?, ?, 'mortgage', 1, 210000, date('now', '+10 day'), 210000, 0)
            """,
            (uuid.uuid4().hex, account_id),
        )
        conn.commit()

    show_payload = _run_cli(["liability", "show"], capsys)
    assert show_payload["command"] == "liability.show"
    assert show_payload["summary"]["total_liabilities"] == 1
    assert show_payload["data"]["total_minimum_due_cents"] == 210000

    upcoming_payload = _run_cli(["liability", "upcoming", "--days", "30"], capsys)
    assert upcoming_payload["command"] == "liability.upcoming"
    assert upcoming_payload["summary"]["total_upcoming"] == 1
    assert upcoming_payload["data"]["total_due_cents"] == 210000


def test_balance_show_filters_null_balances_by_default(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, is_active,
                balance_current_cents, iso_currency_code
            ) VALUES (?, 'Test Bank', 'Checking', 'checking', 1, 12300, 'USD')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, is_active,
                balance_available_cents, iso_currency_code
            ) VALUES (?, 'Test Bank', 'Savings', 'savings', 1, 45600, 'USD')
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO accounts (
                id, institution_name, account_name, account_type, is_active, iso_currency_code
            ) VALUES (?, 'Test Bank', 'CSV Imported', 'checking', 1, 'USD')
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    default_payload = _run_cli(["balance", "show"], capsys)
    assert default_payload["status"] == "success"
    assert default_payload["summary"]["total_accounts"] == 2
    account_names = {row["account_name"] for row in default_payload["data"]["accounts"]}
    assert account_names == {"Checking", "Savings"}

    all_payload = _run_cli(["balance", "show", "--all"], capsys)
    assert all_payload["status"] == "success"
    assert all_payload["summary"]["total_accounts"] == 3
    all_account_names = {row["account_name"] for row in all_payload["data"]["accounts"]}
    assert all_account_names == {"Checking", "Savings", "CSV Imported"}
