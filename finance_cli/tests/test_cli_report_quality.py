"""Smoke tests for improved CLI report formatting (--format cli paths)."""

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


def _run_cli_format(args: list[str], capsys) -> str:
    """Run CLI with --format cli and return the printed output."""
    code = main(args)
    assert code == 0
    return capsys.readouterr().out


def _setup_db(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    return db_path


def _seed_accounts_and_transactions(conn, today: str = "2026-02-15") -> dict:
    """Seed a minimal set of accounts and transactions for smoke testing."""
    checking_id = uuid.uuid4().hex
    credit_id = uuid.uuid4().hex
    cat_id = uuid.uuid4().hex
    cat_parent_id = uuid.uuid4().hex

    conn.execute(
        """
        INSERT INTO accounts (
            id, plaid_account_id, institution_name, account_name, account_type, is_active,
            balance_current_cents, balance_available_cents, iso_currency_code
        ) VALUES (?, 'acct_check', 'Test Bank', 'Checking', 'checking', 1, 500000, 490000, 'USD')
        """,
        (checking_id,),
    )
    conn.execute(
        """
        INSERT INTO accounts (
            id, plaid_account_id, institution_name, account_name, account_type, is_active,
            balance_current_cents, balance_limit_cents, iso_currency_code
        ) VALUES (?, 'acct_cc', 'Test Bank', 'Card', 'credit_card', 1, 120000, 300000, 'USD')
        """,
        (credit_id,),
    )

    conn.execute(
        "INSERT INTO categories (id, name, is_system, level) VALUES (?, 'Food & Drink', 0, 0)",
        (cat_parent_id,),
    )
    conn.execute(
        "INSERT INTO categories (id, name, is_system, parent_id, level) VALUES (?, 'Dining', 0, ?, 1)",
        (cat_id, cat_parent_id),
    )

    for i, (desc, amt) in enumerate([
        ("Coffee Shop", -450),
        ("Restaurant", -3200),
        ("Grocery Store", -8500),
    ]):
        conn.execute(
            """
            INSERT INTO transactions (
                id, account_id, date, description, amount_cents,
                category_id, is_active, is_reviewed, source
            ) VALUES (?, ?, ?, ?, ?, ?, 1, 0, 'manual')
            """,
            (uuid.uuid4().hex, checking_id, today, desc, amt, cat_id),
        )

    conn.execute(
        """
        INSERT INTO subscriptions (
            id, vendor_name, category_id, amount_cents, frequency,
            next_expected, is_active, use_type, is_auto_detected
        ) VALUES (?, 'Netflix', ?, 1599, 'monthly', NULL, 1, 'Personal', 1)
        """,
        (uuid.uuid4().hex, cat_id),
    )
    conn.execute(
        """
        INSERT INTO subscriptions (
            id, vendor_name, category_id, amount_cents, frequency,
            next_expected, is_active, use_type, is_auto_detected
        ) VALUES (?, 'Old Service', ?, 999, 'monthly', NULL, 0, 'Personal', 1)
        """,
        (uuid.uuid4().hex, cat_id),
    )

    conn.commit()
    return {
        "checking_id": checking_id,
        "credit_id": credit_id,
        "cat_id": cat_id,
        "cat_parent_id": cat_parent_id,
    }


def _get_or_create_category(conn, name: str, parent_id: str | None = None) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    if parent_id:
        conn.execute(
            "INSERT INTO categories (id, name, parent_id, is_system, level) VALUES (?, ?, ?, 0, 1)",
            (category_id, name, parent_id),
        )
    else:
        conn.execute(
            "INSERT INTO categories (id, name, is_system, level) VALUES (?, ?, 0, 0)",
            (category_id, name),
        )
    return category_id


def _seed_payment_exclusion_dataset(conn) -> dict[str, str]:
    checking_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, plaid_account_id, institution_name, account_name, account_type, is_active,
            balance_current_cents, balance_available_cents, iso_currency_code
        ) VALUES (?, 'acct_check', 'Test Bank', 'Checking', 'checking', 1, 500000, 490000, 'USD')
        """,
        (checking_id,),
    )

    parent_id = _get_or_create_category(conn, "Food & Drink")
    dining_id = _get_or_create_category(conn, "Dining", parent_id=parent_id)
    payments_id = _get_or_create_category(conn, "Payments & Transfers")

    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id, is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, '2026-02-15', 'Restaurant', -3200, ?, 0, 1, 0, 'manual')
        """,
        (uuid.uuid4().hex, checking_id, dining_id),
    )
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id, is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, '2026-02-15', 'Credit Card Payment', -50000, ?, 1, 1, 0, 'manual')
        """,
        (uuid.uuid4().hex, checking_id, payments_id),
    )
    conn.execute(
        """
        INSERT INTO budgets (
            id, category_id, period, amount_cents, effective_from, effective_to
        ) VALUES (?, ?, 'monthly', 10000, '2026-01-01', NULL)
        """,
        (uuid.uuid4().hex, dining_id),
    )
    conn.commit()
    return {
        "dining_id": dining_id,
        "payments_id": payments_id,
    }


class TestBalanceNetWorthCliReport:
    def test_net_worth_cli_shows_breakdown(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn)

        output = _run_cli_format(["balance", "net-worth", "--format", "cli"], capsys)
        assert "Net Worth:" in output
        assert "Checking:" in output
        assert "Credit Card:" in output
        assert "Assets:" in output
        assert "Liabilities:" in output
        assert "$" in output

    def test_net_worth_json_unchanged(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn)

        payload = _run_cli(["balance", "net-worth"], capsys)
        assert payload["data"]["assets_cents"] == 500000
        assert payload["data"]["liabilities_cents"] == 120000
        assert "breakdown" in payload["data"]


class TestLiquidityCliReport:
    def test_liquidity_cli_shows_dashboard(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn)

        output = _run_cli_format(["liquidity", "--format", "cli"], capsys)
        assert "Liquidity" in output
        assert "Liquid Balance:" in output
        assert "Credit Owed:" in output
        assert "Projected Net:" in output
        assert "$" in output


class TestDailyCliReport:
    def test_daily_cli_shows_header_and_columns(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn, today="2026-02-15")

        output = _run_cli_format(["daily", "--date", "2026-02-15", "--format", "cli"], capsys)
        assert "2026-02-15" in output
        assert "transaction" in output.lower()
        assert "total:" in output.lower() or "total" in output.lower()
        assert "$" in output

    def test_daily_json_includes_account_name(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn, today="2026-02-15")

        payload = _run_cli(["daily", "--date", "2026-02-15"], capsys)
        txns = payload["data"]["transactions"]
        assert len(txns) > 0
        assert "account_name" in txns[0]

    def test_daily_empty_date_no_crash(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn, today="2026-02-15")

        output = _run_cli_format(["daily", "--date", "2020-01-01", "--format", "cli"], capsys)
        assert "No transactions" in output


class TestWeeklyCliReport:
    def test_weekly_cli_shows_header_and_total(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn, today="2026-02-12")

        output = _run_cli_format(["weekly", "--week", "2026-W07", "--format", "cli"], capsys)
        assert "W07" in output
        assert "Total:" in output
        assert "$" in output

    def test_weekly_compare_shows_deltas(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn, today="2026-02-12")

        output = _run_cli_format(["weekly", "--week", "2026-W07", "--compare", "--format", "cli"], capsys)
        assert "vs" in output
        assert "\u0394" in output or "delta" in output.lower()

    def test_weekly_empty_no_crash(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        initialize_database(db_path)

        output = _run_cli_format(["weekly", "--week", "2020-W01", "--format", "cli"], capsys)
        assert "No transactions" in output or "W01" in output


class TestSubsListCliReport:
    def test_subs_list_default_active_only(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn)

        output = _run_cli_format(["subs", "list", "--format", "cli"], capsys)
        assert "active" in output.lower()
        assert "/mo" in output
        assert "Netflix" in output
        assert "inactive hidden" in output.lower()

    def test_subs_list_all_shows_inactive(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn)

        output = _run_cli_format(["subs", "list", "--all", "--format", "cli"], capsys)
        assert "Old Service" in output
        assert "Netflix" in output

    def test_subs_list_json_unchanged(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_accounts_and_transactions(conn)

        payload = _run_cli(["subs", "list"], capsys)
        assert payload["data"]["subscriptions"] is not None
        assert len(payload["data"]["subscriptions"]) == 2  # both active and inactive in JSON

    def test_subs_list_empty_no_crash(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        initialize_database(db_path)

        output = _run_cli_format(["subs", "list", "--format", "cli"], capsys)
        assert "No subscriptions" in output or "0 active" in output


class TestPaymentExclusion:
    def test_weekly_totals_exclude_payments(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_payment_exclusion_dataset(conn)

        payload = _run_cli(["weekly", "--week", "2026-W07"], capsys)
        categories = payload["data"]["categories"]
        assert payload["summary"]["total_amount"] == -32.0
        assert all(row["group_name"] != "Payments & Transfers" for row in categories)

    def test_daily_keeps_payment_rows_but_excludes_from_total(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_payment_exclusion_dataset(conn)

        payload = _run_cli(["daily", "--date", "2026-02-15"], capsys)
        descriptions = [row["description"] for row in payload["data"]["transactions"]]
        assert "Restaurant" in descriptions
        assert "Credit Card Payment" in descriptions
        assert payload["summary"]["total_transactions"] == 2
        assert payload["summary"]["total_amount"] == -32.0

    def test_budget_status_excludes_payment_rows_from_actuals(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            ids = _seed_payment_exclusion_dataset(conn)

        payload = _run_cli(["budget", "status", "--month", "2026-02"], capsys)
        row = next(item for item in payload["data"]["status"] if item["category_id"] == ids["dining_id"])
        assert row["actual_cents"] == -3200

    def test_liquidity_excludes_payment_rows_from_expense_window(self, tmp_path, monkeypatch, capsys):
        db_path = _setup_db(tmp_path, monkeypatch)
        with connect(db_path) as conn:
            _seed_payment_exclusion_dataset(conn)

        payload = _run_cli(["liquidity"], capsys)
        assert payload["data"]["expense_90d_cents"] == 3200
