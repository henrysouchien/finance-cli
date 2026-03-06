"""Tests for the financial health summary command."""

from __future__ import annotations

import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import summary_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _ns(**kwargs) -> Namespace:
    defaults = {"format": "json", "view": "all"}
    defaults.update(kwargs)
    return Namespace(**defaults)


def _seed_account(conn, *, account_type="checking", balance_cents=0, institution="Test Bank") -> str:
    aid = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO accounts (id, institution_name, account_name, account_type,
           balance_current_cents, is_active) VALUES (?, ?, ?, ?, ?, 1)""",
        (aid, institution, f"{account_type} account", account_type, balance_cents),
    )
    conn.commit()
    return aid


def _seed_category(conn, name="Dining") -> str:
    cid = uuid.uuid4().hex
    conn.execute("INSERT INTO categories (id, name, level) VALUES (?, ?, 1)", (cid, name))
    conn.commit()
    return cid


def _seed_transaction(conn, *, account_id, amount_cents, category_id=None, date="2026-03-01",
                      is_payment=0, is_reviewed=1) -> str:
    tid = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO transactions (id, account_id, amount_cents, date, description,
           category_id, is_payment, is_active, is_reviewed, source)
           VALUES (?, ?, ?, ?, 'test', ?, ?, 1, ?, 'manual')""",
        (tid, account_id, amount_cents, date, category_id, is_payment, is_reviewed),
    )
    conn.commit()
    return tid


def test_summary_empty_db(db_path: Path) -> None:
    """Summary on empty DB should return zeros and no errors."""
    with connect(db_path) as conn:
        result = summary_cmd.handle_summary(_ns(), conn)
    assert "data" in result
    assert "summary" in result
    assert "cli_report" in result
    data = result["data"]
    assert data["net_worth"] == 0
    assert data["liquid_cash"] == 0
    assert data["income_30d"] == 0
    assert data["expense_30d"] == 0
    assert data["unreviewed"] == 0
    assert data["uncategorized"] == 0


def test_summary_with_balances(db_path: Path) -> None:
    """Summary should reflect account balances."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=1000000)
        _seed_account(conn, account_type="credit_card", balance_cents=-500000)
        result = summary_cmd.handle_summary(_ns(), conn)
    data = result["data"]
    assert data["liquid_cash"] == 10000.0
    assert data["total_debt"] == 5000.0
    assert data["net_worth"] == 5000.0


def test_summary_excludes_aliases(db_path: Path) -> None:
    """Aliased accounts should not be double-counted in balances."""
    with connect(db_path) as conn:
        plaid_id = _seed_account(conn, account_type="checking", balance_cents=1000000)
        hash_id = _seed_account(conn, account_type="checking", balance_cents=1000000, institution="Hash")
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_id, plaid_id),
        )
        conn.commit()
        result = summary_cmd.handle_summary(_ns(), conn)
    # Only the non-aliased account should count
    assert result["data"]["liquid_cash"] == 10000.0


def test_summary_cash_flow(db_path: Path) -> None:
    """Income and expense should be computed correctly."""
    with connect(db_path) as conn:
        aid = _seed_account(conn, account_type="checking", balance_cents=0)
        cid = _seed_category(conn, "Dining")
        _seed_transaction(conn, account_id=aid, amount_cents=300000, category_id=cid)  # income
        _seed_transaction(conn, account_id=aid, amount_cents=-150000, category_id=cid)  # expense
        result = summary_cmd.handle_summary(_ns(), conn)
    data = result["data"]
    assert data["income_30d"] == 3000.0
    assert data["expense_30d"] == 1500.0
    assert data["savings_rate"] == pytest.approx(0.5)


def test_summary_zero_income_guards(db_path: Path) -> None:
    """When income is 0, savings_rate and debt_to_income should be None."""
    with connect(db_path) as conn:
        aid = _seed_account(conn, account_type="checking", balance_cents=0)
        cid = _seed_category(conn, "Dining")
        _seed_transaction(conn, account_id=aid, amount_cents=-50000, category_id=cid)
        result = summary_cmd.handle_summary(_ns(), conn)
    data = result["data"]
    assert data["savings_rate"] is None
    assert data["debt_to_income"] is None


def test_summary_zero_expense_guard(db_path: Path) -> None:
    """When expense is 0, emergency_fund_months should be None."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=1000000)
        result = summary_cmd.handle_summary(_ns(), conn)
    assert result["data"]["emergency_fund_months"] is None


def test_summary_view_filter(db_path: Path) -> None:
    """The --view flag should filter transactions by use_type."""
    with connect(db_path) as conn:
        aid = _seed_account(conn, account_type="checking", balance_cents=0)
        cid = _seed_category(conn, "Dining")
        tid = _seed_transaction(conn, account_id=aid, amount_cents=-100000, category_id=cid)
        conn.execute("UPDATE transactions SET use_type = 'Business' WHERE id = ?", (tid,))
        conn.commit()
        result_personal = summary_cmd.handle_summary(_ns(view="personal"), conn)
        result_business = summary_cmd.handle_summary(_ns(view="business"), conn)
    # Business txn should not appear in personal view
    assert result_personal["data"]["expense_30d"] == 0
    assert result_business["data"]["expense_30d"] == 1000.0


def test_summary_parser_registration() -> None:
    """The register function should add a 'summary' subparser."""
    import argparse
    parser = argparse.ArgumentParser()
    format_parent = argparse.ArgumentParser(add_help=False)
    format_parent.add_argument("--format", default="json")
    subs = parser.add_subparsers(dest="command")
    summary_cmd.register(subs, format_parent)
    args = parser.parse_args(["summary"])
    assert args.command == "summary"
    assert hasattr(args, "func")


def test_summary_data_health_counts(db_path: Path) -> None:
    """Unreviewed and uncategorized counts should be accurate."""
    with connect(db_path) as conn:
        aid = _seed_account(conn, account_type="checking", balance_cents=0)
        cid = _seed_category(conn, "Dining")
        # Unreviewed + categorized
        _seed_transaction(conn, account_id=aid, amount_cents=-5000, category_id=cid, is_reviewed=0)
        # Reviewed + uncategorized
        _seed_transaction(conn, account_id=aid, amount_cents=-3000, category_id=None, is_reviewed=1)
        result = summary_cmd.handle_summary(_ns(), conn)
    assert result["data"]["unreviewed"] == 1
    assert result["data"]["uncategorized"] == 1
