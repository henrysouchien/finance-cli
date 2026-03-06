"""Tests for the net worth projection command."""

from __future__ import annotations

import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import projection_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _ns(**kwargs) -> Namespace:
    defaults = {"format": "json", "months": 12}
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


def _seed_transaction(conn, *, account_id, amount_cents, date="2026-02-15",
                      is_payment=0) -> str:
    tid = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO transactions (id, account_id, amount_cents, date, description,
           is_payment, is_active, is_reviewed, source)
           VALUES (?, ?, ?, ?, 'test', ?, 1, 1, 'manual')""",
        (tid, account_id, amount_cents, date, is_payment),
    )
    conn.commit()
    return tid


def test_projection_empty_db(db_path: Path) -> None:
    """Projection on empty DB returns all zeros."""
    with connect(db_path) as conn:
        result = projection_cmd.handle_projection(_ns(), conn)
    assert "data" in result
    assert "summary" in result
    assert "cli_report" in result
    data = result["data"]
    assert data["current"]["net_worth_cents"] == 0
    assert data["current"]["liquid_cash_cents"] == 0
    assert data["net_savings_cents"] == 0
    # All projection rows should be zero
    for row in data["projection"]:
        assert row["net_worth_cents"] == 0


def test_projection_with_balances(db_path: Path) -> None:
    """Projection reflects seeded account balances at month 0."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=500_000)
        _seed_account(conn, account_type="investment", balance_cents=10_000_000)
        _seed_account(conn, account_type="credit_card", balance_cents=-200_000)
        result = projection_cmd.handle_projection(_ns(), conn)
    current = result["data"]["current"]
    assert current["liquid_cash_cents"] == 500_000
    assert current["investment_cents"] == 10_000_000
    assert current["credit_card_debt_cents"] == 200_000
    assert current["net_worth_cents"] == 500_000 + 10_000_000 - 200_000


def test_projection_debt_decreases_over_time(db_path: Path) -> None:
    """Credit card debt should decrease over projection months when liability data exists."""
    with connect(db_path) as conn:
        aid = _seed_account(conn, account_type="credit_card", balance_cents=-500_000)
        conn.execute(
            """INSERT INTO liabilities (id, account_id, liability_type, apr_purchase,
               minimum_payment_cents, is_active) VALUES (?, ?, 'credit', 20.0, 10000, 1)""",
            (uuid.uuid4().hex, aid),
        )
        conn.commit()
        result = projection_cmd.handle_projection(_ns(months=12), conn)

    projection = result["data"]["projection"]
    # Find month 0 and a later month
    month0 = next(r for r in projection if r["month"] == 0)
    month_later = next(r for r in projection if r["month"] > 0)
    # Debt should decrease (or at least not increase) with min payments
    assert month_later["credit_card_debt_cents"] <= month0["credit_card_debt_cents"]


def test_projection_investment_growth_compounds(db_path: Path) -> None:
    """Investment growth should compound using Decimal math."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="investment", balance_cents=1_000_000)
        result = projection_cmd.handle_projection(_ns(months=12), conn)

    projection = result["data"]["projection"]
    month0 = next(r for r in projection if r["month"] == 0)
    month12 = next(r for r in projection if r["month"] == 12)
    # 7% annual growth on $10,000 -> ~$10,700
    assert month12["investments_cents"] > month0["investments_cents"]
    # Verify approximate value (should be close to 1_000_000 * 1.07 = 1_070_000)
    assert 1_069_000 <= month12["investments_cents"] <= 1_073_000


def test_projection_negative_net_savings(db_path: Path) -> None:
    """Negative net savings reflected in decreasing liquid cash."""
    with connect(db_path) as conn:
        aid = _seed_account(conn, account_type="checking", balance_cents=1_000_000)
        # Seed expenses in last 3 complete months (use dates that are complete months)
        for month in ["2025-12-15", "2026-01-15", "2026-02-15"]:
            _seed_transaction(conn, account_id=aid, amount_cents=-500_00, date=month)
            _seed_transaction(conn, account_id=aid, amount_cents=100_00, date=month)
        result = projection_cmd.handle_projection(_ns(months=12), conn)

    data = result["data"]
    assert data["net_savings_cents"] < 0
    projection = data["projection"]
    month0 = next(r for r in projection if r["month"] == 0)
    month12 = next(r for r in projection if r["month"] == 12)
    assert month12["liquid_cash_cents"] < month0["liquid_cash_cents"]


def test_projection_zero_investments(db_path: Path) -> None:
    """Zero investments handled correctly."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=500_000)
        result = projection_cmd.handle_projection(_ns(months=12), conn)
    projection = result["data"]["projection"]
    for row in projection:
        assert row["investments_cents"] == 0


def test_projection_parser_registration() -> None:
    """projection_cmd.register does not raise."""
    import argparse
    parser = argparse.ArgumentParser()
    format_parent = argparse.ArgumentParser(add_help=False)
    format_parent.add_argument("--format", default="json")
    subparsers = parser.add_subparsers(dest="command")
    projection_cmd.register(subparsers, format_parent)
    args = parser.parse_args(["projection", "--months", "6"])
    assert args.months == 6
