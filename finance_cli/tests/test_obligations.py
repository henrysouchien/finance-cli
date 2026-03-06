"""Tests for the liability obligations command."""

from __future__ import annotations

import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import liability_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _ns(**kwargs) -> Namespace:
    defaults = {"format": "json"}
    defaults.update(kwargs)
    return Namespace(**defaults)


def _seed_account(conn, *, account_type="credit_card", balance_cents=-50000) -> str:
    aid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO accounts (id, institution_name, account_name, account_type, "
        "balance_current_cents, is_active) VALUES (?, 'Test Bank', 'Test Card', ?, ?, 1)",
        (aid, account_type, balance_cents),
    )
    conn.commit()
    return aid


def test_obligations_empty_db(db_path: Path) -> None:
    """Obligations on empty DB should return zeros."""
    with connect(db_path) as conn:
        result = liability_cmd.handle_obligations(_ns(), conn)
    assert result["data"]["grand_total"] == 0
    assert result["data"]["recurring_flows"] == []
    assert result["data"]["debt_payments"] == []
    assert result["data"]["subscriptions"] == []
    assert "Fixed Monthly Obligations" in result["cli_report"]


def test_obligations_all_three_sources(db_path: Path) -> None:
    """Obligations should aggregate recurring flows, liabilities, and subscriptions."""
    with connect(db_path) as conn:
        # Recurring flow: $2950/mo rent
        conn.execute(
            "INSERT INTO recurring_flows (id, name, flow_type, amount_cents, frequency, is_active) "
            "VALUES (?, 'Rent', 'expense', 295000, 'monthly', 1)",
            (uuid.uuid4().hex,),
        )
        # Liability: $341/mo minimum
        aid = _seed_account(conn)
        conn.execute(
            "INSERT INTO liabilities (id, account_id, liability_type, is_active, minimum_payment_cents) "
            "VALUES (?, ?, 'credit', 1, 34100)",
            (uuid.uuid4().hex, aid),
        )
        # Subscription: $100/mo
        conn.execute(
            "INSERT INTO subscriptions (id, vendor_name, amount_cents, frequency, is_active) "
            "VALUES (?, 'Anthropic', 10000, 'monthly', 1)",
            (uuid.uuid4().hex,),
        )
        conn.commit()
        result = liability_cmd.handle_obligations(_ns(), conn)

    data = result["data"]
    assert data["recurring_total"] == 2950.0
    assert data["debt_total"] == 341.0
    assert data["subscription_total"] == 100.0
    assert data["grand_total"] == 2950.0 + 341.0 + 100.0


def test_obligations_frequency_normalization(db_path: Path) -> None:
    """Non-monthly frequencies should be normalized to monthly equivalents."""
    with connect(db_path) as conn:
        # Weekly $100 flow -> ~$433/mo (100 * 52/12)
        conn.execute(
            "INSERT INTO recurring_flows (id, name, flow_type, amount_cents, frequency, is_active) "
            "VALUES (?, 'Weekly expense', 'expense', 10000, 'weekly', 1)",
            (uuid.uuid4().hex,),
        )
        # Yearly $1200 subscription -> $100/mo
        conn.execute(
            "INSERT INTO subscriptions (id, vendor_name, amount_cents, frequency, is_active) "
            "VALUES (?, 'Annual Service', 120000, 'yearly', 1)",
            (uuid.uuid4().hex,),
        )
        conn.commit()
        result = liability_cmd.handle_obligations(_ns(), conn)

    data = result["data"]
    # Weekly: 10000 * 52 / 12 = 43333 cents
    assert data["recurring_total_cents"] == 43333
    # Yearly: 120000 / 12 = 10000 cents
    assert data["subscription_total_cents"] == 10000


def test_obligations_liability_coalesce(db_path: Path) -> None:
    """Liabilities should fall back to next_monthly_payment_cents when minimum is NULL."""
    with connect(db_path) as conn:
        aid = _seed_account(conn)
        conn.execute(
            "INSERT INTO liabilities (id, account_id, liability_type, is_active, "
            "minimum_payment_cents, next_monthly_payment_cents) VALUES (?, ?, 'credit', 1, NULL, 25000)",
            (uuid.uuid4().hex, aid),
        )
        conn.commit()
        result = liability_cmd.handle_obligations(_ns(), conn)
    assert result["data"]["debt_total"] == 250.0


def test_obligations_parser_registration() -> None:
    """The obligations subparser should be registered under liability."""
    import argparse
    parser = argparse.ArgumentParser()
    format_parent = argparse.ArgumentParser(add_help=False)
    format_parent.add_argument("--format", default="json")
    subs = parser.add_subparsers(dest="command")
    liability_cmd.register(subs, format_parent)
    args = parser.parse_args(["liability", "obligations"])
    assert args.liability_command == "obligations"
    assert hasattr(args, "func")
