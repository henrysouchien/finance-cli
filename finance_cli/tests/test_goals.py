"""Tests for the goal tracking command."""

from __future__ import annotations

import uuid
from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import goal_cmd
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


def _seed_account(conn, *, account_type="checking", balance_cents=0) -> str:
    aid = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO accounts (id, institution_name, account_name, account_type,
           balance_current_cents, is_active) VALUES (?, 'Test', ?, ?, ?, 1)""",
        (aid, f"{account_type} account", account_type, balance_cents),
    )
    conn.commit()
    return aid


def _seed_transaction(conn, *, account_id, amount_cents, date="2026-02-15") -> str:
    tid = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO transactions (id, account_id, amount_cents, date, description,
           is_payment, is_active, is_reviewed, source)
           VALUES (?, ?, ?, ?, 'test', 0, 1, 1, 'manual')""",
        (tid, account_id, amount_cents, date),
    )
    conn.commit()
    return tid


def test_set_goal_stored(db_path: Path) -> None:
    """Set a goal and verify it is stored."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=500_000)
        result = goal_cmd.handle_set(
            _ns(name="Emergency Fund", target=25000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
    assert result["data"]["goal"]["name"] == "Emergency Fund"
    assert result["data"]["goal"]["metric"] == "liquid_cash"
    assert result["data"]["goal"]["target_cents"] == 2_500_000
    assert result["data"]["goal"]["starting_cents"] == 500_000


def test_list_goals_empty(db_path: Path) -> None:
    """List goals on empty DB returns empty list."""
    with connect(db_path) as conn:
        result = goal_cmd.handle_list(_ns(), conn)
    assert result["data"]["goals"] == []
    assert result["summary"]["count"] == 0


def test_list_goals_with_data(db_path: Path) -> None:
    """List goals after setting one."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=500_000)
        goal_cmd.handle_set(
            _ns(name="Fund", target=10000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        result = goal_cmd.handle_list(_ns(), conn)
    assert result["summary"]["count"] == 1
    assert result["data"]["goals"][0]["name"] == "Fund"


def test_status_up_goal(db_path: Path) -> None:
    """Status with an 'up' goal shows correct progress."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=500_000)
        goal_cmd.handle_set(
            _ns(name="Save", target=10000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        # Now increase the balance
        conn.execute(
            "UPDATE accounts SET balance_current_cents = 750000 WHERE account_type = 'checking'"
        )
        conn.commit()
        result = goal_cmd.handle_status(_ns(), conn)

    goals = result["data"]["goals"]
    assert len(goals) == 1
    g = goals[0]
    # starting=5000, current=7500, target=10000, progress=(7500-5000)/(10000-5000)*100=50%
    assert g["progress_pct"] == 50.0


def test_status_down_goal_target_zero(db_path: Path) -> None:
    """Down goal (e.g. Debt Free, target=0) shows correct progress."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="credit_card", balance_cents=-1_000_000)
        goal_cmd.handle_set(
            _ns(name="Debt Free", target=0, metric="total_debt", direction="down", deadline=None),
            conn,
        )
        # Reduce debt
        conn.execute(
            "UPDATE accounts SET balance_current_cents = -600000 WHERE account_type = 'credit_card'"
        )
        conn.commit()
        result = goal_cmd.handle_status(_ns(), conn)

    g = result["data"]["goals"][0]
    # starting=10000 cents debt, current=6000 cents debt, target=0
    # progress = (10000-6000)/(10000-0)*100 = 40%
    assert g["progress_pct"] == 40.0


def test_progress_clamped_0_100(db_path: Path) -> None:
    """Progress is clamped between 0 and 100."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=500_000)
        goal_cmd.handle_set(
            _ns(name="Save", target=10000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        # Balance goes below starting
        conn.execute(
            "UPDATE accounts SET balance_current_cents = 100000 WHERE account_type = 'checking'"
        )
        conn.commit()
        result = goal_cmd.handle_status(_ns(), conn)

    g = result["data"]["goals"][0]
    assert g["progress_pct"] == 0.0  # clamped to 0

    with connect(db_path) as conn:
        # Balance exceeds target
        conn.execute(
            "UPDATE accounts SET balance_current_cents = 1500000 WHERE account_type = 'checking'"
        )
        conn.commit()
        result = goal_cmd.handle_status(_ns(), conn)
    g = result["data"]["goals"][0]
    assert g["progress_pct"] == 100.0  # clamped to 100


def test_savings_rate_goal(db_path: Path) -> None:
    """Savings rate goal works with percentage."""
    with connect(db_path) as conn:
        aid = _seed_account(conn, account_type="checking", balance_cents=500_000)
        # Seed income and expense in last 3 months
        for month in ["2025-12-15", "2026-01-15", "2026-02-15"]:
            _seed_transaction(conn, account_id=aid, amount_cents=100_00, date=month)
            _seed_transaction(conn, account_id=aid, amount_cents=-80_00, date=month)
        result = goal_cmd.handle_set(
            _ns(name="Save More", target=30, metric="savings_rate", direction="up", deadline=None),
            conn,
        )
    assert result["data"]["goal"]["metric"] == "savings_rate"
    assert result["data"]["goal"]["target_pct"] == 30


def test_time_to_target_zero_trend(db_path: Path) -> None:
    """Time-to-target with zero trend returns None."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=500_000)
        goal_cmd.handle_set(
            _ns(name="Save", target=10000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        # No transactions = zero trend
        result = goal_cmd.handle_status(_ns(), conn)

    g = result["data"]["goals"][0]
    assert g["estimated_months"] is None


def test_upsert_updates_existing(db_path: Path) -> None:
    """Setting same name updates existing goal."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=500_000)
        goal_cmd.handle_set(
            _ns(name="Fund", target=10000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        goal_cmd.handle_set(
            _ns(name="Fund", target=20000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        result = goal_cmd.handle_list(_ns(), conn)
    assert result["summary"]["count"] == 1
    assert result["data"]["goals"][0]["target_cents"] == 2_000_000


def test_parser_registration() -> None:
    """goal_cmd.register does not raise."""
    import argparse
    parser = argparse.ArgumentParser()
    format_parent = argparse.ArgumentParser(add_help=False)
    format_parent.add_argument("--format", default="json")
    subparsers = parser.add_subparsers(dest="command")
    goal_cmd.register(subparsers, format_parent)
    args = parser.parse_args(["goal", "set", "--name", "Test", "--target", "1000", "--metric", "liquid_cash"])
    assert args.name == "Test"
    assert args.target == 1000.0
