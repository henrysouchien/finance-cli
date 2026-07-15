"""Tests for ``goal_cmd.handle_find`` — name-keyed lookup used by collision
detection and post-write goal_id recovery in the savings-goal skill."""

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


def _seed_account(conn, *, account_type="savings", balance_cents=0) -> str:
    aid = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO accounts (id, institution_name, account_name, account_type,
           balance_current_cents, is_active) VALUES (?, 'Test', ?, ?, ?, 1)""",
        (aid, f"{account_type} account", account_type, balance_cents),
    )
    conn.commit()
    return aid


def test_find_returns_none_for_missing_goal(db_path: Path) -> None:
    """No row -> data.goal is None, summary.found is False, no raise."""
    with connect(db_path) as conn:
        result = goal_cmd.handle_find(_ns(name="nothing-here"), conn)
    assert result["data"]["goal"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["name"] == "nothing-here"
    assert result["summary"]["is_active"] is None


def test_find_returns_active_row(db_path: Path) -> None:
    """Find an active goal by exact name returns the row including id + updated_at."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="savings", balance_cents=500_000)
        goal_cmd.handle_set(
            _ns(
                name="down-payment-2027",
                target=20000,
                metric="liquid_cash",
                direction="up",
                deadline="2027-11-15",
            ),
            conn,
        )
        result = goal_cmd.handle_find(_ns(name="down-payment-2027"), conn)
    assert result["summary"]["found"] is True
    assert result["summary"]["is_active"] is True
    goal = result["data"]["goal"]
    assert goal is not None
    assert goal["name"] == "down-payment-2027"
    assert goal["metric"] == "liquid_cash"
    assert goal["target_cents"] == 20_000 * 100
    # id and updated_at are required for the savings-goal race-safety pattern.
    assert goal["id"]
    assert goal["updated_at"]


def test_find_active_only_skips_inactive(db_path: Path) -> None:
    """Default lookup filters is_active=1, so a soft-deleted row is invisible."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="savings", balance_cents=500_000)
        goal_cmd.handle_set(
            _ns(name="vacation-2026", target=5000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        conn.execute("UPDATE goals SET is_active = 0 WHERE name = ?", ("vacation-2026",))
        conn.commit()

        active_only = goal_cmd.handle_find(_ns(name="vacation-2026"), conn)
        with_inactive = goal_cmd.handle_find(
            _ns(name="vacation-2026", include_inactive=True),
            conn,
        )

    assert active_only["summary"]["found"] is False
    assert active_only["data"]["goal"] is None

    assert with_inactive["summary"]["found"] is True
    assert with_inactive["summary"]["is_active"] is False
    assert with_inactive["data"]["goal"]["name"] == "vacation-2026"


def test_find_include_inactive_returns_active_row_too(db_path: Path) -> None:
    """include_inactive=True is a superset — active rows still surface."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="savings", balance_cents=500_000)
        goal_cmd.handle_set(
            _ns(name="wedding-fund", target=20000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        result = goal_cmd.handle_find(_ns(name="wedding-fund", include_inactive=True), conn)
    assert result["summary"]["found"] is True
    assert result["summary"]["is_active"] is True
    assert result["data"]["goal"]["name"] == "wedding-fund"


def test_find_recovers_updated_at_advancing_on_reset(db_path: Path) -> None:
    """The post-write recovery path needs a fresh updated_at after each goal_set.

    INSERT OR REPLACE re-runs ``datetime('now')`` for the updated_at column, so
    the artificially backdated value is replaced. We can't rely on
    ``second > first`` because the inserts may land within the same second of
    wall-clock time, but we can verify the backdated value is no longer present
    AND the same row id is preserved.
    """
    with connect(db_path) as conn:
        _seed_account(conn, account_type="savings", balance_cents=500_000)
        goal_cmd.handle_set(
            _ns(name="car-down", target=10000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        first = goal_cmd.handle_find(_ns(name="car-down"), conn)
        backdated = "2020-01-01 00:00:00"
        conn.execute(
            "UPDATE goals SET updated_at = ? WHERE name = ?",
            (backdated, "car-down"),
        )
        conn.commit()
        goal_cmd.handle_set(
            _ns(name="car-down", target=12000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        second = goal_cmd.handle_find(_ns(name="car-down"), conn)

    assert first["data"]["goal"]["id"] == second["data"]["goal"]["id"]
    assert second["data"]["goal"]["updated_at"] != backdated
    assert second["data"]["goal"]["target_cents"] == 12_000 * 100


def test_find_returns_cli_report(db_path: Path) -> None:
    """CLI surface gets a one-line human-readable summary."""
    with connect(db_path) as conn:
        _seed_account(conn, account_type="savings", balance_cents=0)
        goal_cmd.handle_set(
            _ns(name="test-goal", target=1000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        result = goal_cmd.handle_find(_ns(name="test-goal"), conn)
    assert "test-goal" in result["cli_report"]
    assert "is_active" in result["cli_report"]
