"""Tests for the spending trends command."""

from __future__ import annotations

import uuid
from argparse import Namespace
from datetime import date
from pathlib import Path

import pytest

from finance_cli.commands import spending_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _ns(**kwargs) -> Namespace:
    defaults = {"format": "json", "view": "all", "months": 6}
    defaults.update(kwargs)
    return Namespace(**defaults)


def _seed_account(conn) -> str:
    aid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO accounts (id, institution_name, account_name, account_type, is_active) "
        "VALUES (?, 'Test', 'Checking', 'checking', 1)",
        (aid,),
    )
    conn.commit()
    return aid


def _seed_category(conn, name="Dining") -> str:
    cid = uuid.uuid4().hex
    conn.execute("INSERT INTO categories (id, name, level) VALUES (?, ?, 1)", (cid, name))
    conn.commit()
    return cid


def _seed_expense(conn, *, account_id, category_id, amount_cents, date) -> str:
    tid = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO transactions (id, account_id, amount_cents, date, description,
           category_id, is_payment, is_active, is_reviewed, source)
           VALUES (?, ?, ?, ?, 'test expense', ?, 0, 1, 1, 'manual')""",
        (tid, account_id, amount_cents, date, category_id),
    )
    conn.commit()
    return tid


def _month_date(offset_from_current: int, day: int = 15) -> str:
    today = date.today()
    month_index = (today.year * 12) + (today.month - 1) + int(offset_from_current)
    year, month_zero = divmod(month_index, 12)
    safe_day = max(1, min(day, 28))
    return date(year, month_zero + 1, safe_day).isoformat()


def _month_key(offset_from_current: int) -> str:
    return _month_date(offset_from_current).rsplit("-", 1)[0]


def test_trends_no_data(db_path: Path) -> None:
    """Trends with no transactions should return empty categories."""
    with connect(db_path) as conn:
        result = spending_cmd.handle_trends(_ns(), conn)
    assert result["data"]["categories"] == []
    assert result["summary"]["total_categories"] == 0
    assert "No spending data" in result["cli_report"]


def test_trends_with_data(db_path: Path) -> None:
    """Trends should produce per-category monthly breakdown."""
    with connect(db_path) as conn:
        aid = _seed_account(conn)
        cid = _seed_category(conn, "Dining")
        _seed_expense(conn, account_id=aid, category_id=cid, amount_cents=-50000, date=_month_date(-1))
        _seed_expense(conn, account_id=aid, category_id=cid, amount_cents=-70000, date=_month_date(0, day=1))
        result = spending_cmd.handle_trends(_ns(months=3), conn)
    cats = result["data"]["categories"]
    assert len(cats) == 1
    assert cats[0]["category"] == "Dining"
    # Should have month entries
    previous_month = _month_key(-1)
    assert previous_month in cats[0]["months"]
    assert cats[0]["months"][previous_month] == 500.0


def test_trends_can_filter_to_preloaded_categories(db_path: Path) -> None:
    """Category filters support intervention preloads without changing default trends."""
    with connect(db_path) as conn:
        aid = _seed_account(conn)
        dining = _seed_category(conn, "Dining")
        travel = _seed_category(conn, "Travel")
        rent = _seed_category(conn, "Rent")
        _seed_expense(conn, account_id=aid, category_id=dining, amount_cents=-50000, date=_month_date(-1))
        _seed_expense(conn, account_id=aid, category_id=travel, amount_cents=-70000, date=_month_date(-1))
        _seed_expense(conn, account_id=aid, category_id=rent, amount_cents=-100000, date=_month_date(-1))

        result = spending_cmd.handle_trends(
            _ns(months=3, view="personal", categories=["Dining", "Travel"]),
            conn,
        )

    assert [row["category"] for row in result["data"]["categories"]] == ["Travel", "Dining"]
    assert result["data"]["category_filter"] == ["Dining", "Travel"]
    assert result["data"]["totals_cents"][_month_key(-1)] == 120000


def test_trends_cli_report_separates_large_dollar_columns(db_path: Path) -> None:
    """Large formatted dollar values should not run into the next column."""
    with connect(db_path) as conn:
        aid = _seed_account(conn)
        dining = _seed_category(conn, "Dining")
        _seed_expense(
            conn,
            account_id=aid,
            category_id=dining,
            amount_cents=-173426,
            date=_month_date(0, day=1),
        )

        result = spending_cmd.handle_trends(_ns(months=1, format="cli"), conn)

    dining_line = next(
        line for line in result["cli_report"].splitlines() if line.startswith("Dining")
    )
    total_line = next(
        line for line in result["cli_report"].splitlines() if line.startswith("TOTAL")
    )

    assert "$1,734.26$1,734.26" not in result["cli_report"]
    assert dining_line.split()[1:3] == ["$1,734.26", "$1,734.26"]
    assert total_line.split()[1:3] == ["$1,734.26", "$1,734.26"]


def test_trends_can_filter_to_uncategorized_preload(db_path: Path) -> None:
    """B-2 can preload Uncategorized when null-category spend is a driver."""
    with connect(db_path) as conn:
        aid = _seed_account(conn)
        dining = _seed_category(conn, "Dining")
        _seed_expense(conn, account_id=aid, category_id=dining, amount_cents=-50000, date=_month_date(-1))
        conn.execute(
            """INSERT INTO transactions (id, account_id, amount_cents, date, description,
               category_id, use_type, is_payment, is_active, is_reviewed, source)
               VALUES (?, ?, -70000, ?, 'uncategorized expense', NULL, 'Personal', 0, 1, 0, 'manual')""",
            (uuid.uuid4().hex, aid, _month_date(-1)),
        )
        conn.commit()

        result = spending_cmd.handle_trends(
            _ns(months=3, view="personal", categories=["Uncategorized"]),
            conn,
        )

    assert [row["category"] for row in result["data"]["categories"]] == ["Uncategorized"]
    assert result["data"]["totals_cents"][_month_key(-1)] == 70000


def test_trends_trend_indicators(db_path: Path) -> None:
    """Trend indicator should flag increases and decreases."""
    with connect(db_path) as conn:
        aid = _seed_account(conn)
        cid = _seed_category(conn, "Dining")
        # Prior months: $100 each
        _seed_expense(conn, account_id=aid, category_id=cid, amount_cents=-10000, date=_month_date(-2))
        _seed_expense(conn, account_id=aid, category_id=cid, amount_cents=-10000, date=_month_date(-1))
        # Last month: $200 (200% of avg = up)
        _seed_expense(conn, account_id=aid, category_id=cid, amount_cents=-20000, date=_month_date(0, day=1))
        result = spending_cmd.handle_trends(_ns(months=3), conn)
    cats = result["data"]["categories"]
    assert cats[0]["trend"] == "\u2191"  # up arrow


def test_trends_months_param(db_path: Path) -> None:
    """The --months parameter should control how many months appear."""
    with connect(db_path) as conn:
        aid = _seed_account(conn)
        cid = _seed_category(conn, "Dining")
        _seed_expense(conn, account_id=aid, category_id=cid, amount_cents=-10000, date=_month_date(0, day=1))
        result = spending_cmd.handle_trends(_ns(months=2), conn)
    assert result["summary"]["months"] == 2
    assert len(result["data"]["months"]) == 2


def test_trends_parser_registration() -> None:
    """The register function should add a 'spending' parser with 'trends' subcommand."""
    import argparse
    parser = argparse.ArgumentParser()
    format_parent = argparse.ArgumentParser(add_help=False)
    format_parent.add_argument("--format", default="json")
    subs = parser.add_subparsers(dest="command")
    spending_cmd.register(subs, format_parent)
    args = parser.parse_args(["spending", "trends", "--months", "3", "--category", "Dining"])
    assert args.command == "spending"
    assert args.spending_command == "trends"
    assert args.months == 3
    assert args.categories == ["Dining"]


def test_trends_excludes_payments(db_path: Path) -> None:
    """Payment transactions should not appear in spending trends."""
    with connect(db_path) as conn:
        aid = _seed_account(conn)
        cid = _seed_category(conn, "Payments & Transfers")
        tid = uuid.uuid4().hex
        conn.execute(
            """INSERT INTO transactions (id, account_id, amount_cents, date, description,
               category_id, is_payment, is_active, is_reviewed, source)
               VALUES (?, ?, -50000, ?, 'payment', ?, 1, 1, 1, 'manual')""",
            (tid, aid, _month_date(0, day=1), cid),
        )
        conn.commit()
        result = spending_cmd.handle_trends(_ns(months=2), conn)
    assert result["data"]["categories"] == []
