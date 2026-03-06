from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from finance_cli import spending_analysis
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_category(
    conn,
    name: str,
    *,
    parent_id: str | None = None,
    is_income: int = 0,
) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, parent_id, is_income, is_system)
        VALUES (?, ?, ?, ?, 0)
        """,
        (category_id, name, parent_id, is_income),
    )
    conn.commit()
    return category_id


def _seed_txn(
    conn,
    *,
    category_id: str,
    amount_cents: int,
    txn_date: str,
    is_payment: int = 0,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions
            (id, date, description, amount_cents, category_id, is_active, is_payment, source)
        VALUES (?, ?, 'TEST', ?, ?, 1, ?, 'manual')
        """,
        (txn_id, txn_date, amount_cents, category_id, is_payment),
    )
    conn.commit()
    return txn_id


def test_load_essential_categories_default(monkeypatch) -> None:
    monkeypatch.setattr(spending_analysis, "load_rules", lambda: SimpleNamespace(raw={}))
    loaded = spending_analysis.load_essential_categories()
    assert loaded == spending_analysis._DEFAULT_ESSENTIAL_CATEGORIES


def test_load_essential_categories_from_rules(monkeypatch) -> None:
    monkeypatch.setattr(
        spending_analysis,
        "load_rules",
        lambda: SimpleNamespace(raw={"essential_categories": ["Entertainment", " Utilities "]}),
    )
    loaded = spending_analysis.load_essential_categories()
    assert loaded == frozenset({"Entertainment", "Utilities"})


def test_is_essential_case_insensitive() -> None:
    essential = frozenset({"Utilities"})
    assert spending_analysis.is_essential("utilities", essential)
    assert spending_analysis.is_essential(" Utilities ", essential)


def test_is_excluded() -> None:
    assert spending_analysis.is_excluded("Payments & Transfers")
    assert spending_analysis.is_excluded("bank charges & fees")
    assert spending_analysis.is_excluded(" Income: Salary ")


def test_is_excluded_rejects_non_excluded() -> None:
    assert spending_analysis.is_excluded("Dining") is False
    assert spending_analysis.is_excluded("Entertainment") is False


def test_category_spending_averages_math(db_path: Path) -> None:
    with connect(db_path) as conn:
        food = _seed_category(conn, "Food & Drink")
        dining = _seed_category(conn, "Dining", parent_id=food)
        utilities = _seed_category(conn, "Utilities")

        _seed_txn(conn, category_id=dining, amount_cents=-1_000, txn_date="2025-11-15")
        _seed_txn(conn, category_id=dining, amount_cents=-2_000, txn_date="2025-12-15")
        _seed_txn(conn, category_id=dining, amount_cents=-3_000, txn_date="2026-01-15")

        _seed_txn(conn, category_id=utilities, amount_cents=-3_000, txn_date="2025-11-20")
        _seed_txn(conn, category_id=utilities, amount_cents=-3_000, txn_date="2025-12-20")
        _seed_txn(conn, category_id=utilities, amount_cents=-3_000, txn_date="2026-01-20")

        rows = spending_analysis.category_spending_averages(
            conn,
            months=3,
            as_of=date(2026, 2, 1),
        )

    by_name = {item.category_name: item for item in rows}

    assert by_name["Dining"].total_cents == 6_000
    assert by_name["Dining"].avg_monthly_cents == 2_000
    assert by_name["Dining"].months_with_data == 3
    assert by_name["Dining"].classification == "discretionary"

    assert by_name["Utilities"].total_cents == 9_000
    assert by_name["Utilities"].avg_monthly_cents == 3_000
    assert by_name["Utilities"].months_with_data == 3
    assert by_name["Utilities"].classification == "essential"
