from __future__ import annotations

from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.starter_setup import (
    starter_budget_propose,
    starter_goal_propose,
    starter_rule_propose,
)


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _add_account(conn, *, account_id: str = "acct_1", account_type: str = "checking", balance: int = 100_000) -> None:
    conn.execute(
        """
        INSERT INTO accounts (id, institution_name, account_name, account_type, balance_current_cents, is_active)
        VALUES (?, 'Test Bank', ?, ?, ?, 1)
        """,
        (account_id, account_id, account_type, balance),
    )


def _add_category(conn, category_id: str, name: str, *, is_income: int = 0) -> None:
    conn.execute(
        "INSERT INTO categories (id, name, is_income, is_system) VALUES (?, ?, ?, 0)",
        (category_id, name, is_income),
    )


def _add_txn(
    conn,
    txn_id: str,
    *,
    date: str,
    amount_cents: int,
    category_id: str | None,
    description: str = "Txn",
) -> None:
    conn.execute(
        """
        INSERT INTO transactions (id, account_id, date, description, amount_cents, category_id, source, is_active)
        VALUES (?, 'acct_1', ?, ?, ?, ?, 'manual', 1)
        """,
        (txn_id, date, description, amount_cents, category_id),
    )


def test_starter_budget_propose_typical(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    with connect(db_path) as conn:
        _add_account(conn)
        _add_category(conn, "cat_grocery", "Groceries")
        _add_category(conn, "cat_dining", "Dining")
        _add_category(conn, "cat_travel", "Travel")
        for month in ("01", "02", "03"):
            _add_txn(conn, f"grocery_{month}", date=f"2026-{month}-05", amount_cents=-50_000, category_id="cat_grocery")
            _add_txn(conn, f"dining_{month}", date=f"2026-{month}-06", amount_cents=-30_000, category_id="cat_dining")
            _add_txn(conn, f"travel_{month}", date=f"2026-{month}-07", amount_cents=-20_000, category_id="cat_travel")

        proposals = starter_budget_propose(conn, "salaried", "save_more", 3)

    assert 3 <= len(proposals) <= 5
    first = proposals[0]
    assert first.category == "Groceries"
    assert first.amount_cents < first.historical_monthly_average_cents
    assert first.period == "monthly"
    assert first.view == "personal"
    assert first.id.startswith("budget_")


def test_starter_budget_propose_sparse_data(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    with connect(db_path) as conn:
        _add_account(conn)
        _add_category(conn, "cat_dining", "Dining")
        _add_category(conn, "cat_travel", "Travel")
        _add_txn(conn, "dining_1", date="2026-03-01", amount_cents=-30_000, category_id="cat_dining")
        _add_txn(conn, "travel_1", date="2026-03-02", amount_cents=-25_000, category_id="cat_travel")

        proposals = starter_budget_propose(conn, "salaried", "spending_clarity", 1)

    assert len(proposals) <= 2
    assert all("averaged" in proposal.rationale for proposal in proposals)


def test_starter_goal_propose_save(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    with connect(db_path) as conn:
        _add_account(conn, balance=50_000)
        _add_category(conn, "cat_dining", "Dining")
        for month in ("01", "02", "03"):
            _add_txn(conn, f"dining_{month}", date=f"2026-{month}-06", amount_cents=-100_000, category_id="cat_dining")

        proposals = starter_goal_propose(conn, "save_more", "salaried")

    assert len(proposals) == 1
    assert proposals[0].name == "3-month emergency fund"
    assert proposals[0].metric == "liquid_cash"
    assert proposals[0].target_cents >= 300_000


def test_starter_goal_propose_debt(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    with connect(db_path) as conn:
        _add_account(conn, account_id="card_low", account_type="credit_card", balance=-80_000)
        _add_account(conn, account_id="card_high", account_type="credit_card", balance=-120_000)
        conn.execute(
            """
            INSERT INTO liabilities (id, account_id, liability_type, apr_purchase, is_active)
            VALUES ('liab_low', 'card_low', 'credit', 18.5, 1),
                   ('liab_high', 'card_high', 'credit', 29.9, 1)
            """
        )

        proposals = starter_goal_propose(conn, "pay_down_debt", "salaried")

    assert len(proposals) == 1
    assert proposals[0].metric == "total_debt"
    assert proposals[0].direction == "down"
    assert "card_high" in proposals[0].name
    assert "29.90%" in proposals[0].rationale


def test_starter_goal_propose_clarity(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    with connect(db_path) as conn:
        _add_account(conn)
        proposals = starter_goal_propose(conn, "spending_clarity", "salaried")

    assert proposals == []


def test_starter_rule_propose_side_hustle(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    with connect(db_path) as conn:
        _add_account(conn)
        _add_category(conn, "cat_software", "Software & Subscriptions")
        _add_category(conn, "cat_office", "Office Expense")
        _add_txn(
            conn,
            "software_1",
            date="2026-03-01",
            amount_cents=-6_000,
            category_id="cat_software",
            description="ADOBE CREATIVE CLOUD",
        )

        proposals = starter_rule_propose(conn, "side_hustle")

    assert len(proposals) == 1
    assert proposals[0].business_category == "Office Expense"
    assert proposals[0].personal_category == "Software & Subscriptions"
    assert proposals[0].match_keywords == ("ADOBE",)


def test_starter_rule_propose_salaried(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    with connect(db_path) as conn:
        _add_account(conn)
        proposals = starter_rule_propose(conn, "salaried")

    assert proposals == []
