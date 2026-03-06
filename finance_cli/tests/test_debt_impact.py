from __future__ import annotations

import argparse
import calendar
import uuid
from argparse import Namespace
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pytest

from finance_cli.commands import debt_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _month_start(months_back: int) -> date:
    current = date.today().replace(day=1)
    start = current
    for _ in range(months_back):
        start = (start - timedelta(days=1)).replace(day=1)
    return start


def _month_date(months_back: int, day: int = 15) -> str:
    start = _month_start(months_back)
    max_day = calendar.monthrange(start.year, start.month)[1]
    return start.replace(day=min(day, max_day)).isoformat()


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


def _seed_credit_account(
    conn,
    *,
    institution_name: str,
    account_name: str,
    card_ending: str,
    balance_current_cents: int,
    balance_limit_cents: int | None = None,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            card_ending, balance_current_cents, balance_limit_cents, is_active
        ) VALUES (?, ?, ?, 'credit_card', ?, ?, ?, 1)
        """,
        (
            account_id,
            institution_name,
            account_name,
            card_ending,
            balance_current_cents,
            balance_limit_cents,
        ),
    )
    conn.commit()
    return account_id


def _seed_credit_liability(
    conn,
    *,
    account_id: str,
    apr_purchase: float | None,
    minimum_payment_cents: int | None = None,
    next_monthly_payment_cents: int | None = None,
) -> str:
    liability_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active,
            apr_purchase, minimum_payment_cents, next_monthly_payment_cents
        ) VALUES (?, ?, 'credit', 1, ?, ?, ?)
        """,
        (liability_id, account_id, apr_purchase, minimum_payment_cents, next_monthly_payment_cents),
    )
    conn.commit()
    return liability_id


def _seed_three_month_spend(conn, category_id: str, monthly_amount_cents: int) -> None:
    for months_back in (1, 2, 3):
        _seed_txn(
            conn,
            category_id=category_id,
            amount_cents=-abs(monthly_amount_cents),
            txn_date=_month_date(months_back),
        )


def _run_impact(conn, *, months: int = 3, cut_pct: int = 50) -> dict:
    return debt_cmd.handle_impact(
        Namespace(months=months, cut_pct=cut_pct, format="json"),
        conn,
    )


def test_classification(db_path: Path) -> None:
    with connect(db_path) as conn:
        utilities = _seed_category(conn, "Utilities")
        dining = _seed_category(conn, "Dining")
        transfers = _seed_category(conn, "Payments & Transfers")

        _seed_three_month_spend(conn, utilities, 3_000)
        _seed_three_month_spend(conn, dining, 2_000)
        _seed_three_month_spend(conn, transfers, 1_000)

        result = _run_impact(conn)

    by_name = {row["category_name"]: row for row in result["data"]["categories"]}
    assert by_name["Utilities"]["classification"] == "essential"
    assert by_name["Dining"]["classification"] == "discretionary"
    assert by_name["Payments & Transfers"]["classification"] == "excluded"


def test_impact_with_debt(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        shopping = _seed_category(conn, "Shopping")
        _seed_three_month_spend(conn, dining, 4_000)
        _seed_three_month_spend(conn, shopping, 3_000)

        account_id = _seed_credit_account(
            conn,
            institution_name="Chase",
            account_name="Freedom",
            card_ending="1111",
            balance_current_cents=-80_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=24.99, minimum_payment_cents=1_200)

        result = _run_impact(conn)

    assert result["data"]["baseline"]["total_debt_cents"] == 80_000
    assert result["data"]["scenarios"]
    assert result["data"]["scenarios"][0]["monthly_savings_cents"] > 0
    assert result["data"]["scenarios"][0]["interest_saved_cents"] > 0


def test_impact_no_debt(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_three_month_spend(conn, dining, 2_500)
        result = _run_impact(conn)

    assert result["data"]["baseline"]["total_debt_cents"] == 0
    assert result["data"]["scenarios"] == []
    assert result["data"]["discretionary_count"] == 1


def test_impact_no_spending(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_credit_account(
            conn,
            institution_name="Barclays",
            account_name="View",
            card_ending="2222",
            balance_current_cents=-25_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=22.22, minimum_payment_cents=800)

        result = _run_impact(conn)

    assert result["data"]["categories"] == []
    assert result["data"]["discretionary_monthly_cents"] == 0
    assert len(result["data"]["scenarios"]) == 2
    assert all(item["monthly_savings_cents"] == 0 for item in result["data"]["scenarios"])


def test_impact_empty_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = _run_impact(conn)

    assert result["data"]["categories"] == []
    assert result["data"]["baseline"]["total_debt_cents"] == 0
    assert result["data"]["scenarios"] == []
    assert result["summary"]["total_categories"] == 0


def test_per_category_estimates_proportional(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        shopping = _seed_category(conn, "Shopping")
        entertainment = _seed_category(conn, "Entertainment")

        _seed_three_month_spend(conn, dining, 1_000)
        _seed_three_month_spend(conn, shopping, 2_000)
        _seed_three_month_spend(conn, entertainment, 3_000)

        account_id = _seed_credit_account(
            conn,
            institution_name="Amex",
            account_name="Gold",
            card_ending="3333",
            balance_current_cents=-70_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=26.99, minimum_payment_cents=1_000)

        result = _run_impact(conn)

    discretionary = [row for row in result["data"]["categories"] if row["classification"] == "discretionary"]
    estimated_total = sum(int(row["est_interest_saved_cents"]) for row in discretionary)
    scenario_total = int(result["data"]["scenarios"][0]["interest_saved_cents"])
    assert abs(estimated_total - scenario_total) <= len(discretionary)


def test_per_category_estimates_zero_discretionary(db_path: Path) -> None:
    with connect(db_path) as conn:
        utilities = _seed_category(conn, "Utilities")
        rent = _seed_category(conn, "Rent")
        _seed_three_month_spend(conn, utilities, 2_000)
        _seed_three_month_spend(conn, rent, 50_000)

        account_id = _seed_credit_account(
            conn,
            institution_name="Discover",
            account_name="IT",
            card_ending="4444",
            balance_current_cents=-30_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=15.99, minimum_payment_cents=900)

        result = _run_impact(conn)

    assert result["data"]["discretionary_count"] == 0
    for row in result["data"]["categories"]:
        assert int(row["est_interest_saved_cents"]) == 0
        assert float(row["est_months_saved"]) == 0.0


def test_custom_cut_pct(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_three_month_spend(conn, dining, 6_000)

        account_id = _seed_credit_account(
            conn,
            institution_name="Citi",
            account_name="Premier",
            card_ending="5555",
            balance_current_cents=-40_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=23.5, minimum_payment_cents=1_200)

        result = _run_impact(conn, cut_pct=30)

    expected = int(
        (
            Decimal(result["data"]["discretionary_monthly_cents"])
            * Decimal("30")
            / Decimal("100")
        ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )
    assert result["data"]["scenarios"][0]["monthly_savings_cents"] == expected
    assert result["data"]["scenarios"][0]["cut_pct"] == 30


def test_cut_pct_100(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_three_month_spend(conn, dining, 4_000)

        account_id = _seed_credit_account(
            conn,
            institution_name="Citi",
            account_name="Double Cash",
            card_ending="6666",
            balance_current_cents=-35_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=19.9, minimum_payment_cents=900)

        result = _run_impact(conn, cut_pct=100)

    assert result["data"]["scenarios"][0]["monthly_savings_cents"] == result["data"]["discretionary_monthly_cents"]


def test_cut_pct_invalid_rejected() -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        debt_cmd._valid_cut_pct("0")
    with pytest.raises(argparse.ArgumentTypeError):
        debt_cmd._valid_cut_pct("-5")
    with pytest.raises(argparse.ArgumentTypeError):
        debt_cmd._valid_cut_pct("101")


def test_custom_months(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=dining, amount_cents=-6_000, txn_date=_month_date(6))

        result_6 = _run_impact(conn, months=6)
        result_3 = _run_impact(conn, months=3)

    assert result_6["data"]["discretionary_monthly_cents"] == 1_000
    assert result_3["data"]["discretionary_monthly_cents"] == 0


def test_only_complete_months(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_three_month_spend(conn, dining, 1_000)
        _seed_txn(conn, category_id=dining, amount_cents=-9_000, txn_date=date.today().isoformat())

        result = _run_impact(conn)

    assert result["data"]["discretionary_monthly_cents"] == 1_000


def test_all_lookback_months_empty(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=dining, amount_cents=-3_000, txn_date=date.today().isoformat())

        result = _run_impact(conn, months=3)

    assert result["data"]["categories"] == []
    assert result["data"]["discretionary_monthly_cents"] == 0


def test_excludes_payments(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=dining, amount_cents=-3_000, txn_date=_month_date(1), is_payment=0)
        _seed_txn(conn, category_id=dining, amount_cents=-6_000, txn_date=_month_date(1), is_payment=1)

        result = _run_impact(conn)

    by_name = {row["category_name"]: row for row in result["data"]["categories"]}
    assert by_name["Dining"]["total_cents"] == 3_000
    assert by_name["Dining"]["avg_monthly_cents"] == 1_000


def test_excludes_income(db_path: Path) -> None:
    with connect(db_path) as conn:
        income = _seed_category(conn, "Income: Salary", is_income=1)
        dining = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=income, amount_cents=-9_000, txn_date=_month_date(1))
        _seed_txn(conn, category_id=dining, amount_cents=-3_000, txn_date=_month_date(1))

        result = _run_impact(conn)

    names = {row["category_name"] for row in result["data"]["categories"]}
    assert "Income: Salary" not in names
    assert result["data"]["discretionary_monthly_cents"] == 1_000


def test_baseline_capped_payoff(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_three_month_spend(conn, dining, 1_000)

        account_id = _seed_credit_account(
            conn,
            institution_name="Capital One",
            account_name="Venture",
            card_ending="7777",
            balance_current_cents=-100_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=29.99, minimum_payment_cents=100)

        result = _run_impact(conn)

    assert result["data"]["baseline"]["fully_paid_off"] is False
    assert result["data"]["baseline"]["months_to_payoff"] is None


def test_scenarios_count(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_three_month_spend(conn, dining, 2_000)

        account_id = _seed_credit_account(
            conn,
            institution_name="Chase",
            account_name="Freedom",
            card_ending="8888",
            balance_current_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=21.0, minimum_payment_cents=700)

        result_default = _run_impact(conn, cut_pct=50)
        result_low = _run_impact(conn, cut_pct=1)

    assert len(result_default["data"]["scenarios"]) == 3
    assert len(result_low["data"]["scenarios"]) == 2


def test_cli_report_sections(db_path: Path) -> None:
    with connect(db_path) as conn:
        dining = _seed_category(conn, "Dining")
        _seed_three_month_spend(conn, dining, 2_000)

        account_id = _seed_credit_account(
            conn,
            institution_name="Chase",
            account_name="Freedom",
            card_ending="9999",
            balance_current_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=18.99, minimum_payment_cents=800)

        result = _run_impact(conn)

    cli_report = result["cli_report"]
    assert "SPENDING IMPACT ANALYSIS" in cli_report
    assert "DEBT CONTEXT" in cli_report
    assert "DISCRETIONARY CATEGORIES" in cli_report
    assert "SCENARIOS" in cli_report
