from __future__ import annotations

import uuid
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from finance_cli import spending_analysis
from finance_cli.commands import subs
from finance_cli.db import connect, initialize_database
from finance_cli.user_rules import CANONICAL_CATEGORIES


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_subscription(
    conn,
    *,
    vendor_name: str,
    category_id: str | None,
    amount_cents: int,
    frequency: str = "monthly",
    is_active: int = 1,
) -> str:
    subscription_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO subscriptions (
            id, vendor_name, category_id, amount_cents, frequency, next_expected, account_id, is_active, use_type, is_auto_detected
        ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, NULL, 0)
        """,
        (subscription_id, vendor_name, category_id, amount_cents, frequency, is_active),
    )
    conn.commit()
    return subscription_id


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


def _run_audit(conn) -> dict:
    return subs.handle_audit(Namespace(format="json"), conn)


def test_classification(db_path: Path) -> None:
    with connect(db_path) as conn:
        utilities = _seed_category(conn, "Utilities")
        entertainment = _seed_category(conn, "Entertainment")
        _seed_subscription(conn, vendor_name="ConEd", category_id=utilities, amount_cents=8_000)
        _seed_subscription(conn, vendor_name="Netflix", category_id=entertainment, amount_cents=1_999)

        result = _run_audit(conn)

    by_vendor = {row["vendor_name"]: row for row in result["data"]["subscriptions"]}
    assert by_vendor["ConEd"]["classification"] == "essential"
    assert by_vendor["Netflix"]["classification"] == "discretionary"


def test_classification_case_insensitive() -> None:
    essential = frozenset({"Utilities"})
    assert subs._is_essential("utilities", essential)
    assert subs._is_essential(" Utilities ", essential)


def test_essential_categories_default() -> None:
    assert isinstance(subs._DEFAULT_ESSENTIAL_CATEGORIES, frozenset)
    assert subs._DEFAULT_ESSENTIAL_CATEGORIES <= CANONICAL_CATEGORIES


def test_essential_categories_from_rules(monkeypatch) -> None:
    monkeypatch.setattr(
        spending_analysis,
        "load_rules",
        lambda: SimpleNamespace(raw={"essential_categories": ["Entertainment", " Utilities "]}),
    )
    loaded = subs._load_essential_categories()
    assert loaded == frozenset({"Entertainment", "Utilities"})
    assert subs._is_essential("entertainment", loaded)


def test_essential_categories_malformed(monkeypatch) -> None:
    monkeypatch.setattr(spending_analysis, "load_rules", lambda: SimpleNamespace(raw={"essential_categories": "bad"}))
    assert subs._load_essential_categories() == subs._DEFAULT_ESSENTIAL_CATEGORIES


def test_essential_categories_empty_list(monkeypatch) -> None:
    monkeypatch.setattr(spending_analysis, "load_rules", lambda: SimpleNamespace(raw={"essential_categories": []}))
    assert subs._load_essential_categories() == frozenset()


def test_essential_categories_non_string_entries(monkeypatch) -> None:
    monkeypatch.setattr(spending_analysis, "load_rules", lambda: SimpleNamespace(raw={"essential_categories": [123, True]}))
    assert subs._load_essential_categories() == subs._DEFAULT_ESSENTIAL_CATEGORIES


def test_audit_with_debt(db_path: Path) -> None:
    with connect(db_path) as conn:
        utilities = _seed_category(conn, "Utilities")
        entertainment = _seed_category(conn, "Entertainment")
        _seed_subscription(conn, vendor_name="Water", category_id=utilities, amount_cents=6_000)
        _seed_subscription(conn, vendor_name="Netflix", category_id=entertainment, amount_cents=2_000)
        _seed_subscription(conn, vendor_name="Disney", category_id=entertainment, amount_cents=1_500)

        first = _seed_credit_account(
            conn,
            institution_name="Chase",
            account_name="Freedom",
            card_ending="1111",
            balance_current_cents=-60_000,
        )
        second = _seed_credit_account(
            conn,
            institution_name="Amex",
            account_name="Gold",
            card_ending="2222",
            balance_current_cents=-40_000,
        )
        _seed_credit_liability(conn, account_id=first, apr_purchase=24.99, minimum_payment_cents=2_000)
        _seed_credit_liability(conn, account_id=second, apr_purchase=16.99, minimum_payment_cents=1_200)

        result = _run_audit(conn)

    data = result["data"]
    assert data["baseline"]["total_debt_cents"] == 100_000
    assert len(data["scenarios"]) == 3
    assert data["discretionary_count"] == 2
    assert data["essential_count"] == 1
    assert data["scenarios"][0]["monthly_savings_cents"] > 0
    assert data["scenarios"][0]["interest_saved_cents"] > 0
    assert data["scenarios"][0]["months_shaved"] > 0


def test_audit_no_debt(db_path: Path) -> None:
    with connect(db_path) as conn:
        utilities = _seed_category(conn, "Utilities")
        entertainment = _seed_category(conn, "Entertainment")
        _seed_subscription(conn, vendor_name="Water", category_id=utilities, amount_cents=6_000)
        _seed_subscription(conn, vendor_name="Netflix", category_id=entertainment, amount_cents=2_000)

        result = _run_audit(conn)

    assert result["data"]["baseline"]["total_debt_cents"] == 0
    assert result["data"]["scenarios"] == []
    by_vendor = {row["vendor_name"]: row for row in result["data"]["subscriptions"]}
    assert by_vendor["Water"]["classification"] == "essential"
    assert by_vendor["Netflix"]["classification"] == "discretionary"


def test_audit_no_subs(db_path: Path) -> None:
    with connect(db_path) as conn:
        card = _seed_credit_account(
            conn,
            institution_name="Citi",
            account_name="Premier",
            card_ending="3333",
            balance_current_cents=-25_000,
        )
        _seed_credit_liability(conn, account_id=card, apr_purchase=19.99, minimum_payment_cents=1_000)
        result = _run_audit(conn)

    assert result["summary"]["total_subscriptions"] == 0
    assert result["data"]["discretionary_count"] == 0
    assert len(result["data"]["scenarios"]) == 3
    assert all(item["monthly_savings_cents"] == 0 for item in result["data"]["scenarios"])
    assert all(item["interest_saved_cents"] == 0 for item in result["data"]["scenarios"])
    assert all(item["months_shaved"] == 0 for item in result["data"]["scenarios"])


def test_audit_empty_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = _run_audit(conn)

    assert result["data"]["subscriptions"] == []
    assert result["data"]["baseline"]["total_debt_cents"] == 0
    assert result["data"]["scenarios"] == []
    assert result["summary"]["total_subscriptions"] == 0


def test_per_sub_estimates_sum(db_path: Path) -> None:
    with connect(db_path) as conn:
        entertainment = _seed_category(conn, "Entertainment")
        _seed_subscription(conn, vendor_name="A", category_id=entertainment, amount_cents=1_000)
        _seed_subscription(conn, vendor_name="B", category_id=entertainment, amount_cents=2_000)
        _seed_subscription(conn, vendor_name="C", category_id=entertainment, amount_cents=3_000)

        card = _seed_credit_account(
            conn,
            institution_name="Chase",
            account_name="Slate",
            card_ending="4444",
            balance_current_cents=-50_000,
        )
        _seed_credit_liability(conn, account_id=card, apr_purchase=25.99, minimum_payment_cents=1_000)

        result = _run_audit(conn)

    discretionary = [item for item in result["data"]["subscriptions"] if item["classification"] == "discretionary"]
    estimated_total = sum(int(item["est_interest_saved_cents"]) for item in discretionary)
    scenario_total = int(result["data"]["scenarios"][0]["interest_saved_cents"])
    assert abs(estimated_total - scenario_total) <= len(discretionary)


def test_per_sub_estimates_zero_discretionary(db_path: Path) -> None:
    with connect(db_path) as conn:
        utilities = _seed_category(conn, "Utilities")
        rent = _seed_category(conn, "Rent")
        _seed_subscription(conn, vendor_name="Water", category_id=utilities, amount_cents=5_000)
        _seed_subscription(conn, vendor_name="Rent", category_id=rent, amount_cents=80_000)

        card = _seed_credit_account(
            conn,
            institution_name="Discover",
            account_name="IT",
            card_ending="5555",
            balance_current_cents=-30_000,
        )
        _seed_credit_liability(conn, account_id=card, apr_purchase=15.99, minimum_payment_cents=900)

        result = _run_audit(conn)

    assert result["data"]["discretionary_count"] == 0
    for item in result["data"]["subscriptions"]:
        assert int(item["est_interest_saved_cents"]) == 0
        assert float(item["est_months_saved"]) == 0.0


def test_baseline_uses_project_interest(db_path: Path) -> None:
    with connect(db_path) as conn:
        entertainment = _seed_category(conn, "Entertainment")
        _seed_subscription(conn, vendor_name="Streaming", category_id=entertainment, amount_cents=7_500)

        card = _seed_credit_account(
            conn,
            institution_name="Barclays",
            account_name="View",
            card_ending="6666",
            balance_current_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=card, apr_purchase=24.24, minimum_payment_cents=600)

        result = _run_audit(conn)

    baseline_months = result["data"]["baseline"]["months_to_payoff"]
    cut_all_months = result["data"]["scenarios"][0]["months_to_payoff"]
    assert baseline_months is not None
    assert cut_all_months is not None
    assert baseline_months >= cut_all_months
    assert result["data"]["baseline"]["total_interest_cents"] >= result["data"]["scenarios"][0]["total_interest_cents"]


def test_baseline_capped_payoff(db_path: Path) -> None:
    with connect(db_path) as conn:
        entertainment = _seed_category(conn, "Entertainment")
        _seed_subscription(conn, vendor_name="Music", category_id=entertainment, amount_cents=1_000)

        card = _seed_credit_account(
            conn,
            institution_name="Capital One",
            account_name="Venture",
            card_ending="7777",
            balance_current_cents=-100_000,
        )
        _seed_credit_liability(conn, account_id=card, apr_purchase=29.99, minimum_payment_cents=100)

        result = _run_audit(conn)

    assert result["data"]["baseline"]["fully_paid_off"] is False
    assert result["data"]["baseline"]["months_to_payoff"] is None


def test_scenario_fully_paid_off_flag(db_path: Path) -> None:
    with connect(db_path) as conn:
        card = _seed_credit_account(
            conn,
            institution_name="Capital One",
            account_name="QuickSilver",
            card_ending="8888",
            balance_current_cents=-90_000,
        )
        _seed_credit_liability(conn, account_id=card, apr_purchase=29.99, minimum_payment_cents=100)
        result = _run_audit(conn)

    scenarios = result["data"]["scenarios"]
    assert len(scenarios) == 3
    assert any(item["fully_paid_off"] is False for item in scenarios)
    for item in scenarios:
        assert "fully_paid_off" in item
        if item["fully_paid_off"] is False:
            assert item["months_to_payoff"] is None


def test_unknown_apr_baseline(db_path: Path) -> None:
    with connect(db_path) as conn:
        entertainment = _seed_category(conn, "Entertainment")
        _seed_subscription(conn, vendor_name="Netflix", category_id=entertainment, amount_cents=2_000)

        card = _seed_credit_account(
            conn,
            institution_name="Apple",
            account_name="Apple Card",
            card_ending="9999",
            balance_current_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=card, apr_purchase=None, minimum_payment_cents=700)

        result = _run_audit(conn)

    baseline = result["data"]["baseline"]
    assert baseline["apr_unknown_count"] > 0
    assert "Avg APR: N/A" in result["cli_report"]


def test_cli_report_sections(db_path: Path) -> None:
    with connect(db_path) as conn:
        entertainment = _seed_category(conn, "Entertainment")
        _seed_subscription(conn, vendor_name="Streaming", category_id=entertainment, amount_cents=1_200)

        card = _seed_credit_account(
            conn,
            institution_name="Chase",
            account_name="Freedom",
            card_ending="1212",
            balance_current_cents=-12_000,
        )
        _seed_credit_liability(conn, account_id=card, apr_purchase=18.99, minimum_payment_cents=500)
        result = _run_audit(conn)

    cli_report = result["cli_report"]
    assert "SUBSCRIPTION AUDIT" in cli_report
    assert "DEBT CONTEXT" in cli_report
    assert "SCENARIOS" in cli_report
