from __future__ import annotations

from argparse import Namespace
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest

from finance_cli.commands import spending_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import run_engine
from finance_cli.interventions.cash_flow import (
    evaluate_c1_forward_burn,
    evaluate_c2_pre_bill_warning,
    evaluate_c3_discretionary_cliff,
    evaluate_c4_income_vs_expense_mtd,
    evaluate_c6_late_deposit_overdraft,
)
from finance_cli.interventions.context import build_context


NOW = datetime(2026, 4, 9, 12, 0, 0)


class _FixedSpendingDate(date):
    @classmethod
    def today(cls) -> date:
        return NOW.date()


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _category_id(conn, name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, ?, 0, 0)
        """,
        (category_id, name, 1 if name.startswith("Income") else 0),
    )
    conn.commit()
    return category_id


def _seed_account(
    conn,
    *,
    account_type: str = "checking",
    balance_cents: int = 0,
    institution_name: str = "Bank",
    is_active: int = 1,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (account_id, institution_name, account_type, account_type, balance_cents, is_active),
    )
    conn.commit()
    return account_id


def _seed_transaction(
    conn,
    *,
    account_id: str,
    category_name: str | None,
    amount_cents: int,
    txn_date: str,
    description: str = "seed",
    is_payment: int = 0,
    is_active: int = 1,
    use_type: str | None = None,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id, use_type,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'manual')
        """,
        (
            txn_id,
            account_id,
            txn_date,
            description,
            amount_cents,
            _category_id(conn, category_name) if category_name is not None else None,
            use_type,
            is_payment,
            is_active,
        ),
    )
    conn.commit()
    return txn_id


def _seed_budget(
    conn,
    *,
    category_name: str,
    amount_cents: int,
    use_type: str = "Personal",
    period: str = "monthly",
    effective_from: str = "2026-04-01",
    effective_to: str | None = None,
) -> str:
    budget_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO budgets (
            id, category_id, period, amount_cents, effective_from, effective_to, use_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            budget_id,
            _category_id(conn, category_name),
            period,
            amount_cents,
            effective_from,
            effective_to,
            use_type,
        ),
    )
    conn.commit()
    return budget_id


def _seed_recurring_flow(
    conn,
    *,
    name: str,
    amount_cents: int,
    day_of_month: int | None,
    account_id: str | None = None,
    frequency: str = "monthly",
    flow_type: str = "expense",
    is_active: bool = True,
    category_name: str | None = None,
) -> str:
    flow_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO recurring_flows (
            id, name, flow_type, amount_cents, frequency, day_of_month, account_id,
            category_id, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            flow_id,
            name,
            flow_type,
            amount_cents,
            frequency,
            day_of_month,
            account_id,
            _category_id(conn, category_name) if category_name is not None else None,
            1 if is_active else 0,
        ),
    )
    conn.commit()
    return flow_id


def _seed_credit_card(conn, *, institution_name: str, balance_cents: int, apr: float, min_payment_cents: int, card_ending: str) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, card_ending, balance_current_cents, is_active
        ) VALUES (?, ?, ?, 'credit_card', ?, ?, 1)
        """,
        (account_id, institution_name, institution_name, card_ending, balance_cents),
    )
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active, apr_purchase, minimum_payment_cents
        ) VALUES (?, ?, 'credit', 1, ?, ?)
        """,
        (uuid.uuid4().hex, account_id, apr, min_payment_cents),
    )
    conn.commit()
    return account_id


def test_c1_fires_on_spend_spike_and_negative_projection(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=20_000)
        for month in ("2025-10-15", "2025-11-15", "2025-12-15", "2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(conn, account_id=checking_id, category_name="Rent", amount_cents=-100_000, txn_date=month)
        _seed_transaction(conn, account_id=checking_id, category_name="Dining", amount_cents=-90_000, txn_date="2026-04-05")

        intervention = evaluate_c1_forward_burn(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "C-1"
    assert "burn through your buffer" in intervention.headline
    assert intervention.action is not None
    assert intervention.action.params == {
        "months": 1,
        "view": "personal",
        "categories": ["Dining"],
    }


def test_c1_does_not_fire_below_threshold(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=500_000)
        for month in ("2025-10-15", "2025-11-15", "2025-12-15", "2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(conn, account_id=checking_id, category_name="Rent", amount_cents=-100_000, txn_date=month)
        _seed_transaction(conn, account_id=checking_id, category_name="Dining", amount_cents=-20_000, txn_date="2026-04-05")

        intervention = evaluate_c1_forward_burn(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c1_action_scope_excludes_uncategorized_current_month_outflows(db_path: Path, monkeypatch) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=20_000)
        for month in ("2025-10-15", "2025-11-15", "2025-12-15", "2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(conn, account_id=checking_id, category_name="Rent", amount_cents=-100_000, txn_date=month)
        _seed_transaction(conn, account_id=checking_id, category_name="Dining", amount_cents=-90_000, txn_date="2026-04-05")
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name=None,
            amount_cents=-300_000,
            txn_date="2026-04-05",
            description="Uncategorized transfer-like outflow",
        )

        intervention = evaluate_c1_forward_burn(conn, build_context(conn, now=NOW))
        assert intervention is not None
        monkeypatch.setattr(spending_cmd, "date", _FixedSpendingDate)
        action_report = spending_cmd.handle_trends(Namespace(**intervention.action.params), conn)

    assert intervention.action is not None
    assert intervention.action.params["categories"] == ["Dining"]
    assert action_report["data"]["totals_cents"]["2026-04"] == 90_000


def test_c2_fires_for_fixed_bill_that_would_leave_low_balance(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=125_000)
        _seed_recurring_flow(
            conn,
            name="Rent",
            amount_cents=90_000,
            day_of_month=12,
            account_id=checking_id,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-6_000,
            txn_date="2026-04-08",
            description="Dinner out",
        )

        intervention = evaluate_c2_pre_bill_warning(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "C-2"
    assert intervention.priority.name == "HIGH"
    assert "Rent ($900.00) hits in 3 days" in intervention.headline
    assert "$350.00 in checking/savings" in intervention.headline
    assert "Dinner out ($60.00)" in intervention.headline
    assert intervention.dollar_impact_cents == 15_000
    assert intervention.action is not None
    assert intervention.action.tool == "set_spending_freeze_flag"
    assert intervention.action.params == {
        "scope": "discretionary",
        "hold_until": "2026-04-12",
        "reason": "Hold discretionary spending until Rent clears",
        "bill_name": "Rent",
        "bill_amount_cents": 90_000,
        "due_date": "2026-04-12",
        "target_balance_after_cents": 35_000,
    }


def test_c2_does_not_fire_when_post_bill_balance_stays_above_threshold(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=200_000)
        _seed_recurring_flow(
            conn,
            name="Rent",
            amount_cents=90_000,
            day_of_month=12,
            account_id=checking_id,
        )

        intervention = evaluate_c2_pre_bill_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c2_projects_other_recurring_flows_before_bill_date(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=100_000)
        _seed_recurring_flow(
            conn,
            name="Insurance",
            amount_cents=40_000,
            day_of_month=10,
            account_id=checking_id,
        )
        _seed_recurring_flow(
            conn,
            name="Rent",
            amount_cents=40_000,
            day_of_month=12,
            account_id=checking_id,
        )

        intervention = evaluate_c2_pre_bill_warning(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "C-2"
    assert "Rent ($400.00) hits in 3 days" in intervention.headline
    assert "$200.00 in checking/savings" in intervention.headline
    assert intervention.action is not None
    assert intervention.action.params["target_balance_after_cents"] == 20_000


def test_c2_includes_income_before_bill_date(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=100_000)
        _seed_recurring_flow(
            conn,
            name="Insurance",
            amount_cents=40_000,
            day_of_month=10,
            account_id=checking_id,
        )
        _seed_recurring_flow(
            conn,
            name="Client deposit",
            amount_cents=30_000,
            day_of_month=11,
            account_id=checking_id,
            flow_type="income",
        )
        _seed_recurring_flow(
            conn,
            name="Rent",
            amount_cents=40_000,
            day_of_month=12,
            account_id=checking_id,
        )

        intervention = evaluate_c2_pre_bill_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c2_ignores_flows_linked_to_non_liquid_accounts(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn, account_type="checking", balance_cents=100_000)
        card_id = _seed_account(conn, account_type="credit_card", balance_cents=-20_000)
        _seed_recurring_flow(
            conn,
            name="Card autopay",
            amount_cents=90_000,
            day_of_month=12,
            account_id=card_id,
        )

        intervention = evaluate_c2_pre_bill_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c2_ignores_monthly_recurring_expenses_that_are_not_fixed_bills(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=100_000)
        _seed_recurring_flow(
            conn,
            name="Streaming",
            amount_cents=90_000,
            day_of_month=12,
            account_id=checking_id,
            category_name="Entertainment",
        )

        intervention = evaluate_c2_pre_bill_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c2_ignores_flows_linked_to_inactive_or_hash_alias_liquid_accounts(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=100_000)
        inactive_id = _seed_account(
            conn,
            account_type="checking",
            balance_cents=100_000,
            is_active=0,
        )
        hash_id = _seed_account(conn, account_type="savings", balance_cents=100_000)
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_id, checking_id),
        )
        conn.commit()
        _seed_recurring_flow(
            conn,
            name="Inactive account rent",
            amount_cents=90_000,
            day_of_month=12,
            account_id=inactive_id,
        )
        _seed_recurring_flow(
            conn,
            name="Hash alias rent",
            amount_cents=90_000,
            day_of_month=12,
            account_id=hash_id,
        )

        intervention = evaluate_c2_pre_bill_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c2_does_not_guess_dates_for_unanchored_invalid_or_due_today_bills(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=100_000)
        _seed_recurring_flow(
            conn,
            name="Weekly utility",
            amount_cents=60_000,
            day_of_month=None,
            account_id=checking_id,
            frequency="weekly",
        )
        _seed_recurring_flow(
            conn,
            name="Bad data",
            amount_cents=90_000,
            day_of_month=99,
            account_id=checking_id,
        )
        _seed_recurring_flow(
            conn,
            name="Due today",
            amount_cents=90_000,
            day_of_month=9,
            account_id=checking_id,
        )
        _seed_recurring_flow(
            conn,
            name="Mortgage",
            amount_cents=90_000,
            day_of_month=20,
            account_id=checking_id,
        )

        intervention = evaluate_c2_pre_bill_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c3_fires_when_discretionary_budget_is_spent_and_another_has_room(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=200_000)
        _seed_budget(conn, category_name="Dining", amount_cents=30_000)
        _seed_budget(conn, category_name="Travel", amount_cents=150_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-30_000,
            txn_date="2026-04-09",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Travel",
            amount_cents=-20_000,
            txn_date="2026-04-05",
        )

        intervention = evaluate_c3_discretionary_cliff(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "C-3"
    assert intervention.move.value == "prescribe"
    assert "Dining is fully spent with 21 days left" in intervention.headline
    assert "Pulling $700.00 from Travel" in intervention.headline
    assert "has $1,300.00 to spare" in intervention.headline
    assert intervention.dollar_impact_cents == 0
    assert intervention.action is not None
    assert intervention.action.tool == "budget_reallocate"
    assert intervention.action.params == {
        "from_category": "Travel",
        "to_category": "Dining",
        "amount": 700.0,
        "period": "monthly",
        "view": "personal",
        "dry_run": False,
    }


def test_c3_ignores_closed_budget_rows_that_overlap_the_month(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=200_000)
        _seed_budget(
            conn,
            category_name="Dining",
            amount_cents=30_000,
            effective_from="2026-04-01",
            effective_to="2026-04-05",
        )
        _seed_budget(conn, category_name="Travel", amount_cents=150_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-30_000,
            txn_date="2026-04-04",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Travel",
            amount_cents=-20_000,
            txn_date="2026-04-05",
        )

        intervention = evaluate_c3_discretionary_cliff(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c3_requires_underused_category_to_have_enough_room(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=200_000)
        _seed_budget(conn, category_name="Dining", amount_cents=30_000)
        _seed_budget(conn, category_name="Travel", amount_cents=100_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-30_000,
            txn_date="2026-04-09",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Travel",
            amount_cents=-80_000,
            txn_date="2026-04-05",
        )

        intervention = evaluate_c3_discretionary_cliff(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c3_does_not_fire_with_less_than_seven_days_left(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=200_000)
        _seed_budget(conn, category_name="Dining", amount_cents=30_000)
        _seed_budget(conn, category_name="Travel", amount_cents=150_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-30_000,
            txn_date="2026-04-24",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Travel",
            amount_cents=-20_000,
            txn_date="2026-04-05",
        )

        intervention = evaluate_c3_discretionary_cliff(
            conn,
            build_context(conn, now=datetime(2026, 4, 24, 12, 0, 0)),
        )

    assert intervention is None


def test_c3_ignores_fixed_and_business_budgets(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=200_000)
        _seed_budget(conn, category_name="Rent", amount_cents=150_000)
        _seed_budget(conn, category_name="Shopping", amount_cents=300_000)
        _seed_budget(conn, category_name="Dining", amount_cents=30_000, use_type="Business")
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Rent",
            amount_cents=-150_000,
            txn_date="2026-04-09",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Shopping",
            amount_cents=-20_000,
            txn_date="2026-04-05",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-30_000,
            txn_date="2026-04-09",
            use_type="Business",
        )

        intervention = evaluate_c3_discretionary_cliff(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c4_fires_when_month_to_date_expenses_exceed_income(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=200_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Income: Consulting",
            amount_cents=150_000,
            txn_date="2026-04-02",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Rent",
            amount_cents=-130_000,
            txn_date="2026-04-03",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-50_000,
            txn_date="2026-04-08",
        )

        intervention = evaluate_c4_income_vs_expense_mtd(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "C-4"
    assert "$1,800.00 against $1,500.00" in intervention.headline
    assert "Over by $300.00" in intervention.headline
    assert intervention.dollar_impact_cents == 30_000
    assert intervention.action is not None
    assert intervention.action.tool == "spending_trends"
    assert intervention.action.params == {
        "months": 1,
        "view": "personal",
        "categories": ["Rent", "Dining"],
    }


def test_c4_ignores_noise_and_uses_executable_spending_trends_action(db_path: Path, monkeypatch) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=200_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Income: Consulting",
            amount_cents=150_000,
            txn_date="2026-04-02",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Rent",
            amount_cents=-130_000,
            txn_date="2026-04-03",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-50_000,
            txn_date="2026-04-08",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Shopping",
            amount_cents=-70_000,
            txn_date="2026-03-31",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-80_000,
            txn_date="2026-04-08",
            is_payment=1,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Shopping",
            amount_cents=-90_000,
            txn_date="2026-04-08",
            is_active=0,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name=None,
            amount_cents=-300_000,
            txn_date="2026-04-08",
            description="Uncategorized transfer-like outflow",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Meals",
            amount_cents=-75_000,
            txn_date="2026-04-08",
            use_type="Business",
        )

        intervention = evaluate_c4_income_vs_expense_mtd(conn, build_context(conn, now=NOW))
        assert intervention is not None
        monkeypatch.setattr(spending_cmd, "date", _FixedSpendingDate)
        action_report = spending_cmd.handle_trends(Namespace(**intervention.action.params), conn)

    assert intervention is not None
    assert "$1,800.00 against $1,500.00" in intervention.headline
    assert intervention.dollar_impact_cents == 30_000
    assert intervention.action is not None
    assert intervention.action.params == {
        "months": 1,
        "view": "personal",
        "categories": ["Rent", "Dining"],
    }
    assert action_report["data"]["totals_cents"]["2026-04"] == 180_000


def test_c4_does_not_fire_when_month_to_date_income_covers_expenses(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=200_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Income: Consulting",
            amount_cents=220_000,
            txn_date="2026-04-02",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Rent",
            amount_cents=-130_000,
            txn_date="2026-04-03",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-50_000,
            txn_date="2026-04-08",
        )

        intervention = evaluate_c4_income_vs_expense_mtd(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c4_engine_ladder_frames_mtd_gap_as_progress_at_risk(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=50_000)
        for month in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(
                conn,
                account_id=checking_id,
                category_name="Income: Salary",
                amount_cents=200_000,
                txn_date=month,
            )
            _seed_transaction(
                conn,
                account_id=checking_id,
                category_name="Rent",
                amount_cents=-100_000,
                txn_date=month,
            )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Income: Consulting",
            amount_cents=150_000,
            txn_date="2026-04-02",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Rent",
            amount_cents=-130_000,
            txn_date="2026-04-03",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Dining",
            amount_cents=-50_000,
            txn_date="2026-04-08",
        )

        result = run_engine(conn, now=NOW)

    c4 = next(item for item in result.interventions if item.pattern_id == "C-4")
    assert c4.dollar_impact_cents == 30_000
    assert c4.tier4_ladder is not None
    assert "progress at risk" in c4.tier4_ladder
    assert "faster" not in c4.tier4_ladder

    c5 = next(item for item in result.interventions if item.pattern_id == "C-5")
    assert "fastest lever right now" not in c5.headline


def test_c6_fires_for_repeated_overdraft_fees_and_next_day_deposits(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(
            conn,
            account_type="checking",
            balance_cents=12_000,
            institution_name="Acme Bank",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-02-10",
            description="Overdraft fee",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Income: Payroll",
            amount_cents=85_000,
            txn_date="2026-02-11",
            description="Payroll deposit",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-12",
            description="NSF returned item fee",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Income: Payroll",
            amount_cents=85_000,
            txn_date="2026-03-13",
            description="Payroll deposit",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-1_200,
            txn_date="2026-03-20",
            description="Monthly service fee",
        )

        intervention = evaluate_c6_late_deposit_overdraft(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "C-6"
    assert intervention.move.value == "pattern_catch"
    assert intervention.priority.name == "HIGH"
    assert "You paid $70.00 in 2 overdraft fees" in intervention.headline
    assert "2 were one-day-late deposits" in intervention.headline
    assert "A $100.00 buffer alert on Acme Bank checking" in intervention.headline
    assert intervention.dollar_impact_cents == 7_000
    assert intervention.action is not None
    assert intervention.action.tool == "set_low_balance_alert"
    assert intervention.action.params == {
        "account_id": checking_id,
        "threshold_cents": 10_000,
        "channel": "telegram",
        "cooldown_hours": 24,
        "label": "Acme Bank checking overdraft buffer alert",
        "dry_run": False,
    }


def test_c6_requires_two_overdraft_fees_on_the_same_liquid_account(db_path: Path) -> None:
    with connect(db_path) as conn:
        first_checking_id = _seed_account(conn, account_type="checking", balance_cents=12_000)
        second_checking_id = _seed_account(conn, account_type="checking", balance_cents=18_000)
        _seed_transaction(
            conn,
            account_id=first_checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-10",
            description="Overdraft fee",
        )
        _seed_transaction(
            conn,
            account_id=second_checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-12",
            description="Overdraft fee",
        )

        intervention = evaluate_c6_late_deposit_overdraft(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c6_counts_distinct_next_day_income_deposit_dates(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=12_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-12",
            description="Overdraft fee",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-12",
            description="NSF returned item fee",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Income: Payroll",
            amount_cents=85_000,
            txn_date="2026-03-13",
            description="Payroll deposit",
        )

        intervention = evaluate_c6_late_deposit_overdraft(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert "1 was a one-day-late deposit" in intervention.headline


def test_c6_does_not_count_next_day_refund_as_late_deposit(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=12_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-12",
            description="Overdraft fee",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-18",
            description="NSF returned item fee",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Shopping",
            amount_cents=4_200,
            txn_date="2026-03-13",
            description="Refund credit",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Shopping",
            amount_cents=1_500,
            txn_date="2026-03-19",
            description="Returned purchase credit",
        )

        intervention = evaluate_c6_late_deposit_overdraft(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert "0 were one-day-late deposits" in intervention.headline


def test_c6_ignores_generic_bank_fees_old_inactive_payment_and_hash_accounts(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=12_000)
        hash_id = _seed_account(conn, account_type="savings", balance_cents=5_000)
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_id, checking_id),
        )
        conn.commit()
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-1_200,
            txn_date="2026-03-20",
            description="Monthly service fee",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-01-01",
            description="Overdraft fee",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-15",
            description="Overdraft fee",
            is_payment=1,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-16",
            description="Overdraft fee",
            is_active=0,
        )
        _seed_transaction(
            conn,
            account_id=hash_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-17",
            description="Overdraft fee",
        )
        _seed_transaction(
            conn,
            account_id=hash_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-18",
            description="Overdraft fee",
        )

        intervention = evaluate_c6_late_deposit_overdraft(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c6_suppresses_when_account_already_has_buffer_alert(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=12_000)
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-02-10",
            description="Overdraft fee",
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            category_name="Bank Fees",
            amount_cents=-3_500,
            txn_date="2026-03-12",
            description="Overdraft fee",
        )
        conn.execute(
            """
            INSERT INTO account_alert_rules (
                id, rule_type, account_id, threshold_cents, channel, label, status,
                cooldown_hours, payload_json, idempotency_key
            )
            VALUES (?, 'low_balance', ?, 10000, 'telegram', 'Existing buffer', 'active', 24, '{}', ?)
            """,
            (uuid.uuid4().hex, checking_id, f"low_balance:{checking_id}:telegram"),
        )
        conn.commit()

        intervention = evaluate_c6_late_deposit_overdraft(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_c5_fires_below_target_computes_months_to_target_and_uses_fallback_ladder(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=50_000)
        for month in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(conn, account_id=checking_id, category_name="Income: Salary", amount_cents=200_000, txn_date=month)
            _seed_transaction(conn, account_id=checking_id, category_name="Rent", amount_cents=-100_000, txn_date=month)

        result = run_engine(conn, now=NOW)

    c5 = next(item for item in result.interventions if item.pattern_id == "C-5")
    assert "2.5 months" in c5.headline
    assert c5.tier4_ladder is not None
    assert "3-month emergency fund" in c5.tier4_ladder


def test_c5_accelerator_suggestion_uses_top_cofired_intervention(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=50_000)
        for month in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(conn, account_id=checking_id, category_name="Income: Salary", amount_cents=200_000, txn_date=month)
            _seed_transaction(conn, account_id=checking_id, category_name="Rent", amount_cents=-100_000, txn_date=month)

        _seed_credit_card(conn, institution_name="High", balance_cents=-90_000, apr=29.99, min_payment_cents=3_000, card_ending="1111")
        _seed_credit_card(conn, institution_name="Mid", balance_cents=-30_000, apr=19.99, min_payment_cents=500, card_ending="2222")
        _seed_credit_card(conn, institution_name="Low", balance_cents=-5_000, apr=9.99, min_payment_cents=200, card_ending="3333")

        result = run_engine(conn, now=NOW)

    c5 = next(item for item in result.interventions if item.pattern_id == "C-5")
    assert "{accelerator_suggestion}" not in c5.headline
    assert "fastest lever right now" in c5.headline
