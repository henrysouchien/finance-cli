from __future__ import annotations

from datetime import datetime
from pathlib import Path
import uuid

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import run_engine
from finance_cli.interventions.context import build_context
from finance_cli.interventions.wealth import (
    _W2_ROTH_FULL_CONTRIBUTION_PHASEOUT_FLOOR_CENTS_BY_YEAR,
    _W2_ROTH_IRA_LIMIT_CENTS_BY_YEAR,
    evaluate_w1_surplus_cash_drag,
    evaluate_w2_roth_ira_contribution_prompt,
    evaluate_w3_surplus_deployment_decision,
    evaluate_w4_goal_aligned_investment_cadence,
)


NOW = datetime(2026, 5, 26, 12, 0, 0)
ROTH_NOW = datetime(2026, 9, 10, 12, 0, 0)
W3_NOW = datetime(2026, 6, 20, 12, 0, 0)
W4_NOW = datetime(2026, 6, 15, 12, 0, 0)


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_checking_account(
    conn,
    *,
    balance_current_cents: int = 820_000,
    institution_name: str = "Cash Bank",
    account_name: str = "Checking",
    is_active: int = 1,
    is_business: int = 0,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active, is_business
        ) VALUES (?, ?, ?, 'checking', ?, ?, ?)
        """,
        (
            account_id,
            institution_name,
            account_name,
            balance_current_cents,
            is_active,
            is_business,
        ),
    )
    conn.commit()
    return account_id


def _seed_investment_account(
    conn,
    *,
    balance_current_cents: int = 500_000,
    institution_name: str = "Brokerage",
    account_name: str = "Taxable",
    is_active: int = 1,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active, is_business
        ) VALUES (?, ?, ?, 'investment', ?, ?, 0)
        """,
        (
            account_id,
            institution_name,
            account_name,
            balance_current_cents,
            is_active,
        ),
    )
    conn.commit()
    return account_id


def _seed_savings_account(
    conn,
    *,
    balance_current_cents: int = 100_000,
    institution_name: str = "Cash Bank",
    account_name: str = "Savings",
    is_active: int = 1,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active, is_business
        ) VALUES (?, ?, ?, 'savings', ?, ?, 0)
        """,
        (
            account_id,
            institution_name,
            account_name,
            balance_current_cents,
            is_active,
        ),
    )
    conn.commit()
    return account_id


def _seed_credit_card(
    conn,
    *,
    balance_current_cents: int = -250_000,
    apr_purchase: float = 24.99,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            card_ending, balance_current_cents, is_active, is_business
        ) VALUES (?, 'High Bank', 'Rewards', 'credit_card', '9999', ?, 1, 0)
        """,
        (account_id, balance_current_cents),
    )
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active, apr_purchase, minimum_payment_cents
        ) VALUES (?, ?, 'credit', 1, ?, 5000)
        """,
        (uuid.uuid4().hex, account_id, apr_purchase),
    )
    conn.commit()
    return account_id


def _category_id(conn, name: str, *, is_income: int) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, ?, 0, 0)
        """,
        (category_id, name, is_income),
    )
    conn.commit()
    return category_id


def _seed_transaction(
    conn,
    *,
    account_id: str,
    amount_cents: int,
    txn_date: str,
    category_name: str,
    is_income: int,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, 'wealth fixture', ?, ?, 0, 1, 1, 'manual')
        """,
        (
            txn_id,
            account_id,
            txn_date,
            amount_cents,
            _category_id(conn, category_name, is_income=is_income),
        ),
    )
    conn.commit()
    return txn_id


def _seed_roth_capacity_history(
    conn,
    *,
    account_id: str,
    monthly_income_cents: int = 500_000,
    monthly_expense_cents: int = 250_000,
    tax_year: int = 2026,
) -> None:
    for month in ("06", "07", "08"):
        _seed_transaction(
            conn,
            account_id=account_id,
            amount_cents=monthly_income_cents,
            txn_date=f"{tax_year}-{month}-15",
            category_name="Income: Pay",
            is_income=1,
        )
        _seed_transaction(
            conn,
            account_id=account_id,
            amount_cents=-monthly_expense_cents,
            txn_date=f"{tax_year}-{month}-20",
            category_name="Personal Spending",
            is_income=0,
        )


def _seed_roth_target(
    conn,
    *,
    status: str = "active",
    contributed_ytd_cents: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO retirement_contribution_targets (
            id, tax_year, account_type, status, monthly_target_cents,
            start_month, end_month, room_remaining_cents, annual_limit_cents,
            contributed_ytd_cents, estimated_tax_savings_cents, deadline,
            reason, source, payload_json, idempotency_key
        ) VALUES (?, 2026, 'roth_ira', ?, 125000, '2026-07', '2026-12',
                  750000, 750000, ?, NULL, '2026-12-31',
                  'Existing target', 'agent', '{}', ?)
        """,
        (
            uuid.uuid4().hex,
            status,
            contributed_ytd_cents,
            f"retirement_target:2026:roth_ira:{status}:{uuid.uuid4().hex}",
        ),
    )
    conn.commit()


def _seed_goal(
    conn,
    *,
    name: str = "College Fund",
    metric: str = "investments",
    target_cents: int = 2_000_000,
    starting_cents: int = 500_000,
    direction: str = "up",
    deadline: str = "2029-06-15",
    is_active: int = 1,
) -> str:
    goal_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO goals (
            id, name, metric, target_cents, starting_cents, direction, deadline, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            goal_id,
            name,
            metric,
            target_cents,
            starting_cents,
            direction,
            deadline,
            is_active,
        ),
    )
    conn.commit()
    return goal_id


def _seed_investment_cadence(
    conn,
    *,
    account_id: str,
    monthly_cents: int = 50_000,
    months: tuple[str, ...] = ("2026-03", "2026-04", "2026-05"),
    is_income: int = 0,
) -> None:
    for month in months:
        _seed_transaction(
            conn,
            account_id=account_id,
            amount_cents=monthly_cents,
            txn_date=f"{month}-15",
            category_name="Investment Transfer",
            is_income=is_income,
        )


def _seed_savings_automation(conn, *, goal_id: str, status: str = "active") -> None:
    conn.execute(
        """
        INSERT INTO savings_automations (
            id, goal_id, status, funding_method, cadence, amount_cents,
            start_date, day_of_month, reason, source, snapshot_json, idempotency_key
        ) VALUES (?, ?, ?, 'auto_transfer', 'monthly', 50000,
                  '2026-06-15', 15, 'Already automated', 'agent', '{}', ?)
        """,
        (
            uuid.uuid4().hex,
            goal_id,
            status,
            f"savings_automation:{goal_id}:{status}:{uuid.uuid4().hex}",
        ),
    )
    conn.commit()


def _seed_recurring_flow(
    conn,
    *,
    name: str = "Rent",
    amount_cents: int = 300_000,
    day_of_month: int | None = 28,
    category_name: str = "Rent",
    frequency: str = "monthly",
    flow_type: str = "expense",
) -> str:
    flow_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO recurring_flows (
            id, name, flow_type, amount_cents, frequency, day_of_month,
            category_id, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            flow_id,
            name,
            flow_type,
            amount_cents,
            frequency,
            day_of_month,
            _category_id(conn, category_name, is_income=0),
        ),
    )
    conn.commit()
    return flow_id


def _seed_snapshots(
    conn,
    account_id: str,
    balances: list[tuple[str, int]] | None = None,
) -> None:
    rows = balances or [
        ("2026-02-20", 800_000),
        ("2026-03-15", 810_000),
        ("2026-05-26", 820_000),
    ]
    for index, (snapshot_date, balance_cents) in enumerate(rows, start=1):
        conn.execute(
            """
            INSERT INTO balance_snapshots (
                id, account_id, balance_current_cents, source, snapshot_date
            ) VALUES (?, ?, ?, 'manual', ?)
            """,
            (f"snap-{account_id}-{index}", account_id, balance_cents, snapshot_date),
        )
    conn.commit()


def test_w1_flags_stable_checking_surplus_for_hysa_transfer(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(
            conn,
            balance_current_cents=820_000,
            institution_name="Cash Bank",
            account_name="Everyday",
        )
        _seed_snapshots(conn, checking_id)

        intervention = evaluate_w1_surplus_cash_drag(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "W-1"
    assert intervention.action is not None
    assert "$8,200.00 has been sitting in Cash Bank Everyday" in intervention.headline
    assert "Using a 4.50% HYSA assumption" in intervention.headline
    assert "moving $6,000.00 could earn roughly $270.00/yr" in intervention.headline
    assert intervention.dollar_impact_cents == 27_000
    assert intervention.action.tool == "flag_account_for_hysa_transfer"
    assert intervention.action.params == {
        "account_id": checking_id,
        "suggested_transfer_cents": 600_000,
        "hysa_apy_bps": 450,
        "current_apy_bps": 0,
        "retained_buffer_cents": 200_000,
        "minimum_balance_cents": 800_000,
        "lookback_days": 90,
        "as_of": "2026-05-26",
        "reason": "Stable checking surplus above the retained buffer since 2026-02-20.",
        "source": "agent",
        "dry_run": False,
    }


def test_w1_uses_stable_surplus_not_temporary_current_spike(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=820_000)
        _seed_snapshots(
            conn,
            checking_id,
            balances=[
                ("2026-02-20", 350_000),
                ("2026-03-15", 810_000),
                ("2026-05-26", 820_000),
            ],
        )

        intervention = evaluate_w1_surplus_cash_drag(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_w1_requires_snapshot_history_across_the_lookback(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=820_000)
        _seed_snapshots(
            conn,
            checking_id,
            balances=[
                ("2026-05-01", 810_000),
                ("2026-05-26", 820_000),
            ],
        )

        intervention = evaluate_w1_surplus_cash_drag(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_w1_requires_recent_snapshot_evidence(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=820_000)
        _seed_snapshots(
            conn,
            checking_id,
            balances=[
                ("2026-02-20", 820_000),
                ("2026-03-15", 820_000),
            ],
        )

        intervention = evaluate_w1_surplus_cash_drag(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_w1_ignores_business_checking(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(
            conn,
            balance_current_cents=820_000,
            is_business=1,
        )
        _seed_snapshots(conn, checking_id)

        intervention = evaluate_w1_surplus_cash_drag(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_w1_suppresses_accounts_with_active_hysa_transfer_flag(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=820_000)
        _seed_snapshots(conn, checking_id)
        conn.execute(
            """
            INSERT INTO hysa_transfer_flags (
                id, account_id, status, current_balance_cents,
                suggested_transfer_cents, retained_buffer_cents, minimum_balance_cents,
                current_apy_bps, hysa_apy_bps, estimated_annual_yield_cents,
                observed_since, lookback_days, reason, source, snapshot_json,
                idempotency_key
            ) VALUES (?, ?, 'active', 820000, 600000, 200000, 200000, 0, 450,
                      27000, '2026-02-20', 90, 'Already flagged', 'agent', '{}', ?)
            """,
            (uuid.uuid4().hex, checking_id, f"hysa_transfer:{checking_id}"),
        )
        conn.commit()

        intervention = evaluate_w1_surplus_cash_drag(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_w1_suppresses_unaddressed_high_apr_card_debt(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=820_000)
        _seed_snapshots(conn, checking_id)
        _seed_credit_card(conn, apr_purchase=24.99)

        intervention = evaluate_w1_surplus_cash_drag(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_w1_still_suppresses_high_apr_card_debt_already_flagged_for_paydown(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=820_000)
        _seed_snapshots(conn, checking_id)
        card_id = _seed_credit_card(conn, apr_purchase=24.99)
        conn.execute(
            """
            INSERT INTO card_paydown_flags (
                id, account_id, status, reason, suggested_payment_cents, source,
                snapshot_json, idempotency_key
            )
            VALUES (?, ?, 'active', 'Already flagged', 250000, 'agent', '{}', ?)
            """,
            (uuid.uuid4().hex, card_id, f"card_paydown:{card_id}"),
        )
        conn.commit()

        intervention = evaluate_w1_surplus_cash_drag(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_w2_roth_ira_contribution_prompt_fires_with_q3_saving_capacity(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_roth_capacity_history(conn, account_id=checking_id)

        intervention = evaluate_w2_roth_ira_contribution_prompt(
            conn,
            build_context(conn, now=ROTH_NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "W-2"
    assert "$7,500.00 of Roth IRA room left" in intervention.headline
    assert "$1,875.00/mo" in intervention.headline
    assert "Projected annual income from the last 3 complete months: $60,000.00." in intervention.detail_bullets
    assert "Known Roth contributions this year: $0.00." in intervention.detail_bullets
    assert "Trailing monthly saving capacity: $2,500.00." in intervention.detail_bullets
    assert intervention.dollar_impact_cents == 750_000
    assert intervention.action is not None
    assert intervention.action.tool == "setup_monthly_transfer_goal"
    assert intervention.action.params == {
        "tax_year": "2026",
        "monthly_transfer_cents": 187_500,
        "room_remaining_cents": 750_000,
        "start_month": "2026-09",
        "end_month": "2026-12",
        "account_type": "roth_ira",
        "annual_limit_cents": 750_000,
        "contributed_ytd_cents": 0,
        "estimated_tax_savings_cents": None,
        "reason": "Q3/Q4 Roth IRA room remains and recent saving capacity can cover the monthly transfer.",
        "update_monthly_plans": True,
        "dry_run": False,
    }


def test_w2_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_roth_capacity_history(conn, account_id=checking_id)

        result = run_engine(conn, now=ROTH_NOW)

    assert any(item.pattern_id == "W-2" for item in result.interventions)
    assert any(item.pattern_id == "W-2" for item in result.get_for_surface("action_queue"))


def test_w2_uses_known_roth_contributions_to_reduce_room(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_roth_capacity_history(conn, account_id=checking_id)
        _seed_roth_target(conn, status="resolved", contributed_ytd_cents=250_000)

        intervention = evaluate_w2_roth_ira_contribution_prompt(
            conn,
            build_context(conn, now=ROTH_NOW),
        )

    assert intervention is not None
    assert intervention.dollar_impact_cents == 500_000
    assert "$5,000.00 of Roth IRA room left" in intervention.headline
    assert intervention.action is not None
    assert intervention.action.params["monthly_transfer_cents"] == 125_000
    assert intervention.action.params["contributed_ytd_cents"] == 250_000


def test_w2_suppresses_before_q3(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        for month in ("02", "03", "04"):
            _seed_transaction(
                conn,
                account_id=checking_id,
                amount_cents=500_000,
                txn_date=f"2026-{month}-15",
                category_name="Income: Pay",
                is_income=1,
            )
            _seed_transaction(
                conn,
                account_id=checking_id,
                amount_cents=-250_000,
                txn_date=f"2026-{month}-20",
                category_name="Personal Spending",
                is_income=0,
            )

        intervention = evaluate_w2_roth_ira_contribution_prompt(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_w2_suppresses_unaddressed_high_apr_card_debt(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_roth_capacity_history(conn, account_id=checking_id)
        _seed_credit_card(conn, apr_purchase=24.99)

        intervention = evaluate_w2_roth_ira_contribution_prompt(
            conn,
            build_context(conn, now=ROTH_NOW),
        )

    assert intervention is None


def test_w2_suppresses_existing_active_roth_target(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_roth_capacity_history(conn, account_id=checking_id)
        _seed_roth_target(conn, status="active")

        intervention = evaluate_w2_roth_ira_contribution_prompt(
            conn,
            build_context(conn, now=ROTH_NOW),
        )

    assert intervention is None


def test_w2_suppresses_when_saving_capacity_cannot_cover_monthly_transfer(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_roth_capacity_history(
            conn,
            account_id=checking_id,
            monthly_income_cents=500_000,
            monthly_expense_cents=350_000,
        )

        intervention = evaluate_w2_roth_ira_contribution_prompt(
            conn,
            build_context(conn, now=ROTH_NOW),
        )

    assert intervention is None


def test_w2_suppresses_when_projected_income_hits_full_roth_phaseout_floor(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_roth_capacity_history(
            conn,
            account_id=checking_id,
            monthly_income_cents=1_300_000,
            monthly_expense_cents=250_000,
        )

        intervention = evaluate_w2_roth_ira_contribution_prompt(
            conn,
            build_context(conn, now=ROTH_NOW),
        )

    assert intervention is None


def test_w2_suppresses_unsupported_tax_year_instead_of_reusing_old_limits(
    db_path: Path,
) -> None:
    unsupported_now = datetime(2027, 9, 10, 12, 0, 0)
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_roth_capacity_history(conn, account_id=checking_id, tax_year=2027)

        intervention = evaluate_w2_roth_ira_contribution_prompt(
            conn,
            build_context(conn, now=unsupported_now),
        )

    assert intervention is None


def test_w2_roth_tax_year_limit_and_phaseout_tables_stay_in_lockstep() -> None:
    assert _W2_ROTH_IRA_LIMIT_CENTS_BY_YEAR
    assert (
        set(_W2_ROTH_IRA_LIMIT_CENTS_BY_YEAR)
        == set(_W2_ROTH_FULL_CONTRIBUTION_PHASEOUT_FLOOR_CENTS_BY_YEAR)
    )


def test_w3_surplus_deployment_prefers_high_apr_card_paydown(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        savings_id = _seed_savings_account(conn, balance_current_cents=100_000)
        card_id = _seed_credit_card(conn, balance_current_cents=-400_000, apr_purchase=24.99)
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=500_000,
            txn_date="2026-06-05",
            category_name="Income: Pay",
            is_income=1,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=-200_000,
            txn_date="2026-06-07",
            category_name="Groceries",
            is_income=0,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=-50_000,
            txn_date="2026-06-08",
            category_name="Savings Transfer",
            is_income=0,
        )
        _seed_transaction(
            conn,
            account_id=savings_id,
            amount_cents=50_000,
            txn_date="2026-06-08",
            category_name="Savings Transfer",
            is_income=0,
        )

        intervention = evaluate_w3_surplus_deployment_decision(
            conn,
            build_context(conn, now=W3_NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "W-3"
    assert intervention.dollar_impact_cents == 62_475
    assert (
        "You have $2,500.00 surplus this month. Paying $2,500.00 toward "
        "High Bank Rewards at 24.99% APR is the strongest math, saving about "
        "$624.75/yr. Want to flag that card for paydown?"
    ) == intervention.headline
    assert "Income month-to-date: $5,000.00." in intervention.detail_bullets
    assert (
        "Known expenses reserved/spent: $2,000.00 ($2,000.00 spent, $0.00 remaining recurring)."
        in intervention.detail_bullets
    )
    assert "Savings/investment transfers already made: $500.00." in intervention.detail_bullets
    assert (
        "Debt option: High Bank Rewards at 24.99% APR, $624.75/yr estimated interest avoided."
        in intervention.detail_bullets
    )
    assert intervention.action is not None
    assert intervention.action.tool == "flag_card_for_paydown"
    assert intervention.action.params == {
        "account_id": card_id,
        "suggested_payment_cents": 250_000,
        "cash_source_account_id": checking_id,
        "interest_saved_annual_cents": 62_475,
        "reason": "Use this month's unallocated surplus against High Bank Rewards at 24.99% APR.",
        "source": "agent",
        "dry_run": False,
    }


def test_w3_subtracts_remaining_known_recurring_expenses(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_credit_card(conn, balance_current_cents=-400_000, apr_purchase=24.99)
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=500_000,
            txn_date="2026-06-05",
            category_name="Income: Pay",
            is_income=1,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=-200_000,
            txn_date="2026-06-07",
            category_name="Groceries",
            is_income=0,
        )
        _seed_recurring_flow(conn, amount_cents=300_000, day_of_month=28)

        intervention = evaluate_w3_surplus_deployment_decision(
            conn,
            build_context(conn, now=W3_NOW),
        )

    assert intervention is None


def test_w3_selects_roth_transfer_when_no_higher_apr_debt(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        _seed_roth_capacity_history(conn, account_id=checking_id)
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=500_000,
            txn_date="2026-09-05",
            category_name="Income: Pay",
            is_income=1,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=-250_000,
            txn_date="2026-09-07",
            category_name="Personal Spending",
            is_income=0,
        )

        intervention = evaluate_w3_surplus_deployment_decision(
            conn,
            build_context(conn, now=ROTH_NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "W-3"
    assert (
        "Using $1,875.00/mo toward Roth IRA room would plan $7,500.00 before year-end"
        in intervention.headline
    )
    assert "Retirement option: $7,500.00 Roth room remains for 2026." in intervention.detail_bullets
    assert intervention.dollar_impact_cents == 750_000
    assert intervention.action is not None
    assert intervention.action.tool == "setup_monthly_transfer_goal"
    assert intervention.action.params == {
        "tax_year": "2026",
        "monthly_transfer_cents": 187_500,
        "room_remaining_cents": 750_000,
        "start_month": "2026-09",
        "end_month": "2026-12",
        "account_type": "roth_ira",
        "annual_limit_cents": 750_000,
        "contributed_ytd_cents": 0,
        "estimated_tax_savings_cents": None,
        "reason": "Use this month's unallocated surplus toward remaining Roth IRA room.",
        "update_monthly_plans": True,
        "dry_run": False,
    }


def test_w3_does_not_offer_roth_transfer_for_unsupported_tax_year(
    db_path: Path,
) -> None:
    unsupported_now = datetime(2027, 9, 10, 12, 0, 0)
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        savings_id = _seed_savings_account(conn, balance_current_cents=100_000)
        goal_id = _seed_goal(
            conn,
            name="Emergency Fund",
            metric="liquid_cash",
            target_cents=1_000_000,
            deadline="2028-12-31",
        )
        _seed_roth_capacity_history(conn, account_id=checking_id, tax_year=2027)
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=500_000,
            txn_date="2027-09-05",
            category_name="Income: Pay",
            is_income=1,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=-250_000,
            txn_date="2027-09-07",
            category_name="Personal Spending",
            is_income=0,
        )

        intervention = evaluate_w3_surplus_deployment_decision(
            conn,
            build_context(conn, now=unsupported_now),
        )

    if intervention is not None:
        assert intervention.pattern_id == "W-3"
        assert "Retirement option" not in intervention.detail_bullets
        assert intervention.action is not None
        assert intervention.action.tool != "setup_monthly_transfer_goal"
        assert intervention.action.params.get("tax_year") != "2027"
        if intervention.action.tool == "setup_savings_automation":
            assert intervention.action.params["goal_id"] == goal_id
            assert intervention.action.params["destination_account_id"] == savings_id


def test_w3_selects_goal_automation_when_no_debt_or_roth_option(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=500_000)
        savings_id = _seed_savings_account(conn, balance_current_cents=100_000)
        goal_id = _seed_goal(
            conn,
            name="Emergency Fund",
            metric="liquid_cash",
            target_cents=1_000_000,
            starting_cents=100_000,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=500_000,
            txn_date="2026-06-05",
            category_name="Income: Pay",
            is_income=1,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=-250_000,
            txn_date="2026-06-07",
            category_name="Personal Spending",
            is_income=0,
        )

        intervention = evaluate_w3_surplus_deployment_decision(
            conn,
            build_context(conn, now=W3_NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "W-3"
    assert intervention.goal_link == goal_id
    assert (
        "Putting $2,500.00 toward Emergency Fund closes part of a $4,000.00 gap"
        in intervention.headline
    )
    assert "Goal option: Emergency Fund gap $4,000.00." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "setup_savings_automation"
    assert intervention.action.params == {
        "goal_id": goal_id,
        "amount_cents": 250_000,
        "start_date": "2026-06-20",
        "cadence": "monthly",
        "funding_method": "auto_transfer",
        "day_of_month": 20,
        "source_account_id": checking_id,
        "destination_account_id": savings_id,
        "target_amount_cents": 1_000_000,
        "projected_end_balance_cents": 9_350_000,
        "goal_date": "2029-06-15",
        "reason": "Use this month's unallocated surplus to fund an active goal.",
        "dry_run": False,
    }


def test_w3_suppresses_goal_option_with_existing_active_automation(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=500_000)
        _seed_savings_account(conn, balance_current_cents=100_000)
        goal_id = _seed_goal(
            conn,
            name="Emergency Fund",
            metric="liquid_cash",
            target_cents=1_000_000,
            starting_cents=100_000,
        )
        _seed_savings_automation(conn, goal_id=goal_id)
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=500_000,
            txn_date="2026-06-05",
            category_name="Income: Pay",
            is_income=1,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=-250_000,
            txn_date="2026-06-07",
            category_name="Personal Spending",
            is_income=0,
        )

        intervention = evaluate_w3_surplus_deployment_decision(
            conn,
            build_context(conn, now=W3_NOW),
        )

    assert intervention is None


def test_w3_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=500_000)
        _seed_savings_account(conn, balance_current_cents=100_000)
        _seed_goal(
            conn,
            name="Emergency Fund",
            metric="liquid_cash",
            target_cents=1_000_000,
            starting_cents=100_000,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=500_000,
            txn_date="2026-06-05",
            category_name="Income: Pay",
            is_income=1,
        )
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=-250_000,
            txn_date="2026-06-07",
            category_name="Personal Spending",
            is_income=0,
        )

        result = run_engine(conn, now=W3_NOW)

    assert any(item.pattern_id == "W-3" for item in result.interventions)
    assert any(item.pattern_id == "W-3" for item in result.get_for_surface("action_queue"))


def test_w4_goal_aligned_investment_cadence_sets_savings_automation(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_checking_account(conn, balance_current_cents=1_000_000)
        investment_id = _seed_investment_account(
            conn,
            balance_current_cents=500_000,
            institution_name="Long Brokerage",
            account_name="Taxable",
        )
        goal_id = _seed_goal(conn)
        _seed_investment_cadence(conn, account_id=investment_id)

        intervention = evaluate_w4_goal_aligned_investment_cadence(
            conn,
            build_context(conn, now=W4_NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "W-4"
    assert intervention.goal_link == goal_id
    assert intervention.dollar_impact_cents == 50_000
    assert (
        "You're on pace for $23,000.00 by 2029-06-15. Want to lock in an "
        "automatic $500.00/mo transfer"
    ) in intervention.headline
    assert "Goal: College Fund target $20,000.00." in intervention.detail_bullets
    assert "Current investment balance: $5,000.00." in intervention.detail_bullets
    assert (
        "Observed investment deposits: $500.00/mo across 2026-03, 2026-04, 2026-05."
        in intervention.detail_bullets
    )
    assert "Destination account: Long Brokerage Taxable." in intervention.detail_bullets
    assert intervention.tier4_ladder == "Goal-linked automation for College Fund"
    assert intervention.action is not None
    assert intervention.action.tool == "setup_savings_automation"
    assert intervention.action.params == {
        "goal_id": goal_id,
        "amount_cents": 50_000,
        "start_date": "2026-06-15",
        "cadence": "monthly",
        "funding_method": "auto_transfer",
        "day_of_month": 15,
        "source_account_id": checking_id,
        "destination_account_id": investment_id,
        "target_amount_cents": 2_000_000,
        "projected_end_balance_cents": 2_300_000,
        "goal_date": "2029-06-15",
        "reason": (
            "Three complete months of investment deposits are already on pace for the "
            "long-horizon goal."
        ),
        "dry_run": False,
    }


def test_w4_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_checking_account(conn, balance_current_cents=1_000_000)
        investment_id = _seed_investment_account(conn, balance_current_cents=500_000)
        _seed_goal(conn)
        _seed_investment_cadence(conn, account_id=investment_id)

        result = run_engine(conn, now=W4_NOW)

    assert any(item.pattern_id == "W-4" for item in result.interventions)
    assert any(item.pattern_id == "W-4" for item in result.get_for_surface("action_queue"))


def test_w4_requires_long_horizon_goal(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_checking_account(conn, balance_current_cents=1_000_000)
        investment_id = _seed_investment_account(conn, balance_current_cents=500_000)
        _seed_goal(conn, deadline="2029-05-14")
        _seed_investment_cadence(conn, account_id=investment_id)

        intervention = evaluate_w4_goal_aligned_investment_cadence(
            conn,
            build_context(conn, now=W4_NOW),
        )

    assert intervention is None


def test_w4_requires_three_complete_months_of_investment_deposits(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_checking_account(conn, balance_current_cents=1_000_000)
        investment_id = _seed_investment_account(conn, balance_current_cents=500_000)
        _seed_goal(conn)
        _seed_investment_cadence(
            conn,
            account_id=investment_id,
            months=("2026-03", "2026-04"),
        )

        intervention = evaluate_w4_goal_aligned_investment_cadence(
            conn,
            build_context(conn, now=W4_NOW),
        )

    assert intervention is None


def test_w4_suppresses_existing_active_savings_automation(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_checking_account(conn, balance_current_cents=1_000_000)
        investment_id = _seed_investment_account(conn, balance_current_cents=500_000)
        goal_id = _seed_goal(conn)
        _seed_investment_cadence(conn, account_id=investment_id)
        _seed_savings_automation(conn, goal_id=goal_id)

        intervention = evaluate_w4_goal_aligned_investment_cadence(
            conn,
            build_context(conn, now=W4_NOW),
        )

    assert intervention is None


def test_w4_requires_observed_cadence_to_reach_goal(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_checking_account(conn, balance_current_cents=1_000_000)
        investment_id = _seed_investment_account(conn, balance_current_cents=500_000)
        _seed_goal(conn, target_cents=5_000_000)
        _seed_investment_cadence(conn, account_id=investment_id)

        intervention = evaluate_w4_goal_aligned_investment_cadence(
            conn,
            build_context(conn, now=W4_NOW),
        )

    assert intervention is None


def test_w4_ignores_income_category_investment_deposits(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_checking_account(conn, balance_current_cents=1_000_000)
        investment_id = _seed_investment_account(conn, balance_current_cents=500_000)
        _seed_goal(conn)
        _seed_investment_cadence(conn, account_id=investment_id, is_income=1)

        intervention = evaluate_w4_goal_aligned_investment_cadence(
            conn,
            build_context(conn, now=W4_NOW),
        )

    assert intervention is None
