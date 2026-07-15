from __future__ import annotations

import json
import uuid
from argparse import Namespace
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.commands import goal_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.debt_calculator import compare_strategies, load_debt_cards, simulate_paydown
from finance_cli.intervention_engine import run_engine
from finance_cli.interventions.context import build_context
from finance_cli.interventions.debt import (
    evaluate_d1_apr_avalanche,
    evaluate_d2_snowball_psychology,
    evaluate_d3_zero_apr_card_swap,
    evaluate_d4_min_payment_trap_warning,
    evaluate_d5_cash_vs_debt_arbitrage,
    evaluate_d6_balance_transfer_opportunity,
    evaluate_d7_debt_streak_reinforcement,
)
from finance_cli.interventions.registry import Move
from finance_cli.strategy_preferences import set_strategy_preference


NOW = datetime(2026, 4, 9, 12, 0, 0)


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
        VALUES (?, ?, 0, 0, 0)
        """,
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_credit_account(
    conn,
    *,
    institution_name: str,
    account_name: str,
    balance_current_cents: int,
    card_ending: str = "1111",
    is_active: int = 1,
    is_business: int = 0,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, card_ending,
            balance_current_cents, is_active, is_business
        ) VALUES (?, ?, ?, 'credit_card', ?, ?, ?, ?)
        """,
        (
            account_id,
            institution_name,
            account_name,
            card_ending,
            balance_current_cents,
            is_active,
            is_business,
        ),
    )
    conn.commit()
    return account_id


def _seed_cash_account(
    conn,
    *,
    account_type: str = "checking",
    balance_current_cents: int,
    institution_name: str = "Bank",
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            account_id,
            institution_name,
            account_name,
            account_type,
            balance_current_cents,
            is_active,
            is_business,
        ),
    )
    conn.commit()
    return account_id


def _seed_credit_liability(
    conn,
    *,
    account_id: str,
    apr_purchase: float,
    minimum_payment_cents: int = 2_000,
    intro_apr_end_date: str | None = None,
    is_overdue: int | None = None,
    past_due_amount_cents: int | None = None,
    current_late_fee_cents: int | None = None,
    last_statement_issue_date: str | None = None,
    next_payment_due_date: str | None = None,
) -> str:
    liability_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active, apr_purchase, minimum_payment_cents,
            intro_apr_end_date, is_overdue, past_due_amount_cents, current_late_fee_cents,
            last_statement_issue_date, next_payment_due_date
        ) VALUES (?, ?, 'credit', 1, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            liability_id,
            account_id,
            apr_purchase,
            minimum_payment_cents,
            intro_apr_end_date,
            is_overdue,
            past_due_amount_cents,
            current_late_fee_cents,
            last_statement_issue_date,
            next_payment_due_date,
        ),
    )
    conn.commit()
    return liability_id


def _seed_credit_purchase(conn, *, account_id: str, amount_cents: int, txn_date: str) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, 'daily spend', ?, ?, 0, 1, 1, 'manual')
        """,
        (txn_id, account_id, txn_date, -abs(amount_cents), _category_id(conn, "Dining")),
    )
    conn.commit()
    return txn_id


def _seed_credit_payment(conn, *, account_id: str, amount_cents: int, txn_date: str) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, 'card payment', ?, ?, 1, 1, 1, 'manual')
        """,
        (txn_id, account_id, txn_date, abs(amount_cents), _category_id(conn, "Debt Payment")),
    )
    conn.commit()
    return txn_id


def _seed_balance_snapshot(
    conn,
    *,
    account_id: str,
    balance_current_cents: int,
    snapshot_date: str,
) -> str:
    snapshot_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO balance_snapshots (
            id, account_id, balance_current_cents, source, snapshot_date
        ) VALUES (?, ?, ?, 'manual', ?)
        """,
        (snapshot_id, account_id, balance_current_cents, snapshot_date),
    )
    conn.commit()
    return snapshot_id


def _seed_expense(conn, *, account_id: str, amount_cents: int, txn_date: str) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, 'expense', ?, ?, 0, 1, 1, 'manual')
        """,
        (txn_id, account_id, txn_date, -abs(amount_cents), _category_id(conn, "Rent")),
    )
    conn.commit()
    return txn_id


def _seed_trailing_monthly_expenses(conn, *, account_id: str, amount_cents: int = 100_000) -> None:
    for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
        _seed_expense(conn, account_id=account_id, amount_cents=amount_cents, txn_date=txn_date)


def _seed_abandoned_goal_or_plan_signal(
    conn,
    *,
    event: str = "feature.plan_abandoned",
    outcome: str = "abandoned",
    created_at: str = "2026-03-01 12:00:00",
    properties: dict[str, object] | None = None,
) -> None:
    payload = properties
    if payload is None and event == "feature.plan_abandoned":
        payload = {"month": "2026-03"}
    if payload is None and event == "feature.goal_abandoned":
        payload = {"goal_id": "goal-2026-03", "goal_name": "Goal 2026-03"}
    conn.execute(
        """
        INSERT INTO analytics_events (event, domain, outcome, properties, source, created_at)
        VALUES (?, 'feature', ?, ?, 'api', ?)
        """,
        (
            event,
            outcome,
            json.dumps(payload, sort_keys=True, separators=(",", ":")) if payload else None,
            created_at,
        ),
    )
    conn.commit()


def _ns(**kwargs) -> Namespace:
    defaults = {"format": "json"}
    defaults.update(kwargs)
    return Namespace(**defaults)


def _seed_manual_loan(
    conn,
    *,
    creditor_name: str,
    current_balance_cents: int,
    interest_rate_pct: float,
    monthly_payment_cents: int = 2_000,
    is_active: int = 1,
    use_type: str = "Personal",
) -> str:
    loan_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO manual_loans (
            id, creditor_name, total_disbursed_cents, current_balance_cents,
            interest_rate_pct, interest_type, monthly_payment_cents, start_date,
            use_type, is_active
        ) VALUES (?, ?, ?, ?, ?, 'simple', ?, '2025-01-01', ?, ?)
        """,
        (
            loan_id,
            creditor_name,
            current_balance_cents,
            current_balance_cents,
            interest_rate_pct,
            monthly_payment_cents,
            use_type,
            is_active,
        ),
    )
    conn.commit()
    return loan_id


def test_d1_fires_with_apr_gap_and_matches_compare_strategies(db_path: Path) -> None:
    with connect(db_path) as conn:
        a1 = _seed_credit_account(conn, institution_name="High", account_name="APR", balance_current_cents=-90_000)
        a2 = _seed_credit_account(conn, institution_name="Mid", account_name="APR", balance_current_cents=-30_000, card_ending="2222")
        a3 = _seed_credit_account(conn, institution_name="Low", account_name="APR", balance_current_cents=-5_000, card_ending="3333")
        _seed_credit_liability(conn, account_id=a1, apr_purchase=29.99, minimum_payment_cents=3_000)
        _seed_credit_liability(conn, account_id=a2, apr_purchase=19.99, minimum_payment_cents=500)
        _seed_credit_liability(conn, account_id=a3, apr_purchase=9.99, minimum_payment_cents=200)

        cards = load_debt_cards(conn)
        comparison = compare_strategies(cards, extra_cents=0, summary_only=True)
        expected = int(comparison["snowball"]["total_interest_cents"]) - int(comparison["avalanche"]["total_interest_cents"])

        intervention = evaluate_d1_apr_avalanche(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-1"
    assert intervention.dollar_impact_cents == expected


def test_d1_includes_manual_loans_in_supported_scope(db_path: Path) -> None:
    with connect(db_path) as conn:
        high_id = _seed_credit_account(conn, institution_name="High APR", account_name="Card", balance_current_cents=-90_000)
        low_id = _seed_credit_account(conn, institution_name="Low APR", account_name="Card", balance_current_cents=-5_000, card_ending="5555")
        _seed_credit_liability(conn, account_id=high_id, apr_purchase=24.99, minimum_payment_cents=3_000)
        _seed_credit_liability(conn, account_id=low_id, apr_purchase=9.99, minimum_payment_cents=200)
        _seed_manual_loan(conn, creditor_name="Family Loan", current_balance_cents=5_000, interest_rate_pct=7.0, monthly_payment_cents=100)

        cards = load_debt_cards(conn)
        intervention = evaluate_d1_apr_avalanche(conn, build_context(conn, now=NOW))

    assert any(card.label.startswith("Loan:") for card in cards)
    assert intervention is not None
    assert intervention.pattern_id == "D-1"


def test_d1_is_suppressed_when_user_explicitly_prefers_snowball(db_path: Path) -> None:
    with connect(db_path) as conn:
        high_id = _seed_credit_account(conn, institution_name="High APR", account_name="Card", balance_current_cents=-90_000)
        low_id = _seed_credit_account(
            conn,
            institution_name="Low APR",
            account_name="Card",
            balance_current_cents=-5_000,
            card_ending="5555",
        )
        _seed_credit_liability(conn, account_id=high_id, apr_purchase=24.99, minimum_payment_cents=3_000)
        _seed_credit_liability(conn, account_id=low_id, apr_purchase=9.99, minimum_payment_cents=200)
        set_strategy_preference(
            conn,
            domain="debt",
            strategy="snowball",
            rationale="User confirmed momentum matters more than max interest savings.",
        )

        result = run_engine(conn, now=NOW)

    assert result.context.strategy_prefs.debt_strategy == "snowball"
    assert all(item.pattern_id != "D-1" for item in result.interventions)


def test_d1_does_not_fire_below_apr_delta_threshold(db_path: Path) -> None:
    with connect(db_path) as conn:
        a1 = _seed_credit_account(conn, institution_name="Bank A", account_name="A", balance_current_cents=-20_000)
        a2 = _seed_credit_account(conn, institution_name="Bank B", account_name="B", balance_current_cents=-15_000, card_ending="3333")
        _seed_credit_liability(conn, account_id=a1, apr_purchase=20.0)
        _seed_credit_liability(conn, account_id=a2, apr_purchase=17.5)

        intervention = evaluate_d1_apr_avalanche(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d2_recommends_snowball_for_small_balance_with_abandoned_plan_signal(db_path: Path) -> None:
    with connect(db_path) as conn:
        small_id = _seed_credit_account(
            conn,
            institution_name="Low Bank",
            account_name="Starter",
            balance_current_cents=-40_000,
            card_ending="1111",
        )
        large_id = _seed_credit_account(
            conn,
            institution_name="Big Bank",
            account_name="Rewards",
            balance_current_cents=-260_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=small_id, apr_purchase=12.0, minimum_payment_cents=2_500)
        _seed_credit_liability(conn, account_id=large_id, apr_purchase=18.0, minimum_payment_cents=9_000)
        goal_cmd.handle_set(
            _ns(name="Debt sprint", target=1000, metric="liquid_cash", direction="up", deadline=None),
            conn,
        )
        goal_cmd.handle_abandon(_ns(name="Debt sprint"), conn)

        cards = load_debt_cards(conn)
        current = simulate_paydown(cards, extra_cents=0, strategy="snowball", summary_only=True)
        proposed = simulate_paydown(
            cards,
            extra_cents=0,
            strategy="snowball",
            summary_only=True,
            lump_sum_cents=37_900,
            lump_sum_month=1,
        )
        expected_saved = int(current["total_interest_cents"]) - int(proposed["total_interest_cents"])

        intervention = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-2"
    assert intervention.move is Move.PRESCRIBE
    assert intervention.priority.name == "HIGH"
    assert intervention.dollar_impact_cents == expected_saved
    assert "Low Bank 1111 is at $400.00" in intervention.headline
    assert "frees $25.00/mo for the next debt" in intervention.headline
    assert "Recent abandoned goal/plan signals: 1" in intervention.detail_bullets[1]
    assert intervention.action is not None
    assert intervention.action.tool == "debt_simulate"
    assert intervention.action.params == {
        "strategy": "snowball",
        "extra_dollars": 0,
        "lump_sum": 379.0,
        "lump_sum_month": 1,
    }


def test_d2_requires_recent_abandoned_goal_or_plan_signal(db_path: Path) -> None:
    with connect(db_path) as conn:
        small_id = _seed_credit_account(
            conn,
            institution_name="Low Bank",
            account_name="Starter",
            balance_current_cents=-40_000,
            card_ending="1111",
        )
        large_id = _seed_credit_account(
            conn,
            institution_name="Big Bank",
            account_name="Rewards",
            balance_current_cents=-260_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=small_id, apr_purchase=12.0, minimum_payment_cents=2_500)
        _seed_credit_liability(conn, account_id=large_id, apr_purchase=18.0, minimum_payment_cents=9_000)

        no_signal = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))
        _seed_abandoned_goal_or_plan_signal(conn, created_at="2025-09-01 12:00:00")
        stale_signal = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))

    assert no_signal is None
    assert stale_signal is None


def test_d2_ignores_abandoned_event_names_without_abandoned_outcome(db_path: Path) -> None:
    with connect(db_path) as conn:
        small_id = _seed_credit_account(
            conn,
            institution_name="Low Bank",
            account_name="Starter",
            balance_current_cents=-40_000,
            card_ending="1111",
        )
        large_id = _seed_credit_account(
            conn,
            institution_name="Big Bank",
            account_name="Rewards",
            balance_current_cents=-260_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=small_id, apr_purchase=12.0, minimum_payment_cents=2_500)
        _seed_credit_liability(conn, account_id=large_id, apr_purchase=18.0, minimum_payment_cents=9_000)
        _seed_abandoned_goal_or_plan_signal(
            conn,
            event="feature.goal_abandoned",
            outcome="succeeded",
            properties={"goal_id": "goal-2026-03", "goal_name": "Goal 2026-03"},
        )
        _seed_abandoned_goal_or_plan_signal(
            conn,
            event="feature.plan_abandoned",
            outcome="succeeded",
            properties={"month": "2026-03"},
        )

        intervention = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d2_requires_smallest_balance_under_twenty_percent(db_path: Path) -> None:
    with connect(db_path) as conn:
        first_id = _seed_credit_account(
            conn,
            institution_name="First Bank",
            account_name="Card",
            balance_current_cents=-50_000,
            card_ending="1111",
        )
        second_id = _seed_credit_account(
            conn,
            institution_name="Second Bank",
            account_name="Card",
            balance_current_cents=-200_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=first_id, apr_purchase=12.0, minimum_payment_cents=2_500)
        _seed_credit_liability(conn, account_id=second_id, apr_purchase=18.0, minimum_payment_cents=9_000)
        _seed_abandoned_goal_or_plan_signal(conn)

        intervention = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d2_requires_known_aprs_for_antipattern_check(db_path: Path) -> None:
    with connect(db_path) as conn:
        small_id = _seed_credit_account(
            conn,
            institution_name="Low Bank",
            account_name="Starter",
            balance_current_cents=-40_000,
            card_ending="1111",
        )
        unknown_id = _seed_credit_account(
            conn,
            institution_name="Unknown Bank",
            account_name="Card",
            balance_current_cents=-260_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=small_id, apr_purchase=12.0, minimum_payment_cents=2_500)
        _seed_credit_liability(conn, account_id=unknown_id, apr_purchase=None, minimum_payment_cents=9_000)
        _seed_abandoned_goal_or_plan_signal(conn)

        intervention = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d2_includes_manual_loans_in_snowball_scope(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="Big Bank",
            account_name="Rewards",
            balance_current_cents=-260_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=18.0, minimum_payment_cents=9_000)
        _seed_manual_loan(
            conn,
            creditor_name="Medical Loan",
            current_balance_cents=40_000,
            interest_rate_pct=12.0,
            monthly_payment_cents=2_500,
        )
        _seed_abandoned_goal_or_plan_signal(conn)

        intervention = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-2"
    assert "Loan: Medical Loan is at $400.00" in intervention.headline


def test_d2_excludes_business_manual_loans(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="Big Bank",
            account_name="Rewards",
            balance_current_cents=-260_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=18.0, minimum_payment_cents=9_000)
        _seed_manual_loan(
            conn,
            creditor_name="Business Bridge",
            current_balance_cents=40_000,
            interest_rate_pct=12.0,
            monthly_payment_cents=2_500,
            use_type="Business",
        )
        _seed_abandoned_goal_or_plan_signal(conn)

        intervention = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d2_suppresses_large_avalanche_savings_without_strong_behavior_signal(db_path: Path) -> None:
    with connect(db_path) as conn:
        small_id = _seed_credit_account(
            conn,
            institution_name="Low Bank",
            account_name="Starter",
            balance_current_cents=-800_000,
            card_ending="1111",
        )
        high_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-10_000_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=small_id, apr_purchase=5.0, minimum_payment_cents=20_000)
        _seed_credit_liability(conn, account_id=high_id, apr_purchase=29.99, minimum_payment_cents=300_000)
        _seed_abandoned_goal_or_plan_signal(conn)

        weak_signal = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))
        _seed_abandoned_goal_or_plan_signal(conn, created_at="2026-03-01 12:05:00")
        duplicate_signal = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))
        _seed_abandoned_goal_or_plan_signal(
            conn,
            event="feature.goal_abandoned",
            created_at="2026-03-02 12:00:00",
            properties={"goal_id": "goal-2026-03", "goal_name": "Goal 2026-03"},
        )
        strong_signal = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))

    assert weak_signal is None
    assert duplicate_signal is None
    assert strong_signal is not None
    assert strong_signal.pattern_id == "D-2"


def test_d2_suppressed_when_user_explicitly_chose_avalanche(db_path: Path) -> None:
    with connect(db_path) as conn:
        small_id = _seed_credit_account(
            conn,
            institution_name="Low Bank",
            account_name="Starter",
            balance_current_cents=-40_000,
            card_ending="1111",
        )
        large_id = _seed_credit_account(
            conn,
            institution_name="Big Bank",
            account_name="Rewards",
            balance_current_cents=-260_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=small_id, apr_purchase=12.0, minimum_payment_cents=2_500)
        _seed_credit_liability(conn, account_id=large_id, apr_purchase=18.0, minimum_payment_cents=9_000)
        _seed_abandoned_goal_or_plan_signal(conn)
        set_strategy_preference(
            conn,
            domain="debt",
            strategy="avalanche",
            rationale="User explicitly chose max interest savings.",
        )

        direct = evaluate_d2_snowball_psychology(conn, build_context(conn, now=NOW))
        result = run_engine(conn, now=NOW)

    assert direct is None
    assert all(item.pattern_id != "D-2" for item in result.interventions)


def test_d1_respects_inactive_accounts(db_path: Path) -> None:
    with connect(db_path) as conn:
        active_id = _seed_credit_account(conn, institution_name="Active", account_name="A", balance_current_cents=-20_000)
        inactive_id = _seed_credit_account(
            conn,
            institution_name="Inactive",
            account_name="B",
            balance_current_cents=-15_000,
            card_ending="4444",
            is_active=0,
        )
        _seed_credit_liability(conn, account_id=active_id, apr_purchase=24.0)
        _seed_credit_liability(conn, account_id=inactive_id, apr_purchase=8.0)

        intervention = evaluate_d1_apr_avalanche(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d3_fires_for_zero_apr_card_high_apr_balance_and_recent_spend(db_path: Path) -> None:
    with connect(db_path) as conn:
        zero_id = _seed_credit_account(
            conn,
            institution_name="Promo Bank",
            account_name="Zero",
            balance_current_cents=0,
            card_ending="0000",
        )
        high_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-180_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=zero_id, apr_purchase=0.0, intro_apr_end_date="2026-10-09")
        _seed_credit_liability(conn, account_id=high_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        _seed_credit_purchase(conn, account_id=high_id, amount_cents=50_000, txn_date="2026-01-15")
        _seed_credit_purchase(conn, account_id=high_id, amount_cents=50_000, txn_date="2026-02-15")
        _seed_credit_purchase(conn, account_id=high_id, amount_cents=50_000, txn_date="2026-03-15")

        intervention = evaluate_d3_zero_apr_card_swap(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-3"
    assert intervention.move is Move.COMPARE
    assert intervention.dollar_impact_cents >= 2_500
    assert "Promo Bank" in intervention.headline
    assert "High Bank" in intervention.headline
    assert intervention.action is not None
    assert intervention.action.tool == "card_rotation_reminder_set"
    assert intervention.action.build_stub is False
    assert intervention.action.params["zero_apr_account_id"] == zero_id
    assert intervention.action.params["paydown_account_id"] == high_id


def test_d3_requires_recent_purchase_spend_on_high_apr_card(db_path: Path) -> None:
    with connect(db_path) as conn:
        zero_id = _seed_credit_account(
            conn,
            institution_name="Promo Bank",
            account_name="Zero",
            balance_current_cents=0,
            card_ending="0000",
        )
        high_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-180_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=zero_id, apr_purchase=0.0, intro_apr_end_date="2026-10-09")
        _seed_credit_liability(conn, account_id=high_id, apr_purchase=24.99, minimum_payment_cents=5_000)

        intervention = evaluate_d3_zero_apr_card_swap(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d3_suppresses_when_late_payment_red_flag_exists(db_path: Path) -> None:
    with connect(db_path) as conn:
        zero_id = _seed_credit_account(
            conn,
            institution_name="Promo Bank",
            account_name="Zero",
            balance_current_cents=0,
            card_ending="0000",
        )
        high_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-180_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=zero_id, apr_purchase=0.0, intro_apr_end_date="2026-10-09")
        _seed_credit_liability(
            conn,
            account_id=high_id,
            apr_purchase=24.99,
            minimum_payment_cents=5_000,
            is_overdue=1,
        )
        _seed_credit_purchase(conn, account_id=high_id, amount_cents=50_000, txn_date="2026-01-15")
        _seed_credit_purchase(conn, account_id=high_id, amount_cents=50_000, txn_date="2026-02-15")
        _seed_credit_purchase(conn, account_id=high_id, amount_cents=50_000, txn_date="2026-03-15")

        intervention = evaluate_d3_zero_apr_card_swap(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d3_suppresses_when_expired_intro_balance_exists(db_path: Path) -> None:
    with connect(db_path) as conn:
        zero_id = _seed_credit_account(
            conn,
            institution_name="Promo Bank",
            account_name="Zero",
            balance_current_cents=0,
            card_ending="0000",
        )
        high_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-180_000,
            card_ending="9999",
        )
        expired_id = _seed_credit_account(
            conn,
            institution_name="Old Promo",
            account_name="Card",
            balance_current_cents=-25_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=zero_id, apr_purchase=0.0, intro_apr_end_date="2026-10-09")
        _seed_credit_liability(conn, account_id=high_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        _seed_credit_liability(conn, account_id=expired_id, apr_purchase=29.99, intro_apr_end_date="2026-02-01")
        _seed_credit_purchase(conn, account_id=high_id, amount_cents=50_000, txn_date="2026-01-15")
        _seed_credit_purchase(conn, account_id=high_id, amount_cents=50_000, txn_date="2026-02-15")
        _seed_credit_purchase(conn, account_id=high_id, amount_cents=50_000, txn_date="2026-03-15")

        intervention = evaluate_d3_zero_apr_card_swap(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d4_warns_on_high_apr_minimum_payment_trap(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-200_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=card_id, amount_cents=5_000, txn_date=txn_date)

        cards = load_debt_cards(conn)
        current = simulate_paydown(cards, extra_cents=0, strategy="avalanche", summary_only=True)
        improved = simulate_paydown(cards, extra_cents=15_000, strategy="avalanche", summary_only=True)
        expected_saved = int(current["total_interest_cents"]) - int(improved["total_interest_cents"])

        intervention = evaluate_d4_min_payment_trap_warning(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-4"
    assert intervention.move is Move.WARN
    assert intervention.priority.name == "MEDIUM"
    assert intervention.dollar_impact_cents == expected_saved
    assert "Min-payment trap:" in intervention.headline
    assert "Adding $150.00/mo" in intervention.headline
    assert f"That's ${expected_saved / 100:,.2f} saved" in intervention.headline
    assert "High Bank 9999 has $2,000.00 at 24.99% APR" in intervention.detail_bullets[0]
    assert "3/3 complete months" in intervention.detail_bullets[0]
    assert "$100.00/mo" in intervention.detail_bullets[1]
    assert "$200.00/mo" in intervention.detail_bullets[2]
    assert intervention.action is not None
    assert intervention.action.tool == "debt_simulate"
    assert intervention.action.params == {"strategy": "compare", "extra_dollars": 150}


def test_d4_requires_recent_near_minimum_payment_history(db_path: Path) -> None:
    with connect(db_path) as conn:
        no_history_id = _seed_credit_account(
            conn,
            institution_name="No History",
            account_name="Card",
            balance_current_cents=-200_000,
            card_ending="1111",
        )
        above_minimum_id = _seed_credit_account(
            conn,
            institution_name="Aggressive",
            account_name="Card",
            balance_current_cents=-200_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=no_history_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        _seed_credit_liability(conn, account_id=above_minimum_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=above_minimum_id, amount_cents=12_000, txn_date=txn_date)

        intervention = evaluate_d4_min_payment_trap_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d4_requires_high_apr_and_personal_credit_card_scope(db_path: Path) -> None:
    with connect(db_path) as conn:
        low_apr_id = _seed_credit_account(
            conn,
            institution_name="Low APR",
            account_name="Card",
            balance_current_cents=-200_000,
            card_ending="1111",
        )
        business_card_id = _seed_credit_account(
            conn,
            institution_name="Business Bank",
            account_name="Card",
            balance_current_cents=-200_000,
            card_ending="2222",
            is_business=1,
        )
        _seed_manual_loan(
            conn,
            creditor_name="High APR Loan",
            current_balance_cents=200_000,
            interest_rate_pct=24.99,
            monthly_payment_cents=5_000,
        )
        _seed_credit_liability(conn, account_id=low_apr_id, apr_purchase=17.99, minimum_payment_cents=5_000)
        _seed_credit_liability(conn, account_id=business_card_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        for account_id in (low_apr_id, business_card_id):
            for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
                _seed_credit_payment(conn, account_id=account_id, amount_cents=5_000, txn_date=txn_date)

        intervention = evaluate_d4_min_payment_trap_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d4_projection_scope_ignores_manual_loans_and_business_cards(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-200_000,
            card_ending="9999",
        )
        unrelated_personal_card_id = _seed_credit_account(
            conn,
            institution_name="Unrelated Bank",
            account_name="Card",
            balance_current_cents=-500_000,
            card_ending="3333",
        )
        business_card_id = _seed_credit_account(
            conn,
            institution_name="Business Bank",
            account_name="Card",
            balance_current_cents=-500_000,
            card_ending="2222",
            is_business=1,
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        _seed_credit_liability(
            conn,
            account_id=unrelated_personal_card_id,
            apr_purchase=29.99,
            minimum_payment_cents=5_000,
        )
        _seed_credit_liability(conn, account_id=business_card_id, apr_purchase=29.99, minimum_payment_cents=5_000)
        _seed_manual_loan(
            conn,
            creditor_name="High APR Loan",
            current_balance_cents=500_000,
            interest_rate_pct=29.99,
            monthly_payment_cents=5_000,
        )
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=card_id, amount_cents=5_000, txn_date=txn_date)

        personal_cards = [card for card in load_debt_cards(conn) if card.card_id == card_id]
        current = simulate_paydown(personal_cards, extra_cents=0, strategy="avalanche", summary_only=True)
        improved = simulate_paydown(personal_cards, extra_cents=15_000, strategy="avalanche", summary_only=True)
        expected_saved = int(current["total_interest_cents"]) - int(improved["total_interest_cents"])

        intervention = evaluate_d4_min_payment_trap_warning(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-4"
    assert intervention.dollar_impact_cents == expected_saved


def test_d4_suppressed_when_user_explicitly_chose_minimum_commitment(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-200_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=card_id, amount_cents=5_000, txn_date=txn_date)
        set_strategy_preference(
            conn,
            domain="debt",
            strategy="minimum_commitment",
            rationale="User explicitly chose a minimum-payment commitment for now.",
        )

        result = run_engine(conn, now=NOW)

    assert all(item.pattern_id != "D-4" for item in result.interventions)


def test_d4_counts_recent_payments_recorded_on_hash_alias(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-200_000,
            card_ending="9999",
        )
        hash_card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Hash",
            balance_current_cents=-200_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_card_id, card_id),
        )
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=hash_card_id, amount_cents=5_000, txn_date=txn_date)
        conn.commit()

        intervention = evaluate_d4_min_payment_trap_warning(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-4"


def test_d5_flags_high_apr_card_when_checking_surplus_exceeds_one_month_buffer(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_cash_account(
            conn,
            balance_current_cents=350_000,
            institution_name="Cash Bank",
            account_name="Everyday",
        )
        _seed_trailing_monthly_expenses(conn, account_id=checking_id, amount_cents=100_000)
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=5_000)

        intervention = evaluate_d5_cash_vs_debt_arbitrage(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-5"
    assert intervention.move is Move.DIAGNOSE
    assert intervention.priority.name == "HIGH"
    assert "You have $3,500.00 in checking" in intervention.headline
    assert "High Bank 9999 is at 24.99%" in intervention.headline
    assert "Throwing $1,200.00 at the card saves about $299.88/yr" in intervention.headline
    assert intervention.dollar_impact_cents == 29_988
    assert intervention.action is not None
    assert intervention.action.tool == "flag_card_for_paydown"
    assert intervention.action.params == {
        "account_id": card_id,
        "suggested_payment_cents": 120_000,
        "cash_source_account_id": checking_id,
        "interest_saved_annual_cents": 29_988,
        "reason": "Use checking surplus above one month of expenses to pay down High Bank 9999 at 24.99% APR.",
        "source": "agent",
        "dry_run": False,
    }


def test_d5_does_not_fire_without_expense_history_for_cash_buffer(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_cash_account(conn, balance_current_cents=350_000)
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=5_000)

        intervention = evaluate_d5_cash_vs_debt_arbitrage(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d5_leaves_one_month_of_expenses_and_requires_meaningful_surplus(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_cash_account(conn, balance_current_cents=190_000)
        _seed_trailing_monthly_expenses(conn, account_id=checking_id, amount_cents=100_000)
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=5_000)

        intervention = evaluate_d5_cash_vs_debt_arbitrage(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d5_does_not_mix_business_cash_into_personal_card_paydown(db_path: Path) -> None:
    with connect(db_path) as conn:
        personal_checking_id = _seed_cash_account(conn, balance_current_cents=50_000)
        business_checking_id = _seed_cash_account(
            conn,
            balance_current_cents=500_000,
            account_name="Business Checking",
            is_business=1,
        )
        _seed_trailing_monthly_expenses(conn, account_id=personal_checking_id, amount_cents=100_000)
        _seed_trailing_monthly_expenses(conn, account_id=business_checking_id, amount_cents=100_000)
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=5_000)

        intervention = evaluate_d5_cash_vs_debt_arbitrage(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d5_does_not_use_personal_cash_for_business_card_paydown(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_cash_account(conn, balance_current_cents=350_000)
        _seed_trailing_monthly_expenses(conn, account_id=checking_id, amount_cents=100_000)
        business_card_id = _seed_credit_account(
            conn,
            institution_name="Business Bank",
            account_name="Card",
            balance_current_cents=-120_000,
            card_ending="9999",
            is_business=1,
        )
        _seed_credit_liability(conn, account_id=business_card_id, apr_purchase=24.99, minimum_payment_cents=5_000)

        intervention = evaluate_d5_cash_vs_debt_arbitrage(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d5_does_not_route_manual_loans_to_card_paydown_tool(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_cash_account(conn, balance_current_cents=350_000)
        _seed_trailing_monthly_expenses(conn, account_id=checking_id, amount_cents=100_000)
        _seed_manual_loan(
            conn,
            creditor_name="High APR Loan",
            current_balance_cents=120_000,
            interest_rate_pct=24.99,
        )

        intervention = evaluate_d5_cash_vs_debt_arbitrage(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d5_suppresses_cards_already_flagged_for_paydown(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_cash_account(conn, balance_current_cents=350_000)
        _seed_trailing_monthly_expenses(conn, account_id=checking_id, amount_cents=100_000)
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=5_000)
        conn.execute(
            """
            INSERT INTO card_paydown_flags (
                id, account_id, status, reason, suggested_payment_cents, source,
                snapshot_json, idempotency_key
            )
            VALUES (?, ?, 'active', 'Already flagged', 50000, 'agent', '{}', ?)
            """,
            (uuid.uuid4().hex, card_id, f"card_paydown:{card_id}"),
        )
        conn.commit()

        intervention = evaluate_d5_cash_vs_debt_arbitrage(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d6_flags_balance_transfer_opportunity_with_net_savings(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-250_000,
            card_ending="9999",
        )
        _seed_credit_liability(
            conn,
            account_id=card_id,
            apr_purchase=24.99,
            minimum_payment_cents=7_500,
            last_statement_issue_date="2026-03-31",
        )

        intervention = evaluate_d6_balance_transfer_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-6"
    assert intervention.move is Move.COMPARE
    assert intervention.dollar_impact_cents == 54_975
    assert "$2,500.00 on High Bank 9999 at 24.99%" in intervention.headline
    assert "3% fee ($75.00)" in intervention.headline
    assert "$624.75 of interest over 12 months" in intervention.headline
    assert "net estimate $549.75" in intervention.headline
    assert intervention.action is not None
    assert intervention.action.tool == "set_balance_transfer_reminder"
    assert intervention.action.params == {
        "account_id": card_id,
        "remind_on": "2026-05-01",
        "balance_transfer_fee_percent": 3.0,
        "channel": "telegram",
        "note": (
            "Compare 0% balance-transfer offers; confirm fee, promo APR length, "
            "credit impact, and payoff plan before applying."
        ),
        "dry_run": False,
    }


def test_d6_requires_trigger_balance_and_apr_thresholds(db_path: Path) -> None:
    with connect(db_path) as conn:
        small_card_id = _seed_credit_account(
            conn,
            institution_name="Small Bank",
            account_name="Card",
            balance_current_cents=-199_999,
            card_ending="1111",
        )
        low_apr_card_id = _seed_credit_account(
            conn,
            institution_name="Low Bank",
            account_name="Card",
            balance_current_cents=-250_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=small_card_id, apr_purchase=24.99)
        _seed_credit_liability(conn, account_id=low_apr_card_id, apr_purchase=17.99)

        intervention = evaluate_d6_balance_transfer_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d6_suppresses_when_late_payment_red_flag_exists(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-250_000,
            card_ending="9999",
        )
        _seed_credit_liability(
            conn,
            account_id=card_id,
            apr_purchase=24.99,
            is_overdue=1,
        )

        intervention = evaluate_d6_balance_transfer_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d6_suppresses_when_expired_intro_balance_exists(db_path: Path) -> None:
    with connect(db_path) as conn:
        high_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-250_000,
            card_ending="9999",
        )
        expired_id = _seed_credit_account(
            conn,
            institution_name="Old Promo",
            account_name="Card",
            balance_current_cents=-50_000,
            card_ending="2222",
        )
        _seed_credit_liability(conn, account_id=high_id, apr_purchase=24.99)
        _seed_credit_liability(conn, account_id=expired_id, apr_purchase=29.99, intro_apr_end_date="2026-02-01")

        intervention = evaluate_d6_balance_transfer_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d6_ignores_business_cards_and_manual_loans(db_path: Path) -> None:
    with connect(db_path) as conn:
        business_card_id = _seed_credit_account(
            conn,
            institution_name="Business Bank",
            account_name="Card",
            balance_current_cents=-250_000,
            card_ending="9999",
            is_business=1,
        )
        _seed_credit_liability(conn, account_id=business_card_id, apr_purchase=24.99)
        _seed_manual_loan(
            conn,
            creditor_name="High APR Loan",
            current_balance_cents=250_000,
            interest_rate_pct=24.99,
        )

        intervention = evaluate_d6_balance_transfer_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d6_suppresses_card_with_pending_balance_transfer_reminder(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-250_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99)
        conn.execute(
            """
            INSERT INTO reminders (
                id, kind, title, body, due_at, channel, status, payload_json, idempotency_key
            ) VALUES (?, 'balance_transfer', 'Title', 'Body', '2026-04-20 09:00:00',
                      'telegram', 'pending', ?, ?)
            """,
            (
                uuid.uuid4().hex,
                f'{{"account_id": "{card_id}"}}',
                f"balance_transfer:{card_id}:2026-04-20:telegram",
            ),
        )
        conn.commit()

        intervention = evaluate_d6_balance_transfer_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d6_suppresses_card_with_pending_balance_transfer_reminder_on_hash_alias(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-250_000,
            card_ending="9999",
        )
        hash_card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Hash",
            balance_current_cents=-250_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99)
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_card_id, card_id),
        )
        conn.execute(
            """
            INSERT INTO reminders (
                id, kind, title, body, due_at, channel, status, payload_json, idempotency_key
            ) VALUES (?, 'balance_transfer', 'Title', 'Body', '2026-04-20 09:00:00',
                      'telegram', 'pending', ?, ?)
            """,
            (
                uuid.uuid4().hex,
                f'{{"account_id": "{hash_card_id}"}}',
                f"balance_transfer:{hash_card_id}:2026-04-20:telegram",
            ),
        )
        conn.commit()

        intervention = evaluate_d6_balance_transfer_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d7_reinforces_three_month_debt_paydown_streak(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=10_000)
        _seed_balance_snapshot(
            conn,
            account_id=card_id,
            balance_current_cents=-200_000,
            snapshot_date="2025-12-31",
        )
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=card_id, amount_cents=30_000, txn_date=txn_date)

        intervention = evaluate_d7_debt_streak_reinforcement(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "D-7"
    assert intervention.move is Move.COACH
    assert intervention.priority.name == "MEDIUM"
    assert intervention.action is None
    assert intervention.tiers == (4,)
    assert intervention.dollar_impact_cents == 62_127
    assert "Three months of above-minimum payments on High Bank 9999" in intervention.headline
    assert "You're $621.27 ahead of the minimum-payment path" in intervention.headline
    assert "Average payment: $300.00/mo vs $100.00 minimum." in intervention.detail_bullets
    assert "Minimum-payment projection would be about $1,821.27 today." in intervention.detail_bullets


def test_d7_is_available_to_engine_without_action_queue_pollution(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=10_000)
        _seed_balance_snapshot(
            conn,
            account_id=card_id,
            balance_current_cents=-200_000,
            snapshot_date="2025-12-31",
        )
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=card_id, amount_cents=30_000, txn_date=txn_date)

        result = run_engine(conn, now=NOW)

    d7 = next(item for item in result.interventions if item.pattern_id == "D-7")
    assert d7.action is None
    assert all(item.pattern_id != "D-7" for item in result.get_for_surface("action_queue"))


def test_d7_requires_starting_snapshot_before_streak_window(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=10_000)
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=card_id, amount_cents=30_000, txn_date=txn_date)

        intervention = evaluate_d7_debt_streak_reinforcement(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d7_rejects_stale_starting_snapshot(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=10_000)
        _seed_balance_snapshot(
            conn,
            account_id=card_id,
            balance_current_cents=-200_000,
            snapshot_date="2025-10-31",
        )
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=card_id, amount_cents=30_000, txn_date=txn_date)

        intervention = evaluate_d7_debt_streak_reinforcement(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d7_requires_every_complete_month_above_minimum(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="High Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=10_000)
        _seed_balance_snapshot(
            conn,
            account_id=card_id,
            balance_current_cents=-200_000,
            snapshot_date="2025-12-31",
        )
        _seed_credit_payment(conn, account_id=card_id, amount_cents=30_000, txn_date="2026-01-15")
        _seed_credit_payment(conn, account_id=card_id, amount_cents=10_000, txn_date="2026-02-15")
        _seed_credit_payment(conn, account_id=card_id, amount_cents=30_000, txn_date="2026-03-15")

        intervention = evaluate_d7_debt_streak_reinforcement(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_d7_ignores_business_cards(db_path: Path) -> None:
    with connect(db_path) as conn:
        card_id = _seed_credit_account(
            conn,
            institution_name="Business Bank",
            account_name="Rewards",
            balance_current_cents=-120_000,
            card_ending="9999",
            is_business=1,
        )
        _seed_credit_liability(conn, account_id=card_id, apr_purchase=24.99, minimum_payment_cents=10_000)
        _seed_balance_snapshot(
            conn,
            account_id=card_id,
            balance_current_cents=-200_000,
            snapshot_date="2025-12-31",
        )
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_credit_payment(conn, account_id=card_id, amount_cents=30_000, txn_date=txn_date)

        intervention = evaluate_d7_debt_streak_reinforcement(conn, build_context(conn, now=NOW))

    assert intervention is None
