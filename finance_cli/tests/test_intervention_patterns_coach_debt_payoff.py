from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.interventions.coach_debt_payoff import (
    evaluate_constant_payment_violation,
    evaluate_dti_threshold_36,
    evaluate_dti_threshold_43,
    evaluate_minimum_only_payments,
)
from finance_cli.interventions.context import build_context
from finance_cli.interventions.registry import Move, Priority
from finance_cli.mcp_server import _debt_payoff_artifact_dir, _render_debt_payoff_artifact


NOW = datetime(2026, 4, 9, 12, 0, 0)


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _category_id(conn: sqlite3.Connection, name: str, *, is_income: bool = False) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, ?, 0, 0)
        """,
        (category_id, name, 1 if is_income else 0),
    )
    conn.commit()
    return category_id


def _seed_account(
    conn: sqlite3.Connection,
    *,
    account_type: str = "checking",
    institution_name: str = "Bank",
    account_name: str = "Account",
    balance_current_cents: int = 0,
    card_ending: str | None = None,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, card_ending,
            balance_current_cents, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, 1)
        """,
        (account_id, institution_name, account_name, account_type, card_ending, balance_current_cents),
    )
    conn.commit()
    return account_id


def _seed_credit_card(
    conn: sqlite3.Connection,
    *,
    institution_name: str,
    balance_current_cents: int,
    minimum_payment_cents: int,
    card_ending: str,
    apr_purchase: float = 24.99,
) -> tuple[str, str]:
    account_id = _seed_account(
        conn,
        account_type="credit_card",
        institution_name=institution_name,
        account_name="Credit Card",
        balance_current_cents=balance_current_cents,
        card_ending=card_ending,
    )
    liability_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active, apr_purchase, minimum_payment_cents
        ) VALUES (?, ?, 'credit', 1, ?, ?)
        """,
        (liability_id, account_id, apr_purchase, minimum_payment_cents),
    )
    conn.commit()
    return account_id, liability_id


def _seed_transaction(
    conn: sqlite3.Connection,
    *,
    account_id: str | None,
    amount_cents: int,
    txn_date: str,
    category_id: str | None = None,
    is_payment: int = 0,
    description: str = "seed",
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, 'manual')
        """,
        (txn_id, account_id, txn_date, description, amount_cents, category_id, is_payment),
    )
    conn.commit()
    return txn_id


def _seed_income(
    conn: sqlite3.Connection,
    *,
    monthly_income_cents: int = 450_000,
    dates: tuple[str, ...] = ("2026-01-15", "2026-02-15", "2026-03-15"),
) -> None:
    checking_id = _seed_account(conn, account_type="checking", account_name="Checking")
    income_category_id = _category_id(conn, "Income: Salary", is_income=True)
    for txn_date in dates:
        _seed_transaction(
            conn,
            account_id=checking_id,
            amount_cents=monthly_income_cents,
            txn_date=txn_date,
            category_id=income_category_id,
            description="payroll",
        )


def _seed_balance_snapshot(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    snapshot_date: str,
    balance_current_cents: int,
) -> str:
    snapshot_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO balance_snapshots (
            id, account_id, snapshot_date, source, balance_current_cents
        ) VALUES (?, ?, ?, 'manual', ?)
        """,
        (snapshot_id, account_id, snapshot_date, balance_current_cents),
    )
    conn.commit()
    return snapshot_id


def _seed_dti_fixture(
    conn: sqlite3.Connection,
    *,
    minimum_payments_cents: tuple[int, int, int],
    snapshot_date: str,
    snapshot_balances_cents: tuple[int, int, int] | None = None,
    current_balances_cents: tuple[int, int, int] = (-300_000, -200_000, -100_000),
) -> list[tuple[str, str]]:
    _seed_income(conn)
    cards: list[tuple[str, str]] = []
    for index, (minimum_payment, current_balance) in enumerate(
        zip(minimum_payments_cents, current_balances_cents, strict=True),
        start=1,
    ):
        cards.append(
            _seed_credit_card(
                conn,
                institution_name=f"Card {index}",
                balance_current_cents=current_balance,
                minimum_payment_cents=minimum_payment,
                card_ending=f"{index}{index}{index}{index}",
            )
        )

    balances = snapshot_balances_cents or current_balances_cents
    for account_id, snapshot_balance in zip((card[0] for card in cards), balances, strict=True):
        _seed_balance_snapshot(
            conn,
            account_id=account_id,
            snapshot_date=snapshot_date,
            balance_current_cents=snapshot_balance,
        )
    return cards


def _seed_minimum_only_history(
    conn: sqlite3.Connection,
    *,
    minimums_cents: tuple[int, ...],
) -> None:
    for index, minimum_payment in enumerate(minimums_cents, start=1):
        account_id, _liability_id = _seed_credit_card(
            conn,
            institution_name=f"Minimum Card {index}",
            balance_current_cents=-(100_000 + index * 10_000),
            minimum_payment_cents=minimum_payment,
            card_ending=f"88{index}{index}",
        )
        for txn_date in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(
                conn,
                account_id=account_id,
                amount_cents=minimum_payment,
                txn_date=txn_date,
                is_payment=1,
                description="minimum payment",
            )


def _seed_constant_payment_fixture(
    conn: sqlite3.Connection,
    *,
    remaining_payment_cents: int,
) -> tuple[str, str]:
    account_a_id, liability_a_id = _seed_credit_card(
        conn,
        institution_name="Cleared Card",
        balance_current_cents=0,
        minimum_payment_cents=40_000,
        card_ending="1000",
    )
    account_b_id, liability_b_id = _seed_credit_card(
        conn,
        institution_name="Remaining Card",
        balance_current_cents=-100_000,
        minimum_payment_cents=10_000,
        card_ending="2000",
    )
    _seed_balance_snapshot(
        conn,
        account_id=account_a_id,
        snapshot_date="2026-03-01",
        balance_current_cents=-40_000,
    )
    _seed_transaction(
        conn,
        account_id=account_b_id,
        amount_cents=remaining_payment_cents,
        txn_date="2026-04-05",
        is_payment=1,
        description="remaining card payment",
    )
    return liability_a_id, liability_b_id


def _write_action_plan_artifact(*, liability_a_id: str, liability_b_id: str) -> Path:
    payload = {
        "generated_at": "2026-03-01T12:00:00Z",
        "smart_goal": "Pay $500 per month until the scoped cards are gone.",
        "strategy": {"name": "avalanche", "why": "Highest APR first."},
        "action_steps": [{"step": "Redirect every cleared minimum", "timeline": "monthly"}],
        "monthly_commitment_cents": 50_000,
        "debts_in_scope": [
            {
                "id": liability_a_id,
                "label": "Cleared Card",
                "minimum_payment_cents": 40_000,
                "balance_cents": 40_000,
            },
            {
                "id": liability_b_id,
                "label": "Remaining Card",
                "minimum_payment_cents": 10_000,
                "balance_cents": 100_000,
            },
        ],
        "target_debt_free_date": "2027-03-01",
        "monitoring_cadence": "monthly",
        "next_check_in": "2026-04-09",
    }
    artifact_dir = _debt_payoff_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / "20260301.md"
    artifact_path.write_text(_render_debt_payoff_artifact(payload), encoding="utf-8")
    return artifact_path


def test_dti_threshold_36_fires_at_38_pct_sustained(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_dti_fixture(
            conn,
            minimum_payments_cents=(60_000, 55_000, 56_000),
            snapshot_date="2026-02-28",
        )

        intervention = evaluate_dti_threshold_36(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "dti_threshold_36"
    assert intervention.move == Move.DIAGNOSE


def test_dti_threshold_36_does_not_fire_at_30_pct(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_dti_fixture(
            conn,
            minimum_payments_cents=(45_000, 45_000, 45_000),
            snapshot_date="2026-02-28",
        )

        intervention = evaluate_dti_threshold_36(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_dti_threshold_43_fires_with_growing_debt(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_dti_fixture(
            conn,
            minimum_payments_cents=(75_000, 70_000, 57_500),
            snapshot_date="2026-03-31",
            snapshot_balances_cents=(-250_000, -150_000, -60_000),
        )

        intervention = evaluate_dti_threshold_43(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "dti_threshold_43"
    assert intervention.move == Move.WARN
    assert intervention.priority == Priority.HIGH


def test_dti_threshold_43_does_not_fire_when_debt_shrinking_and_not_sustained(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_dti_fixture(
            conn,
            minimum_payments_cents=(75_000, 70_000, 57_500),
            snapshot_date="2026-03-31",
            snapshot_balances_cents=(-350_000, -250_000, -150_000),
        )

        intervention = evaluate_dti_threshold_43(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_minimum_only_payments_fires_with_2_debts_3_months(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_minimum_only_history(conn, minimums_cents=(10_000, 8_000))

        intervention = evaluate_minimum_only_payments(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "minimum_only_payments"
    assert intervention.move == Move.DIAGNOSE


def test_minimum_only_payments_does_not_fire_with_only_1_debt(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_minimum_only_history(conn, minimums_cents=(10_000,))

        intervention = evaluate_minimum_only_payments(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_constant_payment_violation_fires_when_freed_payment_absorbed(db_path: Path) -> None:
    with connect(db_path) as conn:
        liability_a_id, liability_b_id = _seed_constant_payment_fixture(conn, remaining_payment_cents=10_000)
        _write_action_plan_artifact(liability_a_id=liability_a_id, liability_b_id=liability_b_id)

        intervention = evaluate_constant_payment_violation(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "constant_payment_violation"
    assert intervention.move == Move.COACH


def test_constant_payment_violation_does_not_fire_when_no_artifact(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_constant_payment_fixture(conn, remaining_payment_cents=10_000)

        intervention = evaluate_constant_payment_violation(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_constant_payment_violation_does_not_fire_when_payment_meets_commitment(db_path: Path) -> None:
    with connect(db_path) as conn:
        liability_a_id, liability_b_id = _seed_constant_payment_fixture(conn, remaining_payment_cents=50_000)
        _write_action_plan_artifact(liability_a_id=liability_a_id, liability_b_id=liability_b_id)

        intervention = evaluate_constant_payment_violation(conn, build_context(conn, now=NOW))

    assert intervention is None
