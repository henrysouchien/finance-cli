from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.interventions.coach_emergency_fund import (
    evaluate_cash_flow_surplus_no_savings,
    evaluate_emergency_fund_drawdown,
    evaluate_income_shock_detected,
    evaluate_liquidity_below_3_months,
)
from finance_cli.interventions.context import build_context
from finance_cli.interventions.registry import Move, Priority
from finance_cli.mcp_server import (
    _emergency_fund_artifact_dir,
    _render_emergency_fund_artifact,
)


NOW = datetime(2026, 6, 15, 12, 0, 0)


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
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active
        ) VALUES (?, ?, ?, ?, ?, 1)
        """,
        (account_id, institution_name, account_name, account_type, balance_current_cents),
    )
    conn.commit()
    return account_id


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


def _seed_essentials(conn: sqlite3.Connection, *, monthly_cents: int, months: tuple[str, ...]) -> str:
    """Seed an essential-category transaction stream so category_spending_averages classifies it."""
    rent_category = _category_id(conn, "Rent", is_income=False)
    account_id = _seed_account(conn, institution_name="Landlord", account_name="Rent")
    for month_start in months:
        _seed_transaction(
            conn,
            account_id=account_id,
            amount_cents=-monthly_cents,
            txn_date=month_start,
            category_id=rent_category,
            description="rent",
        )
    return rent_category


def _seed_monthly_income(
    conn: sqlite3.Connection,
    *,
    month_amounts: dict[str, int],
    account_id: str | None = None,
) -> str:
    income_category = _category_id(conn, "Income: Salary", is_income=True)
    if account_id is None:
        account_id = _seed_account(conn, institution_name="Employer", account_name="Payroll")
    for month_key, amount_cents in month_amounts.items():
        _seed_transaction(
            conn,
            account_id=account_id,
            amount_cents=amount_cents,
            txn_date=f"{month_key}-15",
            category_id=income_category,
            description="payroll",
        )
    return account_id


def _write_emergency_fund_artifact(
    *,
    account_ids: list[str],
    drawdown_events_classified: list[dict] | None = None,
    generated_at: str = "2026-04-01T10:00:00Z",
) -> Path:
    payload = {
        "generated_at": generated_at,
        "last_modified_at": generated_at,
        "smart_goal": "Build a 3-month emergency fund.",
        "target_phase": "full",
        "target_balance_cents": 1_900_000,
        "monthly_commitment_cents": 50_000,
        "essential_monthly_expenses_cents": 380_000,
        "target_multiplier_months": 5.0,
        "account_ids_in_fund": account_ids,
        "tier_balances_target": [
            {"account_id": aid, "target_balance_cents": 1_900_000, "role": "buffer"}
            for aid in account_ids
        ],
        "action_steps": [{"step": "Open HYSA", "timeline": "2026-04-15"}],
        "drawdown_rules_user_defined": "Job loss, medical bill > $500, urgent car repair.",
        "replenishment_commitment": "Pause new spending.",
        "drawdown_events_classified": drawdown_events_classified or [],
    }
    artifact_dir = _emergency_fund_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / "20260401.md"
    artifact_path.write_text(_render_emergency_fund_artifact(payload), encoding="utf-8")
    return artifact_path


# ---------------------------------------------------------------------------
# liquidity_below_3_months
# ---------------------------------------------------------------------------


def test_liquidity_below_3_months_fires_when_ratio_under_3_sustained(db_path: Path) -> None:
    with connect(db_path) as conn:
        savings_id = _seed_account(
            conn,
            account_type="savings",
            institution_name="Bank",
            account_name="Savings",
            balance_current_cents=500_000,  # current liquid $5k
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-03-05", "2026-04-05", "2026-05-05"),
        )
        # Prior-month-end (2026-05-31) snapshot also at $5k so ratio also below 3
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-05-31",
            balance_current_cents=500_000,
        )

        intervention = evaluate_liquidity_below_3_months(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "liquidity_below_3_months"
    assert intervention.move == Move.DIAGNOSE


def test_liquidity_below_3_months_does_not_fire_when_ratio_above_3(db_path: Path) -> None:
    with connect(db_path) as conn:
        savings_id = _seed_account(
            conn,
            account_type="savings",
            institution_name="Bank",
            account_name="Savings",
            balance_current_cents=2_000_000,  # $20k > 3 months of $3,800 essentials
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-03-05", "2026-04-05", "2026-05-05"),
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-05-31",
            balance_current_cents=2_000_000,
        )

        intervention = evaluate_liquidity_below_3_months(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_liquidity_below_3_months_does_not_fire_without_prior_month_snapshot(db_path: Path) -> None:
    """No prior-month-end snapshot -> cannot prove sustained -> do not fire."""
    with connect(db_path) as conn:
        _seed_account(
            conn,
            account_type="savings",
            institution_name="Bank",
            account_name="Savings",
            balance_current_cents=500_000,
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-03-05", "2026-04-05", "2026-05-05"),
        )
        # NO snapshot seeded

        intervention = evaluate_liquidity_below_3_months(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_liquidity_below_3_months_does_not_fire_when_prior_above_3(db_path: Path) -> None:
    """Current below but prior above 3 -> not sustained -> do not fire."""
    with connect(db_path) as conn:
        savings_id = _seed_account(
            conn,
            account_type="savings",
            institution_name="Bank",
            account_name="Savings",
            balance_current_cents=500_000,  # current ratio = 1.3
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-03-05", "2026-04-05", "2026-05-05"),
        )
        # Prior-month-end snapshot at $2M so ratio was healthy
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-05-31",
            balance_current_cents=2_000_000,
        )

        intervention = evaluate_liquidity_below_3_months(conn, build_context(conn, now=NOW))

    assert intervention is None


# ---------------------------------------------------------------------------
# cash_flow_surplus_no_savings
# ---------------------------------------------------------------------------


def test_cash_flow_surplus_fires_when_surplus_above_threshold_and_balance_flat(db_path: Path) -> None:
    with connect(db_path) as conn:
        savings_id = _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=200_000,
        )
        # 90 days back from NOW=2026-06-15 = 2026-03-17. Seed snapshots at both endpoints, flat.
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-03-17",
            balance_current_cents=200_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=200_000,
        )
        # Seed strong cash-flow surplus over the window
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 600_000,
                "2026-05": 600_000,
                "2026-06": 600_000,
            },
        )
        # Some essential expense (rent) so net surplus is large
        _seed_essentials(
            conn,
            monthly_cents=200_000,
            months=("2026-04-05", "2026-05-05", "2026-06-05"),
        )

        intervention = evaluate_cash_flow_surplus_no_savings(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "cash_flow_surplus_no_savings"
    assert intervention.move == Move.COACH


def test_cash_flow_surplus_does_not_fire_when_balance_growth_matches_surplus(db_path: Path) -> None:
    with connect(db_path) as conn:
        savings_id = _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=1_200_000,
        )
        # Start window at $200k, end at $1.2M => growth $1M >> half of $1.2M surplus
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-03-17",
            balance_current_cents=200_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_200_000,
        )
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 600_000,
                "2026-05": 600_000,
                "2026-06": 600_000,
            },
        )
        _seed_essentials(
            conn,
            monthly_cents=200_000,
            months=("2026-04-05", "2026-05-05", "2026-06-05"),
        )

        intervention = evaluate_cash_flow_surplus_no_savings(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_cash_flow_surplus_does_not_fire_when_no_snapshots_at_endpoints(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=200_000,
        )
        # No balance_snapshots seeded => suppressed for safety
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 600_000,
                "2026-05": 600_000,
                "2026-06": 600_000,
            },
        )

        intervention = evaluate_cash_flow_surplus_no_savings(conn, build_context(conn, now=NOW))

    assert intervention is None


# ---------------------------------------------------------------------------
# emergency_fund_drawdown_no_replenishment
# ---------------------------------------------------------------------------


def test_drawdown_does_not_fire_when_no_artifact(db_path: Path) -> None:
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings")
        _seed_balance_snapshot(conn, account_id=savings_id, snapshot_date="2026-05-01", balance_current_cents=500_000)
        _seed_balance_snapshot(conn, account_id=savings_id, snapshot_date="2026-06-15", balance_current_cents=300_000)

        intervention = evaluate_emergency_fund_drawdown(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_drawdown_fires_when_drop_over_20_pct_without_replenishment(db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Set a UserContext so the artifact dir resolves under tmp_path."""
    from finance_cli.user_context import UserContext, reset_user_context, set_user_context

    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        with connect(db_path) as conn:
            savings_id = _seed_account(conn, account_type="savings", balance_current_cents=70_000)
            # 60-day window leading up to NOW=2026-06-15
            _seed_balance_snapshot(
                conn,
                account_id=savings_id,
                snapshot_date="2026-04-10",
                balance_current_cents=100_000,
            )
            _seed_balance_snapshot(
                conn,
                account_id=savings_id,
                snapshot_date="2026-06-10",
                balance_current_cents=70_000,
            )
            artifact_path = _write_emergency_fund_artifact(account_ids=[savings_id])

            intervention = evaluate_emergency_fund_drawdown(conn, build_context(conn, now=NOW))

        assert intervention is not None
        assert intervention.pattern_id == "emergency_fund_drawdown_no_replenishment"
        assert intervention.move == Move.COACH
        assert intervention.priority == Priority.HIGH
        assert intervention.action is not None
        assert intervention.action.params["artifact_path"] == artifact_path.name
    finally:
        reset_user_context(token)


def test_drawdown_suppresses_when_event_already_classified(db_path: Path) -> None:
    from finance_cli.user_context import UserContext, reset_user_context, set_user_context

    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        with connect(db_path) as conn:
            savings_id = _seed_account(conn, account_type="savings", balance_current_cents=70_000)
            _seed_balance_snapshot(
                conn,
                account_id=savings_id,
                snapshot_date="2026-04-10",
                balance_current_cents=100_000,
            )
            _seed_balance_snapshot(
                conn,
                account_id=savings_id,
                snapshot_date="2026-06-10",
                balance_current_cents=70_000,
            )
            # Classified entry with matching account_ids + overlapping interval + matching balances
            _write_emergency_fund_artifact(
                account_ids=[savings_id],
                drawdown_events_classified=[
                    {
                        "event_id": "evt_001",
                        "artifact_path": "20260401.md",
                        "account_ids": [savings_id],
                        "pre_drop_date": "2026-04-11",
                        "low_date": "2026-06-11",
                        "pre_drop_balance_cents": 100_000,
                        "low_balance_cents": 70_000,
                        "user_classified_as_emergency": True,
                        "classification_recorded_at": "2026-06-12",
                    }
                ],
            )

            intervention = evaluate_emergency_fund_drawdown(conn, build_context(conn, now=NOW))

        assert intervention is None
    finally:
        reset_user_context(token)


def test_drawdown_suppresses_for_non_emergency_classification_too(db_path: Path) -> None:
    """Both classifications suppress re-fire (R6 alignment)."""
    from finance_cli.user_context import UserContext, reset_user_context, set_user_context

    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        with connect(db_path) as conn:
            savings_id = _seed_account(conn, account_type="savings", balance_current_cents=70_000)
            _seed_balance_snapshot(
                conn,
                account_id=savings_id,
                snapshot_date="2026-04-10",
                balance_current_cents=100_000,
            )
            _seed_balance_snapshot(
                conn,
                account_id=savings_id,
                snapshot_date="2026-06-10",
                balance_current_cents=70_000,
            )
            _write_emergency_fund_artifact(
                account_ids=[savings_id],
                drawdown_events_classified=[
                    {
                        "event_id": "evt_002",
                        "artifact_path": "20260401.md",
                        "account_ids": [savings_id],
                        "pre_drop_date": "2026-04-11",
                        "low_date": "2026-06-11",
                        "pre_drop_balance_cents": 100_000,
                        "low_balance_cents": 70_000,
                        "user_classified_as_emergency": False,
                        "classification_recorded_at": "2026-06-12",
                    }
                ],
            )

            intervention = evaluate_emergency_fund_drawdown(conn, build_context(conn, now=NOW))

        assert intervention is None
    finally:
        reset_user_context(token)


# ---------------------------------------------------------------------------
# income_shock_detected
# ---------------------------------------------------------------------------


def test_income_shock_fires_when_current_below_60_pct_of_median(db_path: Path) -> None:
    with connect(db_path) as conn:
        # 12 prior months at $5k, current month at $2.5k (50% of median)
        prior_months = {
            f"2025-{month:02d}": 500_000
            for month in range(6, 13)
        }
        prior_months.update({
            "2026-01": 500_000,
            "2026-02": 500_000,
            "2026-03": 500_000,
            "2026-04": 500_000,
            "2026-05": 500_000,
            "2026-06": 250_000,
        })
        _seed_monthly_income(conn, month_amounts=prior_months)

        intervention = evaluate_income_shock_detected(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "income_shock_detected"
    assert intervention.move == Move.WARN
    assert intervention.priority == Priority.HIGH


def test_income_shock_does_not_fire_when_current_above_80_pct(db_path: Path) -> None:
    with connect(db_path) as conn:
        prior_months = {
            f"2025-{month:02d}": 500_000
            for month in range(6, 13)
        }
        prior_months.update({
            "2026-01": 500_000,
            "2026-02": 500_000,
            "2026-03": 500_000,
            "2026-04": 500_000,
            "2026-05": 500_000,
            "2026-06": 450_000,  # 90% of median
        })
        _seed_monthly_income(conn, month_amounts=prior_months)

        intervention = evaluate_income_shock_detected(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_income_shock_suppressed_when_current_month_tx_count_too_low(db_path: Path) -> None:
    """Data-completeness gate: if current month has < 50% of median tx count, suppress."""
    with connect(db_path) as conn:
        # Seed 12 prior months with 2 income transactions each (median tx_count = 2)
        income_category = _category_id(conn, "Income: Salary", is_income=True)
        account_id = _seed_account(conn, institution_name="Employer", account_name="Payroll")
        for year_month in [f"2025-{m:02d}" for m in range(6, 13)] + [
            "2026-01", "2026-02", "2026-03", "2026-04", "2026-05"
        ]:
            for half in ("15", "28"):
                _seed_transaction(
                    conn,
                    account_id=account_id,
                    amount_cents=250_000,
                    txn_date=f"{year_month}-{half}",
                    category_id=income_category,
                    description="payroll",
                )
        # Current month: only 0 income transactions => count 0 < 0.5 * 2 = 1 => suppressed
        # (even though income is technically below threshold)

        intervention = evaluate_income_shock_detected(conn, build_context(conn, now=NOW))

    assert intervention is None
