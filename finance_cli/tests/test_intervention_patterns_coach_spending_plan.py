"""Deterministic intervention tests for ``coach_spending_plan`` patterns.

Five patterns:
  - chronic_monthly_deficit (entry signal — 2 months net-negative + no plan)
  - creeping_overspend_no_plan (entry signal — 3mo expense >= 10% above 6mo + flat income)
  - monthly_variance_review (artifact-driven — post-month-boundary review window)
  - directional_variance_pattern (artifact-driven — same-direction >= 25% across 2 months)
  - cross_skill_commitment_drift (artifact-driven — sibling vs this drift > 10%)
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.interventions.coach_spending_plan import (
    evaluate_chronic_monthly_deficit,
    evaluate_creeping_overspend_no_plan,
    evaluate_cross_skill_commitment_drift,
    evaluate_directional_variance_pattern,
    evaluate_monthly_variance_review,
)
from finance_cli.interventions.context import build_context
from finance_cli.interventions.registry import Move
from finance_cli.mcp_server import (
    _render_emergency_fund_artifact,
    _render_spending_plan_artifact,
    _emergency_fund_artifact_dir,
    _spending_plan_artifact_dir,
    coach_debt_payoff_artifact_save,
)


NOW = datetime(2026, 6, 8, 12, 0, 0)  # day 8 of June; prior month June 1 - May 31


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


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
    account_id: str,
    amount_cents: int,
    txn_date: str,
    category_id: str | None = None,
    description: str = "seed",
    use_type: str | None = "Personal",
    is_reviewed: int = 1,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id,
            is_payment, is_active, is_reviewed, use_type, source
        ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, ?, ?, 'manual')
        """,
        (txn_id, account_id, txn_date, description, amount_cents, category_id, is_reviewed, use_type),
    )
    conn.commit()
    return txn_id


def _seed_monthly_income(
    conn: sqlite3.Connection,
    *,
    month_amounts: dict[str, int],
) -> str:
    income_category = _category_id(conn, "Income: Salary", is_income=True)
    account_id = _seed_account(
        conn, account_type="checking", institution_name="Employer", account_name="Payroll"
    )
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


def _seed_monthly_expense(
    conn: sqlite3.Connection,
    *,
    month_amounts: dict[str, int],
    category_name: str = "Groceries",
) -> str:
    expense_category = _category_id(conn, category_name, is_income=False)
    account_id = _seed_account(
        conn,
        account_type="checking",
        institution_name="Store",
        account_name=category_name,
    )
    for month_key, amount_cents in month_amounts.items():
        _seed_transaction(
            conn,
            account_id=account_id,
            amount_cents=-abs(amount_cents),
            txn_date=f"{month_key}-05",
            category_id=expense_category,
            description=f"{category_name} purchase",
        )
    return account_id


def _spending_plan_payload(
    *,
    generated_at: str = "2026-06-07T12:00:00Z",
    debt_alloc_cents: int | None = 60_000,
    efund_alloc_cents: int | None = 30_000,
    variance_history: list[dict] | None = None,
    last_directional_flag_at: dict[str, str] | None = None,
    last_drift_classified: dict[str, dict] | None = None,
    last_review_recorded_at: str | None = None,
    by_category_extras: list[dict] | None = None,
    use_top_level_sides: bool = True,
) -> dict:
    """Build a spending-plan artifact payload.

    Set ``use_top_level_sides=False`` to omit the canonical top-level keys
    and rely on the intervention's by_category fallback path. Pass
    ``by_category_extras`` to inject the sibling commitments as inline
    entries (commonly with ``type`` set to ``"debt_paydown"`` /
    ``"savings_transfer"``).
    """
    by_category = [
        {
            "category_id": "cat_rent",
            "category_name": "Rent",
            "type": "essential",
            "monthly_cents": 200_000,
            "anchor_3mo_avg_cents": 200_000,
        },
        {
            "category_id": "cat_dining",
            "category_name": "Dining",
            "type": "discretionary",
            "monthly_cents": 40_000,
            "anchor_3mo_avg_cents": 38_000,
        },
    ]
    if by_category_extras:
        by_category.extend(by_category_extras)

    allocations: dict[str, object] = {"by_category": by_category}
    if use_top_level_sides:
        if efund_alloc_cents is not None:
            allocations["emergency_fund"] = {
                "monthly_cents": efund_alloc_cents,
                "sourced_from": "coach_emergency_fund",
            }
        if debt_alloc_cents is not None:
            allocations["debt_paydown"] = {
                "monthly_cents": debt_alloc_cents,
                "sourced_from": "coach_debt_payoff",
            }

    payload = {
        "generated_at": generated_at,
        "last_modified_at": generated_at,
        "strategy": "percentage_50_30_20",
        "expected_monthly_income_cents": 700_000,
        "expected_monthly_expenses_cents": 580_000,
        "expected_essential_monthly_cents": 380_000,
        "expected_discretionary_monthly_cents": 200_000,
        "review_cadence": "monthly",
        "next_review_at": "2026-07-07",
        "allocations": allocations,
        "periodic_reservations": [],
        "mirror_status": {"state": "ok", "failed_categories": [], "recorded_at": "2026-06-07"},
    }
    if variance_history is not None:
        payload["variance_history"] = variance_history
    if last_directional_flag_at is not None:
        payload["last_directional_flag_at"] = last_directional_flag_at
    if last_drift_classified is not None:
        payload["last_drift_classified"] = last_drift_classified
    if last_review_recorded_at is not None:
        payload["last_review_recorded_at"] = last_review_recorded_at
    return payload


def _write_spending_plan_artifact(**payload_overrides) -> Path:
    payload = _spending_plan_payload(**payload_overrides)
    artifact_dir = _spending_plan_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stem = payload["generated_at"][:10].replace("-", "")
    artifact_path = artifact_dir / f"{stem}.md"
    artifact_path.write_text(_render_spending_plan_artifact(payload), encoding="utf-8")
    return artifact_path


def _write_emergency_fund_artifact(
    *,
    monthly_commitment_cents: int = 30_000,
    generated_at: str = "2026-05-17T12:00:00Z",
) -> Path:
    payload = {
        "generated_at": generated_at,
        "last_modified_at": generated_at,
        "smart_goal": "Build 3-month e-fund by 2026-12.",
        "target_phase": "full",
        "target_balance_cents": 1_140_000,
        "monthly_commitment_cents": monthly_commitment_cents,
        "essential_monthly_expenses_cents": 380_000,
        "target_multiplier_months": 3.0,
        "account_ids_in_fund": ["acct_hysa"],
        "tier_balances_target": [{"account_id": "acct_hysa", "target_balance_cents": 1_140_000}],
        "action_steps": [{"step": "Open HYSA", "timeline": "2026-06-01"}],
        "drawdown_rules_user_defined": "Job loss; major medical; urgent car repair.",
        "replenishment_commitment": "Pause discretionary until rebuilt.",
    }
    artifact_dir = _emergency_fund_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stem = generated_at[:10].replace("-", "")
    artifact_path = artifact_dir / f"{stem}.md"
    artifact_path.write_text(_render_emergency_fund_artifact(payload), encoding="utf-8")
    return artifact_path


def _write_debt_payoff_artifact(
    *,
    monthly_commitment_cents: int = 60_000,
    generated_at: str = "2026-04-29T12:00:00Z",
) -> Path:
    """Use the live MCP tool — generates a real artifact in the live artifact dir."""
    payload = {
        "generated_at": generated_at,
        "smart_goal": "Pay off CC debt by 2027-06.",
        "strategy": "avalanche",
        "action_steps": [{"step": "Commit $X/mo", "timeline": "2026-05-01"}],
        "monthly_commitment_cents": monthly_commitment_cents,
        "debts_in_scope": [{"id": "liab_card_a", "label": "Card A"}],
    }
    result = coach_debt_payoff_artifact_save(action_plan_payload=payload, dry_run=False)
    return Path(result["data"]["artifact_path"])


# ---------------------------------------------------------------------------
# chronic_monthly_deficit
# ---------------------------------------------------------------------------


def test_chronic_monthly_deficit_fires_when_two_months_net_negative_and_no_plan(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        # Income covers ~half of expenses for both April + May
        _seed_monthly_income(conn, month_amounts={"2026-04": 400_000, "2026-05": 400_000})
        _seed_monthly_expense(
            conn,
            month_amounts={"2026-04": 700_000, "2026-05": 700_000},
            category_name="Rent",
        )

        intervention = evaluate_chronic_monthly_deficit(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "chronic_monthly_deficit"
    assert intervention.move == Move.DIAGNOSE
    assert intervention.action is not None
    assert intervention.action.params == {"name": "coach_spending_plan"}


def test_chronic_monthly_deficit_suppressed_when_plan_is_fresh(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_monthly_income(conn, month_amounts={"2026-04": 400_000, "2026-05": 400_000})
        _seed_monthly_expense(
            conn,
            month_amounts={"2026-04": 700_000, "2026-05": 700_000},
            category_name="Rent",
        )
        _write_spending_plan_artifact(generated_at="2026-06-01T12:00:00Z")

        intervention = evaluate_chronic_monthly_deficit(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_chronic_monthly_deficit_does_not_fire_when_only_one_month_negative(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        # April positive, May negative
        _seed_monthly_income(conn, month_amounts={"2026-04": 800_000, "2026-05": 400_000})
        _seed_monthly_expense(
            conn,
            month_amounts={"2026-04": 700_000, "2026-05": 700_000},
            category_name="Rent",
        )

        intervention = evaluate_chronic_monthly_deficit(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_chronic_monthly_deficit_suppressed_on_high_data_quality_gap(db_path: Path) -> None:
    """20% gap ratio (uncategorized OR unreviewed) suppresses the entry signal."""
    with connect(db_path) as conn:
        income_account = _seed_monthly_income(
            conn, month_amounts={"2026-04": 400_000, "2026-05": 400_000}
        )
        _seed_monthly_expense(
            conn,
            month_amounts={"2026-04": 700_000, "2026-05": 700_000},
            category_name="Rent",
        )
        # Stuff the 60d window with unreviewed transactions so gap >= 20%.
        unreviewed_account = _seed_account(conn, account_type="checking", account_name="Noise")
        for day in range(20):
            _seed_transaction(
                conn,
                account_id=unreviewed_account,
                amount_cents=-1_00,
                txn_date=f"2026-05-{day+1:02d}",
                category_id=None,
                description=f"unreviewed_{day}",
                is_reviewed=0,
            )
        del income_account

        intervention = evaluate_chronic_monthly_deficit(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


# ---------------------------------------------------------------------------
# creeping_overspend_no_plan
# ---------------------------------------------------------------------------


def test_creeping_overspend_fires_on_3mo_creep_with_flat_income(db_path: Path) -> None:
    with connect(db_path) as conn:
        # Flat income across all 6 months.
        _seed_monthly_income(
            conn,
            month_amounts={
                "2025-12": 600_000,
                "2026-01": 600_000,
                "2026-02": 600_000,
                "2026-03": 600_000,
                "2026-04": 600_000,
                "2026-05": 600_000,
            },
        )
        # 3mo avg 500k vs 6mo avg ~440k = ~14% creep
        _seed_monthly_expense(
            conn,
            month_amounts={
                "2025-12": 380_000,
                "2026-01": 380_000,
                "2026-02": 380_000,
                "2026-03": 500_000,
                "2026-04": 500_000,
                "2026-05": 500_000,
            },
            category_name="Lifestyle",
        )

        intervention = evaluate_creeping_overspend_no_plan(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "creeping_overspend_no_plan"
    assert intervention.move == Move.WARN
    assert intervention.action is not None
    assert intervention.action.params == {"name": "coach_spending_plan"}


def test_creeping_overspend_suppressed_when_income_grew(db_path: Path) -> None:
    """Income grew >5% — creep is funded, not a signal."""
    with connect(db_path) as conn:
        _seed_monthly_income(
            conn,
            month_amounts={
                "2025-12": 500_000,
                "2026-01": 500_000,
                "2026-02": 500_000,
                "2026-03": 800_000,
                "2026-04": 800_000,
                "2026-05": 800_000,
            },
        )
        _seed_monthly_expense(
            conn,
            month_amounts={
                "2025-12": 380_000,
                "2026-01": 380_000,
                "2026-02": 380_000,
                "2026-03": 500_000,
                "2026-04": 500_000,
                "2026-05": 500_000,
            },
            category_name="Lifestyle",
        )

        intervention = evaluate_creeping_overspend_no_plan(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_creeping_overspend_suppressed_when_plan_fresh(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_monthly_income(
            conn,
            month_amounts={
                "2025-12": 600_000,
                "2026-01": 600_000,
                "2026-02": 600_000,
                "2026-03": 600_000,
                "2026-04": 600_000,
                "2026-05": 600_000,
            },
        )
        _seed_monthly_expense(
            conn,
            month_amounts={
                "2025-12": 380_000,
                "2026-01": 380_000,
                "2026-02": 380_000,
                "2026-03": 500_000,
                "2026-04": 500_000,
                "2026-05": 500_000,
            },
            category_name="Lifestyle",
        )
        _write_spending_plan_artifact(generated_at="2026-06-01T12:00:00Z")

        intervention = evaluate_creeping_overspend_no_plan(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


# ---------------------------------------------------------------------------
# monthly_variance_review
# ---------------------------------------------------------------------------


def test_monthly_variance_review_fires_inside_window_with_unreviewed_prior_month(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _write_spending_plan_artifact(generated_at="2026-05-01T12:00:00Z")

        intervention = evaluate_monthly_variance_review(
            conn,
            build_context(conn, now=NOW),  # NOW is June 8 -> 8 days after May 31
        )

    assert intervention is not None
    assert intervention.pattern_id == "monthly_variance_review"
    assert intervention.move == Move.COACH


def test_monthly_variance_review_suppressed_when_prior_month_already_reviewed(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _write_spending_plan_artifact(
            generated_at="2026-05-01T12:00:00Z",
            last_review_recorded_at="2026-06-03",  # already reviewed May
        )

        intervention = evaluate_monthly_variance_review(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_monthly_variance_review_suppressed_outside_window(db_path: Path) -> None:
    with connect(db_path) as conn:
        _write_spending_plan_artifact(generated_at="2026-05-01T12:00:00Z")

        # Day 3 of the month — before the 6-day floor
        early_now = datetime(2026, 6, 3, 12, 0, 0)
        intervention = evaluate_monthly_variance_review(
            conn,
            build_context(conn, now=early_now),
        )

    assert intervention is None


def test_monthly_variance_review_suppressed_when_no_artifact(db_path: Path) -> None:
    with connect(db_path) as conn:
        intervention = evaluate_monthly_variance_review(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


# ---------------------------------------------------------------------------
# directional_variance_pattern
# ---------------------------------------------------------------------------


def test_directional_variance_fires_on_two_months_same_direction(db_path: Path) -> None:
    with connect(db_path) as conn:
        variance_history = [
            {
                "month": "2026-04",
                "per_category": [
                    {
                        "category_id": "cat_dining",
                        "category_name": "Dining",
                        "variance_pct": 32.0,
                        "classification": "signal",
                    }
                ],
                "overall": {"plan_total_cents": 580_000, "actual_total_cents": 590_000, "variance_pct": 1.7},
            },
            {
                "month": "2026-05",
                "per_category": [
                    {
                        "category_id": "cat_dining",
                        "category_name": "Dining",
                        "variance_pct": 28.0,
                        "classification": "directional",
                    }
                ],
                "overall": {"plan_total_cents": 580_000, "actual_total_cents": 595_000, "variance_pct": 2.6},
            },
        ]
        _write_spending_plan_artifact(
            generated_at="2026-03-15T12:00:00Z",
            variance_history=variance_history,
        )

        intervention = evaluate_directional_variance_pattern(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "directional_variance_pattern"
    assert "Dining" in intervention.headline


def test_directional_variance_suppressed_when_only_one_month_history(db_path: Path) -> None:
    with connect(db_path) as conn:
        variance_history = [
            {
                "month": "2026-05",
                "per_category": [
                    {"category_id": "cat_dining", "category_name": "Dining", "variance_pct": 32.0}
                ],
                "overall": {"plan_total_cents": 580_000, "actual_total_cents": 590_000, "variance_pct": 1.7},
            }
        ]
        _write_spending_plan_artifact(
            generated_at="2026-04-15T12:00:00Z",
            variance_history=variance_history,
        )

        intervention = evaluate_directional_variance_pattern(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_directional_variance_suppressed_within_60d_per_category_floor(db_path: Path) -> None:
    with connect(db_path) as conn:
        variance_history = [
            {
                "month": "2026-04",
                "per_category": [
                    {"category_id": "cat_dining", "category_name": "Dining", "variance_pct": 32.0}
                ],
                "overall": {"plan_total_cents": 580_000, "actual_total_cents": 590_000, "variance_pct": 1.7},
            },
            {
                "month": "2026-05",
                "per_category": [
                    {"category_id": "cat_dining", "category_name": "Dining", "variance_pct": 28.0}
                ],
                "overall": {"plan_total_cents": 580_000, "actual_total_cents": 595_000, "variance_pct": 2.6},
            },
        ]
        _write_spending_plan_artifact(
            generated_at="2026-03-15T12:00:00Z",
            variance_history=variance_history,
            last_directional_flag_at={"cat_dining": "2026-05-20"},  # 19 days before NOW
        )

        intervention = evaluate_directional_variance_pattern(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_directional_variance_suppressed_on_mixed_directions(db_path: Path) -> None:
    with connect(db_path) as conn:
        variance_history = [
            {
                "month": "2026-04",
                "per_category": [
                    {"category_id": "cat_dining", "category_name": "Dining", "variance_pct": 32.0}
                ],
                "overall": {"plan_total_cents": 580_000, "actual_total_cents": 590_000, "variance_pct": 1.7},
            },
            {
                "month": "2026-05",
                "per_category": [
                    {"category_id": "cat_dining", "category_name": "Dining", "variance_pct": -28.0}
                ],
                "overall": {"plan_total_cents": 580_000, "actual_total_cents": 560_000, "variance_pct": -3.4},
            },
        ]
        _write_spending_plan_artifact(
            generated_at="2026-03-15T12:00:00Z",
            variance_history=variance_history,
        )

        intervention = evaluate_directional_variance_pattern(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


# ---------------------------------------------------------------------------
# cross_skill_commitment_drift
# ---------------------------------------------------------------------------


def test_cross_skill_drift_fires_when_efund_sibling_differs_more_than_10pct(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _write_emergency_fund_artifact(monthly_commitment_cents=50_000)
        _write_spending_plan_artifact(efund_alloc_cents=30_000)  # 40% drift

        intervention = evaluate_cross_skill_commitment_drift(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "cross_skill_commitment_drift"
    assert intervention.move == Move.COACH


def test_cross_skill_drift_suppressed_within_noise_gate(db_path: Path) -> None:
    """When both values are under $50, drift is noise — suppress."""
    with connect(db_path) as conn:
        _write_emergency_fund_artifact(monthly_commitment_cents=3_000)
        _write_spending_plan_artifact(efund_alloc_cents=4_000)  # huge % but absolute $10

        intervention = evaluate_cross_skill_commitment_drift(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_cross_skill_drift_suppressed_when_no_sibling_artifact(db_path: Path) -> None:
    with connect(db_path) as conn:
        _write_spending_plan_artifact(efund_alloc_cents=30_000)

        intervention = evaluate_cross_skill_commitment_drift(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_cross_skill_drift_suppressed_by_matching_classified_tuple(db_path: Path) -> None:
    """Same drift already classified within 5% tolerance -> suppress indefinitely."""
    with connect(db_path) as conn:
        _write_emergency_fund_artifact(monthly_commitment_cents=50_000)
        _write_spending_plan_artifact(
            efund_alloc_cents=30_000,
            last_drift_classified={
                "emergency_fund": {
                    "classified_at": "2026-02-15",  # > 60d before NOW (well outside floor)
                    "sibling_value_cents": 50_000,
                    "this_plan_value_cents": 30_000,
                }
            },
        )

        intervention = evaluate_cross_skill_commitment_drift(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_cross_skill_drift_fires_via_by_category_fallback_for_emergency_fund(
    db_path: Path,
) -> None:
    """Live-drive quirk #2 fix: when the agent inlines the e-fund commitment in
    by_category (type='savings_transfer') without a top-level allocations.emergency_fund
    key, the drift intervention should still detect the drift.
    """
    with connect(db_path) as conn:
        _write_emergency_fund_artifact(monthly_commitment_cents=50_000)
        _write_spending_plan_artifact(
            efund_alloc_cents=None,  # No top-level allocations.emergency_fund
            use_top_level_sides=True,  # debt_paydown still top-level
            debt_alloc_cents=60_000,
            by_category_extras=[
                {
                    "category_id": "cat_efund",
                    "category_name": "Emergency Fund",
                    "type": "savings_transfer",
                    "monthly_cents": 30_000,
                }
            ],
        )

        intervention = evaluate_cross_skill_commitment_drift(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "cross_skill_commitment_drift"
    assert "emergency-fund" in intervention.headline


def test_cross_skill_drift_fires_via_by_category_fallback_for_debt_paydown(
    db_path: Path,
) -> None:
    """Same fallback path on the debt side — agent used type='debt_paydown' in by_category."""
    with connect(db_path) as conn:
        _write_debt_payoff_artifact(monthly_commitment_cents=100_000)
        _write_spending_plan_artifact(
            debt_alloc_cents=None,  # No top-level allocations.debt_paydown
            use_top_level_sides=True,  # efund still top-level
            efund_alloc_cents=30_000,
            by_category_extras=[
                {
                    "category_id": "cat_cc_paydown",
                    "category_name": "CC Paydown",
                    "type": "debt_paydown",
                    "monthly_cents": 60_000,
                }
            ],
        )

        intervention = evaluate_cross_skill_commitment_drift(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "cross_skill_commitment_drift"
    assert "debt-payoff" in intervention.headline


def test_cross_skill_drift_top_level_wins_over_by_category(db_path: Path) -> None:
    """When both shapes are present, top-level is the canonical value (no double-count)."""
    with connect(db_path) as conn:
        _write_emergency_fund_artifact(monthly_commitment_cents=50_000)
        _write_spending_plan_artifact(
            efund_alloc_cents=30_000,  # Top-level says 30,000 — drift detected (40% off)
            by_category_extras=[
                # by_category also has a savings_transfer entry — but should be ignored
                # because the top-level key wins. If we double-counted (summed to 80,000),
                # the drift would be 38% off in the OPPOSITE direction and the headline
                # would still say emergency-fund — but the dollar_impact would be wrong.
                {
                    "category_id": "cat_efund",
                    "category_name": "Emergency Fund",
                    "type": "savings_transfer",
                    "monthly_cents": 50_000,
                }
            ],
        )

        intervention = evaluate_cross_skill_commitment_drift(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is not None
    # Drift dollar_impact must reflect top-level value (50_000 sibling − 30_000 this = 20_000),
    # NOT the summed by_category value (50_000 + 30_000 = 80_000 → 30_000 diff).
    assert intervention.dollar_impact_cents == 20_000


def test_cross_skill_drift_suppressed_within_60d_refire_floor(db_path: Path) -> None:
    """Even on a materially-different new drift, 60d floor blocks re-fire."""
    with connect(db_path) as conn:
        _write_emergency_fund_artifact(monthly_commitment_cents=80_000)  # NEW drift vs classified
        _write_spending_plan_artifact(
            efund_alloc_cents=30_000,
            last_drift_classified={
                "emergency_fund": {
                    "classified_at": "2026-05-20",  # 19 days before NOW — inside 60d floor
                    "sibling_value_cents": 50_000,
                    "this_plan_value_cents": 30_000,
                }
            },
        )

        intervention = evaluate_cross_skill_commitment_drift(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None
