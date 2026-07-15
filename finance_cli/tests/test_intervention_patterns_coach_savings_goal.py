"""Deterministic intervention tests for ``coach_savings_goal`` patterns.

Mirrors the e-fund + debt-payoff intervention test structure. Three patterns:
  - cash_flow_surplus_no_savings_goal (entry signal — surplus + e-fund met + no active engagement)
  - savings_goal_stall (artifact-driven — 60-day progress shortfall)
  - savings_goal_milestone_hit (artifact-driven — threshold cross + hit_at gating)
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.interventions.coach_savings_goal import (
    evaluate_cash_flow_surplus_no_savings_goal,
    evaluate_savings_goal_milestone_hit,
    evaluate_savings_goal_stall,
)
from finance_cli.interventions.context import build_context
from finance_cli.interventions.registry import Move
from finance_cli.mcp_server import (
    _render_savings_goal_artifact,
    _savings_goal_artifact_dir,
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
    account_type: str = "savings",
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
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, 1, 'manual')
        """,
        (txn_id, account_id, txn_date, description, amount_cents, category_id),
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
    rent_category = _category_id(conn, "Rent", is_income=False)
    account_id = _seed_account(conn, account_type="checking", institution_name="Landlord", account_name="Rent")
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


def _seed_monthly_income(conn: sqlite3.Connection, *, month_amounts: dict[str, int]) -> str:
    income_category = _category_id(conn, "Income: Salary", is_income=True)
    account_id = _seed_account(conn, account_type="checking", institution_name="Employer", account_name="Payroll")
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


def _seed_goal(conn: sqlite3.Connection, *, name: str, target_cents: int, metric: str = "liquid_cash", is_active: int = 1) -> str:
    goal_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO goals (id, name, metric, target_cents, starting_cents,
                           direction, deadline, is_active, created_at, updated_at)
        VALUES (?, ?, ?, ?, 0, 'up', NULL, ?, datetime('now'), datetime('now'))
        """,
        (goal_id, name, metric, target_cents, is_active),
    )
    conn.commit()
    return goal_id


def _write_savings_goal_artifact(
    *,
    account_ids: list[str],
    target_balance_cents: int = 2_000_000,
    monthly_commitment_cents: int = 100_000,
    target_phase: str = "full",
    milestones: list[dict] | None = None,
    generated_at: str = "2026-03-15T10:00:00Z",
    goal_name: str = "down-payment-2027",
) -> Path:
    if milestones is None:
        milestones = [
            {"threshold_pct": 25, "threshold_cents": 500_000, "target_date": "2026-11-15", "hit_at": None},
            {"threshold_pct": 50, "threshold_cents": 1_000_000, "target_date": "2027-03-15", "hit_at": None},
            {"threshold_pct": 75, "threshold_cents": 1_500_000, "target_date": "2027-07-15", "hit_at": None},
            {"threshold_pct": 100, "threshold_cents": 2_000_000, "target_date": "2027-11-15", "hit_at": None},
        ]
    payload = {
        "generated_at": generated_at,
        "last_modified_at": generated_at,
        "goal_name": goal_name,
        "smart_goal": "Save $20,000 by 2027-11-15 ($1,000/mo for 18 months).",
        "target_phase": target_phase,
        "target_balance_cents": target_balance_cents,
        "monthly_commitment_cents": monthly_commitment_cents,
        "goal_horizon_months": 18,
        "target_met_date": "2027-11-15",
        "account_ids_in_goal": [
            {"account_id": aid, "role": "primary", "target_balance_cents": target_balance_cents}
            for aid in account_ids
        ],
        "action_steps": [{"step": "Open HYSA", "timeline": "2026-03-22"}],
        "milestones": milestones,
        "user_decision": "full",
        "monitoring_cadence": "monthly",
        "next_check_in": "2026-04-15",
    }
    artifact_dir = _savings_goal_artifact_dir()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stem = generated_at[:10].replace("-", "")
    artifact_path = artifact_dir / f"{stem}.md"
    artifact_path.write_text(_render_savings_goal_artifact(payload), encoding="utf-8")
    return artifact_path


# ---------------------------------------------------------------------------
# cash_flow_surplus_no_savings_goal — entry signal
# ---------------------------------------------------------------------------


def test_surplus_no_savings_goal_fires_when_surplus_above_threshold_and_efund_built(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        # Liquid balance >= 3 months of essentials ($380k essentials × 3 = $1.14M)
        savings_id = _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=1_200_000,
        )
        # Snapshot-backed coverage — required by the plan's MAX(snapshot_date <= ?) pattern
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_200_000,
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-03-05", "2026-04-05", "2026-05-05"),
        )
        # Strong cash-flow surplus over 90 days
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 600_000,
                "2026-05": 600_000,
                "2026-06": 600_000,
            },
        )

        intervention = evaluate_cash_flow_surplus_no_savings_goal(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "cash_flow_surplus_no_savings_goal"
    assert intervention.move == Move.DIAGNOSE
    assert intervention.action is not None
    assert intervention.action.params == {"name": "coach_savings_goal"}


def test_surplus_no_savings_goal_does_not_fire_when_efund_coverage_below_three_months(
    db_path: Path,
) -> None:
    """E-fund's surplus pattern owns this window when coverage < 3 months."""
    with connect(db_path) as conn:
        _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=500_000,  # ~1.3 months @ $380k essentials
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-03-05", "2026-04-05", "2026-05-05"),
        )
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 600_000,
                "2026-05": 600_000,
                "2026-06": 600_000,
            },
        )

        intervention = evaluate_cash_flow_surplus_no_savings_goal(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_surplus_no_savings_goal_does_not_fire_when_surplus_below_threshold(db_path: Path) -> None:
    """Threshold gate at $1,500 / 90-day window.

    Window per NOW=2026-06-15 is (2026-03-17, 2026-06-15] so essentials and
    income must land at dates inside that range with matching counts to make
    net ≈ 0.
    """
    with connect(db_path) as conn:
        _seed_account(conn, account_type="savings", balance_current_cents=1_200_000)
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-04-05", "2026-05-05", "2026-06-05"),
        )
        # Tiny surplus — income just covers expenses
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 380_000,
                "2026-05": 380_000,
                "2026-06": 380_000,
            },
        )

        intervention = evaluate_cash_flow_surplus_no_savings_goal(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_surplus_no_savings_goal_does_not_fire_when_active_engagement_exists(
    db_path: Path,
) -> None:
    """Anti-duplicate gate: active goal named after the savings-goal artifact suppresses re-fire."""
    with connect(db_path) as conn:
        savings_id = _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=1_200_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_200_000,
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-03-05", "2026-04-05", "2026-05-05"),
        )
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 600_000,
                "2026-05": 600_000,
                "2026-06": 600_000,
            },
        )
        # Active engagement: artifact present + matching goal row active
        _write_savings_goal_artifact(account_ids=[savings_id], goal_name="down-payment-2027")
        _seed_goal(conn, name="down-payment-2027", target_cents=2_000_000)

        intervention = evaluate_cash_flow_surplus_no_savings_goal(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_surplus_no_savings_goal_fires_when_artifact_present_but_goal_inactive(
    db_path: Path,
) -> None:
    """Soft-deleted goal row doesn't count as engagement — re-prompt is correct."""
    with connect(db_path) as conn:
        savings_id = _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=1_200_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_200_000,
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-03-05", "2026-04-05", "2026-05-05"),
        )
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 600_000,
                "2026-05": 600_000,
                "2026-06": 600_000,
            },
        )
        _write_savings_goal_artifact(account_ids=[savings_id], goal_name="vacation-2025")
        _seed_goal(conn, name="vacation-2025", target_cents=5_000_00, is_active=0)

        intervention = evaluate_cash_flow_surplus_no_savings_goal(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is not None


# ---------------------------------------------------------------------------
# savings_goal_stall — artifact-driven maintenance
# ---------------------------------------------------------------------------


def test_stall_fires_when_progress_under_half_expected(db_path: Path) -> None:
    """Expected: $1k/mo × 2 months = $2,000 progress. Actual: $400 → < 50% threshold."""
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=1_200_000)
        # NOW=2026-06-15, window start = 2026-04-16
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-04-16",
            balance_current_cents=1_200_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_240_000,  # $400 progress vs $2,000 expected
        )
        # Artifact generated_at is older than the window so the runway requirement is met
        _write_savings_goal_artifact(
            account_ids=[savings_id],
            generated_at="2026-03-15T10:00:00Z",
        )

        intervention = evaluate_savings_goal_stall(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "savings_goal_stall"
    assert intervention.move == Move.COACH


def test_stall_does_not_fire_when_progress_on_track(db_path: Path) -> None:
    """Progress = $2,200 > 50% of $2,000 expected → no fire."""
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=1_220_000)
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-04-16",
            balance_current_cents=1_000_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_220_000,  # $2,200 progress
        )
        _write_savings_goal_artifact(account_ids=[savings_id])

        intervention = evaluate_savings_goal_stall(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_stall_does_not_fire_when_no_artifact(db_path: Path) -> None:
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=1_200_000)
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-04-16",
            balance_current_cents=1_200_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_240_000,
        )

        intervention = evaluate_savings_goal_stall(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_stall_does_not_fire_when_artifact_too_recent(db_path: Path) -> None:
    """Artifact generated_at inside the 60-day window → insufficient runway → suppress."""
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=1_200_000)
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-05-15",
            balance_current_cents=1_200_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_200_000,
        )
        # Artifact only 1 month old — can't assess 60-day stall yet
        _write_savings_goal_artifact(
            account_ids=[savings_id],
            generated_at="2026-05-15T10:00:00Z",
        )

        intervention = evaluate_savings_goal_stall(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_stall_does_not_fire_when_commitment_is_zero(db_path: Path) -> None:
    """Zero commitment = no baseline to compare against."""
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=1_200_000)
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-04-16",
            balance_current_cents=1_200_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_200_000,
        )
        _write_savings_goal_artifact(
            account_ids=[savings_id],
            monthly_commitment_cents=0,
        )

        intervention = evaluate_savings_goal_stall(conn, build_context(conn, now=NOW))

    assert intervention is None


# ---------------------------------------------------------------------------
# savings_goal_milestone_hit — artifact-driven maintenance
# ---------------------------------------------------------------------------


def test_milestone_hit_fires_when_balance_crosses_first_unhit_threshold(db_path: Path) -> None:
    """Balance $550k >= 25% milestone ($500k); first unhit milestone fires."""
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=550_000)
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=550_000,
        )
        _write_savings_goal_artifact(account_ids=[savings_id])

        intervention = evaluate_savings_goal_milestone_hit(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "savings_goal_milestone_hit"
    assert intervention.move == Move.COACH
    # The action payload tells the playbook which milestone to update.
    assert intervention.action is not None
    assert intervention.action.tool == "coach_savings_goal_artifact_read"
    assert intervention.action.params["threshold_pct"] == 25
    assert intervention.action.params["threshold_cents"] == 500_000
    assert intervention.action.params["milestone_index"] == 0
    # Plan §"Intervention Registry Entries": "Never surface absolute filesystem
    # paths in user-facing prompt text." artifact_path must be relative — the
    # filename only — so the playbook reconstructs the full path itself.
    raw_path = intervention.action.params["artifact_path"]
    assert isinstance(raw_path, str)
    assert raw_path == "20260315.md"
    assert "/" not in raw_path
    assert "\\" not in raw_path


def test_milestone_hit_skips_already_hit_threshold(db_path: Path) -> None:
    """First unhit milestone after the 25% one is the 50% milestone; balance crosses it."""
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=1_100_000)
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_100_000,
        )
        milestones = [
            {"threshold_pct": 25, "threshold_cents": 500_000, "target_date": "2026-11-15", "hit_at": "2026-05-01"},
            {"threshold_pct": 50, "threshold_cents": 1_000_000, "target_date": "2027-03-15", "hit_at": None},
            {"threshold_pct": 75, "threshold_cents": 1_500_000, "target_date": "2027-07-15", "hit_at": None},
            {"threshold_pct": 100, "threshold_cents": 2_000_000, "target_date": "2027-11-15", "hit_at": None},
        ]
        _write_savings_goal_artifact(account_ids=[savings_id], milestones=milestones)

        intervention = evaluate_savings_goal_milestone_hit(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action.params["threshold_pct"] == 50


def test_milestone_hit_does_not_fire_when_balance_below_all_unhit_thresholds(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=300_000)
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=300_000,
        )
        _write_savings_goal_artifact(account_ids=[savings_id])

        intervention = evaluate_savings_goal_milestone_hit(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_milestone_hit_does_not_fire_when_all_milestones_already_hit(db_path: Path) -> None:
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=2_100_000)
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=2_100_000,
        )
        milestones = [
            {"threshold_pct": 25, "threshold_cents": 500_000, "target_date": "2026-11-15", "hit_at": "2026-05-01"},
            {"threshold_pct": 50, "threshold_cents": 1_000_000, "target_date": "2027-03-15", "hit_at": "2026-06-01"},
            {"threshold_pct": 75, "threshold_cents": 1_500_000, "target_date": "2027-07-15", "hit_at": "2026-06-05"},
            {"threshold_pct": 100, "threshold_cents": 2_000_000, "target_date": "2027-11-15", "hit_at": "2026-06-10"},
        ]
        _write_savings_goal_artifact(account_ids=[savings_id], milestones=milestones)

        intervention = evaluate_savings_goal_milestone_hit(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_milestone_hit_uses_revision_aware_latest_artifact(db_path: Path) -> None:
    """Regression for the sort-by-filename bug: ``20260607-r2.md`` lexically
    sorts BEFORE ``20260607.md``, so a naive ``sorted(glob('*.md'))[-1]``
    would pick the stale base. The evaluator's ``_latest_savings_goal_artifact``
    must use the revision-aware ``_latest_artifact_path`` helper so milestone
    detection runs against the latest revision's milestone schedule.
    """
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=550_000)
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=550_000,
        )
        # Base artifact: $1M starter target with 25% at $250k — already hit
        base_milestones = [
            {"threshold_pct": 25, "threshold_cents": 250_000, "target_date": "2026-08-15", "hit_at": "2026-05-01"},
            {"threshold_pct": 50, "threshold_cents": 500_000, "target_date": "2026-11-15", "hit_at": "2026-06-01"},
            {"threshold_pct": 75, "threshold_cents": 750_000, "target_date": "2027-02-15", "hit_at": None},
            {"threshold_pct": 100, "threshold_cents": 1_000_000, "target_date": "2027-05-15", "hit_at": None},
        ]
        _write_savings_goal_artifact(
            account_ids=[savings_id],
            target_balance_cents=1_000_000,
            generated_at="2026-06-07T10:00:00Z",
            milestones=base_milestones,
        )
        # Revised artifact: $2M full target with NO milestones hit yet — first
        # unhit milestone is 25% at $500k, which the $550k balance has crossed.
        # If the evaluator picks the base, it sees only 75%/100% unhit and
        # neither is reached; if it picks -r2, the 25% milestone fires.
        revised_milestones = [
            {"threshold_pct": 25, "threshold_cents": 500_000, "target_date": "2026-11-15", "hit_at": None},
            {"threshold_pct": 50, "threshold_cents": 1_000_000, "target_date": "2027-03-15", "hit_at": None},
            {"threshold_pct": 75, "threshold_cents": 1_500_000, "target_date": "2027-07-15", "hit_at": None},
            {"threshold_pct": 100, "threshold_cents": 2_000_000, "target_date": "2027-11-15", "hit_at": None},
        ]
        # Write directly to -r2 path so we exercise the lexicographic-sort trap.
        from finance_cli.mcp_server import (
            _render_savings_goal_artifact,
            _savings_goal_artifact_dir,
        )

        revised_payload = {
            "generated_at": "2026-06-07T18:30:00Z",
            "last_modified_at": "2026-06-07T18:30:00Z",
            "goal_name": "down-payment-2027",
            "smart_goal": "Save $20,000 by 2027-11-15.",
            "target_phase": "full",
            "target_balance_cents": 2_000_000,
            "monthly_commitment_cents": 100_000,
            "goal_horizon_months": 18,
            "target_met_date": "2027-11-15",
            "account_ids_in_goal": [
                {"account_id": savings_id, "role": "primary", "target_balance_cents": 2_000_000}
            ],
            "action_steps": [{"step": "Open HYSA", "timeline": "2026-06-15"}],
            "milestones": revised_milestones,
            "user_decision": "full",
        }
        revised_path = _savings_goal_artifact_dir() / "20260607-r2.md"
        revised_path.write_text(_render_savings_goal_artifact(revised_payload), encoding="utf-8")

        intervention = evaluate_savings_goal_milestone_hit(conn, build_context(conn, now=NOW))

    assert intervention is not None
    # The 25% milestone in the revised plan is at $500k — $550k crosses it.
    assert intervention.action.params["threshold_cents"] == 500_000
    # And the action payload identifies the -r2 file as the artifact to update.
    assert intervention.action.params["artifact_path"] == "20260607-r2.md"


def test_milestone_hit_does_not_fire_when_no_snapshots(db_path: Path) -> None:
    """Without balance_snapshots we can't measure current balance — suppress."""
    with connect(db_path) as conn:
        savings_id = _seed_account(conn, account_type="savings", balance_current_cents=550_000)
        # No balance_snapshots seeded
        _write_savings_goal_artifact(account_ids=[savings_id])

        intervention = evaluate_savings_goal_milestone_hit(conn, build_context(conn, now=NOW))

    assert intervention is None


# ---------------------------------------------------------------------------
# Disjointness regression: e-fund vs savings-goal surplus pattern coexistence
# ---------------------------------------------------------------------------


def test_surplus_no_savings_goal_uses_snapshot_not_live_balance(db_path: Path) -> None:
    """Regression for Codex P1 (R1): coverage must derive from balance_snapshots
    via MAX(snapshot_date <= as_of), NOT from accounts.balance_current_cents.

    Setup: live balance reads 3+ months of essentials but the latest snapshot
    is below 3 months — the pattern must suppress because the snapshot view
    is the authoritative as-of measure (and matches the disjointness gate the
    e-fund pattern uses).
    """
    with connect(db_path) as conn:
        # Live balance ~$1.2M (>= 3 months of $380k essentials)
        savings_id = _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=1_200_000,
        )
        # Latest snapshot is only ~$500k — coverage shows < 2 months.
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=500_000,
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-04-05", "2026-05-05", "2026-06-05"),
        )
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 600_000,
                "2026-05": 600_000,
                "2026-06": 600_000,
            },
        )

        intervention = evaluate_cash_flow_surplus_no_savings_goal(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_efund_and_savings_goal_surplus_patterns_are_disjoint_across_coverage(
    db_path: Path,
) -> None:
    """Regression for Codex P1 (R2): the two surplus patterns must not BOTH fire
    when emergency-fund coverage is >= 3 months. e-fund's pattern defers above
    the threshold; savings-goal's pattern owns that window.
    """
    from finance_cli.interventions.coach_emergency_fund import (
        evaluate_cash_flow_surplus_no_savings,
    )

    with connect(db_path) as conn:
        savings_id = _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=1_200_000,
        )
        # Snapshots at both 90-day endpoints — required by e-fund's evaluator
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-03-17",
            balance_current_cents=1_200_000,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=1_200_000,
        )
        _seed_essentials(
            conn,
            monthly_cents=380_000,
            months=("2026-04-05", "2026-05-05", "2026-06-05"),
        )
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 600_000,
                "2026-05": 600_000,
                "2026-06": 600_000,
            },
        )

        ctx = build_context(conn, now=NOW)
        efund_intervention = evaluate_cash_flow_surplus_no_savings(conn, ctx)
        savings_goal_intervention = evaluate_cash_flow_surplus_no_savings_goal(conn, ctx)

    # E-fund pattern should defer once coverage >= 3 months (~$1.14M target hit)
    assert efund_intervention is None
    # Savings-goal pattern owns this window
    assert savings_goal_intervention is not None
    assert savings_goal_intervention.pattern_id == "cash_flow_surplus_no_savings_goal"


def test_surplus_disjointness_at_rounding_boundary(db_path: Path) -> None:
    """Regression for Codex R2 P1: coverage gate must use raw Decimal ratio,
    not a quantized-to-0.01 ratio.

    Setup: liquid=$299,500 + essential=$100,000 -> raw coverage = 2.995. A
    quantized-to-0.01 view rounds this to 3.00, which would let savings-goal
    fire while e-fund's unrounded gate still sees 2.995 < 3 and ALSO fires.
    Both patterns must remain disjoint at the boundary.
    """
    from finance_cli.interventions.coach_emergency_fund import (
        evaluate_cash_flow_surplus_no_savings,
    )

    with connect(db_path) as conn:
        savings_id = _seed_account(
            conn,
            account_type="savings",
            balance_current_cents=299_500,
        )
        # Snapshots at the 90-day endpoints so e-fund's evaluator can run.
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-03-17",
            balance_current_cents=299_500,
        )
        _seed_balance_snapshot(
            conn,
            account_id=savings_id,
            snapshot_date="2026-06-15",
            balance_current_cents=299_500,
        )
        # $100k/mo essentials across the 3 complete trailing months (March,
        # April, May) so category_spending_averages reads $100k/mo and the
        # coverage ratio lands at exactly 2.995 ($299,500 / $100,000).
        _seed_essentials(
            conn,
            monthly_cents=100_000,
            months=("2026-03-05", "2026-04-05", "2026-05-05"),
        )
        _seed_monthly_income(
            conn,
            month_amounts={
                "2026-04": 300_000,
                "2026-05": 300_000,
                "2026-06": 300_000,
            },
        )

        ctx = build_context(conn, now=NOW)
        efund_intervention = evaluate_cash_flow_surplus_no_savings(conn, ctx)
        savings_goal_intervention = evaluate_cash_flow_surplus_no_savings_goal(conn, ctx)

    # At raw coverage = 2.995 (< 3.0), e-fund's surplus pattern owns the window
    # and savings-goal defers. The reverse (savings-goal fires + e-fund silent)
    # is exercised by test_efund_and_savings_goal_surplus_patterns_are_disjoint_across_coverage.
    assert efund_intervention is not None
    assert efund_intervention.pattern_id == "cash_flow_surplus_no_savings"
    assert savings_goal_intervention is None


# ---------------------------------------------------------------------------
# Registry smoke test (per plan §"PR-C scope")
# ---------------------------------------------------------------------------


def test_all_three_patterns_register_on_import() -> None:
    from finance_cli.interventions.registry import PATTERN_REGISTRY

    assert "cash_flow_surplus_no_savings_goal" in PATTERN_REGISTRY
    assert "savings_goal_stall" in PATTERN_REGISTRY
    assert "savings_goal_milestone_hit" in PATTERN_REGISTRY
