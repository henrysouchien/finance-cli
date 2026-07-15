"""Tests for ``coach_savings_goal_check_unlock_conditions`` — live-data cross-skill
unlock gate that powers Phase 9's starter→full transition prompt.

Coverage per the plan §"Phase 9: Monitor" branches:
  - debt-only blocker + debt cleared → eligible
  - efund-only blocker + e-fund target met → eligible
  - both blocker + both cleared → eligible
  - debt-only blocker + debt NOT cleared → not eligible
  - debt blocker + missing debt-payoff prerequisite artifact → not eligible
  - target_phase=full → not_starter_only
  - no savings-goal artifact → no_savings_goal_artifact
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from finance_cli.commands import db_cmd  # noqa: F401  (ensures migrations module is registered)
from finance_cli.db import connect, initialize_database
from finance_cli.mcp_server import (
    coach_debt_payoff_artifact_save,
    coach_emergency_fund_artifact_save,
    coach_savings_goal_artifact_save,
    coach_savings_goal_check_unlock_conditions,
)
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


@pytest.fixture()
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        yield tmp_path
    finally:
        reset_user_context(token)


def _savings_goal_payload(**overrides) -> dict:
    payload = {
        "generated_at": "2026-06-07T12:00:00Z",
        "goal_name": "down-payment-2027",
        "smart_goal": "Save $20,000 by 2027-11-15 ($1,000/mo for 18 months).",
        "target_phase": "starter_only",
        "target_balance_cents": 200_000,
        "monthly_commitment_cents": 50_000,
        "goal_horizon_months": 4,
        "target_met_date": "2026-10-07",
        "account_ids_in_goal": [
            {"account_id": "acct_hysa_001", "role": "primary", "target_balance_cents": 200_000},
        ],
        "action_steps": [{"step": "Open HYSA", "timeline": "2026-06-15"}],
        "milestones": [
            {"threshold_pct": 100, "threshold_cents": 200_000, "target_date": "2026-10-07", "hit_at": None},
        ],
        "user_decision": "starter_then_debt",
        "unlock_blocker": "debt",
        "original_full_target_balance_cents": 2_000_000,
        "original_full_monthly_commitment_cents": 100_000,
        "original_full_target_met_date": "2027-11-15",
        "original_full_goal_horizon_months": 18,
    }
    payload.update(overrides)
    return payload


def _debt_payoff_payload(debts_in_scope: list[dict]) -> dict:
    return {
        "generated_at": "2026-05-01T12:00:00Z",
        "smart_goal": "Pay off $5,000 in CC debt by 2026-12-31.",
        "strategy": {"name": "avalanche", "why": "highest APR first"},
        "action_steps": [{"step": "Stop new card spending", "timeline": "2026-05-10"}],
        "monthly_commitment_cents": 50_000,
        "debts_in_scope": debts_in_scope,
    }


def _efund_payload(account_ids: list[str], target_balance_cents: int) -> dict:
    return {
        "generated_at": "2026-04-15T12:00:00Z",
        "smart_goal": "Build a 3-month emergency fund by 2026-09.",
        "target_phase": "full",
        "target_balance_cents": target_balance_cents,
        "monthly_commitment_cents": 30_000,
        "essential_monthly_expenses_cents": 380_000,
        "target_multiplier_months": 3.0,
        "account_ids_in_fund": account_ids,
        "tier_balances_target": [{"account_id": aid, "target_balance_cents": target_balance_cents, "role": "buffer"} for aid in account_ids],
        "action_steps": [{"step": "Open HYSA", "timeline": "2026-04-22"}],
        "drawdown_rules_user_defined": "Real emergency = job loss or urgent repair.",
        "replenishment_commitment": "Resume contributions next month.",
    }


def _seed_account(conn, *, account_id: str, account_type: str, balance_cents: int) -> None:
    conn.execute(
        """INSERT INTO accounts (id, institution_name, account_name, account_type,
           balance_current_cents, is_active) VALUES (?, 'Test', ?, ?, ?, 1)""",
        (account_id, f"{account_type} account", account_type, balance_cents),
    )
    conn.commit()


def _seed_liability(conn, *, account_id: str, balance_cents: int) -> str:
    """Insert a credit-card liability row + matching account_balance entry.

    Credit-card balances are stored as NEGATIVE values in accounts.balance_current_cents
    per the debt-payoff precedent (coach_debt_payoff.py:722 uses ABS()).
    """
    liability_id = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO liabilities (id, account_id, liability_type, is_active)
           VALUES (?, ?, 'credit', 1)""",
        (liability_id, account_id),
    )
    conn.execute(
        "UPDATE accounts SET balance_current_cents = ? WHERE id = ?",
        (-balance_cents, account_id),
    )
    conn.commit()
    return liability_id


def _seed_manual_loan(conn, *, balance_cents: int) -> str:
    loan_id = uuid.uuid4().hex
    conn.execute(
        """INSERT INTO manual_loans (id, creditor_name, description,
           total_disbursed_cents, current_balance_cents, interest_rate_pct,
           interest_type, start_date, use_type, is_active)
           VALUES (?, 'Family member', 'Bridge loan', ?, ?, 0.0, 'none',
                   '2026-01-01', 'Personal', 1)""",
        (loan_id, balance_cents, balance_cents),
    )
    conn.commit()
    return loan_id


def _seed_balance_snapshot(
    conn, *, account_id: str, balance_cents: int, snapshot_date: str | None = None
) -> None:
    if snapshot_date is None:
        snapshot_date = datetime.now(timezone.utc).date().isoformat()
    conn.execute(
        """INSERT INTO balance_snapshots (id, account_id, balance_current_cents,
           source, snapshot_date) VALUES (?, ?, ?, 'sync', ?)""",
        (uuid.uuid4().hex, account_id, balance_cents, snapshot_date),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_unlock_no_savings_goal_artifact(data_dir: Path) -> None:
    del data_dir
    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is False
    assert result["summary"]["reason"] == "no_savings_goal_artifact"
    assert result["data"]["unlock_blocker"] is None


def test_unlock_target_phase_full_returns_not_starter_only(data_dir: Path) -> None:
    del data_dir
    coach_savings_goal_artifact_save(
        plan_payload=_savings_goal_payload(
            target_phase="full",
            unlock_blocker=None,
            original_full_target_balance_cents=None,
            original_full_monthly_commitment_cents=None,
            original_full_target_met_date=None,
            original_full_goal_horizon_months=None,
            user_decision="full",
        ),
        dry_run=False,
    )
    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is False
    assert result["summary"]["reason"] == "not_starter_only"
    assert result["data"]["unlock_blocker"] is None


def test_unlock_debt_blocker_debt_cleared_is_eligible(data_dir: Path) -> None:
    """debt blocker + zero in-scope debt -> unlock_eligible True."""
    db_path = data_dir / "finance.db"
    with connect(db_path) as conn:
        _seed_account(conn, account_id="cc_001", account_type="credit_card", balance_cents=0)
        liability_id = _seed_liability(conn, account_id="cc_001", balance_cents=0)

    coach_debt_payoff_artifact_save(
        action_plan_payload=_debt_payoff_payload(
            debts_in_scope=[{"id": liability_id, "label": "Card 1", "source": "liability"}],
        ),
        dry_run=False,
    )
    coach_savings_goal_artifact_save(
        plan_payload=_savings_goal_payload(unlock_blocker="debt"),
        dry_run=False,
    )

    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is True
    assert result["summary"]["blocker_resolved"] == "debt"
    assert result["data"]["unlock_blocker"] == "debt"
    assert result["data"]["evidence"]["debt_cleared"] is True
    assert result["data"]["evidence"]["debt_in_scope_sum_cents"] == 0
    assert result["data"]["evidence"]["efund_target_met"] is None
    assert result["data"]["evidence"]["missing_prerequisite_artifacts"] == []


def test_unlock_debt_blocker_debt_remaining_is_not_eligible(data_dir: Path) -> None:
    """debt blocker + non-trivial remaining balance -> unlock_eligible False."""
    db_path = data_dir / "finance.db"
    with connect(db_path) as conn:
        _seed_account(conn, account_id="cc_002", account_type="credit_card", balance_cents=0)
        liability_id = _seed_liability(conn, account_id="cc_002", balance_cents=200_000)

    coach_debt_payoff_artifact_save(
        action_plan_payload=_debt_payoff_payload(
            debts_in_scope=[{"id": liability_id, "label": "Card 1", "source": "liability"}],
        ),
        dry_run=False,
    )
    coach_savings_goal_artifact_save(
        plan_payload=_savings_goal_payload(unlock_blocker="debt"),
        dry_run=False,
    )

    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is False
    assert result["summary"]["blocker_resolved"] == "none"
    assert result["data"]["evidence"]["debt_cleared"] is False
    assert result["data"]["evidence"]["debt_in_scope_sum_cents"] == 200_000


def test_unlock_efund_blocker_target_met_is_eligible(data_dir: Path) -> None:
    """efund blocker + balance >= target -> unlock_eligible True."""
    db_path = data_dir / "finance.db"
    with connect(db_path) as conn:
        _seed_account(conn, account_id="hysa_001", account_type="savings", balance_cents=1_200_000)
        _seed_balance_snapshot(conn, account_id="hysa_001", balance_cents=1_200_000)

    coach_emergency_fund_artifact_save(
        plan_payload=_efund_payload(account_ids=["hysa_001"], target_balance_cents=1_140_000),
        dry_run=False,
    )
    coach_savings_goal_artifact_save(
        plan_payload=_savings_goal_payload(unlock_blocker="efund", user_decision="starter_then_efund"),
        dry_run=False,
    )

    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is True
    assert result["summary"]["blocker_resolved"] == "efund"
    assert result["data"]["evidence"]["efund_target_met"] is True
    assert result["data"]["evidence"]["efund_balance_sum_cents"] == 1_200_000
    assert result["data"]["evidence"]["efund_target_balance_cents"] == 1_140_000
    assert result["data"]["evidence"]["debt_cleared"] is None  # informational, not evaluated


def test_unlock_both_blocker_requires_both_cleared(data_dir: Path) -> None:
    """both blocker + both prerequisites resolved -> unlock_eligible True."""
    db_path = data_dir / "finance.db"
    with connect(db_path) as conn:
        _seed_account(conn, account_id="cc_003", account_type="credit_card", balance_cents=0)
        liability_id = _seed_liability(conn, account_id="cc_003", balance_cents=0)
        _seed_account(conn, account_id="hysa_002", account_type="savings", balance_cents=1_200_000)
        _seed_balance_snapshot(conn, account_id="hysa_002", balance_cents=1_200_000)

    coach_debt_payoff_artifact_save(
        action_plan_payload=_debt_payoff_payload(
            debts_in_scope=[{"id": liability_id, "source": "liability"}],
        ),
        dry_run=False,
    )
    coach_emergency_fund_artifact_save(
        plan_payload=_efund_payload(account_ids=["hysa_002"], target_balance_cents=1_140_000),
        dry_run=False,
    )
    coach_savings_goal_artifact_save(
        plan_payload=_savings_goal_payload(unlock_blocker="both"),
        dry_run=False,
    )

    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is True
    assert result["summary"]["blocker_resolved"] == "both"
    assert result["data"]["evidence"]["debt_cleared"] is True
    assert result["data"]["evidence"]["efund_target_met"] is True


def test_unlock_both_blocker_only_one_cleared_is_not_eligible(data_dir: Path) -> None:
    """both blocker + only debt cleared (efund still under target) -> not eligible."""
    db_path = data_dir / "finance.db"
    with connect(db_path) as conn:
        _seed_account(conn, account_id="cc_004", account_type="credit_card", balance_cents=0)
        liability_id = _seed_liability(conn, account_id="cc_004", balance_cents=0)
        _seed_account(conn, account_id="hysa_003", account_type="savings", balance_cents=500_000)
        _seed_balance_snapshot(conn, account_id="hysa_003", balance_cents=500_000)

    coach_debt_payoff_artifact_save(
        action_plan_payload=_debt_payoff_payload(
            debts_in_scope=[{"id": liability_id, "source": "liability"}],
        ),
        dry_run=False,
    )
    coach_emergency_fund_artifact_save(
        plan_payload=_efund_payload(account_ids=["hysa_003"], target_balance_cents=1_140_000),
        dry_run=False,
    )
    coach_savings_goal_artifact_save(
        plan_payload=_savings_goal_payload(unlock_blocker="both"),
        dry_run=False,
    )

    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is False
    assert result["summary"]["blocker_resolved"] == "none"
    assert result["data"]["evidence"]["debt_cleared"] is True
    assert result["data"]["evidence"]["efund_target_met"] is False


def test_unlock_missing_prerequisite_artifact_is_not_eligible(data_dir: Path) -> None:
    """efund blocker + no efund artifact -> unlock_eligible False with missing record."""
    coach_savings_goal_artifact_save(
        plan_payload=_savings_goal_payload(unlock_blocker="efund", user_decision="starter_then_efund"),
        dry_run=False,
    )
    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is False
    assert result["summary"]["reason"] == "missing_prerequisite_artifact"
    assert "emergency_fund" in result["data"]["evidence"]["missing_prerequisite_artifacts"]


def test_unlock_debt_cleared_below_tolerance_still_counts_as_cleared(data_dir: Path) -> None:
    """A residual $30 balance is within the $50 tolerance and counts as cleared."""
    db_path = data_dir / "finance.db"
    with connect(db_path) as conn:
        _seed_account(conn, account_id="cc_005", account_type="credit_card", balance_cents=0)
        liability_id = _seed_liability(conn, account_id="cc_005", balance_cents=3_000)

    coach_debt_payoff_artifact_save(
        action_plan_payload=_debt_payoff_payload(
            debts_in_scope=[{"id": liability_id, "source": "liability"}],
        ),
        dry_run=False,
    )
    coach_savings_goal_artifact_save(
        plan_payload=_savings_goal_payload(unlock_blocker="debt"),
        dry_run=False,
    )

    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is True
    assert result["data"]["evidence"]["debt_cleared"] is True
    assert result["data"]["evidence"]["debt_in_scope_sum_cents"] == 3_000


def test_unlock_manual_loan_in_scope_counted(data_dir: Path) -> None:
    """Manual loans (source='manual_loan') are summed via the manual_loans table."""
    db_path = data_dir / "finance.db"
    with connect(db_path) as conn:
        loan_id = _seed_manual_loan(conn, balance_cents=0)

    coach_debt_payoff_artifact_save(
        action_plan_payload=_debt_payoff_payload(
            debts_in_scope=[{"id": loan_id, "source": "manual_loan"}],
        ),
        dry_run=False,
    )
    coach_savings_goal_artifact_save(
        plan_payload=_savings_goal_payload(unlock_blocker="debt"),
        dry_run=False,
    )

    result = coach_savings_goal_check_unlock_conditions(savings_goal_artifact_path=None)
    assert result["summary"]["unlock_eligible"] is True
    assert result["data"]["evidence"]["debt_cleared"] is True
    assert result["data"]["evidence"]["debt_in_scope_sum_cents"] == 0
