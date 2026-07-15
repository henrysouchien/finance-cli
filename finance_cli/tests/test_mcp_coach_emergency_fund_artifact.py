from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_emergency_fund_artifact_read,
    coach_emergency_fund_artifact_save,
    spending_essential_monthly,
)
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


@pytest.fixture()
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        yield tmp_path
    finally:
        reset_user_context(token)


def _valid_payload(**overrides) -> dict:
    payload = {
        "generated_at": "2026-06-07T12:00:00Z",
        "smart_goal": "Build a 3-month emergency fund by 2027-06.",
        "target_phase": "full",
        "target_balance_cents": 1_900_000,
        "monthly_commitment_cents": 50_000,
        "essential_monthly_expenses_cents": 380_000,
        "target_multiplier_months": 5.0,
        "account_ids_in_fund": ["acct_001"],
        "tier_balances_target": [
            {"account_id": "acct_001", "target_balance_cents": 1_900_000, "role": "buffer"},
        ],
        "action_steps": [
            {"step": "Open HYSA at any FDIC-insured online bank.", "timeline": "2026-06-15", "quick_win": True},
            {"step": "Set up paycheck split.", "timeline": "2026-06-22"},
        ],
        "drawdown_rules_user_defined": "Real emergency = job loss, medical bill > $500, urgent car repair.",
        "replenishment_commitment": "Pause new spending until rebuilt to pre-drawdown level.",
        "milestones": [{"name": "starter_$1000", "target_balance_cents": 100_000}],
        "monitoring_cadence": "monthly",
        "next_check_in": "2026-07-07",
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("smart_goal")
    response = coach_emergency_fund_artifact_save(plan_payload=payload, dry_run=True)

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "smart_goal" in response["message"]


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_emergency_fund_artifact_save(plan_payload=_valid_payload(), dry_run=True)
    artifact_path = Path(result["data"]["artifact_path"])
    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == data_dir / "artifacts" / "coach_emergency_fund" / "20260607.md"
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_emergency_fund_artifact_save(plan_payload=_valid_payload(), dry_run=False)
    artifact_path = data_dir / "artifacts" / "coach_emergency_fund" / "20260607.md"
    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Emergency Fund Plan" in content
    assert "## Drawdown Rules (User-Defined)" in content
    assert "## Account Configuration" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_emergency_fund_artifact_save(plan_payload=payload, dry_run=False)
    result = coach_emergency_fund_artifact_read(date=None)
    saved = result["data"]["plan_payload"]
    assert saved["smart_goal"] == payload["smart_goal"]
    assert saved["target_phase"] == payload["target_phase"]
    assert saved["target_balance_cents"] == payload["target_balance_cents"]
    assert saved["account_ids_in_fund"] == payload["account_ids_in_fund"]
    assert saved["drawdown_rules_user_defined"] == payload["drawdown_rules_user_defined"]


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_emergency_fund_artifact_read(date=None)
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_artifact_save_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_emergency_fund_artifact_save(plan_payload=dict(payload), dry_run=False)
    second = coach_emergency_fund_artifact_save(plan_payload=dict(payload), dry_run=False)
    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    # generated_at is preserved across update_in_place; last_modified_at bumps
    # to real time (which differs from the test payload's future-dated
    # generated_at, so we only check identity / movement rather than ordering).
    assert first["data"]["generated_at"] == second["data"]["generated_at"]
    assert first["data"]["last_modified_at"] == first["data"]["generated_at"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_artifact_save_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_emergency_fund_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-07T10:00:00Z"),
        dry_run=False,
    )
    second = coach_emergency_fund_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-07T15:00:00Z"),
        dry_run=False,
    )
    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    first_path = data_dir / "artifacts" / "coach_emergency_fund" / "20260607.md"
    second_path = data_dir / "artifacts" / "coach_emergency_fund" / "20260607-r2.md"
    assert Path(first["data"]["artifact_path"]) == first_path
    assert Path(second["data"]["artifact_path"]) == second_path
    assert first_path.exists() and second_path.exists()


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    coach_emergency_fund_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-07T10:00:00Z"),
        dry_run=False,
    )
    coach_emergency_fund_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-07T15:00:00Z"),
        dry_run=False,
    )
    base_result = coach_emergency_fund_artifact_read(date="2026-06-07")
    assert base_result["data"] is not None
    assert Path(base_result["data"]["artifact_path"]).name == "20260607-r2.md"

    r2_result = coach_emergency_fund_artifact_read(date="20260607-r2")
    assert r2_result["data"] is not None
    assert Path(r2_result["data"]["artifact_path"]).name == "20260607-r2.md"

    base_only = coach_emergency_fund_artifact_read(date="20260607")
    # Bare date returns latest revision.
    assert Path(base_only["data"]["artifact_path"]).name == "20260607-r2.md"


def test_artifact_read_no_match_for_missing_revision(data_dir: Path) -> None:
    coach_emergency_fund_artifact_save(plan_payload=_valid_payload(), dry_run=False)
    result = coach_emergency_fund_artifact_read(date="20260607-r5")
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] == "no_artifact_for_date"


def test_spending_essential_monthly_returns_zeros_when_no_data(data_dir: Path) -> None:
    """Smoke test the read-only spending tool against an empty user DB."""
    del data_dir
    # Initialize the DB schema so the underlying query doesn't blow up.
    from finance_cli.db import connect, initialize_database
    import os

    initialize_database(Path(os.environ["FINANCE_CLI_DB"]))
    with connect(Path(os.environ["FINANCE_CLI_DB"])):
        pass

    result = spending_essential_monthly(months=3)
    assert result["data"]["essential_monthly_cents"] == 0
    assert result["data"]["discretionary_monthly_cents"] == 0
    assert result["data"]["months_in_window"] == 3
    assert isinstance(result["data"]["essential_categories"], list)
    assert isinstance(result["data"]["breakdown"], list)


def test_spending_essential_monthly_rejects_zero_months(data_dir: Path) -> None:
    del data_dir
    response = spending_essential_monthly(months=0)

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "months" in response["message"]
