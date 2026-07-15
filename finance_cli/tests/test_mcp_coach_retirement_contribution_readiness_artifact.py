"""Save/read tests for ``coach_retirement_contribution_readiness_artifact_*`` tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_retirement_contribution_readiness_artifact_read,
    coach_retirement_contribution_readiness_artifact_save,
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
        "generated_at": "2026-06-22T12:00:00Z",
        "tax_year": 2026,
        "readiness_status": "contribution_ready",
        "household_profile": {
            "filing_status": "single",
            "age_by_tax_year_end": 40,
            "annual_salary_cents": 12_000_000,
            "taxable_income_cents": 9_500_000,
            "modified_agi_cents": 12_000_000,
            "earned_compensation_cents": 12_000_000,
            "input_quality_notes": [],
        },
        "cash_flow_context": {
            "monthly_surplus_capacity_cents": 80_000,
            "essential_monthly_expenses_cents": 420_000,
            "emergency_fund_months": 3.2,
            "high_interest_debt_cents": 0,
            "high_interest_apr_pct": 0.0,
            "existing_commitments_cents": 0,
        },
        "employer_plan_context": {
            "has_workplace_plan": True,
            "employer_match_rate_pct": 50.0,
            "employer_match_limit_pct": 6.0,
            "employee_contributed_ytd_cents": 300_000,
            "plan_notes": [],
        },
        "hsa_context": {
            "hsa_eligible_hdhp": False,
            "family_coverage": False,
            "contributed_ytd_cents": 0,
        },
        "ira_context": {
            "other_ira_contributions_cents": 0,
            "roth_room_cents": 750_000,
        },
        "priority_result": {
            "helper": "advisory_contribution_priority",
            "source_tax_year": 2026,
            "supported_tax_years": [2025, 2026],
            "limits_source": {
                "retirement_limits": "IRS Notice 2025-67",
                "hsa_limits": "IRS Rev. Proc. 2025-19",
                "roth_ira_worksheet": "IRS Pub. 590-A Worksheet 2-2",
            },
            "unsupported_year": False,
            "data_needed": [],
            "steps": [
                {
                    "order": 1,
                    "account": "workplace_plan_match",
                    "action": "Capture employer match",
                    "annual_amount_cents": 720_000,
                    "monthly_equivalent_cents": 60_000,
                    "priority_rank": "P1_high",
                    "reason": "Employer match is available.",
                }
            ],
        },
        "selected_commitment": {
            "account_type": "workplace_plan_match",
            "monthly_target_cents": 60_000,
            "start_month": "2026-07",
            "end_month": "2026-12",
            "room_remaining_cents": 360_000,
            "write_tool": "setup_monthly_transfer_goal",
            "write_status": "not_requested",
        },
        "readiness_flags": [],
        "cross_skill_context": {
            "debt_payoff_artifact": "absent",
            "emergency_fund_artifact": "present",
            "savings_goal_artifact": "absent",
            "spending_plan_artifact": "present",
        },
        "next_actions": [
            {
                "action": "Confirm payroll match formula before target write",
                "owner": "user",
                "due": "2026-07-01",
                "status": "open",
            }
        ],
        "referrals": [],
        "scope_notes": [
            "Education only; no securities, fund, or ERISA plan interpretation.",
        ],
        "next_check_in": "2026-07-22",
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("cash_flow_context")
    response = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "cash_flow_context" in response["message"]


def test_artifact_save_rejects_unknown_readiness_status(data_dir: Path) -> None:
    del data_dir
    response = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(readiness_status="invest_now"),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "readiness_status" in response["message"]


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=True,
    )
    artifact_path = Path(result["data"]["artifact_path"])
    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == (
        data_dir
        / "artifacts"
        / "coach_retirement_contribution_readiness"
        / "20260622.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    artifact_path = (
        data_dir
        / "artifacts"
        / "coach_retirement_contribution_readiness"
        / "20260622.md"
    )
    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Retirement Contribution Readiness Plan" in content
    assert "## Priority Result" in content
    assert "## Generated machine-readable footer" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_retirement_contribution_readiness_artifact_save(
        plan_payload=payload,
        dry_run=False,
    )
    result = coach_retirement_contribution_readiness_artifact_read(date=None)
    saved = result["data"]["plan_payload"]
    assert saved["readiness_status"] == "contribution_ready"
    assert saved["tax_year"] == 2026
    assert saved["priority_result"]["helper"] == "advisory_contribution_priority"
    assert saved["selected_commitment"]["write_status"] == "not_requested"


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_retirement_contribution_readiness_artifact_read(date=None)
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )
    second = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_same_day_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    second = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    assert Path(first["data"]["artifact_path"]).name == "20260622.md"
    assert Path(second["data"]["artifact_path"]).name == "20260622-r2.md"


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    latest = coach_retirement_contribution_readiness_artifact_read(date="2026-06-22")
    assert latest["data"] is not None
    assert Path(latest["data"]["artifact_path"]).name == "20260622-r2.md"

    r2 = coach_retirement_contribution_readiness_artifact_read(date="20260622-r2")
    assert r2["data"] is not None
    assert Path(r2["data"]["artifact_path"]).name == "20260622-r2.md"


def test_artifact_read_no_match_for_missing_revision(data_dir: Path) -> None:
    del data_dir
    coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    result = coach_retirement_contribution_readiness_artifact_read(date="20260622-r5")
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] == "no_artifact_for_date"


def test_unsupported_tax_year_must_be_data_needed(data_dir: Path) -> None:
    del data_dir
    priority = {
        **_valid_payload()["priority_result"],
        "source_tax_year": 2027,
        "supported_tax_years": [2025, 2026],
        "limits_source": {},
        "unsupported_year": True,
        "data_needed": ["Unsupported tax year; use provider payroll figures."],
        "steps": [],
    }
    response = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(
            tax_year=2027,
            readiness_status="contribution_ready",
            priority_result=priority,
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "readiness_status=data_needed" in response["message"]


def test_unsupported_tax_year_requires_structured_helper_note(data_dir: Path) -> None:
    del data_dir
    priority = {
        **_valid_payload()["priority_result"],
        "source_tax_year": 2027,
        "supported_tax_years": [2025, 2026],
        "limits_source": {},
        "unsupported_year": False,
        "data_needed": [],
        "steps": [],
    }
    response = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(
            tax_year=2027,
            readiness_status="data_needed",
            priority_result=priority,
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "priority_result.unsupported_year=True" in response["message"]


def test_unsupported_tax_year_data_needed_payload_is_valid(data_dir: Path) -> None:
    priority = {
        **_valid_payload()["priority_result"],
        "source_tax_year": 2027,
        "supported_tax_years": [2025, 2026],
        "limits_source": {},
        "unsupported_year": True,
        "data_needed": ["Unsupported tax year; use provider payroll figures."],
        "steps": [],
    }
    result = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(
            tax_year=2027,
            readiness_status="data_needed",
            priority_result=priority,
            readiness_flags=["unsupported_tax_year"],
        ),
        dry_run=True,
    )

    assert result["summary"]["valid"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["plan_payload"]["readiness_status"] == "data_needed"


def test_supported_tax_year_cannot_claim_unsupported_helper_result(data_dir: Path) -> None:
    del data_dir
    priority = {
        **_valid_payload()["priority_result"],
        "unsupported_year": True,
        "data_needed": ["Should not be set for supported 2026 helper output."],
    }
    response = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=_valid_payload(priority_result=priority),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "cannot mark priority_result.unsupported_year=True" in response["message"]


def test_user_confirmed_written_requires_target_write_evidence(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload["selected_commitment"] = {
        **payload["selected_commitment"],
        "write_status": "user_confirmed_written",
    }
    response = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "write_result evidence" in response["message"]


def test_user_confirmed_written_with_target_write_evidence_is_valid(
    data_dir: Path,
) -> None:
    payload = _valid_payload()
    payload["selected_commitment"] = {
        **payload["selected_commitment"],
        "write_status": "user_confirmed_written",
        "write_result": {
            "tool_name": "setup_monthly_transfer_goal",
            "success": True,
        },
    }
    result = coach_retirement_contribution_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert result["summary"]["valid"] is True
    assert result["data"]["plan_payload"]["selected_commitment"]["write_result"][
        "tool_name"
    ] == "setup_monthly_transfer_goal"
