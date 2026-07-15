"""Save/read tests for ``coach_homebuying_readiness_artifact_*`` MCP tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_homebuying_readiness_artifact_read,
    coach_homebuying_readiness_artifact_save,
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
        "generated_at": "2026-06-21T12:00:00Z",
        "household_profile": {
            "buyer_type": "first_time",
            "timeline": "3_12_months",
            "gross_monthly_income_cents": 900_000,
            "current_rent_cents": 240_000,
            "target_area": "Durham, NC",
            "household_notes": ["wants payment stability"],
        },
        "affordability_scenarios": [
            {
                "scenario_id": "baseline",
                "home_price_cents": 42_000_000,
                "down_payment_cents": 4_200_000,
                "loan_amount_cents": 37_800_000,
                "rate_assumption": {
                    "value_pct": 6.75,
                    "source": "user_provided",
                    "as_of": "2026-06-21",
                },
                "term_years": 30,
                "monthly_principal_interest_cents": 245_200,
                "property_tax_monthly_cents": 50_000,
                "insurance_monthly_cents": 18_000,
                "hoa_monthly_cents": 0,
                "pmi_monthly_cents": 22_000,
                "maintenance_reserve_monthly_cents": 35_000,
                "monthly_housing_payment_cents": 335_200,
                "monthly_homeownership_cost_cents": 370_200,
            }
        ],
        "cash_to_close": {
            "down_payment_cents": 4_200_000,
            "closing_cost_estimate_cents": 1_260_000,
            "moving_cost_estimate_cents": 250_000,
            "cash_to_close_total_cents": 5_710_000,
            "liquid_cash_cents": 6_800_000,
            "reserve_after_close_cents": 1_090_000,
            "reserve_target_cents": 1_800_000,
            "reserve_gap_cents": 710_000,
        },
        "ratios": {
            "front_end_ratio_pct": 37.2,
            "back_end_ratio_pct": 45.7,
            "full_homeownership_cost_ratio_pct": 41.1,
            "other_monthly_debt_payments_cents": 76_000,
            "ratio_notes": ["Ratios use user-provided gross income."],
        },
        "credit_readiness": {
            "user_reported_score_band": "700_739",
            "card_utilization_flags": [],
            "report_review_status": "not_started",
            "hard_inquiry_notes": [],
        },
        "readiness_status": "fix_first",
        "readiness_flags": ["reserve_gap"],
        "cross_skill_context": {
            "debt_payoff_artifact": "present",
            "emergency_fund_artifact": "present",
            "savings_goal_artifact": "absent",
            "spending_plan_artifact": "present",
        },
        "preapproval_checklist": [
            "Review all three credit reports",
            "Collect income documents",
        ],
        "next_actions": [
            {
                "action": "Build reserve gap before preapproval",
                "owner": "user",
                "due": "2026-09-01",
                "status": "open",
            }
        ],
        "referrals": [
            {
                "referral_id": "referrals.hud-approved-housing-counselor",
                "reason": "Independent pre-purchase counseling.",
            }
        ],
        "scope_notes": [
            "This plan does not guarantee mortgage approval.",
        ],
        "next_check_in": "2026-09-01",
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("cash_to_close")
    response = coach_homebuying_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "cash_to_close" in response["message"]


def test_artifact_save_requires_at_least_one_scenario(data_dir: Path) -> None:
    del data_dir
    response = coach_homebuying_readiness_artifact_save(
        plan_payload=_valid_payload(affordability_scenarios=[]),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "at least one scenario" in response["message"]


def test_artifact_save_rejects_missing_ratios_when_gross_income_known(
    data_dir: Path,
) -> None:
    del data_dir
    payload = _valid_payload()
    payload["ratios"] = {
        "other_monthly_debt_payments_cents": 76_000,
        "ratio_notes": ["Missing for test"],
    }
    response = coach_homebuying_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "gross income is unknown" in response["message"]


def test_artifact_save_allows_missing_ratios_when_income_unknown_with_note(
    data_dir: Path,
) -> None:
    payload = _valid_payload()
    payload["household_profile"] = {
        **payload["household_profile"],
        "gross_monthly_income_cents": None,
    }
    payload["ratios"] = {
        "other_monthly_debt_payments_cents": 76_000,
        "ratio_notes": ["DTI omitted because gross income was not provided."],
    }
    result = coach_homebuying_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    artifact_path = Path(result["data"]["artifact_path"])
    assert result["summary"]["valid"] is True
    assert result["summary"]["saved"] is False
    assert artifact_path == (
        data_dir / "artifacts" / "coach_homebuying_readiness" / "20260621.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_homebuying_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=True,
    )
    artifact_path = Path(result["data"]["artifact_path"])
    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == (
        data_dir / "artifacts" / "coach_homebuying_readiness" / "20260621.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_homebuying_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    artifact_path = data_dir / "artifacts" / "coach_homebuying_readiness" / "20260621.md"
    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Homebuying Readiness Plan" in content
    assert "## Affordability Scenarios" in content
    assert "## Cash to Close" in content
    assert "## Generated machine-readable footer" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_homebuying_readiness_artifact_save(plan_payload=payload, dry_run=False)
    result = coach_homebuying_readiness_artifact_read(date=None)
    saved = result["data"]["plan_payload"]
    assert saved["readiness_status"] == payload["readiness_status"]
    assert saved["household_profile"]["timeline"] == "3_12_months"
    assert saved["affordability_scenarios"][0]["scenario_id"] == "baseline"
    assert saved["cash_to_close"]["reserve_gap_cents"] == 710_000


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_homebuying_readiness_artifact_read(date=None)
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_artifact_save_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_homebuying_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )
    second = coach_homebuying_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )
    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert first["data"]["generated_at"] == second["data"]["generated_at"]
    assert first["data"]["last_modified_at"] == first["data"]["generated_at"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_artifact_save_different_generated_at_writes_new_revision(
    data_dir: Path,
) -> None:
    first = coach_homebuying_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-21T10:00:00Z"),
        dry_run=False,
    )
    second = coach_homebuying_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-21T15:00:00Z"),
        dry_run=False,
    )
    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    first_path = data_dir / "artifacts" / "coach_homebuying_readiness" / "20260621.md"
    second_path = (
        data_dir / "artifacts" / "coach_homebuying_readiness" / "20260621-r2.md"
    )
    assert Path(first["data"]["artifact_path"]) == first_path
    assert Path(second["data"]["artifact_path"]) == second_path
    assert first_path.exists() and second_path.exists()


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    coach_homebuying_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-21T10:00:00Z"),
        dry_run=False,
    )
    coach_homebuying_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-21T15:00:00Z"),
        dry_run=False,
    )
    base_result = coach_homebuying_readiness_artifact_read(date="2026-06-21")
    assert base_result["data"] is not None
    assert Path(base_result["data"]["artifact_path"]).name == "20260621-r2.md"

    r2_result = coach_homebuying_readiness_artifact_read(date="20260621-r2")
    assert r2_result["data"] is not None
    assert Path(r2_result["data"]["artifact_path"]).name == "20260621-r2.md"


def test_artifact_read_no_match_for_missing_revision(data_dir: Path) -> None:
    coach_homebuying_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    result = coach_homebuying_readiness_artifact_read(date="20260621-r5")
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] == "no_artifact_for_date"
