"""Save/read tests for ``coach_risk_insurance_readiness_artifact_*`` tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_risk_insurance_readiness_artifact_read,
    coach_risk_insurance_readiness_artifact_save,
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
        "readiness_status": "review_recommended",
        "household_context": {
            "dependents_count": 2,
            "homeowner": "yes",
            "vehicle_owner": "yes",
            "self_employed": "unknown",
            "employer_benefits_available": "yes",
        },
        "liquidity_context": {
            "emergency_fund_months": 2.4,
            "essential_monthly_expenses_cents": 420_000,
        },
        "coverage_inventory": {
            "health": {
                "known": True,
                "deductible_cents": 200_000,
                "out_of_pocket_max_cents": 850_000,
            },
            "disability": {
                "known": False,
                "employer_coverage": "unknown",
            },
            "life": {
                "known": True,
                "beneficiary_review_needed": "unknown",
            },
            "property_liability": {
                "known": True,
                "homeowners_or_renters": "homeowners",
                "auto": "yes",
            },
        },
        "risk_flags": [
            {
                "flag_id": "missing_disability_income_context",
                "severity": "medium",
                "rationale": "Employer disability benefit is unknown.",
            }
        ],
        "professional_handoffs": [
            {
                "type": "insurance_agent",
                "reason": "Review disability and property/liability coverage details.",
            }
        ],
        "planning_implications": [
            "Pause aggressive investing until disability income context is known."
        ],
        "data_gaps": ["Confirm disability-income benefit period."],
        "next_actions": [
            {
                "label": "Find benefits summary",
                "owner": "user",
                "due": "2026-07-01",
                "status": "open",
            }
        ],
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("coverage_inventory")
    response = coach_risk_insurance_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "coverage_inventory" in response["message"]


def test_artifact_save_rejects_unknown_readiness_status(data_dir: Path) -> None:
    del data_dir
    response = coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(readiness_status="policy_ready"),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "readiness_status" in response["message"]


def test_artifact_save_refer_requires_handoff_reason(data_dir: Path) -> None:
    del data_dir
    response = coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(
            readiness_status="refer",
            professional_handoffs=[{"type": "none", "reason": ""}],
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "readiness_status=refer" in response["message"]


@pytest.mark.parametrize(
    "override",
    [
        {"coverage_inventory": {"recommended_policy": "Replace existing coverage"}},
        {
            "coverage_inventory": {
                "health": {
                    "known": True,
                    "coverage_amount_recommendation": 500_000,
                }
            }
        },
        {"next_actions": [{"label": "Appeal claim", "claim_strategy": "appeal"}]},
        {
            "professional_handoffs": [
                {
                    "type": "attorney",
                    "reason": "Review documents.",
                    "legal_advice": "Sue the carrier.",
                }
            ]
        },
        {
            "risk_flags": [
                {
                    "flag_id": "underwriting",
                    "severity": "low",
                    "underwriting_recommendation": "Use preferred class.",
                }
            ]
        },
        {
            "next_actions": [
                {
                    "label": "Buy product",
                    "insurance_product_recommendation": "Specific rider.",
                }
            ]
        },
    ],
)
def test_artifact_save_rejects_policy_and_claim_recommendation_fields(
    data_dir: Path,
    override: dict,
) -> None:
    del data_dir
    response = coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(**override),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store insurance policy" in response["message"]


def test_artifact_save_rejects_free_text_insurance_advice(data_dir: Path) -> None:
    del data_dir
    response = coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(
            next_actions=[
                {
                    "label": "Buy $750k of term life from Acme Insurance",
                    "owner": "user",
                    "due": "2026-07-01",
                    "status": "open",
                }
            ],
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store insurance advice text" in response["message"]


def test_artifact_save_rejects_unknown_flag_severity(data_dir: Path) -> None:
    del data_dir
    response = coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(
            risk_flags=[
                {
                    "flag_id": "unknown",
                    "severity": "critical",
                    "rationale": "Severity is outside the contract.",
                }
            ]
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "risk_flags[0].severity" in response["message"]


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=True,
    )
    artifact_path = Path(result["data"]["artifact_path"])

    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == (
        data_dir / "artifacts" / "coach_risk_insurance_readiness" / "20260622.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    artifact_path = (
        data_dir / "artifacts" / "coach_risk_insurance_readiness" / "20260622.md"
    )

    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Risk and Insurance Readiness Plan" in content
    assert "## Coverage Inventory" in content
    assert "## Generated machine-readable footer" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_risk_insurance_readiness_artifact_save(
        plan_payload=payload,
        dry_run=False,
    )
    result = coach_risk_insurance_readiness_artifact_read(date=None)
    saved = result["data"]["plan_payload"]

    assert saved["readiness_status"] == "review_recommended"
    assert saved["coverage_inventory"]["health"]["deductible_cents"] == 200_000
    assert saved["risk_flags"][0]["flag_id"] == "missing_disability_income_context"
    assert saved["next_actions"][0]["label"] == "Find benefits summary"


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_risk_insurance_readiness_artifact_read(date=None)

    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_risk_insurance_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )
    second = coach_risk_insurance_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_same_day_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    second = coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    assert Path(first["data"]["artifact_path"]).name == "20260622.md"
    assert Path(second["data"]["artifact_path"]).name == "20260622-r2.md"


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    del data_dir
    coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    coach_risk_insurance_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    latest = coach_risk_insurance_readiness_artifact_read(date="2026-06-22")
    assert latest["data"] is not None
    assert Path(latest["data"]["artifact_path"]).name == "20260622-r2.md"

    r2 = coach_risk_insurance_readiness_artifact_read(date="20260622-r2")
    assert r2["data"] is not None
    assert Path(r2["data"]["artifact_path"]).name == "20260622-r2.md"
