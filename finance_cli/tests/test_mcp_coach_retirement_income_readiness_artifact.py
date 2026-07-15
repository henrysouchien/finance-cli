"""Save/read tests for ``coach_retirement_income_readiness_artifact_*`` tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_retirement_income_readiness_artifact_read,
    coach_retirement_income_readiness_artifact_save,
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
        "readiness_status": "professional_review_needed",
        "household_timeline": {
            "current_age_band": "60-64",
            "target_retirement_timing": "2028",
            "employment_or_employer_coverage_context": "employer coverage active",
        },
        "income_sources": {
            "social_security_estimate_status": "sourced",
            "pension_status": "needs_plan_document",
            "retirement_account_status": "partial",
            "taxable_account_status": "unknown",
            "annuity_status": "none",
        },
        "health_and_risk_context": {
            "medicare_timing_status": "review_needed",
            "long_term_care_or_disability_context": "unknown",
        },
        "cash_flow_context": {
            "current_essential_monthly_cents": 520_000,
            "target_retirement_spending_cents": None,
            "income_gap_estimate_cents": None,
        },
        "milestones": [
            {
                "name": "social_security_claiming_window",
                "status": "active",
                "source_url": "https://www.ssa.gov/benefits/retirement/planner/agereduction.html",
            }
        ],
        "rmd_context": {
            "relevance": "future",
            "source_metadata": {
                "source_year": 2026,
                "source_url": "https://www.irs.gov/retirement-plans/required-minimum-distributions",
            },
        },
        "professional_handoffs": [
            {
                "type": "fiduciary",
                "trigger": "The user is nearing a retirement income transition.",
                "question_to_ask": "What tradeoffs should I review before implementation decisions?",
            }
        ],
        "boundary_response": {
            "prohibited_request_detected": False,
            "user_request_preserved_for_professional": None,
        },
        "questions_to_ask": [
            "What documents should I bring to a fiduciary review?"
        ],
        "documents_to_gather": [
            "Social Security statement",
            "Pension summary plan description",
        ],
        "data_gaps": ["Target retirement spending is unknown."],
        "next_actions": [
            {
                "label": "Gather current benefit and account statements",
                "owner": "user",
                "status": "open",
            }
        ],
        "scope_notes": [
            "Education and readiness only; no claiming, withdrawal, conversion, annuity, Medicare-plan, tax, legal, or investment recommendation.",
        ],
        "next_check_in": "2026-07-22",
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("rmd_context")
    response = coach_retirement_income_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "rmd_context" in response["message"]


def test_artifact_save_rejects_unknown_readiness_status(data_dir: Path) -> None:
    del data_dir
    response = coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(readiness_status="claiming_ready"),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "readiness_status" in response["message"]


@pytest.mark.parametrize(
    "override",
    [
        {"income_sources": {"recommended_claiming_age": 67}},
        {"next_actions": [{"label": "Schedule transfer", "transfer_schedule": "monthly"}]},
        {"rmd_context": {"relevance": "current", "rmd_amount": 12_000}},
        {"boundary_response": {"prohibited_request_detected": False, "medicare_plan": "Plan A"}},
    ],
)
def test_artifact_save_rejects_recommendation_and_write_fields(
    data_dir: Path,
    override: dict,
) -> None:
    del data_dir
    response = coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(**override),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store retirement-income" in response["message"]


def test_artifact_save_rejects_free_text_implementation_advice(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(
            next_actions=[
                {
                    "label": "Claim Social Security at 62 now",
                    "owner": "user",
                    "status": "open",
                }
            ],
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store retirement-income implementation advice text" in response[
        "message"
    ]


def test_artifact_save_allows_preserved_user_request_for_professional(
    data_dir: Path,
) -> None:
    del data_dir
    result = coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(
            boundary_response={
                "prohibited_request_detected": True,
                "user_request_preserved_for_professional": (
                    "Should I claim Social Security at 62?"
                ),
            },
        ),
        dry_run=True,
    )

    assert result["summary"]["valid"] is True
    assert result["data"]["plan_payload"]["boundary_response"][
        "user_request_preserved_for_professional"
    ] == "Should I claim Social Security at 62?"


def test_artifact_save_requires_handoff_for_prohibited_request(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(
            readiness_status="inventory_ready",
            professional_handoffs=[],
            boundary_response={
                "prohibited_request_detected": True,
                "user_request_preserved_for_professional": (
                    "Should I take the pension lump sum?"
                ),
            },
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "requires at least one professional_handoff" in response["message"]


def test_artifact_save_requires_source_metadata_for_current_rmd(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(
            rmd_context={"relevance": "current", "source_metadata": None},
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "source_year and source_url" in response["message"]


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_retirement_income_readiness_artifact_save(
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
        / "coach_retirement_income_readiness"
        / "20260622.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    artifact_path = (
        data_dir
        / "artifacts"
        / "coach_retirement_income_readiness"
        / "20260622.md"
    )

    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Retirement Income Readiness Plan" in content
    assert "## RMD Context" in content
    assert "## Generated machine-readable footer" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_retirement_income_readiness_artifact_save(
        plan_payload=payload,
        dry_run=False,
    )
    result = coach_retirement_income_readiness_artifact_read(date=None)
    saved = result["data"]["plan_payload"]

    assert saved["readiness_status"] == "professional_review_needed"
    assert saved["income_sources"]["social_security_estimate_status"] == "sourced"
    assert saved["rmd_context"]["source_metadata"]["source_year"] == 2026
    assert saved["professional_handoffs"][0]["type"] == "fiduciary"


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_retirement_income_readiness_artifact_read(date=None)

    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_retirement_income_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )
    second = coach_retirement_income_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_same_day_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    second = coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    assert Path(first["data"]["artifact_path"]).name == "20260622.md"
    assert Path(second["data"]["artifact_path"]).name == "20260622-r2.md"


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    del data_dir
    coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    coach_retirement_income_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    latest = coach_retirement_income_readiness_artifact_read(date="2026-06-22")
    assert latest["data"] is not None
    assert Path(latest["data"]["artifact_path"]).name == "20260622-r2.md"

    r2 = coach_retirement_income_readiness_artifact_read(date="20260622-r2")
    assert r2["data"] is not None
    assert Path(r2["data"]["artifact_path"]).name == "20260622-r2.md"
