"""Save/read tests for ``coach_financial_plan_intake_artifact_*`` tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_financial_plan_intake_artifact_read,
    coach_financial_plan_intake_artifact_save,
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
        "snapshot_status": "complete",
        "household_context": {
            "household_type": "single",
            "dependents_count": 0,
            "employment_context": "salaried",
            "notes": ["User wants a cross-domain planning sequence."],
        },
        "goals": [
            {
                "goal_id": "goal-debt",
                "name": "Pay down debt",
                "time_horizon": "short",
                "priority": "high",
                "source": "user",
                "notes": "User wants breathing room before investing.",
            }
        ],
        "assets_liabilities": {
            "liquid_cash_cents": 250_000,
            "investment_balance_cents": 0,
            "retirement_balance_cents": 800_000,
            "debt_total_cents": 420_000,
            "high_interest_debt_cents": 420_000,
        },
        "cash_flow": {
            "monthly_income_cents": 500_000,
            "essential_expenses_cents": 320_000,
            "monthly_surplus_capacity_cents": 80_000,
            "volatility_notes": [],
        },
        "domain_readiness": {
            "debt": "active_plan",
            "emergency_fund": "data_needed",
            "investment": "fix_first",
            "retirement": "ready",
            "tax": "data_needed",
            "insurance": "data_needed",
            "estate": "data_needed",
        },
        "sibling_artifacts": [
            {
                "skill": "coach_debt_payoff",
                "latest_date": "2026-06-01",
                "summary": "Debt payoff plan exists.",
            }
        ],
        "planning_sequence": [
            {
                "next_skill": "coach_emergency_fund",
                "rationale": "Emergency reserve facts are needed before investing.",
                "status": "recommended",
            }
        ],
        "professional_handoffs": [
            {
                "type": "none",
                "reason": None,
                "status": "not_needed",
            }
        ],
        "data_gaps": ["Confirm monthly essential expenses over the last 90 days."],
        "monitoring": {
            "next_review_date": "2026-07-22",
        },
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("cash_flow")
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "cash_flow" in response["message"]


def test_artifact_save_rejects_unknown_snapshot_status(data_dir: Path) -> None:
    del data_dir
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(snapshot_status="portfolio_ready"),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "snapshot_status" in response["message"]


def test_artifact_save_requires_sequence_unless_data_needed(data_dir: Path) -> None:
    del data_dir
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(planning_sequence=[]),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "planning_sequence" in response["message"]


def test_artifact_save_rejects_non_dict_sequence_entries(data_dir: Path) -> None:
    del data_dir
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(planning_sequence=["coach_debt_payoff"]),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "planning_sequence[0] must be a dict" in response["message"]


def test_artifact_save_allows_empty_sequence_when_data_needed(data_dir: Path) -> None:
    result = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(
            snapshot_status="data_needed",
            planning_sequence=[],
        ),
        dry_run=True,
    )

    assert result["summary"]["valid"] is True
    assert result["summary"]["saved"] is False


def test_artifact_save_requires_handoff_reason(data_dir: Path) -> None:
    del data_dir
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(
            professional_handoffs=[
                {
                    "type": "ria",
                    "reason": "",
                    "status": "recommended",
                }
            ],
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "professional_handoffs[0].reason" in response["message"]


def test_artifact_save_rejects_unknown_handoff_type(data_dir: Path) -> None:
    del data_dir
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(
            professional_handoffs=[
                {
                    "type": "portfolio_manager",
                    "reason": "User wants securities advice.",
                    "status": "recommended",
                }
            ],
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "professional_handoffs[0].type" in response["message"]


def test_artifact_save_rejects_unknown_domain_status(data_dir: Path) -> None:
    del data_dir
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(
            domain_readiness={"debt": "maybe", "investment": "ready"}
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "domain_readiness.debt" in response["message"]


def test_artifact_save_rejects_security_selection_fields(data_dir: Path) -> None:
    del data_dir
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(domain_readiness={"selected_security": "VOO"}),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store securities" in response["message"]


def test_artifact_save_rejects_tax_filing_recommendation_fields(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(
            domain_readiness={"tax_filing_position": "claim_credit"}
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "tax filing" in response["message"]


def test_artifact_save_rejects_legal_document_text_fields(data_dir: Path) -> None:
    del data_dir
    response = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(
            sibling_artifacts=[
                {
                    "skill": "coach_estate_document_readiness",
                    "legal_document_text": "I leave everything to...",
                }
            ]
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "legal" in response["message"]


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=True,
    )
    artifact_path = Path(result["data"]["artifact_path"])

    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == (
        data_dir / "artifacts" / "coach_financial_plan_intake" / "20260622.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    artifact_path = (
        data_dir / "artifacts" / "coach_financial_plan_intake" / "20260622.md"
    )

    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Financial Planning Snapshot" in content
    assert "## Planning Sequence" in content
    assert "## Generated machine-readable footer" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_financial_plan_intake_artifact_save(plan_payload=payload, dry_run=False)
    result = coach_financial_plan_intake_artifact_read(date=None)
    saved = result["data"]["plan_payload"]

    assert saved["snapshot_status"] == "complete"
    assert saved["planning_sequence"][0]["next_skill"] == "coach_emergency_fund"
    assert saved["cash_flow"]["monthly_surplus_capacity_cents"] == 80_000
    assert saved["monitoring"]["next_review_date"] == "2026-07-22"


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_financial_plan_intake_artifact_read(date=None)

    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_financial_plan_intake_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )
    second = coach_financial_plan_intake_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_same_day_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    second = coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    assert Path(first["data"]["artifact_path"]).name == "20260622.md"
    assert Path(second["data"]["artifact_path"]).name == "20260622-r2.md"


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    coach_financial_plan_intake_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    latest = coach_financial_plan_intake_artifact_read(date="2026-06-22")
    assert latest["data"] is not None
    assert Path(latest["data"]["artifact_path"]).name == "20260622-r2.md"

    r2 = coach_financial_plan_intake_artifact_read(date="20260622-r2")
    assert r2["data"] is not None
    assert Path(r2["data"]["artifact_path"]).name == "20260622-r2.md"
