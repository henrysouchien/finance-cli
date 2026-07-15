"""Save/read round-trip tests for ``coach_tax_readiness_artifact_*`` MCP tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_tax_readiness_artifact_read,
    coach_tax_readiness_artifact_save,
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
        "generated_at": "2026-02-01T12:00:00Z",
        "tax_year": 2026,
        "profile": {
            "filing_status_assumption": "single",
            "income_types": ["w2", "schedule_c"],
            "has_business_activity": True,
            "has_contractor_payments": False,
        },
        "preparation_route": {
            "route": "credentialed_preparer",
            "rationale": "Schedule C activity plus state filing question.",
            "referrals": ["tax.tax-preparation-options"],
        },
        "document_checklist": [
            {
                "item": "W-2",
                "status": "needed",
                "owner": "user",
                "notes": "Expected by late January.",
            },
            {
                "item": "Schedule C expense summary",
                "status": "ready",
                "owner": "cashnerd",
            },
        ],
        "business_readiness": {
            "schedule_c_map_present": True,
            "estimated_tax_reviewed": True,
        },
        "withholding_plan": {
            "action": "Use referrals.irs-tax-withholding-estimator after W-2 arrives.",
        },
        "estimated_tax_plan": {
            "action": "Review biz_estimated_tax after Q1 closes.",
        },
        "risk_flags": [
            {"flag": "state_filing_question", "route": "credentialed_preparer"},
        ],
        "referrals": [
            {
                "referral_id": "referrals.irs-tax-withholding-estimator",
                "reason": "W-4 modeling belongs in the IRS estimator.",
            }
        ],
        "next_actions": [
            {
                "action": "Upload or gather W-2",
                "owner": "user",
                "due": "2026-02-15",
                "status": "open",
            }
        ],
        "next_check_in": "2026-02-15",
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("preparation_route")
    response = coach_tax_readiness_artifact_save(plan_payload=payload, dry_run=True)

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "preparation_route" in response["message"]


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_tax_readiness_artifact_save(plan_payload=_valid_payload(), dry_run=True)
    artifact_path = Path(result["data"]["artifact_path"])
    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == data_dir / "artifacts" / "coach_tax_readiness" / "20260201.md"
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_tax_readiness_artifact_save(plan_payload=_valid_payload(), dry_run=False)
    artifact_path = data_dir / "artifacts" / "coach_tax_readiness" / "20260201.md"
    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Tax Readiness Plan" in content
    assert "## Document Checklist" in content
    assert "## Business Readiness" in content
    assert "## Generated machine-readable footer" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_tax_readiness_artifact_save(plan_payload=payload, dry_run=False)
    result = coach_tax_readiness_artifact_read(date=None)
    saved = result["data"]["plan_payload"]
    assert saved["tax_year"] == payload["tax_year"]
    assert saved["preparation_route"]["route"] == "credentialed_preparer"
    assert saved["document_checklist"][0]["item"] == "W-2"
    assert saved["next_actions"][0]["action"] == "Upload or gather W-2"


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_tax_readiness_artifact_read(date=None)
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_artifact_save_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_tax_readiness_artifact_save(plan_payload=dict(payload), dry_run=False)
    second = coach_tax_readiness_artifact_save(plan_payload=dict(payload), dry_run=False)
    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert first["data"]["generated_at"] == second["data"]["generated_at"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_artifact_save_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_tax_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-02-01T10:00:00Z"),
        dry_run=False,
    )
    second = coach_tax_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-02-01T15:00:00Z"),
        dry_run=False,
    )
    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    first_path = data_dir / "artifacts" / "coach_tax_readiness" / "20260201.md"
    second_path = data_dir / "artifacts" / "coach_tax_readiness" / "20260201-r2.md"
    assert Path(first["data"]["artifact_path"]) == first_path
    assert Path(second["data"]["artifact_path"]) == second_path
    assert first_path.exists() and second_path.exists()


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    coach_tax_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-02-01T10:00:00Z"),
        dry_run=False,
    )
    coach_tax_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-02-01T15:00:00Z"),
        dry_run=False,
    )
    base_result = coach_tax_readiness_artifact_read(date="2026-02-01")
    assert base_result["data"] is not None
    assert Path(base_result["data"]["artifact_path"]).name == "20260201-r2.md"

    r2_result = coach_tax_readiness_artifact_read(date="20260201-r2")
    assert r2_result["data"] is not None
    assert Path(r2_result["data"]["artifact_path"]).name == "20260201-r2.md"


def test_artifact_read_no_match_for_missing_revision(data_dir: Path) -> None:
    coach_tax_readiness_artifact_save(plan_payload=_valid_payload(), dry_run=False)
    result = coach_tax_readiness_artifact_read(date="20260201-r5")
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] == "no_artifact_for_date"
