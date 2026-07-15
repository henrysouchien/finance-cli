"""Save/read tests for ``coach_estate_document_readiness_artifact_*`` tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_estate_document_readiness_artifact_read,
    coach_estate_document_readiness_artifact_save,
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


def _document_inventory(**overrides) -> dict:
    inventory = {
        "will": {
            "status": "unknown",
            "last_reviewed": None,
            "notes": "User has not checked whether a will exists.",
        },
        "financial_power_of_attorney": {
            "status": "unknown",
            "last_reviewed": None,
            "notes": "",
        },
        "healthcare_proxy_or_medical_poa": {
            "status": "unknown",
            "last_reviewed": None,
            "notes": "",
        },
        "advance_directive_or_living_will": {
            "status": "unknown",
            "last_reviewed": None,
            "notes": "",
        },
        "hipaa_release": {
            "status": "unknown",
            "last_reviewed": None,
            "notes": "",
        },
        "trust": {
            "status": "not_applicable",
            "last_reviewed": None,
            "notes": "No trust identified in intake.",
        },
        "guardianship_nomination": {
            "status": "unknown",
            "last_reviewed": None,
            "notes": "",
        },
        "beneficiary_designations": {
            "status": "stale",
            "last_reviewed": "2021",
            "notes": "User wants to review retirement and life insurance forms.",
        },
        "digital_assets_inventory": {
            "status": "missing",
            "last_reviewed": None,
            "notes": "",
        },
        "emergency_contacts_and_storage": {
            "status": "missing",
            "last_reviewed": None,
            "notes": "No emergency document location shared yet.",
        },
    }
    inventory.update(overrides)
    return inventory


def _valid_payload(**overrides) -> dict:
    payload = {
        "generated_at": "2026-06-22T12:00:00Z",
        "readiness_status": "checklist_ready",
        "legal_boundary_acknowledged": True,
        "jurisdiction_context": {
            "state_or_region": "North Carolina",
            "state_specific_law_not_interpreted": True,
        },
        "household_context": {
            "marital_status_known": True,
            "dependents_known": True,
            "minor_children_known": False,
            "homeownership_known": True,
            "business_owner_known": False,
            "recent_life_events": ["home_purchase"],
        },
        "document_inventory": _document_inventory(),
        "beneficiary_review": {
            "accounts_to_review": [
                {"account_type": "401k", "nickname": "Work plan"},
                {"account_type": "life_insurance", "nickname": "Term policy"},
            ],
            "mismatch_flags": ["last_reviewed_before_home_purchase"],
            "user_tasks": [
                "Check beneficiary forms directly with each provider.",
            ],
        },
        "referral_context": {
            "attorney_recommended": False,
            "reasons": [],
            "specialist_resources": ["attorney"],
        },
        "next_actions": [
            {
                "action": "List which documents exist and where they are stored.",
                "owner": "user",
                "due": "2026-07-15",
                "status": "open",
            }
        ],
        "next_check_in": "2026-07-22",
        "scope_notes": [
            "Metadata only; no document text, legal interpretation, or drafting.",
        ],
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("document_inventory")
    response = coach_estate_document_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "document_inventory" in response["message"]


def test_artifact_save_rejects_unknown_readiness_status(data_dir: Path) -> None:
    del data_dir
    response = coach_estate_document_readiness_artifact_save(
        plan_payload=_valid_payload(readiness_status="draft_will"),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "readiness_status" in response["message"]


def test_artifact_save_requires_legal_boundary_acknowledged(data_dir: Path) -> None:
    del data_dir
    response = coach_estate_document_readiness_artifact_save(
        plan_payload=_valid_payload(legal_boundary_acknowledged=False),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "legal_boundary_acknowledged" in response["message"]


def test_artifact_save_rejects_unknown_document_status(data_dir: Path) -> None:
    del data_dir
    response = coach_estate_document_readiness_artifact_save(
        plan_payload=_valid_payload(
            document_inventory=_document_inventory(
                will={
                    "status": "needs_clause_review",
                    "last_reviewed": None,
                    "notes": "",
                }
            )
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "document_inventory.will.status" in response["message"]


def test_artifact_save_rejects_document_body_text(data_dir: Path) -> None:
    payload = _valid_payload()
    payload["document_inventory"]["will"]["will_text"] = "I leave everything to..."
    response = coach_estate_document_readiness_artifact_save(
        plan_payload=payload,
        dry_run=False,
    )

    artifact_dir = data_dir / "artifacts" / "coach_estate_document_readiness"
    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store legal document content" in response["message"]
    assert not artifact_dir.exists()


def test_artifact_save_rejects_long_notes(data_dir: Path) -> None:
    del data_dir
    response = coach_estate_document_readiness_artifact_save(
        plan_payload=_valid_payload(
            document_inventory=_document_inventory(
                will={
                    "status": "present",
                    "last_reviewed": "2021",
                    "notes": "x" * 281,
                }
            )
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "short metadata" in response["message"]


def test_attorney_recommended_requires_referral_flag_and_reason(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_estate_document_readiness_artifact_save(
        plan_payload=_valid_payload(readiness_status="attorney_recommended"),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "referral_context.attorney_recommended=true" in response["message"]


def test_attorney_recommended_payload_is_valid_with_reason(data_dir: Path) -> None:
    result = coach_estate_document_readiness_artifact_save(
        plan_payload=_valid_payload(
            readiness_status="attorney_recommended",
            referral_context={
                "attorney_recommended": True,
                "reasons": ["User asked whether trust language should be changed."],
                "specialist_resources": ["attorney"],
            },
        ),
        dry_run=True,
    )

    assert result["summary"]["valid"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["plan_payload"]["referral_context"][
        "attorney_recommended"
    ] is True


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_estate_document_readiness_artifact_save(
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
        / "coach_estate_document_readiness"
        / "20260622.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_estate_document_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    artifact_path = (
        data_dir / "artifacts" / "coach_estate_document_readiness" / "20260622.md"
    )
    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Estate Document Readiness Checklist" in content
    assert "## Document Inventory" in content
    assert "## Generated machine-readable footer" in content
    assert "I leave everything to" not in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_estate_document_readiness_artifact_save(
        plan_payload=payload,
        dry_run=False,
    )
    result = coach_estate_document_readiness_artifact_read(date=None)
    saved = result["data"]["plan_payload"]
    assert saved["readiness_status"] == "checklist_ready"
    assert saved["document_inventory"]["beneficiary_designations"]["status"] == "stale"
    assert saved["beneficiary_review"]["accounts_to_review"][0]["account_type"] == "401k"


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_estate_document_readiness_artifact_read(date=None)
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_estate_document_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )
    second = coach_estate_document_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_same_day_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_estate_document_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    second = coach_estate_document_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    assert Path(first["data"]["artifact_path"]).name == "20260622.md"
    assert Path(second["data"]["artifact_path"]).name == "20260622-r2.md"

    latest = coach_estate_document_readiness_artifact_read(date="2026-06-22")
    assert latest["data"] is not None
    assert Path(latest["data"]["artifact_path"]).name == "20260622-r2.md"

    r2 = coach_estate_document_readiness_artifact_read(date="20260622-r2")
    assert r2["data"] is not None
    assert Path(r2["data"]["artifact_path"]).name == "20260622-r2.md"
