"""Save/read tests for ``coach_advisor_handoff_readiness_artifact_*`` tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_advisor_handoff_readiness_artifact_read,
    coach_advisor_handoff_readiness_artifact_save,
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
        "handoff_status": "handoff_ready",
        "request_classification": {
            "user_request": "Should I buy VOO or keep this as cash?",
            "release_mode": "referral_handoff",
            "prohibited_response_if_unsupervised": True,
        },
        "professional_type": {
            "primary": "ria",
            "rationale": "Specific securities allocation questions need a fiduciary investment review.",
        },
        "cashnerd_context": {
            "relevant_artifacts": ["coach_investment_readiness:20260622"],
            "key_facts": ["User has not selected a supervised investment-advice path."],
            "user_questions": ["Should I buy VOO or use a different ETF?"],
        },
        "handoff_questions": [
            "Are you acting as a fiduciary for this engagement?",
            "How are you compensated?",
            "How should I evaluate this ETF question?",
        ],
        "documents_to_bring": [
            "Current investment account statement",
            "Cash-flow snapshot",
        ],
        "disclosures_to_surface": ["scope_boundary", "conflict_of_interest"],
        "boundary_response": {
            "user_facing_summary": "CashNerd can prepare the facts and questions, but it is not choosing the security.",
            "refused_topics": ["specific ETF recommendation"],
            "allowed_help": ["Prepare a fiduciary-review packet."],
        },
        "next_actions": [
            {
                "label": "Schedule a fiduciary adviser conversation",
                "owner": "user",
                "status": "open",
            }
        ],
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("boundary_response")
    response = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "boundary_response" in response["message"]


def test_artifact_save_rejects_unknown_handoff_status(data_dir: Path) -> None:
    del data_dir
    response = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(handoff_status="advisor_selected"),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "handoff_status" in response["message"]


def test_artifact_save_rejects_unknown_release_mode(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload(
        request_classification={
            "user_request": "What advisor should I use?",
            "release_mode": "advisor_matching",
            "prohibited_response_if_unsupervised": True,
        }
    )
    response = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "request_classification.release_mode" in response["message"]


def test_artifact_save_allows_prohibited_user_request_as_context(
    data_dir: Path,
) -> None:
    del data_dir
    result = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=True,
    )

    assert result["summary"]["valid"] is True
    assert result["data"]["plan_payload"]["request_classification"][
        "user_request"
    ] == "Should I buy VOO or keep this as cash?"


def test_artifact_save_requires_professional_rationale_for_handoff(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(
            professional_type={"primary": "ria", "rationale": ""},
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "professional_type.rationale" in response["message"]


def test_artifact_save_rejects_unknown_professional_for_handoff(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(
            professional_type={
                "primary": "unknown",
                "rationale": "The user needs professional review.",
            },
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "professional_type.primary cannot be unknown" in response["message"]


def test_artifact_save_requires_boundary_lists_for_prohibited_request(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(
            boundary_response={
                "user_facing_summary": "CashNerd can prepare a packet.",
                "refused_topics": [],
                "allowed_help": ["Prepare facts."],
            }
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "requires refused_topics" in response["message"]


@pytest.mark.parametrize(
    "override",
    [
        {"next_actions": [{"label": "Pick adviser", "recommended_advisor": "RIA X"}]},
        {
            "boundary_response": {
                "user_facing_summary": "CashNerd can prepare a packet.",
                "refused_topics": ["specific ETF recommendation"],
                "allowed_help": ["Prepare facts."],
                "tax_filing_position": "Claim the credit.",
            }
        },
        {
            "cashnerd_context": {
                "relevant_artifacts": [],
                "key_facts": [],
                "user_questions": [],
                "selected_security": "VOO",
            }
        },
    ],
)
def test_artifact_save_rejects_regulated_recommendation_fields(
    data_dir: Path,
    override: dict,
) -> None:
    del data_dir
    response = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(**override),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store advisor selection" in response["message"]


def test_artifact_save_rejects_free_text_regulated_answer(data_dir: Path) -> None:
    del data_dir
    response = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(
            next_actions=[
                {
                    "label": "Buy VOO in an 80/20 allocation this week",
                    "owner": "user",
                    "status": "open",
                }
            ],
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store regulated advice text" in response["message"]


def test_artifact_save_requires_referral_compensation_disclosure(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(
            referral_metadata={"referral_fee": "10% of first-year fee"},
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "referral_compensation" in response["message"]


def test_artifact_save_accepts_disclosed_referral_compensation(
    data_dir: Path,
) -> None:
    del data_dir
    result = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(
            referral_metadata={"referral_fee": "10% of first-year fee"},
            disclosures_to_surface=[
                "scope_boundary",
                "conflict_of_interest",
                "referral_compensation",
            ],
        ),
        dry_run=True,
    )

    assert result["summary"]["valid"] is True


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=True,
    )
    artifact_path = Path(result["data"]["artifact_path"])

    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == (
        data_dir / "artifacts" / "coach_advisor_handoff_readiness" / "20260622.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    artifact_path = (
        data_dir / "artifacts" / "coach_advisor_handoff_readiness" / "20260622.md"
    )

    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Advisor Handoff Readiness Packet" in content
    assert "## Request Classification" in content
    assert "## Generated machine-readable footer" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_advisor_handoff_readiness_artifact_save(
        plan_payload=payload,
        dry_run=False,
    )
    result = coach_advisor_handoff_readiness_artifact_read(date=None)
    saved = result["data"]["plan_payload"]

    assert saved["handoff_status"] == "handoff_ready"
    assert saved["professional_type"]["primary"] == "ria"
    assert saved["request_classification"]["release_mode"] == "referral_handoff"
    assert saved["handoff_questions"][0] == (
        "Are you acting as a fiduciary for this engagement?"
    )


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_advisor_handoff_readiness_artifact_read(date=None)

    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )
    second = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_same_day_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    second = coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    assert Path(first["data"]["artifact_path"]).name == "20260622.md"
    assert Path(second["data"]["artifact_path"]).name == "20260622-r2.md"


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    del data_dir
    coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    coach_advisor_handoff_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    latest = coach_advisor_handoff_readiness_artifact_read(date="2026-06-22")
    assert latest["data"] is not None
    assert Path(latest["data"]["artifact_path"]).name == "20260622-r2.md"

    r2 = coach_advisor_handoff_readiness_artifact_read(date="20260622-r2")
    assert r2["data"] is not None
    assert Path(r2["data"]["artifact_path"]).name == "20260622-r2.md"
