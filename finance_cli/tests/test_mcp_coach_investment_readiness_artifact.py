"""Save/read tests for ``coach_investment_readiness_artifact_*`` tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_investment_readiness_artifact_read,
    coach_investment_readiness_artifact_save,
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


def _selected_action(**overrides) -> dict:
    action = {
        "action_id": "fund_investment_account",
        "amount_cents": 25_000,
        "cadence": "monthly",
        "source_account_label": "Checking",
        "destination_account_label": "Brokerage",
        "rationale": "Surplus remains after emergency-fund and debt checks.",
        "scope_label": "cash_movement_only",
        "user_confirmed": False,
        "money_movement_intent_id": None,
        "write_status": "not_requested",
    }
    action.update(overrides)
    return action


def _valid_payload(**overrides) -> dict:
    payload = {
        "generated_at": "2026-06-22T12:00:00Z",
        "readiness_status": "account_funding_ready",
        "user_goal": {
            "stated_goal": "start investing",
            "time_horizon": "long",
            "target_account_type": "taxable_brokerage",
            "investment_account_id": "acct-brokerage",
        },
        "cash_flow_context": {
            "monthly_surplus_capacity_cents": 50_000,
            "essential_monthly_expenses_cents": 400_000,
            "emergency_fund_months": 3.5,
            "high_interest_debt_cents": 0,
            "high_interest_apr_pct": 0.0,
            "near_term_goal_conflicts": [],
        },
        "retirement_tax_context": {
            "employer_match_available": False,
            "tax_advantaged_room_known": False,
            "supported_tax_year": 2026,
            "source_metadata": {},
        },
        "risk_context": {
            "risk_capacity_notes": ["Long horizon and cash reserve look plausible."],
            "risk_tolerance_user_statements": [],
            "liquidity_need_notes": [],
            "time_horizon_notes": ["User described this as long-term money."],
        },
        "candidate_actions": [_selected_action()],
        "selected_action": _selected_action(),
        "boundary": {
            "prohibited_topics_surfaced": [],
            "referral_recommended": False,
            "referral_reason": None,
        },
        "data_gaps": [
            "Confirm whether brokerage destination is ACH-enabled and supported.",
        ],
        "next_actions": [
            {
                "label": "Review account-funding setup",
                "owner": "user",
                "due": None,
                "status": "open",
            }
        ],
        "monitoring": {
            "next_check_in": "2026-07-22",
        },
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("cash_flow_context")
    response = coach_investment_readiness_artifact_save(
        plan_payload=payload,
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "cash_flow_context" in response["message"]


def test_artifact_save_rejects_unknown_readiness_status(data_dir: Path) -> None:
    del data_dir
    response = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(readiness_status="buy_now"),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "readiness_status" in response["message"]


def test_artifact_save_requires_cash_movement_scope_for_funding_action(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(
            selected_action=_selected_action(scope_label="investment_selection"),
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "scope_label must be cash_movement_only" in response["message"]


def test_artifact_save_rejects_security_selection_fields(data_dir: Path) -> None:
    del data_dir
    response = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(
            selected_action=_selected_action(ticker="VOO"),
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store securities" in response["message"]


def test_artifact_save_rejects_fund_name_fields(data_dir: Path) -> None:
    del data_dir
    response = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(
            selected_action=_selected_action(fund_name="Total Market Index Fund"),
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store securities" in response["message"]


def test_artifact_save_rejects_allocation_fields(data_dir: Path) -> None:
    del data_dir
    response = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(
            risk_context={
                "risk_capacity_notes": [],
                "risk_tolerance_user_statements": [],
                "liquidity_need_notes": [],
                "time_horizon_notes": [],
                "target_allocation": "80/20",
            },
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "allocation" in response["message"]


def test_artifact_save_rejects_free_text_investment_advice(data_dir: Path) -> None:
    del data_dir
    response = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(
            next_actions=[
                {
                    "label": "Buy VOO in an 80/20 allocation this week",
                    "owner": "user",
                    "due": None,
                    "status": "open",
                }
            ],
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "may not store investment advice text" in response["message"]


def test_artifact_save_rejects_live_money_movement_write_status(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(
            selected_action=_selected_action(write_status="transfer_submitted"),
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "draft intents only" in response["message"]


def test_artifact_save_rejects_live_transfer_ids(data_dir: Path) -> None:
    del data_dir
    response = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(
            selected_action=_selected_action(dwolla_transfer_id="transfer-123"),
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "draft intents only" in response["message"]


def test_artifact_save_rejects_successful_non_draft_money_movement_evidence(
    data_dir: Path,
) -> None:
    del data_dir
    response = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(
            selected_action=_selected_action(
                write_status="draft_intent_created",
                money_movement_intent_id="move-123",
                write_result={
                    "success": True,
                    "tool_name": "money_movement_transfer_submit",
                },
            ),
        ),
        dry_run=True,
    )

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "draft money-movement intent only" in response["message"]


def test_artifact_save_allows_draft_intent_metadata(data_dir: Path) -> None:
    result = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(
            readiness_status="draft_move_ready",
            selected_action=_selected_action(
                write_status="draft_intent_created",
                money_movement_intent_id="draft-move-123",
                write_result={
                    "success": True,
                    "tool_name": "money_movement_draft_intent_create",
                },
            ),
        ),
        dry_run=True,
    )

    artifact_path = Path(result["data"]["artifact_path"])
    assert result["summary"]["valid"] is True
    assert result["summary"]["saved"] is False
    assert artifact_path == (
        data_dir / "artifacts" / "coach_investment_readiness" / "20260622.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=True,
    )
    artifact_path = Path(result["data"]["artifact_path"])
    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == (
        data_dir / "artifacts" / "coach_investment_readiness" / "20260622.md"
    )
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(),
        dry_run=False,
    )
    artifact_path = data_dir / "artifacts" / "coach_investment_readiness" / "20260622.md"
    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Investment Readiness Plan" in content
    assert "## Candidate Actions" in content
    assert "## Generated machine-readable footer" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_investment_readiness_artifact_save(plan_payload=payload, dry_run=False)
    result = coach_investment_readiness_artifact_read(date=None)
    saved = result["data"]["plan_payload"]
    assert saved["readiness_status"] == "account_funding_ready"
    assert saved["selected_action"]["scope_label"] == "cash_movement_only"
    assert saved["cash_flow_context"]["monthly_surplus_capacity_cents"] == 50_000
    assert saved["monitoring"]["next_check_in"] == "2026-07-22"


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_investment_readiness_artifact_read(date=None)
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_investment_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )
    second = coach_investment_readiness_artifact_save(
        plan_payload=dict(payload),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_same_day_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    second = coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    assert Path(first["data"]["artifact_path"]).name == "20260622.md"
    assert Path(second["data"]["artifact_path"]).name == "20260622-r2.md"


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T10:00:00Z"),
        dry_run=False,
    )
    coach_investment_readiness_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-22T15:00:00Z"),
        dry_run=False,
    )

    latest = coach_investment_readiness_artifact_read(date="2026-06-22")
    assert latest["data"] is not None
    assert Path(latest["data"]["artifact_path"]).name == "20260622-r2.md"

    r2 = coach_investment_readiness_artifact_read(date="20260622-r2")
    assert r2["data"] is not None
    assert Path(r2["data"]["artifact_path"]).name == "20260622-r2.md"
