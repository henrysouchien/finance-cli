from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_debt_payoff_artifact_read,
    coach_debt_payoff_artifact_save,
)
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


@pytest.fixture()
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        yield tmp_path
    finally:
        reset_user_context(token)


def _valid_payload() -> dict:
    return {
        "generated_at": "2026-04-09T12:00:00Z",
        "smart_goal": "Pay $500 per month toward the scoped credit cards.",
        "strategy": {"name": "avalanche", "why": "Highest APR first."},
        "action_steps": [
            {"step": "Pay all minimums", "timeline": "monthly"},
            {"step": "Send extra cash to the highest APR card", "timeline": "monthly"},
        ],
        "monthly_commitment_cents": 50_000,
        "debts_in_scope": ["liability_a", "liability_b"],
        "target_debt_free_date": "2027-04-09",
        "monitoring_cadence": "monthly",
        "next_check_in": "2026-05-09",
    }


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("smart_goal")

    response = coach_debt_payoff_artifact_save(action_plan_payload=payload, dry_run=True)

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "smart_goal" in response["message"]
    assert response["error"] == response["message"]


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_debt_payoff_artifact_save(action_plan_payload=_valid_payload(), dry_run=True)
    artifact_path = Path(result["data"]["artifact_path"])

    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert artifact_path == data_dir / "artifacts" / "coach_debt_payoff" / "20260409.md"
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_debt_payoff_artifact_save(action_plan_payload=_valid_payload(), dry_run=False)
    artifact_path = data_dir / "artifacts" / "coach_debt_payoff" / "20260409.md"

    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    assert "# Debt Payoff Action Plan" in artifact_path.read_text(encoding="utf-8")


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_debt_payoff_artifact_save(action_plan_payload=payload, dry_run=False)

    result = coach_debt_payoff_artifact_read(date=None)
    saved_payload = result["data"]["action_plan_payload"]

    assert saved_payload["smart_goal"] == payload["smart_goal"]
    assert saved_payload["strategy"] == payload["strategy"]
    assert saved_payload["action_steps"] == payload["action_steps"]
    assert saved_payload["monthly_commitment_cents"] == payload["monthly_commitment_cents"]
    assert saved_payload["debts_in_scope"] == payload["debts_in_scope"]


def test_artifact_read_returns_found_false_when_no_artifact(data_dir: Path) -> None:
    del data_dir

    result = coach_debt_payoff_artifact_read(date=None)

    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}
