"""Save/read round-trip tests for ``coach_spending_plan_artifact_*`` MCP tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_spending_plan_artifact_read,
    coach_spending_plan_artifact_save,
    data_quality_gap_ratio,
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
        "generated_at": "2026-06-07T12:00:00Z",
        "strategy": "percentage_50_30_20",
        "expected_monthly_income_cents": 700_000,
        "expected_monthly_expenses_cents": 580_000,
        "expected_essential_monthly_cents": 380_000,
        "expected_discretionary_monthly_cents": 200_000,
        "review_cadence": "monthly",
        "next_review_at": "2026-07-07",
        "allocations": {
            "by_category": [
                {
                    "category_id": "cat_rent",
                    "category_name": "Rent",
                    "type": "essential",
                    "monthly_cents": 200_000,
                    "anchor_3mo_avg_cents": 200_000,
                },
                {
                    "category_id": "cat_groceries",
                    "category_name": "Groceries",
                    "type": "essential",
                    "monthly_cents": 60_000,
                    "anchor_3mo_avg_cents": 58_000,
                    "notes": "+5% headroom",
                },
            ],
            "emergency_fund": {"monthly_cents": 30_000, "sourced_from": "coach_emergency_fund"},
            "debt_paydown": {"monthly_cents": 60_000, "sourced_from": "coach_debt_payoff"},
        },
        "periodic_reservations": [
            {
                "item_name": "Vehicle registration",
                "annual_cents": 24_000,
                "monthly_reserve_cents": 2_000,
                "next_hit_estimated": "2027-03",
            }
        ],
        "mirror_status": {"state": "ok", "failed_categories": [], "recorded_at": "2026-06-07"},
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("strategy")
    response = coach_spending_plan_artifact_save(plan_payload=payload, dry_run=True)

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "strategy" in response["message"]


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_spending_plan_artifact_save(plan_payload=_valid_payload(), dry_run=True)
    artifact_path = Path(result["data"]["artifact_path"])
    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == data_dir / "artifacts" / "coach_spending_plan" / "20260607.md"
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_spending_plan_artifact_save(plan_payload=_valid_payload(), dry_run=False)
    artifact_path = data_dir / "artifacts" / "coach_spending_plan" / "20260607.md"
    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Spending Plan" in content
    assert "## Allocations (per category, monthly)" in content
    assert "## Periodic Reservations (annual / 12)" in content
    assert "## Mirror Status" in content


def test_artifact_save_then_read_round_trip(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    coach_spending_plan_artifact_save(plan_payload=payload, dry_run=False)
    result = coach_spending_plan_artifact_read(date=None)
    saved = result["data"]["plan_payload"]
    assert saved["strategy"] == payload["strategy"]
    assert saved["expected_monthly_income_cents"] == payload["expected_monthly_income_cents"]
    assert saved["allocations"]["emergency_fund"]["monthly_cents"] == 30_000
    assert saved["allocations"]["debt_paydown"]["monthly_cents"] == 60_000


def test_artifact_read_returns_found_false_when_no_directory(data_dir: Path) -> None:
    del data_dir
    result = coach_spending_plan_artifact_read(date=None)
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_artifact_save_same_generated_at_updates_in_place(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    first = coach_spending_plan_artifact_save(plan_payload=dict(payload), dry_run=False)
    second = coach_spending_plan_artifact_save(plan_payload=dict(payload), dry_run=False)
    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    assert first["data"]["generated_at"] == second["data"]["generated_at"]
    assert first["data"]["last_modified_at"] == first["data"]["generated_at"]
    assert second["data"]["last_modified_at"] != second["data"]["generated_at"]


def test_artifact_save_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first = coach_spending_plan_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-07T10:00:00Z"),
        dry_run=False,
    )
    second = coach_spending_plan_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-07T15:00:00Z"),
        dry_run=False,
    )
    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    first_path = data_dir / "artifacts" / "coach_spending_plan" / "20260607.md"
    second_path = data_dir / "artifacts" / "coach_spending_plan" / "20260607-r2.md"
    assert Path(first["data"]["artifact_path"]) == first_path
    assert Path(second["data"]["artifact_path"]) == second_path
    assert first_path.exists() and second_path.exists()


def test_artifact_read_specific_revision(data_dir: Path) -> None:
    coach_spending_plan_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-07T10:00:00Z"),
        dry_run=False,
    )
    coach_spending_plan_artifact_save(
        plan_payload=_valid_payload(generated_at="2026-06-07T15:00:00Z"),
        dry_run=False,
    )
    base_result = coach_spending_plan_artifact_read(date="2026-06-07")
    assert base_result["data"] is not None
    # Bare date returns latest revision.
    assert Path(base_result["data"]["artifact_path"]).name == "20260607-r2.md"

    r2_result = coach_spending_plan_artifact_read(date="20260607-r2")
    assert r2_result["data"] is not None
    assert Path(r2_result["data"]["artifact_path"]).name == "20260607-r2.md"


def test_artifact_read_no_match_for_missing_revision(data_dir: Path) -> None:
    coach_spending_plan_artifact_save(plan_payload=_valid_payload(), dry_run=False)
    result = coach_spending_plan_artifact_read(date="20260607-r5")
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] == "no_artifact_for_date"


# ---------------------------------------------------------------------------
# data_quality_gap_ratio MCP wrapper
# ---------------------------------------------------------------------------


def test_data_quality_gap_ratio_returns_zero_on_empty_db(data_dir: Path) -> None:
    """Empty DB should not crash; ratio is 0.0 with total_count=0."""
    del data_dir
    from finance_cli.db import connect, initialize_database
    import os

    initialize_database(Path(os.environ["FINANCE_CLI_DB"]))
    with connect(Path(os.environ["FINANCE_CLI_DB"])):
        pass

    result = data_quality_gap_ratio(view="personal", date_from="2026-01-01", date_to="2026-06-30")
    assert result["data"]["gap_ratio"] == 0.0
    assert result["data"]["total_count"] == 0
    assert result["data"]["uncat_or_unreviewed_count"] == 0
    assert result["data"]["view"] == "personal"


def test_data_quality_gap_ratio_rejects_invalid_view(data_dir: Path) -> None:
    del data_dir
    from finance_cli.db import connect, initialize_database
    import os

    initialize_database(Path(os.environ["FINANCE_CLI_DB"]))
    with connect(Path(os.environ["FINANCE_CLI_DB"])):
        pass

    response = data_quality_gap_ratio(view="bogus", date_from="2026-01-01", date_to="2026-06-30")
    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "view" in response["message"]
