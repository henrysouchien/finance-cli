"""Save/read round-trip tests for ``coach_savings_goal_artifact_*`` MCP tools."""

from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.mcp_server import (
    coach_savings_goal_artifact_read,
    coach_savings_goal_artifact_save,
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
        "goal_name": "down-payment-2027",
        "smart_goal": "Save $20,000 by 2027-11-15 ($1,000/mo for 18 months).",
        "target_phase": "full",
        "target_balance_cents": 2_000_000,
        "monthly_commitment_cents": 100_000,
        "goal_horizon_months": 18,
        "target_met_date": "2027-11-15",
        "account_ids_in_goal": [
            {"account_id": "acct_hysa_001", "role": "primary", "target_balance_cents": 2_000_000},
        ],
        "action_steps": [
            {"step": "Open HYSA at any FDIC-insured online bank.", "timeline": "2026-06-15", "quick_win": True},
            {"step": "Set up paycheck-split allocation.", "timeline": "2026-06-22"},
        ],
        "milestones": [
            {"threshold_pct": 25, "threshold_cents": 500_000, "target_date": "2026-11-15", "hit_at": None},
            {"threshold_pct": 50, "threshold_cents": 1_000_000, "target_date": "2027-03-15", "hit_at": None},
            {"threshold_pct": 75, "threshold_cents": 1_500_000, "target_date": "2027-07-15", "hit_at": None},
            {"threshold_pct": 100, "threshold_cents": 2_000_000, "target_date": "2027-11-15", "hit_at": None},
        ],
        "user_decision": "full",
        "monitoring_cadence": "monthly",
        "next_check_in": "2026-07-07",
    }
    payload.update(overrides)
    return payload


def test_artifact_save_validates_required_keys(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("goal_name")
    response = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "goal_name" in response["message"]


def test_artifact_save_dry_run_does_not_write(data_dir: Path) -> None:
    result = coach_savings_goal_artifact_save(plan_payload=_valid_payload(), dry_run=True)
    artifact_path = Path(result["data"]["artifact_path"])
    assert result["data"]["dry_run"] is True
    assert result["summary"]["saved"] is False
    assert result["data"]["save_mode"] == "create"
    assert artifact_path == data_dir / "artifacts" / "coach_savings_goal" / "20260607.md"
    assert not artifact_path.exists()


def test_artifact_save_writes_markdown_to_expected_path(data_dir: Path) -> None:
    result = coach_savings_goal_artifact_save(plan_payload=_valid_payload(), dry_run=False)
    artifact_path = data_dir / "artifacts" / "coach_savings_goal" / "20260607.md"
    assert Path(result["data"]["artifact_path"]) == artifact_path
    assert artifact_path.exists()
    content = artifact_path.read_text(encoding="utf-8")
    assert "# Savings Goal Plan" in content
    assert "down-payment-2027" in content
    assert "## Generated machine-readable footer" in content
    # YAML footer uses 4-backtick fence to escape the nested ```yaml token.
    assert "````yaml" in content


def test_artifact_save_read_round_trip(data_dir: Path) -> None:
    del data_dir
    coach_savings_goal_artifact_save(plan_payload=_valid_payload(), dry_run=False)
    read = coach_savings_goal_artifact_read(date=None)
    assert read["summary"]["found"] is True
    plan = read["data"]["plan_payload"]
    assert plan["goal_name"] == "down-payment-2027"
    assert plan["target_balance_cents"] == 2_000_000
    assert plan["monthly_commitment_cents"] == 100_000
    assert plan["target_phase"] == "full"
    assert plan["user_decision"] == "full"
    assert plan["milestones"][0]["threshold_pct"] == 25


def test_same_date_same_generated_at_updates_in_place(data_dir: Path) -> None:
    payload = _valid_payload()
    first = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=False)
    second = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=False)

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "update_in_place"
    assert first["data"]["artifact_path"] == second["data"]["artifact_path"]
    files = sorted((data_dir / "artifacts" / "coach_savings_goal").glob("*.md"))
    assert len(files) == 1


def test_same_date_different_generated_at_writes_new_revision(data_dir: Path) -> None:
    first_payload = _valid_payload()
    second_payload = _valid_payload(generated_at="2026-06-07T18:30:00Z")

    first = coach_savings_goal_artifact_save(plan_payload=first_payload, dry_run=False)
    second = coach_savings_goal_artifact_save(plan_payload=second_payload, dry_run=False)

    assert first["data"]["save_mode"] == "create"
    assert second["data"]["save_mode"] == "new_revision"
    assert first["data"]["artifact_path"] != second["data"]["artifact_path"]
    files = sorted((data_dir / "artifacts" / "coach_savings_goal").glob("*.md"))
    assert len(files) == 2
    names = {f.name for f in files}
    assert names == {"20260607.md", "20260607-r2.md"}


def test_artifact_save_fills_generated_at_when_absent(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload()
    payload.pop("generated_at")
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=False)
    assert result["data"]["generated_at"]
    assert result["data"]["last_modified_at"] == result["data"]["generated_at"]


def test_artifact_read_missing_returns_not_found(data_dir: Path) -> None:
    del data_dir
    result = coach_savings_goal_artifact_read(date=None)
    assert result["data"] is None
    assert result["summary"]["found"] is False
    assert result["summary"]["reason"] in {"no_directory", "no_artifacts"}


def test_artifact_read_date_none_returns_latest_revision_not_base(data_dir: Path) -> None:
    """Regression for the sort-by-filename bug: ``20260607-r2.md`` lexically
    sorts BEFORE ``20260607.md`` because ``-`` (0x2D) precedes ``.`` (0x2E),
    so a naive ``sorted(glob('*.md'))[-1]`` returns the stale base artifact
    when same-day revisions exist. The read path must be revision-aware.
    """
    del data_dir
    base_payload = _valid_payload()
    revised_payload = _valid_payload(
        generated_at="2026-06-07T18:30:00Z",
        target_balance_cents=2_500_000,
    )
    coach_savings_goal_artifact_save(plan_payload=base_payload, dry_run=False)
    coach_savings_goal_artifact_save(plan_payload=revised_payload, dry_run=False)

    read = coach_savings_goal_artifact_read(date=None)
    assert read["summary"]["found"] is True
    # The latest read must surface -r2's payload, not the base.
    assert read["data"]["plan_payload"]["target_balance_cents"] == 2_500_000
    assert read["data"]["artifact_path"].endswith("20260607-r2.md")


def _valid_starter_payload(**overrides) -> dict:
    """Compliant starter_only payload — milestones subdivide the STARTER target,
    and all original_full_* fields are populated so Phase 9 accepted-unlock
    can restore the full plan deterministically.
    """
    payload = _valid_payload(
        target_phase="starter_only",
        target_balance_cents=200_000,
        monthly_commitment_cents=50_000,
        goal_horizon_months=4,
        target_met_date="2026-10-07",
        unlock_blocker="debt",
        original_full_target_balance_cents=2_000_000,
        original_full_monthly_commitment_cents=100_000,
        original_full_target_met_date="2027-11-15",
        original_full_goal_horizon_months=18,
        user_decision="starter_then_debt",
        milestones=[
            {"threshold_pct": 25, "threshold_cents": 50_000, "target_date": "2026-07-07", "hit_at": None},
            {"threshold_pct": 50, "threshold_cents": 100_000, "target_date": "2026-08-07", "hit_at": None},
            {"threshold_pct": 75, "threshold_cents": 150_000, "target_date": "2026-09-07", "hit_at": None},
            {"threshold_pct": 100, "threshold_cents": 200_000, "target_date": "2026-10-07", "hit_at": None},
        ],
    )
    payload.update(overrides)
    return payload


def test_artifact_round_trip_preserves_unlock_blocker_and_original_full_fields(
    data_dir: Path,
) -> None:
    del data_dir
    coach_savings_goal_artifact_save(plan_payload=_valid_starter_payload(), dry_run=False)
    read = coach_savings_goal_artifact_read(date=None)
    plan = read["data"]["plan_payload"]
    assert plan["target_phase"] == "starter_only"
    assert plan["unlock_blocker"] == "debt"
    assert plan["original_full_target_balance_cents"] == 2_000_000
    assert plan["original_full_monthly_commitment_cents"] == 100_000
    assert plan["original_full_target_met_date"] == "2027-11-15"
    assert plan["original_full_goal_horizon_months"] == 18
    assert plan["user_decision"] == "starter_then_debt"


# ---------------------------------------------------------------------------
# Validation gates — starter/full contract enforcement
# (live-drive surfaced an LLM-discipline gap: the agent saved a starter_only
# artifact without original_full_* / unlock_blocker fields, and with milestones
# scaled to the FULL trajectory instead of the starter. Both would silently
# break the Phase 9 accepted-unlock flow downstream.)
# ---------------------------------------------------------------------------


def test_validation_rejects_invalid_target_phase(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload(target_phase="partial")
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "target_phase" in result["message"]


def test_validation_rejects_starter_only_without_unlock_blocker(data_dir: Path) -> None:
    del data_dir
    payload = _valid_starter_payload()
    payload["unlock_blocker"] = None
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "unlock_blocker" in result["message"]


def test_validation_rejects_starter_only_with_invalid_unlock_blocker(
    data_dir: Path,
) -> None:
    del data_dir
    payload = _valid_starter_payload(unlock_blocker="something_else")
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "unlock_blocker" in result["message"]


def test_validation_rejects_starter_only_missing_original_full_fields(
    data_dir: Path,
) -> None:
    """Each missing original_full_* field is called out — Phase 9 accepted-unlock
    needs ALL four to restore the full plan without parsing prose.
    """
    del data_dir
    payload = _valid_starter_payload()
    payload["original_full_monthly_commitment_cents"] = None
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "original_full_monthly_commitment_cents" in result["message"]


def test_validation_rejects_starter_only_with_negative_original_full_int(
    data_dir: Path,
) -> None:
    del data_dir
    payload = _valid_starter_payload(original_full_target_balance_cents=-100_000)
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "original_full_target_balance_cents" in result["message"]


def test_validation_rejects_full_phase_with_unlock_blocker(data_dir: Path) -> None:
    """Full-phase artifact must have unlock_blocker=None — there is no prior
    skill gate to unlock past once the user is on the full track.
    """
    del data_dir
    payload = _valid_payload(unlock_blocker="debt")
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "unlock_blocker" in result["message"]


def test_validation_rejects_full_phase_with_original_full_fields(data_dir: Path) -> None:
    """Full-phase artifact must have all original_full_* fields None or absent —
    the artifact represents the active plan, no starter-phase to restore from.
    """
    del data_dir
    payload = _valid_payload(original_full_target_balance_cents=2_000_000)
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "original_full" in result["message"]


def test_validation_rejects_milestone_threshold_above_target(data_dir: Path) -> None:
    """The exact live-drive bug: starter_only target $5,000 but milestones
    scaled to the full $20k trajectory. The 50/75/100% milestones would never
    fire against the active artifact's target."""
    del data_dir
    payload = _valid_starter_payload(
        milestones=[
            {"threshold_pct": 25, "threshold_cents": 500_000, "target_date": "2026-07-07", "hit_at": None},
            {"threshold_pct": 50, "threshold_cents": 1_000_000, "target_date": "2026-08-07", "hit_at": None},
        ],
    )
    # target_balance_cents=200_000 < 500_000 first milestone
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "threshold_cents" in result["message"]
    assert "target_balance_cents" in result["message"]


def test_validation_rejects_non_list_milestones(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload(milestones={"not": "a list"})
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "milestones" in result["message"]


def test_validation_rejects_zero_threshold_cents(data_dir: Path) -> None:
    del data_dir
    payload = _valid_payload(
        milestones=[
            {"threshold_pct": 25, "threshold_cents": 0, "target_date": "2026-11-15", "hit_at": None},
        ],
    )
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "threshold_cents" in result["message"]


def test_validation_accepts_full_phase_with_original_full_fields_explicitly_null(
    data_dir: Path,
) -> None:
    """Setting original_full_* to None explicitly (not just absent) is valid for
    full-phase — covers the accepted-unlock write flow where the agent nulls
    these fields after restoring the full plan.
    """
    del data_dir
    payload = _valid_payload(
        original_full_target_balance_cents=None,
        original_full_monthly_commitment_cents=None,
        original_full_target_met_date=None,
        original_full_goal_horizon_months=None,
        unlock_blocker=None,
    )
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=False)
    assert result["summary"]["saved"] is True


def test_validation_accepts_compliant_starter_only_payload(data_dir: Path) -> None:
    """Smoke: the canonical compliant starter_only payload saves cleanly."""
    del data_dir
    result = coach_savings_goal_artifact_save(
        plan_payload=_valid_starter_payload(),
        dry_run=False,
    )
    assert result["summary"]["saved"] is True
    plan = result["data"]["plan_payload"]
    assert plan["target_phase"] == "starter_only"
    assert plan["unlock_blocker"] == "debt"
    assert plan["target_balance_cents"] == 200_000
    # Milestones subdivide the starter, not the full
    assert max(m["threshold_cents"] for m in plan["milestones"]) == 200_000


def test_validation_rejects_malformed_original_full_target_met_date(data_dir: Path) -> None:
    """Strict YYYY-MM-DD parse — non-empty strings like "next summer" fail."""
    del data_dir
    payload = _valid_starter_payload(original_full_target_met_date="next summer")
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "original_full_target_met_date" in result["message"]


def test_validation_rejects_bool_for_int_field(data_dir: Path) -> None:
    """``int(True) == 1`` would sneak through a naive coercion — explicit reject."""
    del data_dir
    payload = _valid_payload(target_balance_cents=True)
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "target_balance_cents" in result["message"]


def test_validation_rejects_fractional_float_for_int_field(data_dir: Path) -> None:
    """A non-integer float (e.g., 500_000.5) is not a valid cents value."""
    del data_dir
    payload = _valid_payload(target_balance_cents=500_000.5)
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"


def test_validation_persists_canonical_int_after_string_coercion(data_dir: Path) -> None:
    """String form ``"500000"`` is accepted for MCP ergonomics but the persisted
    payload should hold canonical int 500000 — downstream consumers (intervention
    evaluators, unlock-check) read these as ints. Covers every numeric field
    the validator coerces.
    """
    del data_dir
    payload = _valid_starter_payload(
        target_balance_cents="200000",
        monthly_commitment_cents="50000",
        goal_horizon_months="4",
        original_full_target_balance_cents="2000000",
        original_full_monthly_commitment_cents="100000",
        original_full_goal_horizon_months="18",
    )
    payload["milestones"][0]["threshold_cents"] = "50000"
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result.get("status") != "error"
    plan = result["data"]["plan_payload"]
    for field in (
        "target_balance_cents",
        "monthly_commitment_cents",
        "goal_horizon_months",
        "original_full_target_balance_cents",
        "original_full_monthly_commitment_cents",
        "original_full_goal_horizon_months",
    ):
        assert isinstance(plan[field], int), f"{field} not canonical int"
    assert plan["target_balance_cents"] == 200_000
    assert plan["monthly_commitment_cents"] == 50_000
    assert plan["goal_horizon_months"] == 4
    assert plan["original_full_target_balance_cents"] == 2_000_000
    assert plan["original_full_monthly_commitment_cents"] == 100_000
    assert plan["original_full_goal_horizon_months"] == 18
    assert plan["milestones"][0]["threshold_cents"] == 50_000
    assert isinstance(plan["milestones"][0]["threshold_cents"], int)


def test_validation_persists_optional_nonnegative_metadata_as_canonical_int(
    data_dir: Path,
) -> None:
    """Optional render/evidence metadata can be zero but should still persist
    canonically when supplied as MCP-friendly strings.
    """
    del data_dir
    payload = _valid_payload(
        current_balance_toward_goal_cents="0",
        gap_cents="200000",
        unlock_evidence={
            "debt_cleared": True,
            "efund_target_met": False,
            "debt_in_scope_sum_cents": "0",
            "efund_balance_sum_cents": "125000",
            "efund_target_balance_cents": "300000",
            "missing_prerequisite_artifacts": [],
            "observed_at": "2026-06-07",
        },
    )
    payload["account_ids_in_goal"][0]["target_balance_cents"] = "2000000"

    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)

    assert result.get("status") != "error"
    plan = result["data"]["plan_payload"]
    assert plan["current_balance_toward_goal_cents"] == 0
    assert isinstance(plan["current_balance_toward_goal_cents"], int)
    assert plan["gap_cents"] == 200_000
    assert isinstance(plan["gap_cents"], int)
    assert plan["account_ids_in_goal"][0]["target_balance_cents"] == 2_000_000
    assert isinstance(plan["account_ids_in_goal"][0]["target_balance_cents"], int)
    evidence = plan["unlock_evidence"]
    assert evidence["debt_in_scope_sum_cents"] == 0
    assert isinstance(evidence["debt_in_scope_sum_cents"], int)
    assert evidence["efund_balance_sum_cents"] == 125_000
    assert isinstance(evidence["efund_balance_sum_cents"], int)
    assert evidence["efund_target_balance_cents"] == 300_000
    assert isinstance(evidence["efund_target_balance_cents"], int)


@pytest.mark.parametrize(
    ("field_path", "mutator"),
    [
        (
            "current_balance_toward_goal_cents",
            lambda payload: payload.update(current_balance_toward_goal_cents=-1),
        ),
        ("gap_cents", lambda payload: payload.update(gap_cents=-1)),
        (
            "account_ids_in_goal[0].target_balance_cents",
            lambda payload: payload["account_ids_in_goal"][0].update(
                target_balance_cents=-1
            ),
        ),
        (
            "unlock_evidence.efund_balance_sum_cents",
            lambda payload: payload.update(
                unlock_evidence={"efund_balance_sum_cents": -1}
            ),
        ),
    ],
)
def test_validation_rejects_negative_optional_nonnegative_metadata(
    data_dir: Path,
    field_path,
    mutator,
) -> None:
    del data_dir
    payload = _valid_payload()
    mutator(payload)
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert field_path in result["message"]
    assert "nonnegative int" in result["message"]


@pytest.mark.parametrize(
    ("field_path", "mutator"),
    [
        (
            "current_balance_toward_goal_cents",
            lambda payload: payload.update(current_balance_toward_goal_cents=1.5),
        ),
        (
            "unlock_evidence.debt_in_scope_sum_cents",
            lambda payload: payload.update(
                unlock_evidence={"debt_in_scope_sum_cents": 1.5}
            ),
        ),
    ],
)
def test_validation_rejects_fractional_optional_nonnegative_metadata(
    data_dir: Path,
    field_path,
    mutator,
) -> None:
    del data_dir
    payload = _valid_payload()
    mutator(payload)
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert field_path in result["message"]


def test_validation_rejects_decimal_optional_nonnegative_metadata(
    data_dir: Path,
) -> None:
    del data_dir
    from decimal import Decimal

    payload = _valid_payload(gap_cents=Decimal("10.5"))
    result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "gap_cents" in result["message"]
    assert "Decimal" in result["message"]


def test_validation_rejects_non_canonical_iso_date_form(data_dir: Path) -> None:
    """Python's ``date.fromisoformat`` accepts compact (``20271115``) and ISO
    week (``2027-W46-1``) forms; the artifact contract requires the canonical
    ``YYYY-MM-DD`` shape so downstream consumers see one consistent format.
    """
    del data_dir
    for bogus in ("20271115", "2027-W46-1"):
        payload = _valid_starter_payload(original_full_target_met_date=bogus)
        result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
        assert result["status"] == "error", f"expected reject for {bogus!r}"
        assert result["error_class"] == "ValueError"
        assert "original_full_target_met_date" in result["message"]


def test_validation_rejects_decimal_smuggling(data_dir: Path) -> None:
    """``int(Decimal("500000.5"))`` silently truncates to 500000 — restrict
    accepted input types so the artifact boundary stays canonical. Covers
    every required numeric field so a Decimal can't smuggle through any of
    them (the stall evaluator reads monthly_commitment_cents directly).
    """
    del data_dir
    from decimal import Decimal

    for field in (
        "target_balance_cents",
        "monthly_commitment_cents",
        "goal_horizon_months",
    ):
        payload = _valid_payload()
        payload[field] = Decimal("100.5")
        result = coach_savings_goal_artifact_save(plan_payload=payload, dry_run=True)
        assert result["status"] == "error", f"expected reject for Decimal in {field}"
        assert result["error_class"] == "ValueError"
        assert field in result["message"]
