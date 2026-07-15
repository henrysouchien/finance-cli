from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from finance_cli import skill_recommendations as subject
from finance_cli.db import connect, initialize_database
from finance_cli.gateway.tools import BRIDGE_TOOLS, READ_ONLY_TOOLS
from finance_cli.interventions.registry import Intervention, InterventionAction, Move, Priority
from finance_cli.skill_state import SkillStateStore
from finance_cli.sync.tool_classification import NO_SYNC_TOOLS


NOW = datetime(2026, 5, 26, 12, 0, 0)


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _store(tmp_path: Path) -> SkillStateStore:
    return SkillStateStore(tmp_path / "skill_state.json")


def _seed_onboarding_complete_data(conn) -> None:
    conn.execute(
        """
        INSERT INTO accounts (id, institution_name, account_name, account_type, is_active)
        VALUES ('acct_1', 'Test Bank', 'Checking', 'checking', 1)
        """
    )
    conn.execute(
        """
        INSERT INTO transactions (id, account_id, date, description, amount_cents, source, is_active)
        VALUES ('txn_1', 'acct_1', '2026-04-01', 'Coffee', -500, 'manual', 1)
        """
    )
    conn.execute(
        """
        INSERT INTO transactions (id, account_id, date, description, amount_cents, source, is_active)
        VALUES ('txn_2', 'acct_1', '2026-05-01', 'Coffee', -600, 'manual', 1)
        """
    )


def _intervention(
    pattern_id: str,
    *,
    action: InterventionAction | None = None,
    move: Move = Move.COACH,
    priority: Priority = Priority.MEDIUM,
    dollar_impact_cents: int = 12_300,
) -> Intervention:
    return Intervention(
        pattern_id=pattern_id,
        move=move,
        tiers=(1,),
        priority=priority,
        headline=f"{pattern_id} headline",
        detail_bullets=(f"{pattern_id} detail",),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=action,
        dollar_impact_cents=dollar_impact_cents,
        goal_link=None,
        log_id=None,
        fired_at=NOW,
        last_fired_at=None,
    )


def _engine(*interventions: Intervention) -> SimpleNamespace:
    return SimpleNamespace(generated_at=NOW, interventions=tuple(interventions), context=None)


def test_recommends_onboarding_before_running_intervention_engine(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)

    def fail_run_engine(*_args, **_kwargs):
        raise AssertionError("intervention engine should not run before onboarding completes")

    monkeypatch.setattr(subject, "run_engine", fail_run_engine)

    with connect(db_path) as conn:
        result = subject.recommend_skills(conn, skill_state_store=store)

    recommendation = result["data"]["recommendations"][0]
    assert result["summary"] == {
        "count": 1,
        "top_skill": "onboarding",
        "source": "onboarding_state",
        "limit": 3,
        "requested_limit": 3,
        "onboarding_complete": False,
    }
    assert recommendation["skill"] == "onboarding"
    assert recommendation["can_activate"] is False
    assert recommendation["action"]["tool"] == "plaid_link"
    assert recommendation["action"]["session_skill_context"] == "onboarding"


def test_recommendations_normalize_non_activatable_coach_skill_actions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    store.set("onboarding", {"complete": True})
    source_action = InterventionAction(
        label="Walk through spending-plan coaching",
        tool="activate_skill",
        params={"name": "coach_spending_plan"},
        build_stub=False,
    )
    first = _intervention(
        "chronic_monthly_deficit",
        action=source_action,
        priority=Priority.HIGH,
        dollar_impact_cents=50_000,
    )
    duplicate = _intervention(
        "monthly_variance_review",
        action=source_action,
        priority=Priority.MEDIUM,
        dollar_impact_cents=25_000,
    )
    monkeypatch.setattr(subject, "run_engine", lambda *_args, **_kwargs: _engine(first, duplicate))

    with connect(db_path) as conn:
        _seed_onboarding_complete_data(conn)
        result = subject.recommend_skills(conn, skill_state_store=store)

    assert result["summary"]["count"] == 1
    recommendation = result["data"]["recommendations"][0]
    assert recommendation["skill"] == "coach_spending_plan"
    assert recommendation["source"] == "intervention_engine"
    assert recommendation["can_activate"] is False
    assert recommendation["source_intervention"]["pattern_id"] == "chronic_monthly_deficit"
    assert recommendation["action"]["tool"] == "get_skill"
    assert recommendation["action"]["params"] == {"name": "coach_spending_plan"}
    assert recommendation["action"]["requires_session_start"] is True
    assert recommendation["action"]["session_skill_context"] == "coach_spending_plan"
    assert recommendation["action"]["source_action"]["tool"] == "activate_skill"


def test_debt_intervention_maps_to_debt_payoff_skill_without_discarding_source_action(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    store.set("onboarding", {"complete": True})
    intervention = _intervention(
        "D-1",
        action=InterventionAction(
            label="Run avalanche simulation",
            tool="debt_simulate",
            params={"strategy": "avalanche", "extra_cents": 0},
            build_stub=False,
        ),
        move=Move.PRESCRIBE,
        priority=Priority.HIGH,
    )
    monkeypatch.setattr(subject, "run_engine", lambda *_args, **_kwargs: _engine(intervention))

    with connect(db_path) as conn:
        _seed_onboarding_complete_data(conn)
        result = subject.recommend_skills(conn, skill_state_store=store)

    recommendation = result["data"]["recommendations"][0]
    assert recommendation["skill"] == "coach_debt_payoff"
    assert recommendation["action"]["tool"] == "get_skill"
    assert recommendation["action"]["source_action"] == {
        "label": "Run avalanche simulation",
        "tool": "debt_simulate",
        "params": {"strategy": "avalanche", "extra_cents": 0},
        "build_stub": False,
    }


def test_profile_priority_fallback_when_engine_has_no_skill_signal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    store.set(
        "onboarding",
        {
            "complete": True,
            "user_type": "salaried",
            "income_stability": "steady",
            "priority": "I want to get credit card debt under control",
            "setup_acknowledged": True,
        },
    )
    monkeypatch.setattr(subject, "run_engine", lambda *_args, **_kwargs: _engine())

    with connect(db_path) as conn:
        _seed_onboarding_complete_data(conn)
        result = subject.recommend_skills(conn, skill_state_store=store)

    recommendation = result["data"]["recommendations"][0]
    assert result["summary"]["source"] == "onboarding_profile"
    assert recommendation["skill"] == "coach_debt_payoff"
    assert recommendation["evidence"]["matched_terms"] == ["credit card", "debt"]


def test_skill_recommendations_tool_classification() -> None:
    assert "skill_recommendations" in READ_ONLY_TOOLS
    assert "skill_recommendations" in NO_SYNC_TOOLS
    assert "skill_recommendations" not in BRIDGE_TOOLS
