from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from pathlib import Path
import threading

import pytest

import finance_cli.intervention_engine as intervention_engine
from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import (
    EngineResult,
    _enforce_anti_patterns,
    build_surface_envelope,
    evaluate_for_surface,
    log_fires,
    rank_interventions,
    run_engine,
)
from finance_cli.interventions.context import FallbackGoal, InterventionContext, StrategyPrefs
from finance_cli.interventions.registry import Intervention, InterventionAction, Move, Priority, RegisteredPattern


NOW = datetime(2026, 4, 9, 12, 0, 0)


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _category_id(conn, name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, ?, 0, 0)
        """,
        (category_id, name, 1 if name.startswith("Income") else 0),
    )
    conn.commit()
    return category_id


def _seed_account(
    conn,
    *,
    account_type: str = "checking",
    balance_cents: int = 0,
    institution_name: str = "Bank",
    account_name: str | None = None,
    is_active: int = 1,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (account_id, institution_name, account_name or account_type, account_type, balance_cents, is_active),
    )
    conn.commit()
    return account_id


def _seed_transaction(
    conn,
    *,
    account_id: str,
    category_name: str,
    amount_cents: int,
    txn_date: str,
    use_type: str | None = None,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id, use_type,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, 'seed', ?, ?, ?, 0, 1, 1, 'manual')
        """,
        (txn_id, account_id, txn_date, amount_cents, _category_id(conn, category_name), use_type),
    )
    conn.commit()
    return txn_id


def _seed_goal(conn, *, name: str, metric: str, target_cents: int, direction: str = "up") -> str:
    goal_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO goals (
            id, name, metric, target_cents, starting_cents, direction, is_active
        ) VALUES (?, ?, ?, ?, 0, ?, 1)
        """,
        (goal_id, name, metric, target_cents, direction),
    )
    conn.commit()
    return goal_id


def _seed_credit_liability(conn, *, account_id: str, apr_purchase: float, minimum_payment_cents: int = 2_000) -> str:
    liability_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active, apr_purchase, minimum_payment_cents
        ) VALUES (?, ?, 'credit', 1, ?, ?)
        """,
        (liability_id, account_id, apr_purchase, minimum_payment_cents),
    )
    conn.commit()
    return liability_id


def _seed_intervention_log(
    conn,
    *,
    pattern_id: str,
    fired_at: str,
    user_action: str = "pending",
    acted_at: str | None = None,
    surface: str = "cli",
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO intervention_log (
            pattern_id, fired_at, surface, user_action, acted_at, headline, payload
        ) VALUES (?, ?, ?, ?, ?, 'seed headline', '{}')
        """,
        (pattern_id, fired_at, surface, user_action, acted_at),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _make_context() -> InterventionContext:
    return InterventionContext(
        now=NOW,
        data_dir=None,
        rules_path=None,
        goals=(),
        fallback_goal=FallbackGoal(label="3-month emergency fund", target_cents=300_000, is_fallback=True),
        strategy_prefs=StrategyPrefs(),
        trailing_3mo_avg_expense_cents=100_000,
        trailing_6mo_avg_expense_cents=90_000,
        recent_fires={},
        recent_dismissals={},
        muted_patterns=frozenset(),
    )


def _make_intervention(
    pattern_id: str,
    *,
    priority: Priority = Priority.MEDIUM,
    dollar_impact_cents: int = 100,
    goal_ladder_delta_cents: int | None = None,
    tiers: tuple[int, ...] = (1,),
    last_fired_at: datetime | None = None,
    action: bool = False,
) -> Intervention:
    return Intervention(
        pattern_id=pattern_id,
        move=Move.WARN,
        tiers=tiers,
        priority=priority,
        headline=f"{pattern_id} headline",
        detail_bullets=(),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=(
            InterventionAction(
                label=f"Run {pattern_id}",
                tool="test_tool",
                params={"pattern_id": pattern_id},
                build_stub=False,
            )
            if action
            else None
        ),
        dollar_impact_cents=dollar_impact_cents,
        goal_link=None,
        log_id=None,
        fired_at=NOW,
        last_fired_at=last_fired_at,
        goal_ladder_delta_cents=goal_ladder_delta_cents,
    )


def test_rank_interventions_orders_by_priority_then_dollar_then_freshness_then_pattern_id() -> None:
    ranked = rank_interventions(
        [
            _make_intervention("B-1", priority=Priority.MEDIUM, dollar_impact_cents=500, last_fired_at=NOW),
            _make_intervention("A-1", priority=Priority.HIGH, dollar_impact_cents=1),
            _make_intervention("C-1", priority=Priority.MEDIUM, dollar_impact_cents=500, last_fired_at=None),
            _make_intervention("D-1", priority=Priority.MEDIUM, dollar_impact_cents=500, last_fired_at=None),
        ]
    )
    assert [item.pattern_id for item in ranked] == ["A-1", "C-1", "D-1", "B-1"]


def test_surface_caps_are_applied_at_read_time() -> None:
    result = EngineResult(
        generated_at=NOW,
        interventions=tuple(_make_intervention(f"X-{idx}", action=True) for idx in range(1, 7)),
        context=_make_context(),
    )
    assert len(result.get_for_surface("dashboard")) == 1
    assert len(result.get_for_surface("action_queue")) == 5
    assert len(result.get_for_surface("agent_prompt")) == 3


def test_action_queue_filters_actionless_observations_before_cap() -> None:
    result = EngineResult(
        generated_at=NOW,
        interventions=(
            tuple(_make_intervention(f"OBS-{idx}", priority=Priority.HIGH) for idx in range(1, 7))
            + tuple(_make_intervention(f"ACT-{idx}", priority=Priority.LOW, action=True) for idx in range(1, 7))
        ),
        context=_make_context(),
    )

    assert [item.pattern_id for item in result.get_for_surface("action_queue")] == [
        "ACT-1",
        "ACT-2",
        "ACT-3",
        "ACT-4",
        "ACT-5",
    ]
    assert result.get_for_surface("dashboard")[0].pattern_id == "OBS-1"


def test_serialize_non_activatable_skill_action_uses_session_context_contract() -> None:
    action = InterventionAction(
        label="Reconcile in coach_spending_plan Phase 5",
        tool="activate_skill",
        params={"name": "coach_spending_plan"},
        build_stub=False,
    )

    serialized = intervention_engine.serialize(action)

    assert serialized["label"] == "Reconcile in coach_spending_plan Phase 5"
    assert serialized["tool"] == "get_skill"
    assert serialized["params"] == {"name": "coach_spending_plan"}
    assert serialized["requires_session_start"] is True
    assert serialized["session_skill_context"] == "coach_spending_plan"
    assert "requires session-start context" in serialized["note"]
    assert serialized["source_action"] == {
        "label": "Reconcile in coach_spending_plan Phase 5",
        "tool": "activate_skill",
        "params": {"name": "coach_spending_plan"},
        "build_stub": False,
    }


def test_serialize_activatable_skill_action_keeps_activate_contract() -> None:
    action = InterventionAction(
        label="Build normalizer",
        tool="activate_skill",
        params={"name": "normalizer_builder"},
        build_stub=False,
    )

    assert intervention_engine.serialize(action) == {
        "label": "Build normalizer",
        "tool": "activate_skill",
        "params": {"name": "normalizer_builder"},
        "build_stub": False,
    }


def test_enforce_anti_patterns_blocks_cooldown_dismissal_and_wrong_context() -> None:
    intervention = _make_intervention("D-1")
    registered = RegisteredPattern(
        id="D-1",
        move=Move.PRESCRIBE,
        tiers=(1, 4),
        priority=Priority.HIGH,
        cooldown=timedelta(days=30),
        tool="debt_simulate",
        evaluate=lambda conn, ctx: None,
        context_check=lambda ctx: False,
    )
    ctx = InterventionContext(
        now=NOW,
        data_dir=None,
        rules_path=None,
        goals=(),
        fallback_goal=FallbackGoal(label="3-month emergency fund", target_cents=300_000, is_fallback=True),
        strategy_prefs=StrategyPrefs(),
        trailing_3mo_avg_expense_cents=100_000,
        trailing_6mo_avg_expense_cents=90_000,
        recent_fires={"D-1": NOW - timedelta(days=1)},
        recent_dismissals={},
        muted_patterns=frozenset(),
    )
    assert _enforce_anti_patterns(intervention, ctx, registered) is False

    ctx = InterventionContext(
        now=NOW,
        data_dir=None,
        rules_path=None,
        goals=(),
        fallback_goal=FallbackGoal(label="3-month emergency fund", target_cents=300_000, is_fallback=True),
        strategy_prefs=StrategyPrefs(),
        trailing_3mo_avg_expense_cents=100_000,
        trailing_6mo_avg_expense_cents=90_000,
        recent_fires={},
        recent_dismissals={"D-1": NOW - timedelta(days=2)},
        muted_patterns=frozenset(),
    )
    assert _enforce_anti_patterns(intervention, ctx, registered) is False

    ctx = _make_context()
    assert _enforce_anti_patterns(intervention, ctx, registered) is False


def test_enforce_anti_patterns_blocks_muted_pattern() -> None:
    intervention = _make_intervention("D-1")
    registered = RegisteredPattern(
        id="D-1",
        move=Move.PRESCRIBE,
        tiers=(1, 4),
        priority=Priority.HIGH,
        cooldown=timedelta(days=30),
        tool="debt_simulate",
        evaluate=lambda conn, ctx: None,
    )
    ctx = InterventionContext(
        now=NOW,
        data_dir=None,
        rules_path=None,
        goals=(),
        fallback_goal=FallbackGoal(label="3-month emergency fund", target_cents=300_000, is_fallback=True),
        strategy_prefs=StrategyPrefs(),
        trailing_3mo_avg_expense_cents=100_000,
        trailing_6mo_avg_expense_cents=90_000,
        recent_fires={},
        recent_dismissals={},
        muted_patterns=frozenset({"D-1"}),
    )

    assert _enforce_anti_patterns(intervention, ctx, registered) is False


def test_run_engine_empty_state(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = run_engine(conn, now=NOW)
    assert result.interventions == ()


def test_run_engine_applies_fallback_goal_ladder(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=50_000)
        for month in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(conn, account_id=checking_id, category_name="Income: Salary", amount_cents=200_000, txn_date=month)
            _seed_transaction(conn, account_id=checking_id, category_name="Rent", amount_cents=-100_000, txn_date=month)

        result = run_engine(conn, now=NOW)

    c5 = next(item for item in result.interventions if item.pattern_id == "C-5")
    assert c5.tier4_is_fallback is True
    assert c5.tier4_ladder is not None
    assert "3-month emergency fund" in c5.tier4_ladder


def test_tier4_ladder_uses_signed_goal_delta_for_shortfalls() -> None:
    shortfall = _make_intervention(
        "C-4",
        tiers=(1, 4),
        dollar_impact_cents=30_000,
        goal_ladder_delta_cents=-30_000,
    )

    [result] = intervention_engine._apply_tier4_ladders(_make_context(), [shortfall])

    assert result.dollar_impact_cents == 30_000
    assert result.tier4_is_fallback is True
    assert result.tier4_ladder is not None
    assert "progress at risk" in result.tier4_ladder
    assert "faster" not in result.tier4_ladder


def test_run_engine_respects_cooldown_and_dismissal_history(db_path: Path) -> None:
    with connect(db_path) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=50_000)
        for month in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(conn, account_id=checking_id, category_name="Income: Salary", amount_cents=200_000, txn_date=month)
            _seed_transaction(conn, account_id=checking_id, category_name="Rent", amount_cents=-100_000, txn_date=month)

        baseline = run_engine(conn, now=NOW)
        assert any(item.pattern_id == "C-5" for item in baseline.interventions)

        _seed_intervention_log(conn, pattern_id="C-5", fired_at="2026-04-01 12:00:00", surface="agent_prompt")
        cooled_down = run_engine(conn, now=NOW)
        assert all(item.pattern_id != "C-5" for item in cooled_down.interventions)

    second_db = db_path.parent / "finance_dismissal.db"
    initialize_database(second_db)
    with connect(second_db) as conn:
        checking_id = _seed_account(conn, account_type="checking", balance_cents=50_000)
        for month in ("2026-01-15", "2026-02-15", "2026-03-15"):
            _seed_transaction(conn, account_id=checking_id, category_name="Income: Salary", amount_cents=200_000, txn_date=month)
            _seed_transaction(conn, account_id=checking_id, category_name="Rent", amount_cents=-100_000, txn_date=month)

        _seed_intervention_log(
            conn,
            pattern_id="C-5",
            fired_at="2026-03-20 09:00:00",
            user_action="dismissed",
            acted_at="2026-04-05 09:00:00",
            surface="agent_prompt",
        )
        dismissed = run_engine(conn, now=NOW)
        assert all(item.pattern_id != "C-5" for item in dismissed.interventions)


def test_log_fires_dedups_pending_rows_within_one_hour_per_surface(db_path: Path) -> None:
    with connect(db_path) as conn:
        intervention = _make_intervention("Z-1")
        first = log_fires(conn, [intervention], surface="cli", now=NOW)
        second = log_fires(conn, [intervention], surface="cli", now=NOW + timedelta(minutes=20))
        third = log_fires(conn, [intervention], surface="agent_prompt", now=NOW + timedelta(minutes=20))

        count = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]

    assert first[0].log_id == second[0].log_id
    assert first[0].log_id != third[0].log_id
    assert int(count) == 2


def test_evaluate_for_surface_without_logging_is_pure_read(db_path: Path) -> None:
    with connect(db_path) as conn:
        high = _seed_account(conn, account_type="credit_card", balance_cents=-90_000, institution_name="High")
        mid = _seed_account(conn, account_type="credit_card", balance_cents=-30_000, institution_name="Mid")
        low = _seed_account(conn, account_type="credit_card", balance_cents=-5_000, institution_name="Low")
        _seed_credit_liability(conn, account_id=high, apr_purchase=29.99, minimum_payment_cents=3_000)
        _seed_credit_liability(conn, account_id=mid, apr_purchase=19.99, minimum_payment_cents=500)
        _seed_credit_liability(conn, account_id=low, apr_purchase=9.99, minimum_payment_cents=200)

        result, surfaced = evaluate_for_surface(conn, "agent_prompt", log_to_surface=None, now=NOW)
        count = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]

    assert isinstance(result, EngineResult)
    assert surfaced
    assert surfaced[0].pattern_id == "D-1"
    assert int(count) == 0


def test_evaluate_for_surface_with_log_to_surface_logs_agent_prompt(db_path: Path) -> None:
    with connect(db_path) as conn:
        high = _seed_account(conn, account_type="credit_card", balance_cents=-90_000, institution_name="High")
        mid = _seed_account(conn, account_type="credit_card", balance_cents=-30_000, institution_name="Mid")
        low = _seed_account(conn, account_type="credit_card", balance_cents=-5_000, institution_name="Low")
        _seed_credit_liability(conn, account_id=high, apr_purchase=29.99, minimum_payment_cents=3_000)
        _seed_credit_liability(conn, account_id=mid, apr_purchase=19.99, minimum_payment_cents=500)
        _seed_credit_liability(conn, account_id=low, apr_purchase=9.99, minimum_payment_cents=200)

        _, surfaced = evaluate_for_surface(conn, "agent_prompt", log_to_surface="agent_prompt", now=NOW)
        row = conn.execute("SELECT surface FROM intervention_log").fetchone()

    assert surfaced[0].log_id is not None
    assert row["surface"] == "agent_prompt"


def test_build_surface_envelope_has_no_cli_metadata() -> None:
    result = EngineResult(
        generated_at=NOW,
        interventions=(_make_intervention("D-1"), _make_intervention("C-1")),
        context=_make_context(),
    )
    surfaced = result.get_for_surface("dashboard")

    envelope = build_surface_envelope(result, surfaced, "dashboard")

    assert envelope["data"]["surface"] == "dashboard"
    assert len(envelope["data"]["interventions"]) == 1
    assert envelope["summary"]["total_candidates"] == 2
    assert "log_surface" not in envelope["data"]
    assert "cli_report" not in envelope


def test_log_fires_serializes_concurrent_dedup_with_begin_immediate(db_path: Path) -> None:
    barrier = threading.Barrier(5)
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            with connect(db_path, busy_timeout=5000) as conn:
                intervention = _make_intervention("Z-2")
                barrier.wait()
                log_fires(conn, [intervention], surface="agent_prompt", now=NOW)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with connect(db_path) as conn:
        count = conn.execute(
            """
            SELECT COUNT(*) AS cnt
              FROM intervention_log
             WHERE pattern_id = 'Z-2'
               AND surface = 'agent_prompt'
               AND user_action = 'pending'
            """
        ).fetchone()["cnt"]

    assert errors == []
    assert int(count) == 1


def test_log_fires_rolls_back_on_exception(db_path: Path, monkeypatch) -> None:
    original_serialize = intervention_engine.serialize

    def flaky_serialize(value):
        if isinstance(value, Intervention) and value.pattern_id == "BAD":
            raise RuntimeError("boom")
        return original_serialize(value)

    monkeypatch.setattr(intervention_engine, "serialize", flaky_serialize)

    with connect(db_path) as conn:
        with pytest.raises(RuntimeError, match="boom"):
            log_fires(
                conn,
                [_make_intervention("OK"), _make_intervention("BAD")],
                surface="agent_prompt",
                now=NOW,
            )
        count = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]

    assert int(count) == 0
