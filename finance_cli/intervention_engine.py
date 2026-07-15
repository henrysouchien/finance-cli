from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable

from .commands.common import fmt_dollars
from .exceptions import ConflictError, NotFoundError, ValidationError
from .interventions import behavior as _behavior  # noqa: F401
from .interventions import cash_flow as _cash_flow  # noqa: F401
from .interventions import debt as _debt  # noqa: F401
from .interventions import income as _income  # noqa: F401
from .interventions import tax as _tax  # noqa: F401
from .interventions.context import FallbackGoal, Goal, InterventionContext, build_context
from .interventions.registry import Intervention, InterventionAction, PATTERN_REGISTRY, RegisteredPattern
from .models import cents_to_dollars
from .skill_constants import NON_ACTIVATABLE_SKILLS


SURFACE_CAPS = {
    "dashboard": 1,
    "action_queue": 5,
    "agent_prompt": 3,
}
LOG_SURFACES = {"dashboard", "action_queue", "agent_prompt", "chat", "telegram", "email", "cli"}


@dataclass(frozen=True)
class EngineResult:
    generated_at: datetime
    interventions: tuple[Intervention, ...]
    context: InterventionContext

    def get_for_surface(self, surface: str) -> tuple[Intervention, ...]:
        if surface not in SURFACE_CAPS:
            raise ValueError(f"Unknown surface: {surface}")
        interventions = self.interventions
        if surface == "action_queue":
            interventions = tuple(item for item in interventions if item.action is not None)
        return interventions[: SURFACE_CAPS[surface]]


def _select_goal(ctx: InterventionContext) -> Goal | FallbackGoal:
    for goal in ctx.goals:
        if goal.target_cents is not None and goal.target_cents > 0:
            return goal
    return ctx.fallback_goal


def _goal_weeks_delta(ctx: InterventionContext, goal_delta_cents: int) -> int | None:
    if int(goal_delta_cents) == 0:
        return None
    goal = _select_goal(ctx)
    target_cents = goal.target_cents
    if target_cents <= 0:
        return None
    return max(1, int(round((abs(int(goal_delta_cents)) / target_cents) * 52)))


def _render_tier4_ladder(ctx: InterventionContext, goal_delta_cents: int) -> tuple[str | None, bool, str | None]:
    weeks = _goal_weeks_delta(ctx, goal_delta_cents)
    goal = _select_goal(ctx)
    if weeks is None:
        return None, isinstance(goal, FallbackGoal), None if isinstance(goal, FallbackGoal) else goal.id
    if isinstance(goal, FallbackGoal):
        if goal_delta_cents < 0:
            return (
                f"If you're aiming at a 3-month emergency fund (~{fmt_dollars(cents_to_dollars(goal.target_cents))}), "
                f"this puts about {weeks} weeks of progress at risk.",
                True,
                None,
            )
        return (
            f"If you're aiming at a 3-month emergency fund (~{fmt_dollars(cents_to_dollars(goal.target_cents))}), "
            f"that's {weeks} weeks faster. Want me to lock that as your goal?",
            True,
            None,
        )
    if goal_delta_cents < 0:
        return (
            f"That's about {weeks} weeks further from {goal.name}.",
            False,
            goal.id,
        )
    return (
        f"That's about {weeks} weeks faster to {goal.name}.",
        False,
        goal.id,
    )


def _goal_ladder_delta_cents(intervention: Intervention) -> int:
    if intervention.goal_ladder_delta_cents is not None:
        return int(intervention.goal_ladder_delta_cents)
    return int(intervention.dollar_impact_cents)


def _enforce_anti_patterns(
    intervention: Intervention,
    ctx: InterventionContext,
    registered: RegisteredPattern,
) -> bool:
    if intervention.pattern_id in ctx.muted_patterns:
        return False

    recent_fire = ctx.recent_fires.get(intervention.pattern_id)
    if recent_fire is not None and recent_fire > (ctx.now - registered.cooldown):
        return False

    recent_dismissal = ctx.recent_dismissals.get(intervention.pattern_id)
    if recent_dismissal is not None and recent_dismissal > (ctx.now - timedelta(days=30)):
        return False

    if registered.context_check is not None and not registered.context_check(ctx):
        return False

    if not ctx.strategy_prefs.is_empty() and registered.strategy_check is not None:
        if not registered.strategy_check(ctx):
            return False

    return True


def rank_interventions(interventions: Iterable[Intervention]) -> tuple[Intervention, ...]:
    return tuple(
        sorted(
            interventions,
            key=lambda item: (
                int(item.priority),
                -int(item.dollar_impact_cents),
                item.last_fired_at or datetime.min,
                item.pattern_id,
            ),
        )
    )


def _apply_tier4_ladders(ctx: InterventionContext, interventions: Iterable[Intervention]) -> list[Intervention]:
    result: list[Intervention] = []
    for intervention in interventions:
        if 4 not in intervention.tiers:
            result.append(intervention)
            continue
        ladder, is_fallback, goal_link = _render_tier4_ladder(ctx, _goal_ladder_delta_cents(intervention))
        result.append(
            replace(
                intervention,
                tier4_ladder=ladder,
                tier4_is_fallback=is_fallback,
                goal_link=goal_link,
            )
        )
    return result


def _render_accelerator_suggestion(ctx: InterventionContext, peer: Intervention | None) -> str:
    if peer is None:
        return ""
    goal_delta_cents = _goal_ladder_delta_cents(peer)
    if goal_delta_cents <= 0:
        return ""
    weeks = _goal_weeks_delta(ctx, goal_delta_cents)
    amount = fmt_dollars(cents_to_dollars(goal_delta_cents))
    if weeks is None:
        return f"The fastest lever right now is another {amount} of impact."
    return f"The fastest lever right now is another {amount} of impact, about {weeks} weeks faster."


def _enrich_c5_accelerator(ctx: InterventionContext, interventions: tuple[Intervention, ...]) -> tuple[Intervention, ...]:
    peer = next(
        (
            item
            for item in interventions
            if item.pattern_id != "C-5" and any(tier in (1, 2) for tier in item.tiers)
        ),
        None,
    )
    enriched: list[Intervention] = []
    for intervention in interventions:
        if intervention.pattern_id != "C-5":
            enriched.append(intervention)
            continue
        suggestion = _render_accelerator_suggestion(ctx, peer)
        headline = intervention.headline.replace("{accelerator_suggestion}", suggestion).strip()
        headline = " ".join(headline.split())
        enriched.append(replace(intervention, headline=headline))
    return tuple(enriched)


def run_engine(
    conn: sqlite3.Connection,
    *,
    rules_path: Path | None = None,
    now: datetime | None = None,
    data_dir: Path | None = None,
) -> EngineResult:
    ctx = build_context(conn, now=now, rules_path=rules_path, data_dir=data_dir)
    candidates: list[Intervention] = []
    for pattern_id in sorted(PATTERN_REGISTRY):
        registered = PATTERN_REGISTRY[pattern_id]
        candidate = registered.evaluate(conn, ctx)
        if candidate is None:
            continue
        if not _enforce_anti_patterns(candidate, ctx, registered):
            continue
        candidates.append(candidate)
    ranked = rank_interventions(_apply_tier4_ladders(ctx, candidates))
    ranked = _enrich_c5_accelerator(ctx, ranked)
    return EngineResult(generated_at=ctx.now, interventions=ranked, context=ctx)


def _parse_logged_at(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value))


def _serialize_log_row(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def _serialize_action(value: InterventionAction) -> dict[str, Any]:
    action = {
        "label": value.label,
        "tool": value.tool,
        "params": value.params,
        "build_stub": value.build_stub,
    }
    if value.tool != "activate_skill":
        return action

    skill_name = str(value.params.get("name") or "")
    if skill_name not in NON_ACTIVATABLE_SKILLS:
        return action

    return {
        "label": value.label,
        "tool": "get_skill",
        "params": {"name": skill_name},
        "build_stub": value.build_stub,
        "requires_session_start": True,
        "session_skill_context": skill_name,
        "note": (
            "This skill requires session-start context. Read the playbook with get_skill, "
            "or start the next chat/session with this skill as context."
        ),
        "source_action": action,
    }


def serialize(value: EngineResult | Intervention | InterventionAction | Any) -> Any:
    if isinstance(value, EngineResult):
        return {
            "generated_at": value.generated_at.isoformat(),
            "all": [serialize(item) for item in value.interventions],
            "dashboard": [serialize(item) for item in value.get_for_surface("dashboard")],
            "action_queue": [serialize(item) for item in value.get_for_surface("action_queue")],
            "agent_prompt": [serialize(item) for item in value.get_for_surface("agent_prompt")],
        }
    if isinstance(value, Intervention):
        return {
            "pattern_id": value.pattern_id,
            "move": value.move.value,
            "tiers": list(value.tiers),
            "priority_rank": int(value.priority),
            "headline": value.headline,
            "detail_bullets": list(value.detail_bullets),
            "tier4_ladder": value.tier4_ladder,
            "tier4_is_fallback": value.tier4_is_fallback,
            "action": serialize(value.action),
            "dollar_impact_cents": int(value.dollar_impact_cents),
            "dollar_impact": cents_to_dollars(int(value.dollar_impact_cents)),
            "goal_link": value.goal_link,
            "log_id": value.log_id,
            "fired_at": value.fired_at.isoformat(),
            "last_fired_at": value.last_fired_at.isoformat() if value.last_fired_at else None,
        }
    if isinstance(value, InterventionAction):
        return _serialize_action(value)
    return value


def evaluate_for_surface(
    conn: sqlite3.Connection,
    surface: str,
    *,
    rules_path: Path | None = None,
    log_to_surface: str | None = None,
    now: datetime | None = None,
) -> tuple[EngineResult, tuple[Intervention, ...]]:
    """Run the engine, slice for surface, optionally log fires.

    Returns (full_engine_result, logged_surfaced) so callers that need
    total_candidates for the envelope get it from engine_result.
    """
    engine_result = run_engine(conn, rules_path=rules_path, now=now)
    surfaced = engine_result.get_for_surface(surface)
    if log_to_surface is not None:
        surfaced = log_fires(conn, surfaced, surface=log_to_surface, now=now)
    return engine_result, surfaced


def build_surface_envelope(
    engine_result: EngineResult,
    surfaced: tuple[Intervention, ...],
    surface: str,
) -> dict[str, Any]:
    """Serialize (engine_result, surfaced) into the standard API envelope.

    CLI-specific fields such as log_surface and cli_report intentionally stay
    in intervention_cmd.handle_list.
    """
    serialized = [serialize(item) for item in surfaced]
    return {
        "data": {"surface": surface, "interventions": serialized},
        "summary": {
            "count": len(serialized),
            "surface": surface,
            "total_candidates": len(engine_result.interventions),
        },
    }


def log_fires(
    conn: sqlite3.Connection,
    interventions: Iterable[Intervention],
    *,
    surface: str,
    now: datetime | None = None,
) -> tuple[Intervention, ...]:
    if surface not in LOG_SURFACES:
        raise ValueError(f"Unknown log surface: {surface}")

    resolved_now = (now or datetime.now()).replace(microsecond=0)
    dedup_cutoff = (resolved_now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    logged: list[Intervention] = []

    conn.execute("BEGIN IMMEDIATE")
    try:
        for intervention in interventions:
            existing = conn.execute(
                """
                SELECT id, fired_at
                  FROM intervention_log
                 WHERE pattern_id = ?
                   AND surface = ?
                   AND user_action = 'pending'
                   AND fired_at >= ?
                 ORDER BY fired_at DESC, id DESC
                 LIMIT 1
                """,
                (intervention.pattern_id, surface, dedup_cutoff),
            ).fetchone()
            if existing is not None:
                logged.append(
                    replace(
                        intervention,
                        log_id=int(existing["id"]),
                        fired_at=_parse_logged_at(existing["fired_at"]) or intervention.fired_at,
                    )
                )
                continue

            fired_at = intervention.fired_at.replace(microsecond=0)
            payload = json.dumps(serialize(intervention), sort_keys=True)
            cursor = conn.execute(
                """
                INSERT INTO intervention_log (
                    pattern_id, fired_at, surface, user_action, dollar_impact_cents, goal_link, headline, payload
                ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)
                """,
                (
                    intervention.pattern_id,
                    fired_at.strftime("%Y-%m-%d %H:%M:%S"),
                    surface,
                    int(intervention.dollar_impact_cents),
                    intervention.goal_link,
                    intervention.headline,
                    payload,
                ),
            )
            logged.append(replace(intervention, log_id=int(cursor.lastrowid), fired_at=fired_at))
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return tuple(logged)


def record_action(
    conn: sqlite3.Connection,
    log_id: int,
    action: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Transition an intervention_log row from pending to acted/dismissed."""
    if action not in ("acted", "dismissed"):
        raise ValidationError(f"Invalid action: {action!r}. Must be 'acted' or 'dismissed'.")

    resolved_now = (now or datetime.now()).replace(microsecond=0)
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM intervention_log WHERE id = ?", (log_id,)).fetchone()
        if row is None:
            conn.rollback()
            raise NotFoundError(f"Intervention log entry {log_id} not found.")
        if row["user_action"] == action:
            conn.rollback()
            return _serialize_log_row(row)
        if row["user_action"] != "pending":
            conn.rollback()
            raise ConflictError(
                f"Cannot {action} intervention {log_id}: already in state '{row['user_action']}'."
            )
        conn.execute(
            "UPDATE intervention_log SET user_action = ?, acted_at = ? WHERE id = ?",
            (action, resolved_now.strftime("%Y-%m-%d %H:%M:%S"), log_id),
        )
        conn.commit()
    except (NotFoundError, ConflictError, ValidationError):
        raise
    except Exception:
        conn.rollback()
        raise

    updated = conn.execute("SELECT * FROM intervention_log WHERE id = ?", (log_id,)).fetchone()
    if updated is None:
        raise NotFoundError(f"Intervention log entry {log_id} not found.")
    return _serialize_log_row(updated)
