"""Deterministic coaching-skill recommendations."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from .intervention_engine import run_engine
from .onboarding import SkillStateReader, detect_user_state
from .skill_constants import NON_ACTIVATABLE_SKILLS, VALID_SKILLS


_CORE_COACH_SKILLS: tuple[str, ...] = (
    "coach_debt_payoff",
    "coach_emergency_fund",
    "coach_savings_goal",
    "coach_spending_plan",
)

_SKILL_TITLES: dict[str, str] = {
    "onboarding": "Onboarding",
    "coach_debt_payoff": "Debt-payoff coaching",
    "coach_emergency_fund": "Emergency-fund coaching",
    "coach_savings_goal": "Savings-goal coaching",
    "coach_spending_plan": "Spending-plan coaching",
}

_PATTERN_SKILL_MAP: dict[str, str] = {
    "D-1": "coach_debt_payoff",
    "D-3": "coach_debt_payoff",
    "dti_threshold_36": "coach_debt_payoff",
    "dti_threshold_43": "coach_debt_payoff",
    "minimum_only_payments": "coach_debt_payoff",
    "constant_payment_violation": "coach_debt_payoff",
    "liquidity_below_3_months": "coach_emergency_fund",
    "cash_flow_surplus_no_savings": "coach_emergency_fund",
    "emergency_fund_drawdown_no_replenishment": "coach_emergency_fund",
    "income_shock_detected": "coach_emergency_fund",
    "cash_flow_surplus_no_savings_goal": "coach_savings_goal",
    "savings_goal_stall": "coach_savings_goal",
    "savings_goal_milestone_hit": "coach_savings_goal",
    "chronic_monthly_deficit": "coach_spending_plan",
    "creeping_overspend_no_plan": "coach_spending_plan",
    "monthly_variance_review": "coach_spending_plan",
    "directional_variance_pattern": "coach_spending_plan",
    "cross_skill_commitment_drift": "coach_spending_plan",
}

_PROFILE_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "coach_debt_payoff",
        ("credit card", "debt", "apr", "payoff", "loan", "minimum payment"),
    ),
    (
        "coach_emergency_fund",
        ("emergency fund", "emergency", "buffer", "rainy day", "safety net"),
    ),
    (
        "coach_savings_goal",
        ("saving", "savings", "goal", "down payment", "trip", "purchase"),
    ),
    (
        "coach_spending_plan",
        ("spending", "budget", "cash flow", "cashflow", "category", "clarity"),
    ),
)


def _get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def _normalize_limit(limit: int) -> int:
    try:
        normalized = int(limit)
    except (TypeError, ValueError):
        normalized = 3
    return max(1, min(normalized, 5))


def _title_for_skill(skill: str) -> str:
    return _SKILL_TITLES.get(skill, skill.replace("_", " ").title())


def _serialize_action(action: Any) -> dict[str, Any] | None:
    if action is None:
        return None
    return {
        "label": _get(action, "label"),
        "tool": _get(action, "tool"),
        "params": dict(_get(action, "params") or {}),
        "build_stub": bool(_get(action, "build_stub", False)),
    }


def _serialize_intervention(intervention: Any) -> dict[str, Any]:
    priority = _get(intervention, "priority_rank", _get(intervention, "priority", 999))
    try:
        priority_rank = int(priority)
    except (TypeError, ValueError):
        priority_rank = 999

    fired_at = _get(intervention, "fired_at")
    if isinstance(fired_at, datetime):
        fired_at_value = fired_at.isoformat()
    else:
        fired_at_value = fired_at

    last_fired_at = _get(intervention, "last_fired_at")
    if isinstance(last_fired_at, datetime):
        last_fired_at_value = last_fired_at.isoformat()
    else:
        last_fired_at_value = last_fired_at

    return {
        "pattern_id": _get(intervention, "pattern_id"),
        "move": _enum_value(_get(intervention, "move")),
        "tiers": list(_get(intervention, "tiers") or []),
        "priority_rank": priority_rank,
        "headline": _get(intervention, "headline"),
        "detail_bullets": list(_get(intervention, "detail_bullets") or []),
        "tier4_ladder": _get(intervention, "tier4_ladder"),
        "tier4_is_fallback": bool(_get(intervention, "tier4_is_fallback", False)),
        "action": _serialize_action(_get(intervention, "action")),
        "dollar_impact_cents": int(_get(intervention, "dollar_impact_cents", 0) or 0),
        "goal_link": _get(intervention, "goal_link"),
        "log_id": _get(intervention, "log_id"),
        "fired_at": fired_at_value,
        "last_fired_at": last_fired_at_value,
    }


def _skill_action(skill: str, *, source_action: dict[str, Any] | None = None) -> dict[str, Any]:
    title = _title_for_skill(skill)
    can_activate = skill not in NON_ACTIVATABLE_SKILLS
    if can_activate:
        action = {
            "label": f"Activate {title}",
            "tool": "activate_skill",
            "params": {"name": skill},
            "requires_session_start": False,
            "session_skill_context": None,
        }
    else:
        action = {
            "label": f"Open {title} playbook",
            "tool": "get_skill",
            "params": {"name": skill},
            "requires_session_start": True,
            "session_skill_context": skill,
            "note": (
                "This skill requires session-start context. Read the playbook with get_skill, "
                "or start the next chat/session with this skill as context."
            ),
        }
    if source_action is not None:
        action["source_action"] = source_action
    return action


def _onboarding_action(next_step: Mapping[str, Any] | None) -> dict[str, Any]:
    if next_step and next_step.get("tool"):
        return {
            "label": "Continue onboarding",
            "tool": next_step["tool"],
            "params": dict(next_step.get("args") or {}),
            "requires_session_start": True,
            "session_skill_context": "onboarding",
            "playbook": {"tool": "get_skill", "params": {"name": "onboarding"}},
        }
    return _skill_action("onboarding")


def _skill_from_intervention(intervention: Any) -> str | None:
    action = _get(intervention, "action")
    if action is not None and _get(action, "tool") == "activate_skill":
        skill = str((_get(action, "params") or {}).get("name") or "")
        if skill in _CORE_COACH_SKILLS:
            return skill

    pattern_id = str(_get(intervention, "pattern_id") or "")
    mapped = _PATTERN_SKILL_MAP.get(pattern_id)
    if mapped in VALID_SKILLS:
        return mapped
    return None


def _evidence_from_intervention(serialized: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "pattern_id": serialized.get("pattern_id"),
        "headline": serialized.get("headline"),
        "detail_bullets": list(serialized.get("detail_bullets") or []),
        "dollar_impact_cents": int(serialized.get("dollar_impact_cents") or 0),
        "tier4_ladder": serialized.get("tier4_ladder"),
    }


def _recommendation_from_intervention(
    intervention: Any,
    *,
    skill: str,
    rank: int,
) -> dict[str, Any]:
    serialized = _serialize_intervention(intervention)
    evidence = _evidence_from_intervention(serialized)
    title = _title_for_skill(skill)
    headline = str(serialized.get("headline") or "").strip()
    reason = headline or f"Current intervention signals point to {title}."
    source_action = serialized.get("action")

    return {
        "skill": skill,
        "title": title,
        "reason": reason,
        "evidence": evidence,
        "priority_rank": rank,
        "source": "intervention_engine",
        "source_intervention": serialized,
        "can_activate": skill not in NON_ACTIVATABLE_SKILLS,
        "action": _skill_action(skill, source_action=source_action),
        "chat_context": {
            "skill": skill,
            "source": "intervention_engine",
            "prompt": reason,
            "evidence": evidence,
        },
    }


def _onboarding_recommendation(onboarding: Mapping[str, Any]) -> dict[str, Any]:
    data = onboarding.get("data") or {}
    next_steps = list(data.get("next_steps") or [])
    next_step = next_steps[0] if next_steps else None
    phase_summary = str(data.get("phase_summary") or "Continue onboarding.")
    reason = (
        str(next_step.get("instruction"))
        if isinstance(next_step, Mapping) and next_step.get("instruction")
        else phase_summary
    )
    evidence = {
        "phase_summary": phase_summary,
        "resume_checkpoint": data.get("resume_checkpoint"),
        "next_step": dict(next_step) if isinstance(next_step, Mapping) else None,
        "signals": dict(data.get("signals") or {}),
    }

    return {
        "skill": "onboarding",
        "title": _title_for_skill("onboarding"),
        "reason": reason,
        "evidence": evidence,
        "priority_rank": 1,
        "source": "onboarding_state",
        "source_intervention": None,
        "can_activate": False,
        "action": _onboarding_action(next_step),
        "chat_context": {
            "skill": "onboarding",
            "source": "onboarding_state",
            "prompt": reason,
            "evidence": evidence,
        },
    }


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        return " ".join(_flatten_text(item) for pair in value.items() for item in pair)
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten_text(item) for item in value)
    return str(value)


def _profile_match(profile: Mapping[str, Any]) -> tuple[str, list[str]] | None:
    haystack = _flatten_text(profile).lower()
    if not haystack:
        return None
    for skill, terms in _PROFILE_KEYWORDS:
        matches = [term for term in terms if term in haystack]
        if matches:
            return skill, matches
    return None


def _profile_recommendation(onboarding: Mapping[str, Any]) -> dict[str, Any] | None:
    data = onboarding.get("data") or {}
    profile = dict(data.get("profile") or {})
    skill_state = data.get("skill_state") or {}
    combined_profile = {**profile, "skill_state": skill_state}
    match = _profile_match(combined_profile)
    if match is None:
        return None

    skill, matched_terms = match
    title = _title_for_skill(skill)
    priority = profile.get("priority") or skill_state.get("priority")
    reason = (
        f"Onboarding profile priority '{priority}' points to {title}."
        if priority
        else f"Onboarding profile points to {title}."
    )
    evidence = {
        "profile": profile,
        "matched_terms": matched_terms,
    }

    return {
        "skill": skill,
        "title": title,
        "reason": reason,
        "evidence": evidence,
        "priority_rank": 1,
        "source": "onboarding_profile",
        "source_intervention": None,
        "can_activate": skill not in NON_ACTIVATABLE_SKILLS,
        "action": _skill_action(skill),
        "chat_context": {
            "skill": skill,
            "source": "onboarding_profile",
            "prompt": reason,
            "evidence": evidence,
        },
    }


def _source_for_recommendations(recommendations: list[dict[str, Any]]) -> str:
    if not recommendations:
        return "none"
    sources = {str(item.get("source")) for item in recommendations}
    if len(sources) == 1:
        return sources.pop()
    return "mixed"


def recommend_skills(
    conn: sqlite3.Connection,
    *,
    skill_state_store: SkillStateReader,
    rules_path: Path | None = None,
    now: datetime | None = None,
    limit: int = 3,
) -> dict[str, Any]:
    """Recommend the next coaching skill from onboarding and intervention state."""
    normalized_limit = _normalize_limit(limit)
    onboarding = detect_user_state(conn, skill_state_store)
    onboarding_data = onboarding.get("data") or {}

    recommendations: list[dict[str, Any]] = []
    if not bool(onboarding_data.get("is_onboarding_complete")):
        recommendations.append(_onboarding_recommendation(onboarding))
    else:
        engine_result = run_engine(conn, rules_path=rules_path, now=now)
        seen_skills: set[str] = set()
        for intervention in engine_result.interventions:
            skill = _skill_from_intervention(intervention)
            if skill is None or skill in seen_skills:
                continue
            seen_skills.add(skill)
            recommendations.append(
                _recommendation_from_intervention(
                    intervention,
                    skill=skill,
                    rank=len(recommendations) + 1,
                )
            )
            if len(recommendations) >= normalized_limit:
                break

        if not recommendations:
            profile_recommendation = _profile_recommendation(onboarding)
            if profile_recommendation is not None:
                recommendations.append(profile_recommendation)

    recommendations = recommendations[:normalized_limit]
    source = _source_for_recommendations(recommendations)
    next_actions = [item["action"] for item in recommendations]
    if not next_actions:
        next_actions = [
            {"tool": "onboarding_detect", "params": {}},
            {"tool": "interventions_get", "params": {"surface": "agent_prompt"}},
        ]

    return {
        "data": {
            "recommendations": recommendations,
            "onboarding": onboarding_data,
            "available_core_skills": list(_CORE_COACH_SKILLS),
            "source": source,
            "next_actions": next_actions,
        },
        "summary": {
            "count": len(recommendations),
            "top_skill": recommendations[0]["skill"] if recommendations else None,
            "source": source,
            "limit": normalized_limit,
            "requested_limit": limit,
            "onboarding_complete": bool(onboarding_data.get("is_onboarding_complete")),
        },
    }
