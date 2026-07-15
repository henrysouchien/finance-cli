"""Read-side progress tracking for coaching skills."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping

import yaml

from .onboarding import SkillStateReader
from .skill_recommendations import recommend_skills


COACHING_SKILLS: tuple[str, ...] = (
    "coach_debt_payoff",
    "coach_emergency_fund",
    "coach_savings_goal",
    "coach_spending_plan",
    "coach_homebuying_readiness",
    "coach_retirement_contribution_readiness",
    "coach_retirement_income_readiness",
    "coach_investment_readiness",
    "coach_financial_plan_intake",
    "coach_estate_document_readiness",
    "coach_risk_insurance_readiness",
    "coach_advisor_handoff_readiness",
)

_SKILL_TITLES: dict[str, str] = {
    "coach_debt_payoff": "Debt-payoff coaching",
    "coach_emergency_fund": "Emergency-fund coaching",
    "coach_savings_goal": "Savings-goal coaching",
    "coach_spending_plan": "Spending-plan coaching",
    "coach_homebuying_readiness": "Homebuying-readiness coaching",
    "coach_retirement_contribution_readiness": "Retirement contribution-readiness coaching",
    "coach_retirement_income_readiness": "Retirement income-readiness coaching",
    "coach_investment_readiness": "Investment-readiness coaching",
    "coach_financial_plan_intake": "Financial planning snapshot",
    "coach_estate_document_readiness": "Estate document-readiness coaching",
    "coach_risk_insurance_readiness": "Risk and insurance readiness coaching",
    "coach_advisor_handoff_readiness": "Advisor handoff readiness",
}

_TOTAL_PHASES = 10
_ARTIFACT_FILENAME_RE = re.compile(r"^(\d{8})(?:-r(\d+))?\.md$")


def _latest_artifact_path(artifact_dir: Path) -> Path | None:
    candidates: list[tuple[str, int, Path]] = []
    if not artifact_dir.exists():
        return None
    for path in artifact_dir.glob("*.md"):
        match = _ARTIFACT_FILENAME_RE.match(path.name)
        if match is None:
            continue
        revision = int(match.group(2)) if match.group(2) else 1
        candidates.append((match.group(1), revision, path))
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1][2]


def _parse_yaml_footer(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1:fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _artifact_for_skill(data_dir: Path, skill: str) -> dict[str, Any]:
    artifact_dir = data_dir / "artifacts" / skill
    artifact_path = _latest_artifact_path(artifact_dir)
    if artifact_path is None:
        return {"found": False, "reason": "no_artifact"}

    payload = _parse_yaml_footer(artifact_path.read_text(encoding="utf-8"))
    generated_at = payload.get("generated_at")
    return {
        "found": True,
        "artifact_path": str(artifact_path),
        "artifact_name": artifact_path.name,
        "generated_at": generated_at,
        "last_modified_at": payload.get("last_modified_at") or generated_at,
        "payload": payload,
    }


def _parse_date_stem(stem: str) -> date | None:
    try:
        return date.fromisoformat(stem)
    except (TypeError, ValueError):
        return None


def _parse_iso_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except (TypeError, ValueError):
        return None


def _session_markers(data_dir: Path, skill: str) -> dict[str, Any]:
    sessions_dir = data_dir / "sessions"
    if not sessions_dir.exists():
        return {
            "markers": [],
            "completed_phase_numbers": [],
            "first_session_date": None,
            "last_session_date": None,
        }

    marker_re = re.compile(rf"\b{re.escape(skill)}:phase(?P<phase>\d+)_[A-Za-z0-9_-]+")
    markers: list[dict[str, Any]] = []
    for path in sorted(sessions_dir.glob("*.md")):
        file_date = _parse_date_stem(path.stem)
        if file_date is None:
            continue
        content = path.read_text(encoding="utf-8")
        for match in marker_re.finditer(content):
            markers.append(
                {
                    "date": file_date.isoformat(),
                    "marker": match.group(0),
                    "phase": int(match.group("phase")),
                }
            )

    phase_numbers = sorted({int(item["phase"]) for item in markers})
    dates = [str(item["date"]) for item in markers]
    return {
        "markers": markers,
        "completed_phase_numbers": phase_numbers,
        "first_session_date": min(dates) if dates else None,
        "last_session_date": max(dates) if dates else None,
    }


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _latest_timestamp(*values: Any) -> str | None:
    parsed = [item for item in (_parse_datetime(value) for value in values) if item is not None]
    if not parsed:
        return None
    return max(parsed).isoformat()


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _outcomes_for_skill(skill: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}

    if skill == "coach_debt_payoff":
        return {
            "monthly_commitment_cents": _as_int(payload.get("monthly_commitment_cents")),
            "target_date": payload.get("target_debt_free_date"),
            "monitoring_cadence": payload.get("monitoring_cadence"),
            "next_check_in": payload.get("next_check_in"),
            "strategy": payload.get("strategy"),
            "debts_in_scope_count": len(payload.get("debts_in_scope") or []),
            "check_in_count": len(payload.get("check_ins") or []),
        }
    if skill == "coach_emergency_fund":
        return {
            "monthly_commitment_cents": _as_int(payload.get("monthly_commitment_cents")),
            "target_balance_cents": _as_int(payload.get("target_balance_cents")),
            "current_balance_cents": _as_int(payload.get("current_liquid_balance_cents")),
            "gap_cents": _as_int(payload.get("gap_cents")),
            "target_date": payload.get("target_met_date"),
            "target_phase": payload.get("target_phase"),
            "monitoring_cadence": payload.get("monitoring_cadence"),
            "next_check_in": payload.get("next_check_in"),
            "classified_drawdown_count": len(payload.get("drawdown_events_classified") or []),
        }
    if skill == "coach_savings_goal":
        return {
            "goal_name": payload.get("goal_name"),
            "monthly_commitment_cents": _as_int(payload.get("monthly_commitment_cents")),
            "target_balance_cents": _as_int(payload.get("target_balance_cents")),
            "current_balance_cents": _as_int(payload.get("current_balance_toward_goal_cents")),
            "gap_cents": _as_int(payload.get("gap_cents")),
            "target_date": payload.get("target_met_date"),
            "target_phase": payload.get("target_phase"),
            "monitoring_cadence": payload.get("monitoring_cadence"),
            "next_check_in": payload.get("next_check_in"),
            "milestone_count": len(payload.get("milestones") or []),
        }
    if skill == "coach_spending_plan":
        return {
            "strategy": payload.get("strategy"),
            "expected_monthly_income_cents": _as_int(payload.get("expected_monthly_income_cents")),
            "expected_monthly_expenses_cents": _as_int(payload.get("expected_monthly_expenses_cents")),
            "review_cadence": payload.get("review_cadence"),
            "next_check_in": payload.get("next_review_at"),
            "variance_record_count": len(payload.get("variance_history") or []),
            "reconciliation_count": len(payload.get("reconciliation_decisions") or []),
        }
    if skill == "coach_homebuying_readiness":
        household_profile = _as_mapping(payload.get("household_profile"))
        cash_to_close = _as_mapping(payload.get("cash_to_close"))
        ratios = _as_mapping(payload.get("ratios"))
        scenarios_value = payload.get("affordability_scenarios")
        scenarios = scenarios_value if isinstance(scenarios_value, list) else []
        first_scenario = _as_mapping(scenarios[0]) if scenarios else {}
        return {
            "readiness_status": payload.get("readiness_status"),
            "timeline": household_profile.get("timeline"),
            "next_check_in": payload.get("next_check_in"),
            "scenario_count": len(scenarios),
            "cash_to_close_total_cents": _as_int(cash_to_close.get("cash_to_close_total_cents")),
            "reserve_gap_cents": _as_int(cash_to_close.get("reserve_gap_cents")),
            "monthly_homeownership_cost_cents": _as_int(
                first_scenario.get("monthly_homeownership_cost_cents")
            ),
            "full_homeownership_cost_ratio_pct": ratios.get(
                "full_homeownership_cost_ratio_pct"
            ),
            "readiness_flag_count": len(payload.get("readiness_flags") or []),
            "next_action_count": len(payload.get("next_actions") or []),
            "referral_count": len(payload.get("referrals") or []),
        }
    if skill == "coach_retirement_contribution_readiness":
        selected_commitment = _as_mapping(payload.get("selected_commitment"))
        priority_result = _as_mapping(payload.get("priority_result"))
        steps_value = priority_result.get("steps")
        steps = steps_value if isinstance(steps_value, list) else []
        return {
            "readiness_status": payload.get("readiness_status"),
            "tax_year": _as_int(payload.get("tax_year")),
            "next_check_in": payload.get("next_check_in"),
            "selected_account_type": selected_commitment.get("account_type"),
            "monthly_target_cents": _as_int(
                selected_commitment.get("monthly_target_cents")
            ),
            "write_status": selected_commitment.get("write_status"),
            "source_tax_year": _as_int(priority_result.get("source_tax_year")),
            "priority_step_count": len(steps),
            "readiness_flag_count": len(payload.get("readiness_flags") or []),
            "next_action_count": len(payload.get("next_actions") or []),
            "referral_count": len(payload.get("referrals") or []),
        }
    if skill == "coach_retirement_income_readiness":
        income_sources = _as_mapping(payload.get("income_sources"))
        rmd_context = _as_mapping(payload.get("rmd_context"))
        boundary_response = _as_mapping(payload.get("boundary_response"))
        return {
            "readiness_status": payload.get("readiness_status"),
            "next_check_in": payload.get("next_check_in"),
            "social_security_estimate_status": income_sources.get(
                "social_security_estimate_status"
            ),
            "pension_status": income_sources.get("pension_status"),
            "retirement_account_status": income_sources.get(
                "retirement_account_status"
            ),
            "rmd_relevance": rmd_context.get("relevance"),
            "prohibited_request_detected": (
                boundary_response.get("prohibited_request_detected") is True
            ),
            "milestone_count": len(payload.get("milestones") or []),
            "professional_handoff_count": len(
                payload.get("professional_handoffs") or []
            ),
            "question_count": len(payload.get("questions_to_ask") or []),
            "document_count": len(payload.get("documents_to_gather") or []),
            "data_gap_count": len(payload.get("data_gaps") or []),
            "next_action_count": len(payload.get("next_actions") or []),
        }
    if skill == "coach_investment_readiness":
        user_goal = _as_mapping(payload.get("user_goal"))
        selected_action = _as_mapping(payload.get("selected_action"))
        boundary = _as_mapping(payload.get("boundary"))
        monitoring = _as_mapping(payload.get("monitoring"))
        return {
            "readiness_status": payload.get("readiness_status"),
            "next_check_in": monitoring.get("next_check_in"),
            "target_account_type": user_goal.get("target_account_type"),
            "selected_action_type": selected_action.get("action_type"),
            "write_status": selected_action.get("write_status"),
            "cash_movement_only": boundary.get("cash_movement_only") is True,
            "no_security_selection": boundary.get("no_security_selection") is True,
            "professional_handoff_recommended": (
                boundary.get("professional_handoff_recommended") is True
            ),
            "candidate_action_count": len(payload.get("candidate_actions") or []),
            "data_gap_count": len(payload.get("data_gaps") or []),
            "next_action_count": len(payload.get("next_actions") or []),
            "review_trigger_count": len(monitoring.get("review_triggers") or []),
        }
    if skill == "coach_financial_plan_intake":
        monitoring = _as_mapping(payload.get("monitoring"))
        domain_readiness = _as_mapping(payload.get("domain_readiness"))
        return {
            "snapshot_status": payload.get("snapshot_status"),
            "next_check_in": monitoring.get("next_review_date"),
            "goal_count": len(payload.get("goals") or []),
            "sibling_artifact_count": len(payload.get("sibling_artifacts") or []),
            "planning_sequence_count": len(payload.get("planning_sequence") or []),
            "professional_handoff_count": len(
                payload.get("professional_handoffs") or []
            ),
            "data_gap_count": len(payload.get("data_gaps") or []),
            "domains_ready_count": sum(
                1 for value in domain_readiness.values() if value == "ready"
            ),
            "domains_data_needed_count": sum(
                1 for value in domain_readiness.values() if value == "data_needed"
            ),
        }
    if skill == "coach_estate_document_readiness":
        document_inventory_value = payload.get("document_inventory")
        document_inventory = (
            document_inventory_value
            if isinstance(document_inventory_value, Mapping)
            else {}
        )
        beneficiary_review = _as_mapping(payload.get("beneficiary_review"))
        referral_context = _as_mapping(payload.get("referral_context"))
        document_status_counts = Counter(
            str(item.get("status"))
            for item in document_inventory.values()
            if isinstance(item, Mapping) and item.get("status")
        )
        return {
            "readiness_status": payload.get("readiness_status"),
            "next_check_in": payload.get("next_check_in"),
            "legal_boundary_acknowledged": (
                payload.get("legal_boundary_acknowledged") is True
            ),
            "document_count": len(document_inventory),
            "document_status_counts": dict(document_status_counts),
            "accounts_to_review_count": len(
                beneficiary_review.get("accounts_to_review") or []
            ),
            "mismatch_flag_count": len(beneficiary_review.get("mismatch_flags") or []),
            "beneficiary_task_count": len(beneficiary_review.get("user_tasks") or []),
            "attorney_recommended": (
                referral_context.get("attorney_recommended") is True
            ),
            "attorney_reason_count": len(referral_context.get("reasons") or []),
            "next_action_count": len(payload.get("next_actions") or []),
            "scope_note_count": len(payload.get("scope_notes") or []),
        }
    if skill == "coach_risk_insurance_readiness":
        household_context = _as_mapping(payload.get("household_context"))
        liquidity_context = _as_mapping(payload.get("liquidity_context"))
        coverage_inventory_value = payload.get("coverage_inventory")
        coverage_inventory = (
            coverage_inventory_value
            if isinstance(coverage_inventory_value, Mapping)
            else {}
        )
        known_coverage_count = sum(
            1
            for item in coverage_inventory.values()
            if isinstance(item, Mapping) and item.get("known") is True
        )
        return {
            "readiness_status": payload.get("readiness_status"),
            "next_check_in": payload.get("next_check_in"),
            "dependents_count": _as_int(household_context.get("dependents_count")),
            "emergency_fund_months": liquidity_context.get("emergency_fund_months"),
            "essential_monthly_expenses_cents": _as_int(
                liquidity_context.get("essential_monthly_expenses_cents")
            ),
            "coverage_inventory_count": len(coverage_inventory),
            "known_coverage_count": known_coverage_count,
            "risk_flag_count": len(payload.get("risk_flags") or []),
            "professional_handoff_count": len(
                payload.get("professional_handoffs") or []
            ),
            "planning_implication_count": len(
                payload.get("planning_implications") or []
            ),
            "data_gap_count": len(payload.get("data_gaps") or []),
            "next_action_count": len(payload.get("next_actions") or []),
        }
    if skill == "coach_advisor_handoff_readiness":
        request_classification = _as_mapping(payload.get("request_classification"))
        professional_type = _as_mapping(payload.get("professional_type"))
        cashnerd_context = _as_mapping(payload.get("cashnerd_context"))
        boundary_response = _as_mapping(payload.get("boundary_response"))
        return {
            "handoff_status": payload.get("handoff_status"),
            "next_check_in": payload.get("next_check_in"),
            "release_mode": request_classification.get("release_mode"),
            "professional_type": professional_type.get("primary"),
            "prohibited_response_if_unsupervised": (
                request_classification.get("prohibited_response_if_unsupervised")
                is True
            ),
            "relevant_artifact_count": len(
                cashnerd_context.get("relevant_artifacts") or []
            ),
            "key_fact_count": len(cashnerd_context.get("key_facts") or []),
            "user_question_count": len(cashnerd_context.get("user_questions") or []),
            "handoff_question_count": len(payload.get("handoff_questions") or []),
            "document_count": len(payload.get("documents_to_bring") or []),
            "disclosure_count": len(payload.get("disclosures_to_surface") or []),
            "refused_topic_count": len(boundary_response.get("refused_topics") or []),
            "allowed_help_count": len(boundary_response.get("allowed_help") or []),
            "next_action_count": len(payload.get("next_actions") or []),
        }
    return {}


def _skill_status(
    *,
    state: Mapping[str, Any],
    artifact: Mapping[str, Any],
    completed_phase_numbers: list[int],
) -> str:
    if bool(state.get("complete")):
        return "completed"
    if artifact.get("found") and 9 in completed_phase_numbers:
        return "monitoring"
    if artifact.get("found"):
        return "plan_saved"
    if state or completed_phase_numbers:
        return "in_progress"
    return "not_started"


def _phase_progress_pct(status: str, completed_phase_numbers: list[int]) -> int:
    completed_count = len(completed_phase_numbers)
    if status in {"plan_saved", "monitoring", "completed"}:
        completed_count = max(completed_count, 9)
    if status in {"monitoring", "completed"}:
        completed_count = _TOTAL_PHASES
    return int(round((completed_count / _TOTAL_PHASES) * 100))


def _recommendations_by_skill(recommendations: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for index, recommendation in enumerate(recommendations, start=1):
        skill = str(recommendation.get("skill") or "")
        if skill and skill not in result:
            result[skill] = {
                "rank": index,
                "source": recommendation.get("source"),
                "reason": recommendation.get("reason"),
                "action": recommendation.get("action"),
            }
    return result


def _check_in_timing(next_check_in: Any, *, today: date) -> dict[str, int | str | None]:
    check_in_date = _parse_iso_date(next_check_in)
    if check_in_date is None:
        return {
            "check_in_status": "unknown",
            "days_overdue": None,
            "days_until": None,
        }

    days_until = (check_in_date - today).days
    if days_until < 0:
        return {
            "check_in_status": "overdue",
            "days_overdue": abs(days_until),
            "days_until": None,
        }
    return {
        "check_in_status": "due_today" if days_until == 0 else "upcoming",
        "days_overdue": 0,
        "days_until": days_until,
    }


def _next_check_ins(
    skills: list[dict[str, Any]],
    *,
    today: date,
) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for skill in skills:
        next_check_in = (skill.get("outcomes") or {}).get("next_check_in")
        if next_check_in:
            values.append(
                {
                    "skill": skill["skill"],
                    "title": skill["title"],
                    "next_check_in": next_check_in,
                    **_check_in_timing(next_check_in, today=today),
                }
            )
    return sorted(values, key=lambda item: str(item["next_check_in"]))


def build_coaching_progress(
    conn: sqlite3.Connection,
    *,
    skill_state_store: SkillStateReader,
    data_dir: Path,
    rules_path: Path | None = None,
    now: datetime | None = None,
    limit: int = 3,
) -> dict[str, Any]:
    """Return progress across the core coaching skills without writing state."""
    recommendation_envelope = recommend_skills(
        conn,
        skill_state_store=skill_state_store,
        rules_path=rules_path,
        now=now,
        limit=limit,
    )
    recommendations = list((recommendation_envelope.get("data") or {}).get("recommendations") or [])
    recommendations_by_skill = _recommendations_by_skill(recommendations)

    skills: list[dict[str, Any]] = []
    for skill in COACHING_SKILLS:
        state = skill_state_store.get(skill)
        artifact = _artifact_for_skill(data_dir, skill)
        session = _session_markers(data_dir, skill)
        completed_phase_numbers = list(session["completed_phase_numbers"])
        status = _skill_status(
            state=state,
            artifact=artifact,
            completed_phase_numbers=completed_phase_numbers,
        )
        recommendation = recommendations_by_skill.get(skill)
        acted_on = bool(state or completed_phase_numbers or artifact.get("found"))
        payload = artifact.get("payload") if isinstance(artifact.get("payload"), dict) else {}

        skills.append(
            {
                "skill": skill,
                "title": _SKILL_TITLES[skill],
                "status": status,
                "acted_on_recommendation": acted_on,
                "recommendation": recommendation,
                "phase": state.get("phase"),
                "completed_phase_numbers": completed_phase_numbers,
                "phase_progress_pct": _phase_progress_pct(status, completed_phase_numbers),
                "first_session_date": session["first_session_date"],
                "last_session_date": session["last_session_date"],
                "last_active_at": _latest_timestamp(
                    state.get("last_active_at"),
                    artifact.get("last_modified_at"),
                    session["last_session_date"],
                ),
                "artifact": {
                    key: value
                    for key, value in artifact.items()
                    if key != "payload"
                },
                "outcomes": _outcomes_for_skill(skill, payload),
            }
        )

    status_counts = Counter(str(skill["status"]) for skill in skills)
    started_count = sum(1 for skill in skills if skill["status"] != "not_started")
    plans_saved_count = sum(1 for skill in skills if bool((skill["artifact"] or {}).get("found")))
    today = now.date() if now is not None else date.today()
    next_check_ins = _next_check_ins(skills, today=today)
    overdue_check_in_count = sum(
        1 for item in next_check_ins if item.get("check_in_status") == "overdue"
    )
    top_recommendation = recommendations[0] if recommendations else None

    return {
        "data": {
            "skills": skills,
            "recommendations": recommendations,
            "recommendation_summary": recommendation_envelope.get("summary") or {},
            "next_check_ins": next_check_ins,
            "status_counts": dict(status_counts),
        },
        "summary": {
            "skills_total": len(skills),
            "skills_started": started_count,
            "plans_saved": plans_saved_count,
            "current_recommendation": top_recommendation.get("skill") if top_recommendation else None,
            "next_check_in_count": len(next_check_ins),
            "overdue_check_in_count": overdue_check_in_count,
            "status_counts": dict(status_counts),
        },
    }
