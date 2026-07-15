from __future__ import annotations

import sqlite3
from typing import Any, Protocol

from finance_cli.onboarding_contract import (
    PhaseEvaluation,
    onboarding_signals,
    is_fully_onboarded,
    phase_entry_payload,
    sanitize_profile,
)


class SkillStateReader(Protocol):
    def get(self, skill_name: str) -> dict[str, Any]:
        ...


_NEXT_STEP_DEFINITIONS: dict[str, dict[str, Any]] = {
    "connect": {
        "step": "connect",
        "tool": "plaid_link",
        "args": {"wait": False, "include_balance": True, "include_liabilities": True},
        "instruction": (
            "Connect your first bank or card account so CashNerd can build the ledger from real data."
        ),
        "priority": 1,
    },
}

_PHASE_SUMMARIES: dict[str, str] = {
    "connect": "Phase 1: Connect accounts and import transactions.",
    "profile": "Phase 2: Capture your work type and income stability.",
    "focus": "Phase 3: Pick the first coaching priority.",
    "setup": "Phase 4: Review starter setup proposals.",
    "complete": "Onboarding complete.",
}


def _next_steps_for_state(
    *,
    is_new_user: bool,
    is_onboarding_complete: bool,
    resume_checkpoint: str | None,
) -> tuple[list[dict[str, Any]], str]:
    if is_onboarding_complete:
        return [], _PHASE_SUMMARIES["complete"]

    step_key = resume_checkpoint or ("connect" if is_new_user else None)
    if step_key is None:
        return [], "Onboarding state detected; inspect skill_state for the next action."

    definition = _NEXT_STEP_DEFINITIONS.get(step_key)
    if definition is None:
        return [], _PHASE_SUMMARIES.get(
            step_key,
            "Onboarding state detected; inspect skill_state for the next action.",
        )
    return [dict(definition)], _PHASE_SUMMARIES.get(step_key, "Continue onboarding.")


def detect_user_state(
    conn: sqlite3.Connection,
    skill_state_store: SkillStateReader,
) -> dict[str, Any]:
    state = skill_state_store.get("onboarding") or {}
    signals = onboarding_signals(conn)
    evaluation = PhaseEvaluation.build(conn, state)
    fully_onboarded = is_fully_onboarded(conn, state)

    is_new_user = (
        not signals["has_accounts"]
        and not signals["has_plaid"]
        and not signals["has_transactions"]
        and not state
    )

    resume_checkpoint: str | None = None
    if is_new_user or fully_onboarded:
        resume_checkpoint = None
    else:
        resume_checkpoint = evaluation.current_phase.id.value

    next_steps, phase_summary = _next_steps_for_state(
        is_new_user=is_new_user,
        is_onboarding_complete=fully_onboarded,
        resume_checkpoint=resume_checkpoint,
    )

    return {
        "data": {
            "is_new_user": is_new_user,
            "is_onboarding_complete": fully_onboarded,
            "resume_checkpoint": resume_checkpoint,
            "next_steps": next_steps,
            "phase_summary": phase_summary,
            "phase": state.get("phase"),
            "profile": sanitize_profile(state),
            "signals": signals,
            "phases": [phase_entry_payload(entry) for entry in evaluation.entries],
            "skill_state": state,
        },
        "summary": {
            "is_new_user": is_new_user,
            "is_onboarding_complete": fully_onboarded,
            "resume_checkpoint": resume_checkpoint,
            "next_step": next_steps[0]["step"] if next_steps else None,
            "phase_summary": phase_summary,
            "phase": state.get("phase"),
        },
    }
