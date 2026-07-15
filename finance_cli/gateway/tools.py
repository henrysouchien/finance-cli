"""Tool classification for the finance gateway — shared by gateway and Telegram bot."""
from __future__ import annotations

from collections.abc import Callable
from functools import cache

from finance_cli import tool_registry
from finance_cli.skill_constants import NON_ACTIVATABLE_SKILLS, VALID_SKILLS as VALID_SKILLS

# Permanent surface-gate classification layered on top of registry metadata.
_WEB_EXCLUDES_STATIC: frozenset[str] = frozenset(
    {
        "setup_check",
        "setup_status",
        "db_backup",
        "db_export_preferences",
        "db_import_preferences",
        "db_backup_verify",
        "session_recap",
        "session_list",
    }
)

# Tools that are still valid library/MCP primitives but should not be exposed by
# CashNerd-operated chat surfaces because they can directly cross regulated
# advice boundaries when used on a user's facts.
REGULATED_SCOPE_EXCLUDED_TOOLS: frozenset[str] = frozenset(
    {
        "advisory_target_allocation",
    }
)

WEB_IMPORT_TOOLS: frozenset[str] = frozenset(
    {
        "ingest_csv",
        "ingest_statement",
    }
)

_BRIDGE_EXCLUDES_STATIC: frozenset[str] = frozenset(
    {
        "get_workflow",
        "get_skill",
        "activate_skill",
        "read_mcp_cache",
        "onboarding_detect",
        "skill_recommendations",
        "coaching_progress",
        "strategy_preference_get",
        "setup_check",
        "setup_status",
        "agent_memory_read",
        "agent_session_search",
        "agent_session_read",
        "skill_state_get",
        "skill_state_set",
        "skill_state_clear",
        "low_balance_alerts_list",
        "contractor_january_prep_flags_list",
        "spending_freeze_flags_list",
        "card_paydown_flags_list",
        "session_recap",
        "session_list",
        "notify_channel_list",
        "interventions_get",
        "statement_normalizer_stage",
        "statement_normalizer_activate",
        "normalizer_update",
        "normalizer_register_institution",
        "statement_normalizer_sample_csv",
        "normalizer_detect",
        "normalizer_validate",
        "statement_normalizer_test",
        "db_backup_verify",
    }
)

_NON_ACTIVATABLE_SKILLS = NON_ACTIVATABLE_SKILLS


@cache
def _all() -> tuple[tuple[str, tool_registry.ToolMetadata], ...]:
    import finance_cli.mcp_server  # noqa: F401

    return tuple(tool_registry.iter_registry())


def _from_registry(
    predicate: Callable[[str, tool_registry.ToolMetadata], bool],
) -> frozenset[str]:
    return frozenset(name for name, meta in _all() if predicate(name, meta))


def _normalizer_tools() -> frozenset[str]:
    return _from_registry(lambda _name, meta: meta.normalizer)


_DERIVATIONS: dict[str, Callable[[], frozenset[str]]] = {
    "READ_ONLY_TOOLS": lambda: _from_registry(lambda _name, meta: meta.read_only),
    "APPROVAL_REQUIRED_TOOLS": lambda: _from_registry(
        lambda _name, meta: meta.approval_required
    ),
    "EXCLUDED_TOOLS": lambda: _from_registry(lambda _name, meta: meta.excluded_from_agent),
    "ALL_NORMALIZER_TOOLS": _normalizer_tools,
    "NORMALIZER_WRITE_TOOLS": lambda: _from_registry(
        lambda _name, meta: meta.normalizer and meta.approval_required
    ),
    "BRIDGE_TOOLS": lambda: _from_registry(
        lambda name, meta: meta.read_only
        and name not in _BRIDGE_EXCLUDES_STATIC
        and name not in REGULATED_SCOPE_EXCLUDED_TOOLS
    ),
    "WEB_EXCLUDED_TOOLS": lambda: _from_registry(
        lambda name, meta: meta.normalizer
        or name in _WEB_EXCLUDES_STATIC
        or name in REGULATED_SCOPE_EXCLUDED_TOOLS
    ),
    "NORMALIZER_SKILL_TOOLS": _normalizer_tools,
    "ONBOARDING_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.onboarding_auto_approved
    ),
    "COACH_DEBT_PAYOFF_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_debt_payoff_auto_approved
    ),
    "COACH_EMERGENCY_FUND_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_emergency_fund_auto_approved
    ),
    "COACH_SAVINGS_GOAL_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_savings_goal_auto_approved
    ),
    "COACH_SPENDING_PLAN_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_spending_plan_auto_approved
    ),
    "COACH_TAX_READINESS_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_tax_readiness_auto_approved
    ),
    "COACH_HOMEBUYING_READINESS_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_homebuying_readiness_auto_approved
    ),
    "COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_retirement_contribution_readiness_auto_approved
    ),
    "COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_retirement_income_readiness_auto_approved
    ),
    "COACH_INVESTMENT_READINESS_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_investment_readiness_auto_approved
    ),
    "COACH_ESTATE_DOCUMENT_READINESS_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_estate_document_readiness_auto_approved
    ),
    "COACH_FINANCIAL_PLAN_INTAKE_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_financial_plan_intake_auto_approved
    ),
    "COACH_RISK_INSURANCE_READINESS_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_risk_insurance_readiness_auto_approved
    ),
    "COACH_ADVISOR_HANDOFF_READINESS_AUTO_APPROVED": lambda: _from_registry(
        lambda _name, meta: meta.coach_advisor_handoff_readiness_auto_approved
    ),
}


@cache
def _derived(name: str) -> frozenset[str]:
    try:
        return _DERIVATIONS[name]()
    except KeyError as exc:
        raise AttributeError(name) from exc


def __getattr__(name: str) -> frozenset[str]:
    return _derived(name)


def web_excluded_tools(skill: str | None = None) -> frozenset[str]:
    """Return web-excluded tools, conditionally unblocking normalizer tools for skill."""
    if skill in ("normalizer_builder", "onboarding"):
        return _derived("WEB_EXCLUDED_TOOLS") - _derived("NORMALIZER_SKILL_TOOLS")
    return _derived("WEB_EXCLUDED_TOOLS")


def needs_approval(tool_name: str) -> bool:
    """Return whether a tool should prompt for user approval."""
    if tool_name in _derived("READ_ONLY_TOOLS"):
        return False
    return True
