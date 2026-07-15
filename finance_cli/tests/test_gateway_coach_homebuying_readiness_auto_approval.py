from __future__ import annotations


def test_coach_homebuying_readiness_auto_approved_set_is_routine_only() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert tools.COACH_HOMEBUYING_READINESS_AUTO_APPROVED == frozenset(
        {
            "agent_session_write",
            "coach_homebuying_readiness_artifact_save",
            "skill_state_clear",
            "skill_state_set",
        }
    )
    assert (
        "coach_homebuying_readiness_artifact_read"
        not in tools.COACH_HOMEBUYING_READINESS_AUTO_APPROVED
    )
    assert "goal_set" not in tools.COACH_HOMEBUYING_READINESS_AUTO_APPROVED
    assert "budget_set" not in tools.COACH_HOMEBUYING_READINESS_AUTO_APPROVED


def test_other_skill_auto_approved_sets_do_not_include_homebuying_save() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    artifact_save = "coach_homebuying_readiness_artifact_save"

    assert artifact_save not in tools.ONBOARDING_AUTO_APPROVED
    assert artifact_save not in tools.COACH_DEBT_PAYOFF_AUTO_APPROVED
    assert artifact_save not in tools.COACH_EMERGENCY_FUND_AUTO_APPROVED
    assert artifact_save not in tools.COACH_SAVINGS_GOAL_AUTO_APPROVED
    assert artifact_save not in tools.COACH_SPENDING_PLAN_AUTO_APPROVED
    assert artifact_save not in tools.COACH_TAX_READINESS_AUTO_APPROVED

    assert {"coach_debt_payoff_artifact_save", "skill_state_set"}.issubset(
        tools.COACH_DEBT_PAYOFF_AUTO_APPROVED
    )
    assert {"coach_emergency_fund_artifact_save", "skill_state_set"}.issubset(
        tools.COACH_EMERGENCY_FUND_AUTO_APPROVED
    )
    assert {"coach_savings_goal_artifact_save", "skill_state_set"}.issubset(
        tools.COACH_SAVINGS_GOAL_AUTO_APPROVED
    )
    assert {"coach_spending_plan_artifact_save", "skill_state_set"}.issubset(
        tools.COACH_SPENDING_PLAN_AUTO_APPROVED
    )
    assert {"coach_tax_readiness_artifact_save", "skill_state_set"}.issubset(
        tools.COACH_TAX_READINESS_AUTO_APPROVED
    )


def test_homebuying_read_tool_is_read_only_and_save_requires_approval() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_homebuying_readiness_artifact_read" in tools.READ_ONLY_TOOLS
    assert (
        "coach_homebuying_readiness_artifact_save" in tools.APPROVAL_REQUIRED_TOOLS
    )


def test_homebuying_readiness_is_valid_non_activatable_skill() -> None:
    from finance_cli.gateway import tools

    assert "coach_homebuying_readiness" in tools.VALID_SKILLS
    assert "coach_homebuying_readiness" in tools._NON_ACTIVATABLE_SKILLS
