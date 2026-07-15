from __future__ import annotations


def test_coach_savings_goal_auto_approved_set_includes_state_and_artifact_save() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    auto_approved = sorted(tools.COACH_SAVINGS_GOAL_AUTO_APPROVED)

    assert {
        "agent_session_write",
        "coach_savings_goal_artifact_save",
        "skill_state_clear",
        "skill_state_set",
    }.issubset(auto_approved)
    assert "coach_savings_goal_artifact_read" not in auto_approved
    assert "coach_savings_goal_check_unlock_conditions" not in auto_approved
    assert "goal_find" not in auto_approved


def test_other_skill_auto_approved_sets_unchanged_after_savings_goal_added() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    debt = tools.COACH_DEBT_PAYOFF_AUTO_APPROVED
    efund = tools.COACH_EMERGENCY_FUND_AUTO_APPROVED
    onboarding = tools.ONBOARDING_AUTO_APPROVED

    assert "coach_savings_goal_artifact_save" not in debt
    assert "coach_savings_goal_artifact_save" not in efund
    assert "coach_savings_goal_artifact_save" not in onboarding

    assert {"coach_debt_payoff_artifact_save", "skill_state_set"}.issubset(debt)
    assert {"coach_emergency_fund_artifact_save", "skill_state_set"}.issubset(efund)
    assert {"skill_state_set", "skill_state_clear"}.issubset(onboarding)


def test_coach_savings_goal_in_non_activatable_skills() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_savings_goal" in tools._NON_ACTIVATABLE_SKILLS


def test_coach_savings_goal_read_tools_are_read_only() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_savings_goal_artifact_read" in tools.READ_ONLY_TOOLS
    assert "coach_savings_goal_check_unlock_conditions" in tools.READ_ONLY_TOOLS
    assert "goal_find" in tools.READ_ONLY_TOOLS


def test_coach_savings_goal_artifact_save_requires_approval() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_savings_goal_artifact_save" in tools.APPROVAL_REQUIRED_TOOLS


def test_coach_savings_goal_in_valid_skills() -> None:
    from finance_cli.gateway import tools

    assert "coach_savings_goal" in tools.VALID_SKILLS
