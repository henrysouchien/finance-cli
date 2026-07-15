from __future__ import annotations


def test_coach_retirement_contribution_readiness_auto_approved_set_is_routine_only() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert tools.COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED == frozenset(
        {
            "agent_session_write",
            "coach_retirement_contribution_readiness_artifact_save",
            "skill_state_clear",
            "skill_state_set",
        }
    )
    assert (
        "coach_retirement_contribution_readiness_artifact_read"
        not in tools.COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED
    )
    assert (
        "set_monthly_retirement_target"
        not in tools.COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED
    )
    assert (
        "setup_monthly_transfer_goal"
        not in tools.COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED
    )


def test_other_skill_auto_approved_sets_do_not_include_retirement_save() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    artifact_save = "coach_retirement_contribution_readiness_artifact_save"

    assert artifact_save not in tools.ONBOARDING_AUTO_APPROVED
    assert artifact_save not in tools.COACH_DEBT_PAYOFF_AUTO_APPROVED
    assert artifact_save not in tools.COACH_EMERGENCY_FUND_AUTO_APPROVED
    assert artifact_save not in tools.COACH_SAVINGS_GOAL_AUTO_APPROVED
    assert artifact_save not in tools.COACH_SPENDING_PLAN_AUTO_APPROVED
    assert artifact_save not in tools.COACH_TAX_READINESS_AUTO_APPROVED
    assert artifact_save not in tools.COACH_HOMEBUYING_READINESS_AUTO_APPROVED


def test_retirement_read_tool_is_read_only_and_save_requires_approval() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_retirement_contribution_readiness_artifact_read" in tools.READ_ONLY_TOOLS
    assert (
        "coach_retirement_contribution_readiness_artifact_save"
        in tools.APPROVAL_REQUIRED_TOOLS
    )


def test_retirement_target_writes_stay_approval_required() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "set_monthly_retirement_target" in tools.APPROVAL_REQUIRED_TOOLS
    assert "setup_monthly_transfer_goal" in tools.APPROVAL_REQUIRED_TOOLS
    assert (
        "set_monthly_retirement_target"
        not in tools.COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED
    )
    assert (
        "setup_monthly_transfer_goal"
        not in tools.COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED
    )
