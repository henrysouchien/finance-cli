from __future__ import annotations


def test_coach_emergency_fund_auto_approved_set_includes_state_and_artifact_save() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    auto_approved = sorted(tools.COACH_EMERGENCY_FUND_AUTO_APPROVED)

    assert {
        "agent_session_write",
        "coach_emergency_fund_artifact_save",
        "skill_state_clear",
        "skill_state_set",
    }.issubset(auto_approved)
    assert "coach_emergency_fund_artifact_read" not in auto_approved
    assert "spending_essential_monthly" not in auto_approved


def test_coach_debt_payoff_auto_approved_unchanged_after_coach_emergency_fund_added() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    auto_approved = tools.COACH_DEBT_PAYOFF_AUTO_APPROVED

    assert {
        "agent_session_write",
        "coach_debt_payoff_artifact_save",
        "skill_state_clear",
        "skill_state_set",
    }.issubset(auto_approved)
    assert "coach_emergency_fund_artifact_save" not in auto_approved


def test_onboarding_auto_approved_unchanged_after_coach_emergency_fund_added() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    auto_approved = tools.ONBOARDING_AUTO_APPROVED

    assert {
        "agent_session_write",
        "skill_state_clear",
        "skill_state_set",
    }.issubset(auto_approved)
    assert "coach_emergency_fund_artifact_save" not in auto_approved


def test_coach_emergency_fund_in_non_activatable_skills() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_emergency_fund" in tools._NON_ACTIVATABLE_SKILLS


def test_coach_emergency_fund_artifact_read_is_read_only() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_emergency_fund_artifact_read" in tools.READ_ONLY_TOOLS
    assert "spending_essential_monthly" in tools.READ_ONLY_TOOLS


def test_coach_emergency_fund_artifact_save_requires_approval() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_emergency_fund_artifact_save" in tools.APPROVAL_REQUIRED_TOOLS
