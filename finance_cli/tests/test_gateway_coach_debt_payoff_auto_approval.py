from __future__ import annotations


def test_coach_debt_payoff_auto_approved_set_includes_state_and_artifact_save() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    auto_approved = sorted(tools.COACH_DEBT_PAYOFF_AUTO_APPROVED)

    assert {
        "agent_session_write",
        "coach_debt_payoff_artifact_save",
        "skill_state_clear",
        "skill_state_set",
    }.issubset(auto_approved)
    assert "coach_debt_payoff_artifact_read" not in auto_approved


def test_onboarding_auto_approved_unchanged_after_coach_debt_payoff_added() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    auto_approved = tools.ONBOARDING_AUTO_APPROVED

    assert {
        "agent_session_write",
        "skill_state_clear",
        "skill_state_set",
    }.issubset(auto_approved)
