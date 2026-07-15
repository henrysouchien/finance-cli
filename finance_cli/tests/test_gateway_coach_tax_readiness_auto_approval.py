from __future__ import annotations


def test_coach_tax_readiness_auto_approved_set_includes_state_and_artifact_save() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    auto_approved = sorted(tools.COACH_TAX_READINESS_AUTO_APPROVED)

    assert {
        "agent_session_write",
        "coach_tax_readiness_artifact_save",
        "skill_state_clear",
        "skill_state_set",
    }.issubset(auto_approved)
    assert "coach_tax_readiness_artifact_read" not in auto_approved
    assert "biz_tax" not in auto_approved
    assert "biz_tax_setup" not in auto_approved


def test_other_skill_auto_approved_sets_unchanged_after_tax_readiness_added() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    debt = tools.COACH_DEBT_PAYOFF_AUTO_APPROVED
    efund = tools.COACH_EMERGENCY_FUND_AUTO_APPROVED
    savings = tools.COACH_SAVINGS_GOAL_AUTO_APPROVED
    spending = tools.COACH_SPENDING_PLAN_AUTO_APPROVED
    onboarding = tools.ONBOARDING_AUTO_APPROVED

    assert "coach_tax_readiness_artifact_save" not in debt
    assert "coach_tax_readiness_artifact_save" not in efund
    assert "coach_tax_readiness_artifact_save" not in savings
    assert "coach_tax_readiness_artifact_save" not in spending
    assert "coach_tax_readiness_artifact_save" not in onboarding

    assert {"coach_debt_payoff_artifact_save", "skill_state_set"}.issubset(debt)
    assert {"coach_emergency_fund_artifact_save", "skill_state_set"}.issubset(efund)
    assert {"coach_savings_goal_artifact_save", "skill_state_set"}.issubset(savings)
    assert {"coach_spending_plan_artifact_save", "skill_state_set"}.issubset(spending)
    assert {"skill_state_set", "skill_state_clear"}.issubset(onboarding)


def test_coach_tax_readiness_in_non_activatable_skills() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_tax_readiness" in tools._NON_ACTIVATABLE_SKILLS


def test_coach_tax_readiness_read_tool_is_read_only() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_tax_readiness_artifact_read" in tools.READ_ONLY_TOOLS


def test_coach_tax_readiness_artifact_save_requires_approval() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_tax_readiness_artifact_save" in tools.APPROVAL_REQUIRED_TOOLS


def test_coach_tax_readiness_in_valid_skills() -> None:
    from finance_cli.gateway import tools

    assert "coach_tax_readiness" in tools.VALID_SKILLS
