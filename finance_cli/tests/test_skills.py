from __future__ import annotations

import json
import sys
from pathlib import Path

from finance_cli.skills import SKILL_FILES, load_skill, load_skill_profile


def test_skill_files_contains_normalizer_builder() -> None:
    assert "normalizer_builder" in SKILL_FILES
    assert SKILL_FILES["normalizer_builder"] == "NORMALIZER_BUILDER_SKILL.md"


def test_skill_files_contains_onboarding() -> None:
    assert "onboarding" in SKILL_FILES
    assert SKILL_FILES["onboarding"] == "ONBOARDING_SKILL.md"


def test_skill_files_contains_coach_debt_payoff() -> None:
    assert "coach_debt_payoff" in SKILL_FILES
    assert SKILL_FILES["coach_debt_payoff"] == "COACH_DEBT_PAYOFF_SKILL.md"


def test_skill_files_contains_coach_emergency_fund() -> None:
    assert "coach_emergency_fund" in SKILL_FILES
    assert SKILL_FILES["coach_emergency_fund"] == "COACH_EMERGENCY_FUND_SKILL.md"


def test_skill_files_contains_coach_savings_goal() -> None:
    assert "coach_savings_goal" in SKILL_FILES
    assert SKILL_FILES["coach_savings_goal"] == "COACH_SAVINGS_GOAL_SKILL.md"


def test_skill_files_contains_coach_spending_plan() -> None:
    assert "coach_spending_plan" in SKILL_FILES
    assert SKILL_FILES["coach_spending_plan"] == "COACH_SPENDING_PLAN_SKILL.md"


def test_skill_files_contains_coach_homebuying_readiness() -> None:
    assert "coach_homebuying_readiness" in SKILL_FILES
    assert SKILL_FILES["coach_homebuying_readiness"] == "COACH_HOMEBUYING_READINESS_SKILL.md"


def test_skill_files_contains_coach_retirement_contribution_readiness() -> None:
    assert "coach_retirement_contribution_readiness" in SKILL_FILES
    assert (
        SKILL_FILES["coach_retirement_contribution_readiness"]
        == "COACH_RETIREMENT_CONTRIBUTION_READINESS_SKILL.md"
    )


def test_skill_files_contains_coach_retirement_income_readiness() -> None:
    assert "coach_retirement_income_readiness" in SKILL_FILES
    assert (
        SKILL_FILES["coach_retirement_income_readiness"]
        == "COACH_RETIREMENT_INCOME_READINESS_SKILL.md"
    )


def test_skill_files_contains_coach_investment_readiness() -> None:
    assert "coach_investment_readiness" in SKILL_FILES
    assert (
        SKILL_FILES["coach_investment_readiness"]
        == "COACH_INVESTMENT_READINESS_SKILL.md"
    )


def test_skill_files_contains_coach_financial_plan_intake() -> None:
    assert "coach_financial_plan_intake" in SKILL_FILES
    assert (
        SKILL_FILES["coach_financial_plan_intake"]
        == "COACH_FINANCIAL_PLAN_INTAKE_SKILL.md"
    )


def test_skill_files_contains_coach_estate_document_readiness() -> None:
    assert "coach_estate_document_readiness" in SKILL_FILES
    assert (
        SKILL_FILES["coach_estate_document_readiness"]
        == "COACH_ESTATE_DOCUMENT_READINESS_SKILL.md"
    )


def test_skill_files_contains_coach_risk_insurance_readiness() -> None:
    assert "coach_risk_insurance_readiness" in SKILL_FILES
    assert (
        SKILL_FILES["coach_risk_insurance_readiness"]
        == "COACH_RISK_INSURANCE_READINESS_SKILL.md"
    )


def test_skill_files_contains_coach_advisor_handoff_readiness() -> None:
    assert "coach_advisor_handoff_readiness" in SKILL_FILES
    assert (
        SKILL_FILES["coach_advisor_handoff_readiness"]
        == "COACH_ADVISOR_HANDOFF_READINESS_SKILL.md"
    )


def test_skill_files_contains_coach_tax_readiness() -> None:
    assert "coach_tax_readiness" in SKILL_FILES
    assert SKILL_FILES["coach_tax_readiness"] == "COACH_TAX_READINESS_SKILL.md"


def test_load_skill_returns_normalizer_builder_content() -> None:
    result = load_skill("normalizer_builder")

    assert result["data"]["name"] == "normalizer_builder"
    assert isinstance(result["data"]["content"], str)
    assert result["summary"]["skill"] == "normalizer_builder"
    assert result["summary"]["file"] == "NORMALIZER_BUILDER_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_unknown_returns_available_list() -> None:
    result = load_skill("nonexistent")

    assert result["summary"]["error"] == "Unknown skill"
    assert result["data"]["available"] == list(SKILL_FILES.keys())
    assert result["summary"]["available"] == list(SKILL_FILES.keys())


def test_load_skill_matches_file_content() -> None:
    raw_file = (
        Path(__file__).resolve().parents[2] / "docs" / "skills" / SKILL_FILES["normalizer_builder"]
    ).read_text(encoding="utf-8").strip()

    result = load_skill("normalizer_builder")

    assert raw_file.startswith("---")
    assert "# Normalizer Builder Skill" in raw_file
    assert result["data"]["content"].startswith("# Normalizer Builder Skill")


def test_load_skill_strips_onboarding_frontmatter() -> None:
    result = load_skill("onboarding")

    assert result["data"]["name"] == "onboarding"
    assert result["data"]["content"].startswith("# AI-Driven Onboarding")
    assert not result["data"]["content"].startswith("---")


def test_load_skill_returns_coach_debt_payoff_content() -> None:
    result = load_skill("coach_debt_payoff")

    assert result["data"]["name"] == "coach_debt_payoff"
    assert result["data"]["content"].startswith("# Coach: Debt Payoff")
    assert result["summary"]["skill"] == "coach_debt_payoff"
    assert result["summary"]["file"] == "COACH_DEBT_PAYOFF_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_spending_plan_content() -> None:
    result = load_skill("coach_spending_plan")

    assert result["data"]["name"] == "coach_spending_plan"
    assert result["data"]["content"].startswith("# Coach: Spending Plan")
    assert result["summary"]["skill"] == "coach_spending_plan"
    assert result["summary"]["file"] == "COACH_SPENDING_PLAN_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_tax_readiness_content() -> None:
    result = load_skill("coach_tax_readiness")

    assert result["data"]["name"] == "coach_tax_readiness"
    assert result["data"]["content"].startswith("# Coach: Tax Readiness")
    assert result["summary"]["skill"] == "coach_tax_readiness"
    assert result["summary"]["file"] == "COACH_TAX_READINESS_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_homebuying_readiness_content() -> None:
    result = load_skill("coach_homebuying_readiness")

    assert result["data"]["name"] == "coach_homebuying_readiness"
    assert result["data"]["content"].startswith("# Coach: Homebuying Readiness")
    assert result["summary"]["skill"] == "coach_homebuying_readiness"
    assert result["summary"]["file"] == "COACH_HOMEBUYING_READINESS_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_retirement_contribution_readiness_content() -> None:
    result = load_skill("coach_retirement_contribution_readiness")

    assert result["data"]["name"] == "coach_retirement_contribution_readiness"
    assert result["data"]["content"].startswith(
        "# Coach: Retirement Contribution Readiness"
    )
    assert result["summary"]["skill"] == "coach_retirement_contribution_readiness"
    assert (
        result["summary"]["file"]
        == "COACH_RETIREMENT_CONTRIBUTION_READINESS_SKILL.md"
    )
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_retirement_income_readiness_content() -> None:
    result = load_skill("coach_retirement_income_readiness")

    assert result["data"]["name"] == "coach_retirement_income_readiness"
    assert result["data"]["content"].startswith(
        "# Coach: Retirement Income Readiness"
    )
    assert result["summary"]["skill"] == "coach_retirement_income_readiness"
    assert result["summary"]["file"] == "COACH_RETIREMENT_INCOME_READINESS_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_investment_readiness_content() -> None:
    result = load_skill("coach_investment_readiness")

    assert result["data"]["name"] == "coach_investment_readiness"
    assert result["data"]["content"].startswith("# Coach: Investment Readiness")
    assert result["summary"]["skill"] == "coach_investment_readiness"
    assert result["summary"]["file"] == "COACH_INVESTMENT_READINESS_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_financial_plan_intake_content() -> None:
    result = load_skill("coach_financial_plan_intake")

    assert result["data"]["name"] == "coach_financial_plan_intake"
    assert result["data"]["content"].startswith("# Coach: Financial Plan Intake")
    assert result["summary"]["skill"] == "coach_financial_plan_intake"
    assert result["summary"]["file"] == "COACH_FINANCIAL_PLAN_INTAKE_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_estate_document_readiness_content() -> None:
    result = load_skill("coach_estate_document_readiness")

    assert result["data"]["name"] == "coach_estate_document_readiness"
    assert result["data"]["content"].startswith(
        "# Coach: Estate Document Readiness"
    )
    assert result["summary"]["skill"] == "coach_estate_document_readiness"
    assert result["summary"]["file"] == "COACH_ESTATE_DOCUMENT_READINESS_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_risk_insurance_readiness_content() -> None:
    result = load_skill("coach_risk_insurance_readiness")

    assert result["data"]["name"] == "coach_risk_insurance_readiness"
    assert result["data"]["content"].startswith(
        "# Coach: Risk And Insurance Readiness"
    )
    assert result["summary"]["skill"] == "coach_risk_insurance_readiness"
    assert result["summary"]["file"] == "COACH_RISK_INSURANCE_READINESS_SKILL.md"
    assert result["summary"]["lines"] > 0


def test_load_skill_returns_coach_advisor_handoff_readiness_content() -> None:
    result = load_skill("coach_advisor_handoff_readiness")

    assert result["data"]["name"] == "coach_advisor_handoff_readiness"
    assert result["data"]["content"].startswith(
        "# Coach: Advisor Handoff Readiness"
    )
    assert result["summary"]["skill"] == "coach_advisor_handoff_readiness"
    assert result["summary"]["file"] == "COACH_ADVISOR_HANDOFF_READINESS_SKILL.md"
    assert result["summary"]["lines"] > 0
    assert "Do not combine phase checkpoints" in result["data"]["content"]
    assert "Phase 0 state, then Phase 0 marker" in result["data"]["content"]
    assert "Preserve the user's original question exactly" in result["data"]["content"]
    assert '"Should I buy VOO?"' in result["data"]["content"]
    assert "prepare a handoff packet for fiduciary review" in result["data"]["content"]
    assert "Schedule an RIA review before making any purchase decision." in result[
        "data"
    ]["content"]
    assert "professional_answer_received=false" in result["data"]["content"]


def test_load_skill_profile_parses_normalizer_builder_frontmatter() -> None:
    profile = load_skill_profile("normalizer_builder")

    assert profile is not None
    assert profile.name == "normalizer_builder"
    assert profile.tool_packs == ["normalizer"]


def test_load_skill_profile_parses_onboarding_frontmatter() -> None:
    profile = load_skill_profile("onboarding")

    assert profile is not None
    assert profile.name == "onboarding"
    assert profile.max_turns == 40
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.timeout == 2700
    assert profile.tool_packs == ["normalizer"]


def test_coach_debt_payoff_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_debt_payoff")

    assert profile is not None
    assert profile.name == "coach_debt_payoff"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True


def test_coach_spending_plan_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_spending_plan")

    assert profile is not None
    assert profile.name == "coach_spending_plan"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_coach_homebuying_readiness_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_homebuying_readiness")

    assert profile is not None
    assert profile.name == "coach_homebuying_readiness"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_coach_retirement_contribution_readiness_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_retirement_contribution_readiness")

    assert profile is not None
    assert profile.name == "coach_retirement_contribution_readiness"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_coach_retirement_income_readiness_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_retirement_income_readiness")

    assert profile is not None
    assert profile.name == "coach_retirement_income_readiness"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_coach_investment_readiness_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_investment_readiness")

    assert profile is not None
    assert profile.name == "coach_investment_readiness"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_coach_financial_plan_intake_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_financial_plan_intake")

    assert profile is not None
    assert profile.name == "coach_financial_plan_intake"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_coach_estate_document_readiness_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_estate_document_readiness")

    assert profile is not None
    assert profile.name == "coach_estate_document_readiness"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_coach_risk_insurance_readiness_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_risk_insurance_readiness")

    assert profile is not None
    assert profile.name == "coach_risk_insurance_readiness"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_coach_advisor_handoff_readiness_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_advisor_handoff_readiness")

    assert profile is not None
    assert profile.name == "coach_advisor_handoff_readiness"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_coach_tax_readiness_profile_has_empty_tool_packs() -> None:
    profile = load_skill_profile("coach_tax_readiness")

    assert profile is not None
    assert profile.name == "coach_tax_readiness"
    assert profile.version == "0.1"
    assert (profile.tool_packs or []) == []
    assert profile.interactive is True
    assert profile.persist_state is True
    assert profile.max_turns == 60
    assert profile.timeout == 3600


def test_load_skill_profile_fields_complete() -> None:
    result = load_skill("onboarding")
    profile = result["data"]["profile"]

    assert set(profile) == {
        "version",
        "model",
        "max_turns",
        "timeout",
        "tool_packs",
        "tool_packs_enabled",
        "persist_state",
        "scope",
        "interactive",
        "metadata",
        "mcp_servers",
        "session_inject_servers",
        "timeout_overrides",
        "state_dir",
        "max_budget_usd",
        "max_retries",
        "initial_message",
        "delivery_label",
        "agent_callable",
        "agent_description",
        "mode",
        "extra_excluded_tools",
    }
    assert isinstance(profile["extra_excluded_tools"], list)
    json.dumps(profile)


def test_skills_loader_does_not_import_agent_gateway(monkeypatch) -> None:
    import importlib

    monkeypatch.delitem(sys.modules, "finance_cli.skills", raising=False)
    monkeypatch.delitem(sys.modules, "agent_gateway.skills", raising=False)

    module = importlib.import_module("finance_cli.skills")

    assert module.load_skill_profile("onboarding") is not None
    assert "agent_gateway.skills" not in sys.modules
