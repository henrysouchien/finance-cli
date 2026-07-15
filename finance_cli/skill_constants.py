"""Shared skill constants with no gateway dependency."""

from __future__ import annotations

from finance_cli.skills import SKILL_FILES

NON_ACTIVATABLE_SKILLS: frozenset[str] = frozenset(
    {
        "onboarding",
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
        "coach_tax_readiness",
    }
)
VALID_SKILLS: frozenset[str] = frozenset(SKILL_FILES.keys())
