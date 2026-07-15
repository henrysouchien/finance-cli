"""Sandbox-safe advisory math helpers."""

from ._types import (
    AllocationRecommendation,
    AnnuitySurrenderAnalysis,
    DebtInvestComparison,
    FeeImpactResult,
    FicaResult,
    FundComparisonResult,
    PriorityStep,
    RothConversionAnalysis,
    RothTraditionalComparison,
    TaxResult,
)
from .account_comparator import roth_conversion_analysis, roth_vs_traditional
from .account_priority import contribution_priority
from .allocation import target_allocation
from .debt_invest import debt_vs_invest
from .product_evaluator import annuity_surrender_analysis, fund_fee_comparison
from .projection import fee_impact, future_value, runway_projection, time_to_goal
from .retirement_limits_data import (
    RETIREMENT_LIMITS,
    RETIREMENT_LIMITS_2025,
    RETIREMENT_LIMITS_2026,
    SUPPORTED_LIMIT_YEARS,
    roth_ira_allowed_contribution_cents,
)
from .tax_brackets import (
    FilingStatus,
    bracket_room,
    federal_tax,
    fica_tax,
    marginal_rate,
    taxable_income_from_gross,
)

__all__ = [
    "AllocationRecommendation",
    "AnnuitySurrenderAnalysis",
    "DebtInvestComparison",
    "FeeImpactResult",
    "FicaResult",
    "FilingStatus",
    "FundComparisonResult",
    "PriorityStep",
    "RETIREMENT_LIMITS",
    "RETIREMENT_LIMITS_2025",
    "RETIREMENT_LIMITS_2026",
    "RothConversionAnalysis",
    "RothTraditionalComparison",
    "SUPPORTED_LIMIT_YEARS",
    "TaxResult",
    "annuity_surrender_analysis",
    "bracket_room",
    "contribution_priority",
    "debt_vs_invest",
    "fee_impact",
    "federal_tax",
    "fica_tax",
    "fund_fee_comparison",
    "future_value",
    "marginal_rate",
    "roth_conversion_analysis",
    "roth_ira_allowed_contribution_cents",
    "roth_vs_traditional",
    "runway_projection",
    "target_allocation",
    "taxable_income_from_gross",
    "time_to_goal",
]
