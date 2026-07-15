"""Shared advisory return types."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal


@dataclass(frozen=True)
class FeeImpactResult:
    current_total_cents: int
    proposed_total_cents: int
    savings_cents: int
    years: int
    annual_return: Decimal
    current_fee_pct: Decimal
    proposed_fee_pct: Decimal


@dataclass(frozen=True)
class TaxResult:
    taxable_income_cents: int
    tax_owed_cents: int
    marginal_rate_pct: Decimal
    effective_rate_pct: Decimal
    filing_status: str
    tax_year: int


@dataclass(frozen=True)
class FicaResult:
    social_security_cents: int
    medicare_cents: int
    additional_medicare_cents: int
    total_cents: int
    w2_wages_applied_cents: int
    se_earnings_base_cents: int
    filing_status: str
    tax_year: int


@dataclass(frozen=True)
class DebtInvestComparison:
    debt_apr: Decimal
    debt_effective_apr: Decimal
    expected_market_return: Decimal
    recommendation: Literal["pay_debt", "invest", "either"]
    reason: str
    debt_payoff_months: int
    debt_interest_saved_cents: int
    investment_value_at_debt_payoff_cents: int
    difference_cents: int


@dataclass(frozen=True)
class RothTraditionalComparison:
    contribution_cents: int
    current_marginal_rate_pct: Decimal
    estimated_retirement_marginal_rate_pct: Decimal
    years_to_retirement: int
    expected_annual_return: Decimal
    roth_after_tax_cents: int
    traditional_after_tax_cents: int
    winner: Literal["roth", "traditional", "tie"]
    advantage_cents: int
    reason: str


@dataclass(frozen=True)
class RothConversionAnalysis:
    conversion_amount_cents: int
    tax_cost_now_cents: int
    current_marginal_rate_pct: Decimal
    estimated_retirement_marginal_rate_pct: Decimal
    years_to_retirement: int
    expected_annual_return: Decimal
    total_wealth_if_converted_cents: int
    total_wealth_if_not_converted_cents: int
    net_advantage_cents: int
    breakeven_years: int | None
    recommendation: Literal["convert", "dont_convert", "marginal"]
    reason: str


@dataclass(frozen=True)
class AnnuitySurrenderAnalysis:
    current_value_cents: int
    surrender_charge_cents: int
    net_after_surrender_cents: int
    guaranteed_annual_rate: Decimal
    years_remaining_guarantee: int
    alternative_annual_return: Decimal
    value_if_kept_cents: int
    value_if_surrendered_cents: int
    advantage_cents: int
    recommendation: Literal["surrender", "keep", "marginal"]
    reason: str


@dataclass(frozen=True)
class FundComparisonResult:
    balance_cents: int
    current_expense_ratio: Decimal
    proposed_expense_ratio: Decimal
    years: int
    annual_return_gross: Decimal
    value_in_current_cents: int
    value_in_proposed_cents: int
    total_savings_cents: int
    capital_gains_tax_cents: int
    net_savings_cents: int
    breakeven_years: Decimal | None
    recommendation: Literal["switch", "stay", "marginal"]
    reason: str


@dataclass(frozen=True)
class AllocationRecommendation:
    age: int
    retirement_age: int
    years_to_retirement: int
    risk_tolerance: Literal["conservative", "moderate", "aggressive"]
    total_equities_pct: Decimal
    total_bonds_pct: Decimal
    us_stocks_pct: Decimal
    international_stocks_pct: Decimal
    us_bonds_pct: Decimal
    international_bonds_pct: Decimal
    reasoning: str


@dataclass(frozen=True)
class PriorityStep:
    order: int
    account: str
    action: str
    annual_amount_cents: int
    monthly_equivalent_cents: int
    priority_rank: Literal["P0_required", "P1_high", "P2_moderate", "P3_low"]
    reason: str
