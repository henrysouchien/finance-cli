"""Debt payoff versus investing comparison helpers.

All rates in this module are fractions: ``Decimal("0.08")`` means 8%.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Literal

from ._decimal_utils import to_decimal
from ._types import DebtInvestComparison
from .projection import future_value


_ONE = Decimal("1")
_MONTHS_PER_YEAR = 12
_MAX_PAYOFF_MONTHS = 2400
_RISK_BANDS = {
    "aggressive": Decimal("0.005"),
    "moderate": Decimal("0.015"),
    "conservative": Decimal("0.03"),
}


def _quantize_cents(value: Decimal) -> int:
    with localcontext() as ctx:
        ctx.prec = 28
        result = value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(result)


def _effective_monthly_rate(annual_rate: Decimal) -> Decimal:
    if annual_rate <= Decimal("-1"):
        raise ValueError("annual rates must be greater than -1")

    with localcontext() as ctx:
        ctx.prec = 28
        return (_ONE + annual_rate) ** (_ONE / Decimal(_MONTHS_PER_YEAR)) - _ONE


def _monthly_interest_cents(balance_cents: int, apr_fraction: Decimal) -> int:
    if balance_cents <= 0 or apr_fraction == 0:
        return 0

    monthly_rate = _effective_monthly_rate(apr_fraction)
    with localcontext() as ctx:
        ctx.prec = 28
        return _quantize_cents(Decimal(balance_cents) * monthly_rate)


def _simulate_to_payoff(
    balance_cents: int,
    apr_fraction: Decimal,
    monthly_payment_cents: int,
) -> tuple[int, int]:
    balance = balance_cents
    total_interest_cents = 0

    for month in range(1, _MAX_PAYOFF_MONTHS + 1):
        if balance <= 0:
            return month - 1, total_interest_cents

        interest_cents = _monthly_interest_cents(balance, apr_fraction)
        if apr_fraction > 0 and monthly_payment_cents <= interest_cents:
            raise ValueError("monthly payment must exceed accrued monthly interest to amortize debt")

        total_interest_cents += interest_cents
        balance += interest_cents
        balance -= min(monthly_payment_cents, balance)

        if balance <= 0:
            return month, total_interest_cents

    raise ValueError(f"debt did not amortize within {_MAX_PAYOFF_MONTHS} months")


def _simulate_fixed_horizon(
    balance_cents: int,
    apr_fraction: Decimal,
    monthly_payment_cents: int,
    months: int,
) -> tuple[int, int]:
    balance = balance_cents
    total_interest_cents = 0

    for _ in range(months):
        if balance <= 0:
            break
        interest_cents = _monthly_interest_cents(balance, apr_fraction)
        total_interest_cents += interest_cents
        balance += interest_cents
        balance -= min(monthly_payment_cents, balance)

    return total_interest_cents, balance


def _future_value_for_months(
    monthly_contribution_cents: int,
    annual_rate: Decimal,
    months: int,
) -> int:
    if months <= 0 or monthly_contribution_cents <= 0:
        return 0

    whole_years, remainder_months = divmod(months, _MONTHS_PER_YEAR)
    balance_cents = future_value(
        principal_cents=0,
        annual_rate=annual_rate,
        years=whole_years,
        monthly_contribution_cents=monthly_contribution_cents,
    )
    if remainder_months == 0:
        return balance_cents

    monthly_rate = _effective_monthly_rate(annual_rate)
    with localcontext() as ctx:
        ctx.prec = 28
        balance = Decimal(balance_cents)
        contribution = Decimal(monthly_contribution_cents)
        for _ in range(remainder_months):
            balance = (balance * (_ONE + monthly_rate)) + contribution
    return _quantize_cents(balance)


def _format_rate(rate_fraction: Decimal) -> str:
    with localcontext() as ctx:
        ctx.prec = 28
        pct = (rate_fraction * Decimal("100")).quantize(Decimal("0.01"))
    return f"{pct}%"


def debt_vs_invest(
    debt_balance_cents: int,
    debt_apr: Decimal | float,
    monthly_extra_payment_cents: int,
    debt_minimum_payment_cents: int,
    expected_market_return: Decimal | float = Decimal("0.08"),
    marginal_tax_rate: Decimal | float = Decimal("0"),
    is_tax_deductible: bool = False,
    risk_tolerance: Literal["conservative", "moderate", "aggressive"] = "moderate",
) -> DebtInvestComparison:
    if debt_balance_cents <= 0:
        raise ValueError("debt_balance_cents must be > 0")
    if monthly_extra_payment_cents < 0:
        raise ValueError("monthly_extra_payment_cents must be >= 0")
    if debt_minimum_payment_cents < 0:
        raise ValueError("debt_minimum_payment_cents must be >= 0")
    if debt_minimum_payment_cents + monthly_extra_payment_cents <= 0:
        raise ValueError("combined monthly payment must be > 0")
    if risk_tolerance not in _RISK_BANDS:
        raise ValueError(f"Unsupported risk_tolerance: {risk_tolerance}.")

    debt_apr_decimal = to_decimal(debt_apr)
    expected_market_return_decimal = to_decimal(expected_market_return)
    marginal_tax_rate_decimal = to_decimal(marginal_tax_rate)

    if debt_apr_decimal < 0:
        raise ValueError("debt_apr must be >= 0")
    if marginal_tax_rate_decimal < 0 or marginal_tax_rate_decimal > 1:
        raise ValueError("marginal_tax_rate must be between 0 and 1")

    with localcontext() as ctx:
        ctx.prec = 28
        debt_effective_apr = (
            debt_apr_decimal * (_ONE - marginal_tax_rate_decimal)
            if is_tax_deductible
            else debt_apr_decimal
        )

    total_monthly_payment_cents = debt_minimum_payment_cents + monthly_extra_payment_cents
    debt_payoff_months, accelerated_interest_cents = _simulate_to_payoff(
        balance_cents=debt_balance_cents,
        apr_fraction=debt_apr_decimal,
        monthly_payment_cents=total_monthly_payment_cents,
    )
    baseline_interest_cents, _ = _simulate_fixed_horizon(
        balance_cents=debt_balance_cents,
        apr_fraction=debt_apr_decimal,
        monthly_payment_cents=debt_minimum_payment_cents,
        months=debt_payoff_months,
    )

    debt_interest_saved_cents = baseline_interest_cents - accelerated_interest_cents
    investment_value_at_debt_payoff_cents = _future_value_for_months(
        monthly_contribution_cents=monthly_extra_payment_cents,
        annual_rate=expected_market_return_decimal,
        months=debt_payoff_months,
    )
    contribution_total_cents = monthly_extra_payment_cents * debt_payoff_months
    investment_gain_cents = investment_value_at_debt_payoff_cents - contribution_total_cents
    difference_cents = debt_interest_saved_cents - investment_gain_cents

    band = _RISK_BANDS[risk_tolerance]
    if debt_effective_apr > expected_market_return_decimal + band:
        recommendation: Literal["pay_debt", "invest", "either"] = "pay_debt"
        reason = (
            f"Effective debt APR {_format_rate(debt_effective_apr)} exceeds expected market "
            f"return {_format_rate(expected_market_return_decimal)} by more than the "
            f"{_format_rate(band)} {risk_tolerance} band."
        )
    elif expected_market_return_decimal > debt_effective_apr + band:
        recommendation = "invest"
        reason = (
            f"Expected market return {_format_rate(expected_market_return_decimal)} exceeds "
            f"effective debt APR {_format_rate(debt_effective_apr)} by more than the "
            f"{_format_rate(band)} {risk_tolerance} band."
        )
    else:
        recommendation = "either"
        reason = (
            f"Effective debt APR {_format_rate(debt_effective_apr)} and expected market "
            f"return {_format_rate(expected_market_return_decimal)} fall within the "
            f"{_format_rate(band)} {risk_tolerance} band."
        )

    return DebtInvestComparison(
        debt_apr=debt_apr_decimal,
        debt_effective_apr=debt_effective_apr,
        expected_market_return=expected_market_return_decimal,
        recommendation=recommendation,
        reason=reason,
        debt_payoff_months=debt_payoff_months,
        debt_interest_saved_cents=debt_interest_saved_cents,
        investment_value_at_debt_payoff_cents=investment_value_at_debt_payoff_cents,
        difference_cents=difference_cents,
    )
