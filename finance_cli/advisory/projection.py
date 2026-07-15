"""Projection helpers for pure advisory math.

All rates are fractions internally: ``Decimal("0.08")`` means 8%.

The default monthly convention uses the effective monthly rate
``(1 + annual_rate) ** (1 / 12) - 1`` instead of ``annual_rate / 12``.
Contributions are end-of-period (ordinary annuity), so deposits are applied
after that month's growth.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP, localcontext

from ._decimal_utils import to_decimal
from ._types import FeeImpactResult


_ONE = Decimal("1")
_MONTHS_PER_YEAR = 12


def _quantize_cents(value: Decimal) -> int:
    with localcontext() as ctx:
        ctx.prec = 28
        result = value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(result)


def _effective_periodic_rate(annual_rate: Decimal, periods_per_year: int) -> Decimal:
    if periods_per_year < 1:
        raise ValueError("compound_periods_per_year must be >= 1")
    if annual_rate <= Decimal("-1"):
        raise ValueError("annual_rate must be greater than -1")

    with localcontext() as ctx:
        ctx.prec = 28
        return (_ONE + annual_rate) ** (_ONE / Decimal(periods_per_year)) - _ONE


def _future_value_from_periodic_rate(
    principal_cents: int,
    periodic_rate: Decimal,
    periods: int,
    contribution_cents: int,
) -> Decimal:
    if periods < 0:
        raise ValueError("periods must be >= 0")

    with localcontext() as ctx:
        ctx.prec = 28
        principal = Decimal(principal_cents)
        contribution = Decimal(contribution_cents)

        if periods == 0:
            return principal
        if periodic_rate == 0:
            return principal + (contribution * Decimal(periods))

        growth_factor = (_ONE + periodic_rate) ** periods
        contribution_factor = (growth_factor - _ONE) / periodic_rate
        return (principal * growth_factor) + (contribution * contribution_factor)


def _time_to_goal_iterative(
    current_cents: int,
    goal_cents: int,
    monthly_contribution_cents: int,
    monthly_rate: Decimal,
    max_months: int,
) -> int | None:
    with localcontext() as ctx:
        ctx.prec = 28
        balance = Decimal(current_cents)
        goal = Decimal(goal_cents)
        contribution = Decimal(monthly_contribution_cents)

        for month in range(1, max_months + 1):
            balance = (balance * (_ONE + monthly_rate)) + contribution
            if balance >= goal:
                return month
    return None


def _time_to_goal_analytic(
    current_cents: int,
    goal_cents: int,
    monthly_contribution_cents: int,
    monthly_rate: Decimal,
    max_months: int,
) -> int | None:
    with localcontext() as ctx:
        ctx.prec = 28
        current = Decimal(current_cents)
        goal = Decimal(goal_cents)
        contribution = Decimal(monthly_contribution_cents)
        base = _ONE + monthly_rate

        if monthly_contribution_cents == 0:
            if current <= 0:
                return None
            ratio = goal / current
        else:
            adjusted_goal = goal + (contribution / monthly_rate)
            adjusted_current = current + (contribution / monthly_rate)
            if adjusted_goal <= 0 or adjusted_current <= 0:
                return None
            ratio = adjusted_goal / adjusted_current

        if ratio <= _ONE:
            candidate = 0
        else:
            estimate = ratio.ln() / base.ln()
            candidate = int(estimate.to_integral_value(rounding=ROUND_CEILING))

    candidate = max(0, candidate)
    goal = Decimal(goal_cents)

    while candidate > 0:
        prior_balance = _future_value_from_periodic_rate(
            principal_cents=current_cents,
            periodic_rate=monthly_rate,
            periods=candidate - 1,
            contribution_cents=monthly_contribution_cents,
        )
        if prior_balance < goal:
            break
        candidate -= 1

    while candidate <= max_months:
        balance = _future_value_from_periodic_rate(
            principal_cents=current_cents,
            periodic_rate=monthly_rate,
            periods=candidate,
            contribution_cents=monthly_contribution_cents,
        )
        if balance >= goal:
            return candidate
        candidate += 1

    return None


def future_value(
    principal_cents: int,
    annual_rate: Decimal | float | int,
    years: int,
    monthly_contribution_cents: int = 0,
    *,
    compound_periods_per_year: int = 12,
) -> int:
    if years < 0:
        raise ValueError("years must be >= 0")

    annual_rate_decimal = to_decimal(annual_rate)
    periodic_rate = _effective_periodic_rate(annual_rate_decimal, compound_periods_per_year)
    periods = years * compound_periods_per_year
    result = _future_value_from_periodic_rate(
        principal_cents=principal_cents,
        periodic_rate=periodic_rate,
        periods=periods,
        contribution_cents=monthly_contribution_cents,
    )
    return _quantize_cents(result)


def fee_impact(
    balance_cents: int,
    current_fee_pct: Decimal | float | int,
    proposed_fee_pct: Decimal | float | int,
    years: int,
    annual_return: Decimal | float | int = Decimal("0.08"),
    monthly_contribution_cents: int = 0,
) -> FeeImpactResult:
    annual_return_decimal = to_decimal(annual_return)
    current_fee_decimal = to_decimal(current_fee_pct)
    proposed_fee_decimal = to_decimal(proposed_fee_pct)

    if annual_return_decimal <= 0:
        raise ValueError("annual_return must be > 0")
    if current_fee_decimal < 0 or proposed_fee_decimal < 0:
        raise ValueError("fee percentages must be >= 0")
    if current_fee_decimal >= annual_return_decimal or proposed_fee_decimal >= annual_return_decimal:
        raise ValueError("fee percentages must be less than annual_return")

    current_total_cents = future_value(
        principal_cents=balance_cents,
        annual_rate=annual_return_decimal - current_fee_decimal,
        years=years,
        monthly_contribution_cents=monthly_contribution_cents,
    )
    proposed_total_cents = future_value(
        principal_cents=balance_cents,
        annual_rate=annual_return_decimal - proposed_fee_decimal,
        years=years,
        monthly_contribution_cents=monthly_contribution_cents,
    )
    return FeeImpactResult(
        current_total_cents=current_total_cents,
        proposed_total_cents=proposed_total_cents,
        savings_cents=proposed_total_cents - current_total_cents,
        years=years,
        annual_return=annual_return_decimal,
        current_fee_pct=current_fee_decimal,
        proposed_fee_pct=proposed_fee_decimal,
    )


def time_to_goal(
    current_cents: int,
    goal_cents: int,
    monthly_contribution_cents: int,
    annual_rate: Decimal | float | int = Decimal("0.08"),
    *,
    max_months: int = 720,
) -> int | None:
    if max_months < 0:
        raise ValueError("max_months must be >= 0")
    if current_cents >= goal_cents:
        return 0

    monthly_rate = _effective_periodic_rate(to_decimal(annual_rate), _MONTHS_PER_YEAR)
    if monthly_rate < 0 or monthly_contribution_cents < 0:
        return _time_to_goal_iterative(
            current_cents=current_cents,
            goal_cents=goal_cents,
            monthly_contribution_cents=monthly_contribution_cents,
            monthly_rate=monthly_rate,
            max_months=max_months,
        )
    if monthly_rate == 0:
        if monthly_contribution_cents <= 0:
            return None
        remaining_cents = goal_cents - current_cents
        months = (remaining_cents + monthly_contribution_cents - 1) // monthly_contribution_cents
        return months if months <= max_months else None

    months = _time_to_goal_analytic(
        current_cents=current_cents,
        goal_cents=goal_cents,
        monthly_contribution_cents=monthly_contribution_cents,
        monthly_rate=monthly_rate,
        max_months=max_months,
    )
    if months is not None:
        return months
    return _time_to_goal_iterative(
        current_cents=current_cents,
        goal_cents=goal_cents,
        monthly_contribution_cents=monthly_contribution_cents,
        monthly_rate=monthly_rate,
        max_months=max_months,
    )


def runway_projection(
    balance_cents: int,
    monthly_spend_cents: int,
    annual_return: Decimal | float | int = Decimal("0.04"),
    *,
    max_months: int = 720,
) -> int | None:
    if max_months < 0:
        raise ValueError("max_months must be >= 0")
    if balance_cents <= 0:
        return 0
    if monthly_spend_cents <= 0:
        return None

    monthly_rate = _effective_periodic_rate(to_decimal(annual_return), _MONTHS_PER_YEAR)
    with localcontext() as ctx:
        ctx.prec = 28
        balance = Decimal(balance_cents)
        monthly_spend = Decimal(monthly_spend_cents)

        if monthly_rate > 0 and balance > monthly_spend:
            next_balance = (balance - monthly_spend) * (_ONE + monthly_rate)
            if next_balance >= balance:
                return None

        for month in range(1, max_months + 1):
            if balance <= monthly_spend:
                return month
            balance = (balance - monthly_spend) * (_ONE + monthly_rate)
            if balance <= 0:
                return month

    return None


__all__ = ["fee_impact", "future_value", "runway_projection", "time_to_goal"]
