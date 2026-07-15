"""Roth versus Traditional contribution and conversion comparisons.

Marginal tax rates are percentages in this module: ``Decimal("22")`` means
22%. Expected returns stay in fraction form: ``Decimal("0.08")`` means 8%.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP, localcontext

from ._decimal_utils import to_decimal
from ._types import RothConversionAnalysis, RothTraditionalComparison
from .projection import future_value


_HUNDRED = Decimal("100")
_ONE = Decimal("1")


def _quantize_cents(value: Decimal) -> int:
    with localcontext() as ctx:
        ctx.prec = 28
        result = value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(result)


def _rate_fraction(rate_pct: Decimal | float | int) -> Decimal:
    rate_decimal = to_decimal(rate_pct)
    if rate_decimal < 0:
        raise ValueError("rates must be >= 0")
    return rate_decimal / _HUNDRED


def roth_vs_traditional(
    contribution_cents: int,
    current_marginal_rate_pct: Decimal | float,
    estimated_retirement_marginal_rate_pct: Decimal | float,
    years_to_retirement: int,
    expected_annual_return: Decimal | float = Decimal("0.08"),
) -> RothTraditionalComparison:
    """Compare Roth and Traditional spendable retirement value.

    `contribution_cents` is modeled on a pretax-equivalent basis. Roth pays tax
    now at the current marginal rate, so the modeled spendable Roth value is
    the grown contribution after applying today's tax rate. Traditional grows
    pretax and is taxed at the estimated retirement marginal rate on withdrawal.

    This gives the standard direction:
    - current rate > retirement rate: Traditional wins
    - current rate < retirement rate: Roth wins
    - equal rates: tie

    The Traditional path is still an upper-bound simplification because it
    implicitly assumes the tax savings are preserved rather than spent.
    """

    if contribution_cents <= 0:
        raise ValueError("contribution_cents must be > 0")
    if years_to_retirement <= 0:
        raise ValueError("years_to_retirement must be > 0")

    current_rate_pct_decimal = to_decimal(current_marginal_rate_pct)
    retirement_rate_pct_decimal = to_decimal(estimated_retirement_marginal_rate_pct)
    expected_annual_return_decimal = to_decimal(expected_annual_return)

    current_rate_fraction = _rate_fraction(current_rate_pct_decimal)
    retirement_rate_fraction = _rate_fraction(retirement_rate_pct_decimal)
    if expected_annual_return_decimal < 0:
        raise ValueError("expected_annual_return must be >= 0")

    grown_contribution_cents = future_value(
        principal_cents=contribution_cents,
        annual_rate=expected_annual_return_decimal,
        years=years_to_retirement,
    )
    roth_after_tax_cents = _quantize_cents(
        Decimal(grown_contribution_cents) * (_ONE - current_rate_fraction)
    )
    traditional_after_tax_cents = _quantize_cents(
        Decimal(grown_contribution_cents) * (_ONE - retirement_rate_fraction)
    )

    if roth_after_tax_cents > traditional_after_tax_cents:
        winner = "roth"
        reason = (
            f"Current marginal rate {current_rate_pct_decimal}% is below the estimated "
            f"retirement rate {retirement_rate_pct_decimal}%, so paying tax now leaves "
            "more modeled spendable dollars."
        )
    elif traditional_after_tax_cents > roth_after_tax_cents:
        winner = "traditional"
        reason = (
            f"Current marginal rate {current_rate_pct_decimal}% is above the estimated "
            f"retirement rate {retirement_rate_pct_decimal}%, so deferring taxes leaves "
            "more modeled spendable dollars."
        )
    else:
        winner = "tie"
        reason = (
            f"Current and estimated retirement marginal rates are both "
            f"{current_rate_pct_decimal}%, so the modeled spendable values tie."
        )

    return RothTraditionalComparison(
        contribution_cents=contribution_cents,
        current_marginal_rate_pct=current_rate_pct_decimal,
        estimated_retirement_marginal_rate_pct=retirement_rate_pct_decimal,
        years_to_retirement=years_to_retirement,
        expected_annual_return=expected_annual_return_decimal,
        roth_after_tax_cents=roth_after_tax_cents,
        traditional_after_tax_cents=traditional_after_tax_cents,
        winner=winner,
        advantage_cents=abs(roth_after_tax_cents - traditional_after_tax_cents),
        reason=reason,
    )


def roth_conversion_analysis(
    conversion_amount_cents: int,
    current_marginal_rate_pct: Decimal | float,
    estimated_retirement_marginal_rate_pct: Decimal | float,
    years_to_retirement: int,
    expected_annual_return: Decimal | float = Decimal("0.08"),
) -> RothConversionAnalysis:
    """Analyze a Roth conversion with a total-wealth framing.

    Assumptions:
    - Conversion tax is paid from outside funds.
    - If you do not convert, those outside funds are invested at the same
      expected return with no taxable-account drag modeled.

    Under those assumptions:
    - convert when retirement rate > current rate
    - do not convert when retirement rate < current rate
    - marginal tie when the rates match
    """

    if conversion_amount_cents <= 0:
        raise ValueError("conversion_amount_cents must be > 0")
    if years_to_retirement <= 0:
        raise ValueError("years_to_retirement must be > 0")

    current_rate_pct_decimal = to_decimal(current_marginal_rate_pct)
    retirement_rate_pct_decimal = to_decimal(estimated_retirement_marginal_rate_pct)
    expected_annual_return_decimal = to_decimal(expected_annual_return)

    current_rate_fraction = _rate_fraction(current_rate_pct_decimal)
    retirement_rate_fraction = _rate_fraction(retirement_rate_pct_decimal)
    if expected_annual_return_decimal < 0:
        raise ValueError("expected_annual_return must be >= 0")

    grown_conversion_cents = future_value(
        principal_cents=conversion_amount_cents,
        annual_rate=expected_annual_return_decimal,
        years=years_to_retirement,
    )
    tax_cost_now_cents = _quantize_cents(Decimal(conversion_amount_cents) * current_rate_fraction)
    total_wealth_if_converted_cents = grown_conversion_cents
    total_wealth_if_not_converted_cents = _quantize_cents(
        Decimal(grown_conversion_cents)
        * ((_ONE - retirement_rate_fraction) + current_rate_fraction)
    )
    net_advantage_cents = (
        total_wealth_if_converted_cents - total_wealth_if_not_converted_cents
    )

    if net_advantage_cents > 0:
        recommendation = "convert"
        breakeven_years = 0
        reason = (
            f"Estimated retirement marginal rate {retirement_rate_pct_decimal}% exceeds the "
            f"current rate {current_rate_pct_decimal}%, so conversion improves modeled "
            "future total wealth."
        )
    elif net_advantage_cents < 0:
        recommendation = "dont_convert"
        breakeven_years = None
        reason = (
            f"Current marginal rate {current_rate_pct_decimal}% exceeds the estimated "
            f"retirement rate {retirement_rate_pct_decimal}%, so keeping the assets "
            "pre-tax preserves more modeled future total wealth."
        )
    else:
        recommendation = "marginal"
        breakeven_years = 0
        reason = (
            f"Current and estimated retirement marginal rates are both "
            f"{current_rate_pct_decimal}%, so the modeled total-wealth paths tie."
        )

    return RothConversionAnalysis(
        conversion_amount_cents=conversion_amount_cents,
        tax_cost_now_cents=tax_cost_now_cents,
        current_marginal_rate_pct=current_rate_pct_decimal,
        estimated_retirement_marginal_rate_pct=retirement_rate_pct_decimal,
        years_to_retirement=years_to_retirement,
        expected_annual_return=expected_annual_return_decimal,
        total_wealth_if_converted_cents=total_wealth_if_converted_cents,
        total_wealth_if_not_converted_cents=total_wealth_if_not_converted_cents,
        net_advantage_cents=net_advantage_cents,
        breakeven_years=breakeven_years,
        recommendation=recommendation,
        reason=reason,
    )
