"""Evaluate simple annuity surrenders and fund-fee switches."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP, localcontext

from ._decimal_utils import to_decimal
from ._types import AnnuitySurrenderAnalysis, FundComparisonResult
from .projection import future_value


_ONE = Decimal("1")
_SWITCH_THRESHOLD_CENTS = 1_000_00


def _quantize_cents(value: Decimal) -> int:
    with localcontext() as ctx:
        ctx.prec = 28
        result = value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(result)


def annuity_surrender_analysis(
    current_value_cents: int,
    surrender_charge_pct: Decimal | float,
    guaranteed_annual_rate: Decimal | float,
    years_remaining_guarantee: int,
    alternative_annual_return: Decimal | float = Decimal("0.08"),
) -> AnnuitySurrenderAnalysis:
    """SCOPE WARNING: simple fixed-rate annuity only.

    Unsupported products:
    - variable annuities
    - annuities with GLWB, GMIB, or GMAB riders
    - indexed annuities
    - single premium immediate annuities (SPIAs)
    - deferred income annuities (DIAs)

    Tax caveat: this helper does not model surrender taxation, basis recovery,
    or early-withdrawal penalties. It is only appropriate for a simple fixed
    product with a known guaranteed annual rate and surrender charge.
    """

    if current_value_cents <= 0:
        raise ValueError("current_value_cents must be > 0")
    if years_remaining_guarantee <= 0:
        raise ValueError("years_remaining_guarantee must be > 0")

    surrender_charge_pct_decimal = to_decimal(surrender_charge_pct)
    guaranteed_annual_rate_decimal = to_decimal(guaranteed_annual_rate)
    alternative_annual_return_decimal = to_decimal(alternative_annual_return)

    if surrender_charge_pct_decimal < 0:
        raise ValueError("surrender_charge_pct must be >= 0")
    if guaranteed_annual_rate_decimal < 0:
        raise ValueError("guaranteed_annual_rate must be >= 0")
    if alternative_annual_return_decimal < 0:
        raise ValueError("alternative_annual_return must be >= 0")

    surrender_charge_cents = _quantize_cents(
        Decimal(current_value_cents) * surrender_charge_pct_decimal
    )
    net_after_surrender_cents = current_value_cents - surrender_charge_cents
    value_if_kept_cents = future_value(
        principal_cents=current_value_cents,
        annual_rate=guaranteed_annual_rate_decimal,
        years=years_remaining_guarantee,
    )
    value_if_surrendered_cents = future_value(
        principal_cents=net_after_surrender_cents,
        annual_rate=alternative_annual_return_decimal,
        years=years_remaining_guarantee,
    )
    advantage_cents = value_if_surrendered_cents - value_if_kept_cents

    if advantage_cents > _quantize_cents(Decimal(surrender_charge_cents) * Decimal("1.5")):
        recommendation = "surrender"
        reason = (
            "The projected value of surrendering and reinvesting beats keeping the "
            "contract by more than 1.5x the surrender charge."
        )
    elif guaranteed_annual_rate_decimal > alternative_annual_return_decimal:
        recommendation = "keep"
        reason = (
            "The annuity's guaranteed annual rate exceeds the modeled alternative return, "
            "so keeping the contract is favored in this simple fixed-rate analysis."
        )
    else:
        recommendation = "marginal"
        reason = (
            "The projected advantage does not clear the surrender-charge hurdle, so this "
            "simple fixed-rate analysis is too close to call."
        )

    return AnnuitySurrenderAnalysis(
        current_value_cents=current_value_cents,
        surrender_charge_cents=surrender_charge_cents,
        net_after_surrender_cents=net_after_surrender_cents,
        guaranteed_annual_rate=guaranteed_annual_rate_decimal,
        years_remaining_guarantee=years_remaining_guarantee,
        alternative_annual_return=alternative_annual_return_decimal,
        value_if_kept_cents=value_if_kept_cents,
        value_if_surrendered_cents=value_if_surrendered_cents,
        advantage_cents=advantage_cents,
        recommendation=recommendation,
        reason=reason,
    )


def fund_fee_comparison(
    balance_cents: int,
    current_expense_ratio: Decimal | float,
    proposed_expense_ratio: Decimal | float,
    years: int,
    annual_return_gross: Decimal | float = Decimal("0.08"),
    unrealized_gain_cents: int = 0,
    capital_gains_tax_rate: Decimal | float = Decimal("0.15"),
) -> FundComparisonResult:
    if balance_cents <= 0:
        raise ValueError("balance_cents must be > 0")
    if years <= 0:
        raise ValueError("years must be > 0")
    if unrealized_gain_cents < 0:
        raise ValueError("unrealized_gain_cents must be >= 0")

    current_expense_ratio_decimal = to_decimal(current_expense_ratio)
    proposed_expense_ratio_decimal = to_decimal(proposed_expense_ratio)
    annual_return_gross_decimal = to_decimal(annual_return_gross)
    capital_gains_tax_rate_decimal = to_decimal(capital_gains_tax_rate)

    if current_expense_ratio_decimal < 0:
        raise ValueError("current_expense_ratio must be >= 0")
    if proposed_expense_ratio_decimal < 0:
        raise ValueError("proposed_expense_ratio must be >= 0")
    if annual_return_gross_decimal < 0:
        raise ValueError("annual_return_gross must be >= 0")
    if capital_gains_tax_rate_decimal < 0:
        raise ValueError("capital_gains_tax_rate must be >= 0")

    value_in_current_cents = future_value(
        principal_cents=balance_cents,
        annual_rate=annual_return_gross_decimal - current_expense_ratio_decimal,
        years=years,
    )
    value_in_proposed_cents = future_value(
        principal_cents=balance_cents,
        annual_rate=annual_return_gross_decimal - proposed_expense_ratio_decimal,
        years=years,
    )
    total_savings_cents = value_in_proposed_cents - value_in_current_cents
    capital_gains_tax_cents = _quantize_cents(
        Decimal(unrealized_gain_cents) * capital_gains_tax_rate_decimal
    )
    net_savings_cents = total_savings_cents - capital_gains_tax_cents

    breakeven_years: Decimal | None = None
    if (
        capital_gains_tax_cents == 0
        and proposed_expense_ratio_decimal < current_expense_ratio_decimal
        and total_savings_cents > 0
    ):
        breakeven_years = Decimal("0")
    else:
        for year in range(1, years + 1):
            value_current_year_cents = future_value(
                principal_cents=balance_cents,
                annual_rate=annual_return_gross_decimal - current_expense_ratio_decimal,
                years=year,
            )
            value_proposed_year_cents = future_value(
                principal_cents=balance_cents,
                annual_rate=annual_return_gross_decimal - proposed_expense_ratio_decimal,
                years=year,
            )
            if value_proposed_year_cents - value_current_year_cents > capital_gains_tax_cents:
                breakeven_years = Decimal(year)
                break

    if (
        net_savings_cents > _SWITCH_THRESHOLD_CENTS
        and breakeven_years is not None
        and breakeven_years < Decimal(years)
    ):
        recommendation = "switch"
        reason = (
            "The lower-fee fund overcomes any upfront tax cost within the modeled horizon "
            "and clears the $1,000 net-savings threshold."
        )
    elif net_savings_cents < 0:
        recommendation = "stay"
        reason = (
            "The modeled fee savings do not offset the upfront tax cost over the chosen "
            "horizon, so staying put preserves more value."
        )
    else:
        recommendation = "marginal"
        reason = (
            "The modeled fee savings are positive but not decisive enough to justify a "
            "clear switch recommendation."
        )

    return FundComparisonResult(
        balance_cents=balance_cents,
        current_expense_ratio=current_expense_ratio_decimal,
        proposed_expense_ratio=proposed_expense_ratio_decimal,
        years=years,
        annual_return_gross=annual_return_gross_decimal,
        value_in_current_cents=value_in_current_cents,
        value_in_proposed_cents=value_in_proposed_cents,
        total_savings_cents=total_savings_cents,
        capital_gains_tax_cents=capital_gains_tax_cents,
        net_savings_cents=net_savings_cents,
        breakeven_years=breakeven_years,
        recommendation=recommendation,
        reason=reason,
    )
