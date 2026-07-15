"""Federal income tax and FICA helpers.

`federal_tax()` expects post-deduction taxable income, matching Form 1040
line 15. Use `taxable_income_from_gross()` when you want the simplified
gross-income-to-taxable-income coaching path first.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Literal

from ._types import FicaResult, TaxResult
from .tax_brackets_data import (
    FICA_CONSTANTS,
    FEDERAL_BRACKETS,
    STANDARD_DEDUCTION,
    SUPPORTED_TAX_YEARS,
)


FilingStatus = Literal[
    "single",
    "married_filing_jointly",
    "married_filing_separately",
    "head_of_household",
]


_HUNDRED = Decimal("100")
_ZERO = Decimal("0")
_FILING_STATUSES = frozenset({
    "single",
    "married_filing_jointly",
    "married_filing_separately",
    "head_of_household",
})


def _quantize_cents(value: Decimal) -> int:
    with localcontext() as ctx:
        ctx.prec = 28
        result = value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(result)


def _require_supported_tax_year(tax_year: int) -> None:
    if tax_year not in SUPPORTED_TAX_YEARS:
        raise ValueError(
            f"Tax year {tax_year} not yet supported. "
            f"Supported: {sorted(SUPPORTED_TAX_YEARS)}."
        )


def _require_supported_filing_status(filing_status: str) -> None:
    if filing_status not in _FILING_STATUSES:
        raise ValueError(f"Unsupported filing status: {filing_status}.")


def _normalized_taxable_income_cents(value: int) -> int:
    return max(value, 0)


def _brackets_for(tax_year: int, filing_status: FilingStatus) -> list[tuple[int, Decimal]]:
    _require_supported_tax_year(tax_year)
    _require_supported_filing_status(filing_status)
    return FEDERAL_BRACKETS[tax_year][filing_status]


def taxable_income_from_gross(
    gross_income_cents: int,
    filing_status: FilingStatus,
    tax_year: int = 2026,
    itemized_deductions_cents: int = 0,
    above_the_line_adjustments_cents: int = 0,
) -> int:
    """Convert gross income to taxable income with a simplified 1040 flow.

    The helper subtracts above-the-line adjustments first, then applies the
    larger of the standard deduction or itemized deductions. Scope limits:
    no QBI deduction, no phaseouts, and no itemized-deduction limitations.
    """

    _require_supported_tax_year(tax_year)
    _require_supported_filing_status(filing_status)

    if gross_income_cents < 0:
        raise ValueError("gross_income_cents must be >= 0")
    if itemized_deductions_cents < 0:
        raise ValueError("itemized_deductions_cents must be >= 0")
    if above_the_line_adjustments_cents < 0:
        raise ValueError("above_the_line_adjustments_cents must be >= 0")

    agi_cents = max(gross_income_cents - above_the_line_adjustments_cents, 0)
    deduction_cents = max(
        STANDARD_DEDUCTION[tax_year][filing_status],
        itemized_deductions_cents,
    )
    return max(agi_cents - deduction_cents, 0)


def marginal_rate(
    taxable_income_cents: int,
    filing_status: FilingStatus,
    tax_year: int = 2026,
) -> Decimal:
    taxable_income_cents = _normalized_taxable_income_cents(taxable_income_cents)
    brackets = _brackets_for(tax_year, filing_status)

    for lower_bound_cents, rate_pct in reversed(brackets):
        if taxable_income_cents >= lower_bound_cents:
            return rate_pct
    return brackets[0][1]


def bracket_room(
    taxable_income_cents: int,
    filing_status: FilingStatus,
    tax_year: int = 2026,
) -> int:
    taxable_income_cents = _normalized_taxable_income_cents(taxable_income_cents)
    brackets = _brackets_for(tax_year, filing_status)

    for index, (lower_bound_cents, _) in enumerate(brackets):
        next_lower_bound_cents = brackets[index + 1][0] if index + 1 < len(brackets) else None
        if next_lower_bound_cents is None:
            if taxable_income_cents >= lower_bound_cents:
                return 0
            continue
        if lower_bound_cents <= taxable_income_cents < next_lower_bound_cents:
            return next_lower_bound_cents - taxable_income_cents
    return brackets[1][0] if taxable_income_cents < brackets[1][0] else 0


def federal_tax(
    taxable_income_cents: int,
    filing_status: FilingStatus,
    tax_year: int = 2026,
) -> TaxResult:
    """Compute federal income tax on post-deduction taxable income.

    `taxable_income_cents` must be post-standard/itemized-deduction taxable
    income, matching Form 1040 line 15.
    """

    taxable_income_cents = _normalized_taxable_income_cents(taxable_income_cents)
    brackets = _brackets_for(tax_year, filing_status)

    if taxable_income_cents == 0:
        return TaxResult(
            taxable_income_cents=0,
            tax_owed_cents=0,
            marginal_rate_pct=brackets[0][1],
            effective_rate_pct=Decimal("0"),
            filing_status=filing_status,
            tax_year=tax_year,
        )

    with localcontext() as ctx:
        ctx.prec = 28
        tax_owed = Decimal("0")

        for index, (lower_bound_cents, rate_pct) in enumerate(brackets):
            if taxable_income_cents <= lower_bound_cents:
                continue

            upper_bound_cents = (
                brackets[index + 1][0]
                if index + 1 < len(brackets)
                else taxable_income_cents
            )
            bracket_taxable_cents = min(taxable_income_cents, upper_bound_cents) - lower_bound_cents
            tax_owed += (Decimal(bracket_taxable_cents) * rate_pct) / _HUNDRED

        tax_owed_cents = _quantize_cents(tax_owed)
        effective_rate_pct = (
            Decimal(tax_owed_cents) / Decimal(taxable_income_cents)
        ) * _HUNDRED

    return TaxResult(
        taxable_income_cents=taxable_income_cents,
        tax_owed_cents=tax_owed_cents,
        marginal_rate_pct=marginal_rate(taxable_income_cents, filing_status, tax_year),
        effective_rate_pct=effective_rate_pct,
        filing_status=filing_status,
        tax_year=tax_year,
    )


def fica_tax(
    gross_wages_cents: int = 0,
    net_se_earnings_cents: int = 0,
    filing_status: FilingStatus = "single",
    tax_year: int = 2026,
) -> FicaResult:
    """Compute employee FICA on wages plus SE tax on self-employment income.

    SE tax base is `net_se_earnings_cents * 0.9235`. The Social Security wage
    base is shared across W-2 wages and SE earnings, while Additional Medicare
    is based on wages plus the SE base with filing-status-specific thresholds.
    """

    _require_supported_tax_year(tax_year)
    _require_supported_filing_status(filing_status)

    if gross_wages_cents < 0:
        raise ValueError("gross_wages_cents must be >= 0")
    if net_se_earnings_cents < 0:
        raise ValueError("net_se_earnings_cents must be >= 0")

    constants = FICA_CONSTANTS[tax_year]
    ss_wage_base_cents = constants["ss_wage_base_cents"]
    additional_threshold_cents = constants["additional_medicare_thresholds_cents"][filing_status]
    w2_wages_applied_cents = min(gross_wages_cents, ss_wage_base_cents)

    with localcontext() as ctx:
        ctx.prec = 28

        se_earnings_base_cents = _quantize_cents(
            Decimal(net_se_earnings_cents) * constants["se_earnings_multiplier"]
        )
        remaining_ss_wage_base_cents = max(ss_wage_base_cents - w2_wages_applied_cents, 0)
        se_ss_taxable_base_cents = min(se_earnings_base_cents, remaining_ss_wage_base_cents)

        social_security_cents = _quantize_cents(
            (Decimal(w2_wages_applied_cents) * constants["ss_rate"])
            + (Decimal(se_ss_taxable_base_cents) * constants["se_ss_rate"])
        )
        medicare_cents = _quantize_cents(
            (Decimal(gross_wages_cents) * constants["medicare_rate"])
            + (Decimal(se_earnings_base_cents) * constants["se_medicare_rate"])
        )

        additional_medicare_income_cents = max(
            gross_wages_cents + se_earnings_base_cents - additional_threshold_cents,
            0,
        )
        additional_medicare_cents = _quantize_cents(
            Decimal(additional_medicare_income_cents) * constants["additional_medicare_rate"]
        )

    total_cents = social_security_cents + medicare_cents + additional_medicare_cents
    return FicaResult(
        social_security_cents=social_security_cents,
        medicare_cents=medicare_cents,
        additional_medicare_cents=additional_medicare_cents,
        total_cents=total_cents,
        w2_wages_applied_cents=w2_wages_applied_cents,
        se_earnings_base_cents=se_earnings_base_cents,
        filing_status=filing_status,
        tax_year=tax_year,
    )
