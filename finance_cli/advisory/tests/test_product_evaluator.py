from __future__ import annotations

from decimal import Decimal

from finance_cli.advisory import (
    AnnuitySurrenderAnalysis,
    FundComparisonResult,
    annuity_surrender_analysis,
    fund_fee_comparison,
)


def test_annuity_5pct_guaranteed_vs_market() -> None:
    result = annuity_surrender_analysis(
        current_value_cents=10_000_000,
        surrender_charge_pct=Decimal("0.02"),
        guaranteed_annual_rate=Decimal("0.05"),
        years_remaining_guarantee=10,
        alternative_annual_return=Decimal("0.08"),
    )

    assert isinstance(result, AnnuitySurrenderAnalysis)
    assert result.recommendation == "surrender"


def test_annuity_guaranteed_beats_alternative_keep() -> None:
    result = annuity_surrender_analysis(
        current_value_cents=10_000_000,
        surrender_charge_pct=Decimal("0"),
        guaranteed_annual_rate=Decimal("0.09"),
        years_remaining_guarantee=10,
        alternative_annual_return=Decimal("0.07"),
    )

    assert result.recommendation == "keep"


def test_annuity_marginal_band() -> None:
    result = annuity_surrender_analysis(
        current_value_cents=10_000_000,
        surrender_charge_pct=Decimal("0.03"),
        guaranteed_annual_rate=Decimal("0.065"),
        years_remaining_guarantee=10,
        alternative_annual_return=Decimal("0.07"),
    )

    assert result.recommendation == "marginal"


def test_fund_fee_075_vs_003_ira() -> None:
    result = fund_fee_comparison(
        balance_cents=50_000_000,
        current_expense_ratio=Decimal("0.0075"),
        proposed_expense_ratio=Decimal("0.0003"),
        years=25,
        annual_return_gross=Decimal("0.08"),
        unrealized_gain_cents=0,
    )

    assert isinstance(result, FundComparisonResult)
    assert result.recommendation == "switch"
    assert result.breakeven_years == Decimal("0")


def test_fund_fee_taxable_with_gain() -> None:
    result = fund_fee_comparison(
        balance_cents=50_000_000,
        current_expense_ratio=Decimal("0.0075"),
        proposed_expense_ratio=Decimal("0.0003"),
        years=25,
        annual_return_gross=Decimal("0.08"),
        unrealized_gain_cents=15_000_000,
        capital_gains_tax_rate=Decimal("0.15"),
    )

    assert result.capital_gains_tax_cents == 2_250_000
    assert result.recommendation == "switch"
    assert result.breakeven_years is not None
    assert result.breakeven_years > Decimal("0")
