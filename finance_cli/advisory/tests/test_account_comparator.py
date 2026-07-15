from __future__ import annotations

from decimal import Decimal

import pytest

from finance_cli.advisory import (
    RothConversionAnalysis,
    RothTraditionalComparison,
    roth_conversion_analysis,
    roth_vs_traditional,
)


def test_roth_equal_rates_tie() -> None:
    result = roth_vs_traditional(
        contribution_cents=700_000,
        current_marginal_rate_pct=Decimal("22"),
        estimated_retirement_marginal_rate_pct=Decimal("22"),
        years_to_retirement=35,
        expected_annual_return=Decimal("0.08"),
    )

    assert isinstance(result, RothTraditionalComparison)
    assert result.winner == "tie"
    assert result.advantage_cents == 0


def test_roth_current_greater_traditional_wins() -> None:
    result = roth_vs_traditional(
        contribution_cents=700_000,
        current_marginal_rate_pct=Decimal("32"),
        estimated_retirement_marginal_rate_pct=Decimal("22"),
        years_to_retirement=30,
        expected_annual_return=Decimal("0.08"),
    )

    assert result.winner == "traditional"
    assert result.advantage_cents > 0


def test_roth_current_less_roth_wins() -> None:
    result = roth_vs_traditional(
        contribution_cents=700_000,
        current_marginal_rate_pct=Decimal("12"),
        estimated_retirement_marginal_rate_pct=Decimal("22"),
        years_to_retirement=30,
        expected_annual_return=Decimal("0.08"),
    )

    assert result.winner == "roth"
    assert result.advantage_cents > 0


def test_conversion_high_to_low_dont_convert() -> None:
    result = roth_conversion_analysis(
        conversion_amount_cents=10_000_000,
        current_marginal_rate_pct=Decimal("22"),
        estimated_retirement_marginal_rate_pct=Decimal("12"),
        years_to_retirement=20,
        expected_annual_return=Decimal("0.08"),
    )

    assert isinstance(result, RothConversionAnalysis)
    assert result.recommendation == "dont_convert"
    assert result.breakeven_years is None
    assert result.net_advantage_cents < 0


def test_conversion_low_to_high_convert() -> None:
    result = roth_conversion_analysis(
        conversion_amount_cents=5_000_000,
        current_marginal_rate_pct=Decimal("12"),
        estimated_retirement_marginal_rate_pct=Decimal("22"),
        years_to_retirement=25,
        expected_annual_return=Decimal("0.08"),
    )

    assert result.recommendation == "convert"
    assert result.net_advantage_cents > 0
    assert result.breakeven_years == 0


def test_roth_vs_traditional_zero_contribution_raises() -> None:
    with pytest.raises(ValueError, match="contribution_cents"):
        roth_vs_traditional(0, Decimal("22"), Decimal("22"), 35)


def test_roth_vs_traditional_zero_years_raises() -> None:
    with pytest.raises(ValueError, match="years_to_retirement"):
        roth_vs_traditional(700_000, Decimal("22"), Decimal("22"), 0)


@pytest.mark.parametrize(
    ("current_rate", "retirement_rate", "expected_return"),
    [
        (Decimal("-1"), Decimal("22"), Decimal("0.08")),
        (Decimal("22"), Decimal("-1"), Decimal("0.08")),
        (Decimal("22"), Decimal("22"), Decimal("-0.01")),
    ],
)
def test_roth_vs_traditional_negative_rates_raise(
    current_rate: Decimal,
    retirement_rate: Decimal,
    expected_return: Decimal,
) -> None:
    with pytest.raises(ValueError, match="rates|expected_annual_return"):
        roth_vs_traditional(700_000, current_rate, retirement_rate, 35, expected_return)


def test_roth_conversion_zero_amount_raises() -> None:
    with pytest.raises(ValueError, match="conversion_amount_cents"):
        roth_conversion_analysis(0, Decimal("22"), Decimal("12"), 20)


def test_roth_conversion_zero_years_raises() -> None:
    with pytest.raises(ValueError, match="years_to_retirement"):
        roth_conversion_analysis(10_000_000, Decimal("22"), Decimal("12"), 0)


@pytest.mark.parametrize(
    ("current_rate", "retirement_rate", "expected_return"),
    [
        (Decimal("-1"), Decimal("12"), Decimal("0.08")),
        (Decimal("22"), Decimal("-1"), Decimal("0.08")),
        (Decimal("22"), Decimal("12"), Decimal("-0.01")),
    ],
)
def test_roth_conversion_negative_rates_raise(
    current_rate: Decimal,
    retirement_rate: Decimal,
    expected_return: Decimal,
) -> None:
    with pytest.raises(ValueError, match="rates|expected_annual_return"):
        roth_conversion_analysis(10_000_000, current_rate, retirement_rate, 20, expected_return)
