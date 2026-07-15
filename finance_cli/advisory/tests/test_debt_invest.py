from decimal import Decimal

import pytest

from finance_cli.advisory import debt_vs_invest
from finance_cli.advisory._types import DebtInvestComparison


def test_credit_card_beats_market_return() -> None:
    result = debt_vs_invest(
        debt_balance_cents=10_000_00,
        debt_apr=Decimal("0.22"),
        monthly_extra_payment_cents=500_00,
        debt_minimum_payment_cents=200_00,
        expected_market_return=Decimal("0.08"),
    )

    assert isinstance(result, DebtInvestComparison)
    assert result.recommendation == "pay_debt"
    assert result.debt_effective_apr == Decimal("0.22")
    assert result.debt_payoff_months > 0


def test_mortgage_rate_favors_investing() -> None:
    result = debt_vs_invest(
        debt_balance_cents=20_000_00,
        debt_apr=Decimal("0.03"),
        monthly_extra_payment_cents=500_00,
        debt_minimum_payment_cents=250_00,
        expected_market_return=Decimal("0.08"),
    )

    assert result.recommendation == "invest"
    assert result.debt_effective_apr == Decimal("0.03")


def test_student_loan_deduction_lowers_effective_apr() -> None:
    result = debt_vs_invest(
        debt_balance_cents=15_000_00,
        debt_apr=Decimal("0.068"),
        monthly_extra_payment_cents=400_00,
        debt_minimum_payment_cents=200_00,
        expected_market_return=Decimal("0.08"),
        marginal_tax_rate=Decimal("0.22"),
        is_tax_deductible=True,
    )

    assert result.recommendation == "invest"
    assert result.debt_effective_apr == Decimal("0.05304")


def test_within_band_returns_either() -> None:
    result = debt_vs_invest(
        debt_balance_cents=12_000_00,
        debt_apr=Decimal("0.07"),
        monthly_extra_payment_cents=400_00,
        debt_minimum_payment_cents=200_00,
        expected_market_return=Decimal("0.08"),
    )

    assert result.recommendation == "either"


def test_aggressive_band_can_flip_recommendation() -> None:
    moderate = debt_vs_invest(
        debt_balance_cents=12_000_00,
        debt_apr=Decimal("0.06"),
        monthly_extra_payment_cents=400_00,
        debt_minimum_payment_cents=200_00,
        expected_market_return=Decimal("0.07"),
        risk_tolerance="moderate",
    )
    aggressive = debt_vs_invest(
        debt_balance_cents=12_000_00,
        debt_apr=Decimal("0.06"),
        monthly_extra_payment_cents=400_00,
        debt_minimum_payment_cents=200_00,
        expected_market_return=Decimal("0.07"),
        risk_tolerance="aggressive",
    )

    assert moderate.recommendation == "either"
    assert aggressive.recommendation == "invest"


def test_conservative_band_can_widen_to_either() -> None:
    moderate = debt_vs_invest(
        debt_balance_cents=12_000_00,
        debt_apr=Decimal("0.06"),
        monthly_extra_payment_cents=400_00,
        debt_minimum_payment_cents=200_00,
        expected_market_return=Decimal("0.08"),
        risk_tolerance="moderate",
    )
    conservative = debt_vs_invest(
        debt_balance_cents=12_000_00,
        debt_apr=Decimal("0.06"),
        monthly_extra_payment_cents=400_00,
        debt_minimum_payment_cents=200_00,
        expected_market_return=Decimal("0.08"),
        risk_tolerance="conservative",
    )

    assert moderate.recommendation == "invest"
    assert conservative.recommendation == "either"


def test_zero_debt_balance_is_rejected() -> None:
    with pytest.raises(ValueError, match="debt_balance_cents must be > 0"):
        debt_vs_invest(
            debt_balance_cents=0,
            debt_apr=Decimal("0.05"),
            monthly_extra_payment_cents=100_00,
            debt_minimum_payment_cents=100_00,
        )
