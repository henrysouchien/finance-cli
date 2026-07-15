from __future__ import annotations

from decimal import Decimal

import pytest

from finance_cli.advisory import AllocationRecommendation, target_allocation


def test_allocation_35_moderate() -> None:
    result = target_allocation(age=35, retirement_age=65, risk_tolerance="moderate")

    assert isinstance(result, AllocationRecommendation)
    assert result.total_equities_pct == Decimal("75")
    assert result.us_stocks_pct == Decimal("45")
    assert result.international_stocks_pct == Decimal("30")


def test_allocation_35_aggressive() -> None:
    result = target_allocation(age=35, risk_tolerance="aggressive")

    assert result.total_equities_pct == Decimal("85")


def test_allocation_65_conservative() -> None:
    result = target_allocation(age=65, retirement_age=65, risk_tolerance="conservative")

    assert result.total_equities_pct == Decimal("35")


def test_allocation_age_20_aggressive_cap() -> None:
    result = target_allocation(age=20, risk_tolerance="aggressive")

    assert result.total_equities_pct == Decimal("100")


def test_allocation_age_below_18_raises() -> None:
    with pytest.raises(ValueError, match="age"):
        target_allocation(age=17)


def test_allocation_age_above_100_raises() -> None:
    with pytest.raises(ValueError, match="age"):
        target_allocation(age=101)


def test_allocation_retirement_age_at_or_below_age_sets_years_to_retirement_zero() -> None:
    at_retirement = target_allocation(age=65, retirement_age=65)
    past_retirement = target_allocation(age=70, retirement_age=65)

    assert at_retirement.years_to_retirement == 0
    assert past_retirement.years_to_retirement == 0
