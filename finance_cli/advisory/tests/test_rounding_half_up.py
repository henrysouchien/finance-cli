from decimal import Decimal

from finance_cli.advisory import future_value


def test_future_value_rounds_half_cents_up() -> None:
    # 50 cents * 1.21 = 60.50 cents, so ROUND_HALF_UP must return 61.
    assert future_value(50, Decimal("0.21"), 1, 0, compound_periods_per_year=1) == 61
