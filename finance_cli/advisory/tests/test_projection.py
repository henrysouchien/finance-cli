from decimal import Decimal, ROUND_HALF_UP, localcontext

from finance_cli.advisory import fee_impact, future_value, runway_projection, time_to_goal
from finance_cli.advisory._types import FeeImpactResult


def _effective_monthly_rate(annual_rate: Decimal) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 28
        return (Decimal("1") + annual_rate) ** (Decimal("1") / Decimal("12")) - Decimal("1")


def _round_cents(value: Decimal) -> int:
    with localcontext() as ctx:
        ctx.prec = 28
        return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def test_future_value_known_answer() -> None:
    assert future_value(100_000_00, Decimal("0.08"), 10, 0) == 215_892_50


def test_future_value_zero_rate_short_circuit() -> None:
    assert future_value(100_000_00, Decimal("0"), 10, 50_000) == 160_000_00


def test_future_value_contribution_dominant_matches_manual_schedule() -> None:
    monthly_rate = _effective_monthly_rate(Decimal("0.08"))
    balance = Decimal("0")

    with localcontext() as ctx:
        ctx.prec = 28
        for _ in range(30 * 12):
            balance = (balance * (Decimal("1") + monthly_rate)) + Decimal("50000")

    expected = _round_cents(balance)
    assert future_value(0, Decimal("0.08"), 30, 500_00) == expected


def test_future_value_uses_effective_monthly_rate_conversion() -> None:
    result = future_value(10_000_00, Decimal("0.12"), 1, 0)

    with localcontext() as ctx:
        ctx.prec = 28
        nominal_monthly_factor = (Decimal("1") + (Decimal("0.12") / Decimal("12"))) ** 12
        nominal_monthly_result = _round_cents(Decimal("1000000") * nominal_monthly_factor)

    assert result == 11_200_00
    assert nominal_monthly_result == 11_268_25
    assert result != nominal_monthly_result


def test_time_to_goal_known_answer() -> None:
    assert time_to_goal(50_000_00, 1_000_000_00, 1_000_00, Decimal("0.08")) == 270


def test_time_to_goal_returns_none_for_unreachable_negative_return() -> None:
    assert time_to_goal(0, 100_000_00, 500_00, Decimal("-0.50")) is None


def test_runway_projection_without_growth() -> None:
    assert runway_projection(1_000_00, 100_00, Decimal("0")) == 10


def test_runway_projection_returns_none_when_growth_offsets_spend() -> None:
    assert runway_projection(100_000_00, 326_30, Decimal("0.04")) is None


def test_fee_impact_savings_are_positive_when_proposed_fee_is_lower() -> None:
    result = fee_impact(
        500_000_00,
        current_fee_pct=Decimal("0.0175"),
        proposed_fee_pct=Decimal("0.0003"),
        years=30,
        annual_return=Decimal("0.08"),
        monthly_contribution_cents=0,
    )

    assert isinstance(result, FeeImpactResult)
    assert result.proposed_total_cents > result.current_total_cents
    assert result.savings_cents > 0
