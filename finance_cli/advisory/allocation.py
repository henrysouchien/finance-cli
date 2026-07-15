"""Age-based target allocation heuristic."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Literal

from ._types import AllocationRecommendation


_HUNDRED = Decimal("100")
_US_STOCK_SPLIT = Decimal("0.6")
_US_BOND_SPLIT = Decimal("0.7")
_RISK_TOLERANCES = frozenset({"conservative", "moderate", "aggressive"})


def _quantize_pct(value: Decimal) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 28
        return value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)


def target_allocation(
    age: int,
    retirement_age: int = 65,
    risk_tolerance: Literal["conservative", "moderate", "aggressive"] = "moderate",
) -> AllocationRecommendation:
    """Return a simple age-based allocation recommendation.

    The glide path depends on `age` only. `retirement_age` is retained for the
    returned `years_to_retirement` field and the explanation string.
    """

    if age < 18 or age > 100:
        raise ValueError("age must be between 18 and 100")
    if risk_tolerance not in _RISK_TOLERANCES:
        raise ValueError(f"Unsupported risk_tolerance: {risk_tolerance}.")

    equity_pct = max(20, min(100, 110 - age))
    if risk_tolerance == "conservative":
        equity_pct = max(10, equity_pct - 10)
    elif risk_tolerance == "aggressive":
        equity_pct = min(100, equity_pct + 10)

    total_equities_pct = _quantize_pct(Decimal(equity_pct))
    total_bonds_pct = _quantize_pct(_HUNDRED - total_equities_pct)
    us_stocks_pct = _quantize_pct(total_equities_pct * _US_STOCK_SPLIT)
    international_stocks_pct = _quantize_pct(total_equities_pct - us_stocks_pct)
    us_bonds_pct = _quantize_pct(total_bonds_pct * _US_BOND_SPLIT)
    international_bonds_pct = _quantize_pct(total_bonds_pct - us_bonds_pct)
    years_to_retirement = max(retirement_age - age, 0)

    reasoning = (
        f"At age {age} with {years_to_retirement} years to retirement, this "
        f"{risk_tolerance} heuristic targets {total_equities_pct}% equities and "
        f"{total_bonds_pct}% bonds using a fixed 60/40 stock split and 70/30 bond split."
    )

    return AllocationRecommendation(
        age=age,
        retirement_age=retirement_age,
        years_to_retirement=years_to_retirement,
        risk_tolerance=risk_tolerance,
        total_equities_pct=total_equities_pct,
        total_bonds_pct=total_bonds_pct,
        us_stocks_pct=us_stocks_pct,
        international_stocks_pct=international_stocks_pct,
        us_bonds_pct=us_bonds_pct,
        international_bonds_pct=international_bonds_pct,
        reasoning=reasoning,
    )
