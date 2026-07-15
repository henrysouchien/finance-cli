"""Retirement contribution limits and Roth IRA phaseouts.

Sources:
- 2025 retirement limits: IRS Notice 2024-80
- 2025 HSA limits: IRS Rev. Proc. 2024-25
- 2026 retirement limits: IRS Notice 2025-67
- 2026 HSA limits: IRS Rev. Proc. 2025-19
- Roth IRA worksheet: IRS Pub. 590-A Worksheet 2-2
"""

from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP, localcontext
from typing import Any, Literal


# ROUND_HALF_UP matches the FM-1a cent convention.
# ROUND_CEILING matches the Pub. 590-A Roth worksheet's "$10 round up" rule.
_TEN_DOLLARS_CENTS = Decimal("1000")
_MIN_PHASEOUT_CONTRIBUTION_CENTS = 200_00

SUPPORTED_LIMIT_YEARS: frozenset[int] = frozenset({2025, 2026})


# 2025 limits: Notice 2024-80, Rev. Proc. 2024-25, SECURE 2.0 §§108-109.
RETIREMENT_LIMITS_2025: dict[str, Any] = {
    "ira_contribution_cents": 7_000_00,  # Notice 2024-80 §1.
    "ira_catchup_cents": 1_000_00,  # SECURE 2.0 §108 indexing stayed at $1,000 for 2025.
    "401k_contribution_cents": 23_500_00,  # Notice 2024-80 §1.
    "401k_catchup_cents": 7_500_00,  # Notice 2024-80 §1.
    "401k_supercatchup_cents": 11_250_00,  # SECURE 2.0 §109.
    "401k_total_limit_cents": 70_000_00,  # Notice 2024-80 §1.
    "hsa_single_cents": 4_300_00,  # Rev. Proc. 2024-25 §3.01.
    "hsa_family_cents": 8_550_00,  # Rev. Proc. 2024-25 §3.01.
    "hsa_catchup_cents": 1_000_00,  # IRC §223(b)(3), unchanged.
    "roth_ira_phaseout_single_cents": (150_000_00, 165_000_00),  # Notice 2024-80 §1.
    "roth_ira_phaseout_mfj_cents": (236_000_00, 246_000_00),  # Notice 2024-80 §1.
    "roth_ira_phaseout_mfs_cents": (0, 10_000_00),  # Pub. 590-A / IRC §408A(c)(3)(C).
    "roth_ira_phaseout_hoh_cents": (150_000_00, 165_000_00),  # Notice 2024-80 §1.
}


# 2026 limits: Notice 2025-67, Rev. Proc. 2025-19, SECURE 2.0 §§108-109.
RETIREMENT_LIMITS_2026: dict[str, Any] = {
    "ira_contribution_cents": 7_500_00,  # Notice 2025-67 §1.
    "ira_catchup_cents": 1_100_00,  # Notice 2025-67 §1 / SECURE 2.0 §108.
    "401k_contribution_cents": 24_500_00,  # Notice 2025-67 §1.
    "401k_catchup_cents": 8_000_00,  # Notice 2025-67 §1.
    "401k_supercatchup_cents": 11_250_00,  # SECURE 2.0 §109; unchanged for 2026.
    "401k_total_limit_cents": 72_000_00,  # Notice 2025-67 §1.
    "hsa_single_cents": 4_400_00,  # Rev. Proc. 2025-19 §3.01.
    "hsa_family_cents": 8_750_00,  # Rev. Proc. 2025-19 §3.01.
    "hsa_catchup_cents": 1_000_00,  # IRC §223(b)(3), unchanged.
    "roth_ira_phaseout_single_cents": (153_000_00, 168_000_00),  # Notice 2025-67 §1.
    "roth_ira_phaseout_mfj_cents": (242_000_00, 252_000_00),  # Notice 2025-67 §1.
    "roth_ira_phaseout_mfs_cents": (0, 10_000_00),  # Pub. 590-A / IRC §408A(c)(3)(C).
    "roth_ira_phaseout_hoh_cents": (153_000_00, 168_000_00),  # Notice 2025-67 §1.
}


RETIREMENT_LIMITS: dict[int, dict[str, Any]] = {
    2025: RETIREMENT_LIMITS_2025,
    2026: RETIREMENT_LIMITS_2026,
}


RothPhaseoutStatus = Literal[
    "single",
    "married_filing_jointly",
    "married_filing_separately",
    "head_of_household",
]

_PHASEOUT_KEYS = {
    "single": "roth_ira_phaseout_single_cents",
    "married_filing_jointly": "roth_ira_phaseout_mfj_cents",
    "married_filing_separately": "roth_ira_phaseout_mfs_cents",
    "head_of_household": "roth_ira_phaseout_hoh_cents",
}


def _quantize_cents(value: Decimal) -> int:
    with localcontext() as ctx:
        ctx.prec = 28
        result = value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(result)


def _require_supported_tax_year(tax_year: int) -> None:
    if tax_year not in SUPPORTED_LIMIT_YEARS:
        raise ValueError(
            f"Tax year {tax_year} not yet supported. "
            f"Supported: {sorted(SUPPORTED_LIMIT_YEARS)}."
        )


def _require_supported_filing_status(filing_status: str) -> None:
    if filing_status not in _PHASEOUT_KEYS:
        raise ValueError(f"Unsupported filing status: {filing_status}.")


def _round_up_to_ten_dollars_cents(value: Decimal) -> int:
    with localcontext() as ctx:
        ctx.prec = 28
        rounded = (value / _TEN_DOLLARS_CENTS).quantize(Decimal("1"), rounding=ROUND_CEILING)
    return _quantize_cents(rounded * _TEN_DOLLARS_CENTS)


def roth_ira_allowed_contribution_cents(
    modified_agi_cents: int,
    filing_status: RothPhaseoutStatus,
    age: int,
    tax_year: int = 2026,
    taxable_compensation_cents: int | None = None,
    other_ira_contributions_cents: int = 0,
) -> int:
    """Return the IRS Worksheet 2-2 Roth IRA limit.

    The worksheet order is literal:
    1. Cap the age-based IRA limit by taxable compensation.
    2. Apply the Roth MAGI phaseout to that capped amount.
    3. Separately subtract other IRA contributions from the same capped amount.
    4. Return the lesser of the phaseout result and the other-IRA result.

    Positive phaseout-reduced values are rounded up to the next $10 and floored
    at $200, matching Pub. 590-A.

    Caller note: `modified_agi_cents` is Roth-IRA MAGI. Many coaching cases can
    use AGI as a close proxy, but uncommon add-backs still exist.
    """

    _require_supported_tax_year(tax_year)
    _require_supported_filing_status(filing_status)

    if modified_agi_cents < 0:
        raise ValueError("modified_agi_cents must be >= 0")
    if age < 0:
        raise ValueError("age must be >= 0")
    if taxable_compensation_cents is not None and taxable_compensation_cents < 0:
        raise ValueError("taxable_compensation_cents must be >= 0")
    if other_ira_contributions_cents < 0:
        raise ValueError("other_ira_contributions_cents must be >= 0")

    limits = RETIREMENT_LIMITS[tax_year]
    full_limit_cents = limits["ira_contribution_cents"]
    if age >= 50:
        full_limit_cents += limits["ira_catchup_cents"]

    line6_cents = (
        min(full_limit_cents, taxable_compensation_cents)
        if taxable_compensation_cents is not None
        else full_limit_cents
    )

    phaseout_start_cents, phaseout_end_cents = limits[_PHASEOUT_KEYS[filing_status]]
    if modified_agi_cents <= phaseout_start_cents:
        line8_cents = line6_cents
    elif modified_agi_cents >= phaseout_end_cents:
        line8_cents = 0
    else:
        with localcontext() as ctx:
            ctx.prec = 28
            reduction = (
                Decimal(line6_cents)
                * Decimal(modified_agi_cents - phaseout_start_cents)
                / Decimal(phaseout_end_cents - phaseout_start_cents)
            )
            line8_raw = Decimal(line6_cents) - reduction
        line8_cents = _round_up_to_ten_dollars_cents(line8_raw)
        if 0 < line8_cents < _MIN_PHASEOUT_CONTRIBUTION_CENTS:
            line8_cents = _MIN_PHASEOUT_CONTRIBUTION_CENTS

    line10_cents = max(0, line6_cents - other_ira_contributions_cents)
    return min(line8_cents, line10_cents)
