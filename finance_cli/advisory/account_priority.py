"""Contribution-priority sequencing for common coaching scenarios.

This is a coaching adaptation of the r/personalfinance wiki flowchart, not a
literal implementation. The ordering is intentionally opinionated:
- HSA comes before Roth IRA when the user is HDHP-eligible.
- The $1,000 starter emergency fund only appears ahead of debt payoff when
  high-interest debt exists.
- Roth IRA is the default IRA step when eligible; Traditional deductibility and
  backdoor workflows are outside this helper's scope.

Caller note: most users will pass W-2 salary as both `annual_salary_cents` and
the default for `earned_compensation_cents`. Self-employed or mixed-income
users are not blocked, but they must provide the correct IRA compensation
figure themselves because the function does not enforce a W-2-only model.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP, localcontext
from typing import Any

from ._decimal_utils import to_decimal
from ._types import PriorityStep
from .retirement_limits_data import RETIREMENT_LIMITS, SUPPORTED_LIMIT_YEARS, roth_ira_allowed_contribution_cents
from .tax_brackets import FilingStatus, marginal_rate


_HUNDRED = Decimal("100")
_ONE = Decimal("1")
_TEN_DOLLARS_CENTS = Decimal("1000")
_MIN_PHASEOUT_CONTRIBUTION_CENTS = 200_00
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


def _currency(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    whole_dollars, remainder = divmod(abs(cents), 100)
    return f"{sign}${whole_dollars:,}.{remainder:02d}"


def _percent_from_fraction(rate: Decimal) -> str:
    return format(rate * _HUNDRED, "f").rstrip("0").rstrip(".")


def _percent_from_percentage(rate: Decimal) -> str:
    return format(rate, "f").rstrip("0").rstrip(".")


def _quantized_cents_from_fraction(base_cents: int, rate: Decimal) -> int:
    return _quantize_cents(Decimal(base_cents) * rate)


def _age_based_401k_catchup_cents(age: int, limits: dict[str, Any]) -> int:
    if 60 <= age <= 63:
        return limits["401k_supercatchup_cents"]
    if age >= 50:
        return limits["401k_catchup_cents"]
    return 0


def _hsa_limit_cents(age: int, hsa_family_coverage: bool, limits: dict[str, Any]) -> int:
    limit_cents = (
        limits["hsa_family_cents"] if hsa_family_coverage else limits["hsa_single_cents"]
    )
    if age >= 55:
        limit_cents += limits["hsa_catchup_cents"]
    return limit_cents


def _round_up_to_ten_dollars_cents(value: Decimal) -> int:
    with localcontext() as ctx:
        ctx.prec = 28
        rounded = (value / _TEN_DOLLARS_CENTS).quantize(Decimal("1"), rounding=ROUND_CEILING)
    return _quantize_cents(rounded * _TEN_DOLLARS_CENTS)


def _roth_worksheet_values(
    *,
    modified_agi_cents: int,
    filing_status: FilingStatus,
    age: int,
    tax_year: int,
    taxable_compensation_cents: int,
    other_ira_contributions_cents: int,
) -> dict[str, int]:
    limits = RETIREMENT_LIMITS[tax_year]
    full_limit_cents = limits["ira_contribution_cents"]
    if age >= 50:
        full_limit_cents += limits["ira_catchup_cents"]

    line6_cents = min(full_limit_cents, taxable_compensation_cents)
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
    return {
        "full_limit_cents": full_limit_cents,
        "phaseout_start_cents": phaseout_start_cents,
        "phaseout_end_cents": phaseout_end_cents,
        "line6_cents": line6_cents,
        "line8_cents": line8_cents,
        "line10_cents": line10_cents,
    }


def _roth_reason(
    *,
    roth_room_cents: int,
    worksheet: dict[str, int],
    modified_agi_cents: int,
    taxable_compensation_cents: int,
    other_ira_contributions_cents: int,
    filing_status: FilingStatus,
    tax_year: int,
    marginal_rate_pct: Decimal,
) -> str:
    marginal_context = (
        f" Your current marginal federal rate is {_percent_from_percentage(marginal_rate_pct)}%."
    )
    if worksheet["line6_cents"] == 0:
        return "No earned compensation reported — IRA contributions require earned income." + marginal_context

    if worksheet["line8_cents"] == 0:
        return (
            f"MAGI {_currency(modified_agi_cents)} exceeds {tax_year} Roth IRA phaseout end "
            f"of {_currency(worksheet['phaseout_end_cents'])} for {filing_status.replace('_', ' ')} filers, "
            "so Roth IRA room is $0.00."
            + marginal_context
        )

    if worksheet["line10_cents"] < worksheet["line8_cents"]:
        return (
            f"You have already contributed {_currency(other_ira_contributions_cents)} to Traditional IRA "
            f"this year; remaining Roth IRA room is {_currency(roth_room_cents)}."
            + marginal_context
        )

    if worksheet["line8_cents"] < worksheet["line6_cents"]:
        return (
            f"MAGI {_currency(modified_agi_cents)} is inside the {tax_year} Roth IRA phaseout band "
            f"({_currency(worksheet['phaseout_start_cents'])}-{_currency(worksheet['phaseout_end_cents'])}), "
            f"reducing your allowed Roth IRA contribution to {_currency(roth_room_cents)}."
            + marginal_context
        )

    if worksheet["line6_cents"] < worksheet["full_limit_cents"]:
        return (
            f"Earned compensation of {_currency(taxable_compensation_cents)} caps IRA contributions below "
            f"the {_currency(worksheet['full_limit_cents'])} statutory limit."
            + marginal_context
        )

    return (
        f"MAGI {_currency(modified_agi_cents)} is below the {tax_year} Roth IRA phaseout start of "
        f"{_currency(worksheet['phaseout_start_cents'])}, and earned compensation supports the full "
        f"{_currency(roth_room_cents)} Roth IRA limit."
        + marginal_context
    )


def contribution_priority(
    taxable_income_cents: int,
    filing_status: FilingStatus,
    modified_agi_cents: int,
    annual_salary_cents: int = 0,
    earned_compensation_cents: int | None = None,
    other_ira_contributions_cents: int = 0,
    tax_year: int = 2026,
    employer_match_pct: Decimal | float = Decimal("0"),
    employer_match_limit_pct: Decimal | float = Decimal("0"),
    has_mega_backdoor: bool = False,
    has_hsa_eligible_hdhp: bool = False,
    hsa_family_coverage: bool = False,
    age: int = 40,
    existing_emergency_fund_cents: int = 0,
    monthly_expenses_cents: int = 0,
    target_emergency_months: int = 3,
    starter_emergency_threshold_cents: int = 1_000_00,
    high_interest_debt_cents: int = 0,
    high_interest_apr: Decimal | float = Decimal("0"),
    high_interest_threshold: Decimal | float = Decimal("0.08"),
    low_interest_debt_cents: int = 0,
    low_interest_apr: Decimal | float = Decimal("0"),
    low_interest_tax_deductible: bool = False,
    expected_market_return: Decimal | float = Decimal("0.08"),
) -> list[PriorityStep]:
    """Return ordered contribution-priority steps for the current tax year.

    `taxable_income_cents` is post-deduction taxable income (Form 1040 line 15)
    used only for marginal-rate context in the reasoning text.

    `modified_agi_cents` is Roth-IRA MAGI. Callers without an exact worksheet
    MAGI often use AGI as a close approximation, but uncommon add-backs can
    still matter.

    `earned_compensation_cents` defaults to `annual_salary_cents` when omitted.
    That is usually acceptable for W-2 users, but self-employed or mixed-income
    cases should pass the correct IRA compensation amount directly.
    """

    _require_supported_tax_year(tax_year)

    int_fields = {
        "taxable_income_cents": taxable_income_cents,
        "modified_agi_cents": modified_agi_cents,
        "annual_salary_cents": annual_salary_cents,
        "other_ira_contributions_cents": other_ira_contributions_cents,
        "age": age,
        "existing_emergency_fund_cents": existing_emergency_fund_cents,
        "monthly_expenses_cents": monthly_expenses_cents,
        "target_emergency_months": target_emergency_months,
        "starter_emergency_threshold_cents": starter_emergency_threshold_cents,
        "high_interest_debt_cents": high_interest_debt_cents,
        "low_interest_debt_cents": low_interest_debt_cents,
    }
    if earned_compensation_cents is not None:
        int_fields["earned_compensation_cents"] = earned_compensation_cents
    for field_name, value in int_fields.items():
        if value < 0:
            raise ValueError(f"{field_name} must be >= 0")

    employer_match_pct_decimal = to_decimal(employer_match_pct)
    employer_match_limit_pct_decimal = to_decimal(employer_match_limit_pct)
    high_interest_apr_decimal = to_decimal(high_interest_apr)
    high_interest_threshold_decimal = to_decimal(high_interest_threshold)
    low_interest_apr_decimal = to_decimal(low_interest_apr)
    expected_market_return_decimal = to_decimal(expected_market_return)

    decimal_fields = {
        "employer_match_pct": employer_match_pct_decimal,
        "employer_match_limit_pct": employer_match_limit_pct_decimal,
        "high_interest_apr": high_interest_apr_decimal,
        "high_interest_threshold": high_interest_threshold_decimal,
        "low_interest_apr": low_interest_apr_decimal,
        "expected_market_return": expected_market_return_decimal,
    }
    for field_name, value in decimal_fields.items():
        if value < 0:
            raise ValueError(f"{field_name} must be >= 0")

    limits = RETIREMENT_LIMITS[tax_year]
    taxable_compensation_cents = (
        annual_salary_cents if earned_compensation_cents is None else earned_compensation_cents
    )
    marginal_rate_pct = marginal_rate(
        taxable_income_cents=taxable_income_cents,
        filing_status=filing_status,
        tax_year=tax_year,
    )
    marginal_rate_fraction = marginal_rate_pct / _HUNDRED

    step_specs: list[dict[str, Any]] = []

    if (
        existing_emergency_fund_cents < starter_emergency_threshold_cents
        and high_interest_debt_cents > 0
    ):
        starter_gap_cents = starter_emergency_threshold_cents - existing_emergency_fund_cents
        step_specs.append(
            {
                "account": "starter_emergency_fund",
                "action": "Build starter emergency fund",
                "annual_amount_cents": starter_gap_cents,
                "priority_rank": "P0_required",
                "reason": (
                    f"Raise cash reserves from {_currency(existing_emergency_fund_cents)} to "
                    f"{_currency(starter_emergency_threshold_cents)} before aggressively attacking debt."
                ),
            }
        )

    match_step_amount_cents = 0
    employer_match_dollars_cents = 0
    has_workplace_plan = (
        has_mega_backdoor
        or employer_match_pct_decimal > 0
        or employer_match_limit_pct_decimal > 0
    )
    if employer_match_pct_decimal > 0 and employer_match_limit_pct_decimal > 0:
        if annual_salary_cents == 0:
            match_reason = (
                "Salary not provided; employer match terms were supplied but the employee contribution "
                "needed to capture the match cannot be computed."
            )
        else:
            match_step_amount_cents = _quantized_cents_from_fraction(
                annual_salary_cents, employer_match_limit_pct_decimal
            )
            employer_match_dollars_cents = _quantized_cents_from_fraction(
                match_step_amount_cents, employer_match_pct_decimal
            )
            match_reason = (
                f"Contribute {_currency(match_step_amount_cents)} to capture the full employer match; "
                f"at {_percent_from_fraction(employer_match_pct_decimal)}% up to "
                f"{_percent_from_fraction(employer_match_limit_pct_decimal)}% of salary, that unlocks about "
                f"{_currency(employer_match_dollars_cents)} of employer money."
            )
        step_specs.append(
            {
                "account": "401k_match",
                "action": "Capture full 401(k) employer match",
                "annual_amount_cents": match_step_amount_cents,
                "priority_rank": "P0_required",
                "reason": match_reason,
            }
        )

    if (
        high_interest_debt_cents > 0
        and high_interest_apr_decimal >= high_interest_threshold_decimal
    ):
        step_specs.append(
            {
                "account": "high_interest_debt",
                "action": "Pay down high-interest debt",
                "annual_amount_cents": 0,
                "priority_rank": "P0_required",
                "reason": (
                    f"The {_percent_from_fraction(high_interest_apr_decimal)}% APR balance is above the "
                    f"{_percent_from_fraction(high_interest_threshold_decimal)}% high-interest threshold, "
                    "so paying it down outranks additional investing after any employer match."
                ),
            }
        )

    emergency_target_cents = target_emergency_months * monthly_expenses_cents
    if existing_emergency_fund_cents < emergency_target_cents:
        emergency_gap_cents = emergency_target_cents - existing_emergency_fund_cents
        step_specs.append(
            {
                "account": "emergency_fund",
                "action": "Finish full emergency fund",
                "annual_amount_cents": emergency_gap_cents,
                "priority_rank": "P1_high",
                "reason": (
                    f"Target emergency reserves are {_currency(emergency_target_cents)} "
                    f"({target_emergency_months} months of {_currency(monthly_expenses_cents)} spending); "
                    f"you are short by {_currency(emergency_gap_cents)}."
                ),
            }
        )

    if has_hsa_eligible_hdhp:
        hsa_limit_cents = _hsa_limit_cents(age, hsa_family_coverage, limits)
        coverage_label = "family" if hsa_family_coverage else "individual"
        step_specs.append(
            {
                "account": "hsa",
                "action": "Max HSA contribution",
                "annual_amount_cents": hsa_limit_cents,
                "priority_rank": "P1_high",
                "reason": (
                    f"The {tax_year} {coverage_label} HSA limit is {_currency(hsa_limit_cents)}; "
                    "HSA dollars are triple tax-advantaged when used for qualified medical expenses."
                ),
            }
        )

    roth_room_cents = roth_ira_allowed_contribution_cents(
        modified_agi_cents=modified_agi_cents,
        filing_status=filing_status,
        age=age,
        tax_year=tax_year,
        taxable_compensation_cents=taxable_compensation_cents,
        other_ira_contributions_cents=other_ira_contributions_cents,
    )
    roth_worksheet = _roth_worksheet_values(
        modified_agi_cents=modified_agi_cents,
        filing_status=filing_status,
        age=age,
        tax_year=tax_year,
        taxable_compensation_cents=taxable_compensation_cents,
        other_ira_contributions_cents=other_ira_contributions_cents,
    )
    step_specs.append(
        {
            "account": "roth_ira",
            "action": "Fund Roth IRA",
            "annual_amount_cents": roth_room_cents,
            "priority_rank": "P1_high",
            "reason": _roth_reason(
                roth_room_cents=roth_room_cents,
                worksheet=roth_worksheet,
                modified_agi_cents=modified_agi_cents,
                taxable_compensation_cents=taxable_compensation_cents,
                other_ira_contributions_cents=other_ira_contributions_cents,
                filing_status=filing_status,
                tax_year=tax_year,
                marginal_rate_pct=marginal_rate_pct,
            ),
        }
    )

    employee_401k_limit_cents = (
        limits["401k_contribution_cents"] + _age_based_401k_catchup_cents(age, limits)
    )
    if has_workplace_plan:
        remaining_401k_room_cents = max(employee_401k_limit_cents - match_step_amount_cents, 0)
        step_specs.append(
            {
                "account": "max_401k",
                "action": "Max remaining employee 401(k)",
                "annual_amount_cents": remaining_401k_room_cents,
                "priority_rank": "P2_moderate",
                "reason": (
                    f"After reserving {_currency(match_step_amount_cents)} for the match step, "
                    f"{_currency(remaining_401k_room_cents)} of employee 401(k) room remains for {tax_year}."
                ),
            }
        )

    if has_mega_backdoor:
        mega_backdoor_room_cents = max(
            limits["401k_total_limit_cents"] - employee_401k_limit_cents - employer_match_dollars_cents,
            0,
        )
        step_specs.append(
            {
                "account": "mega_backdoor_roth",
                "action": "Use mega backdoor Roth after-tax space",
                "annual_amount_cents": mega_backdoor_room_cents,
                "priority_rank": "P2_moderate",
                "reason": (
                    f"Using the {tax_year} total 401(k) limit of {_currency(limits['401k_total_limit_cents'])}, "
                    f"about {_currency(mega_backdoor_room_cents)} of after-tax room remains after "
                    f"{_currency(employee_401k_limit_cents)} of employee contributions and "
                    f"{_currency(employer_match_dollars_cents)} of employer match."
                ),
            }
        )

    if low_interest_debt_cents > 0:
        effective_low_interest_apr = low_interest_apr_decimal
        tax_note = ""
        if low_interest_tax_deductible:
            effective_low_interest_apr = low_interest_apr_decimal * (_ONE - marginal_rate_fraction)
            tax_note = (
                f" After the tax deduction, the effective APR is "
                f"{_percent_from_fraction(effective_low_interest_apr)}%."
            )

        if effective_low_interest_apr > expected_market_return_decimal:
            direction = "prepaying the debt is favored"
        elif effective_low_interest_apr < expected_market_return_decimal:
            direction = "investing is favored on expected return"
        else:
            direction = "either direction is mathematically similar"

        step_specs.append(
            {
                "account": "low_interest_debt_or_invest",
                "action": "Compare low-interest debt prepayment vs investing",
                "annual_amount_cents": 0,
                "priority_rank": "P3_low",
                "reason": (
                    f"Low-interest debt costs {_percent_from_fraction(low_interest_apr_decimal)}% versus an "
                    f"assumed {_percent_from_fraction(expected_market_return_decimal)}% market return;"
                    f"{tax_note} in this simplified comparison, {direction}. This step is directional only."
                ),
            }
        )

    steps: list[PriorityStep] = []
    for index, spec in enumerate(step_specs, start=1):
        annual_amount_cents = spec["annual_amount_cents"]
        steps.append(
            PriorityStep(
                order=index,
                account=spec["account"],
                action=spec["action"],
                annual_amount_cents=annual_amount_cents,
                monthly_equivalent_cents=annual_amount_cents // 12,
                priority_rank=spec["priority_rank"],
                reason=spec["reason"],
            )
        )
    return steps
