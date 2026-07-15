from __future__ import annotations

from decimal import Decimal

import pytest

from finance_cli.advisory import PriorityStep, contribution_priority


def test_prime_directive_high_interest_debt() -> None:
    result = contribution_priority(
        taxable_income_cents=60_000_00,
        filing_status="single",
        modified_agi_cents=60_000_00,
        annual_salary_cents=70_000_00,
        employer_match_pct=Decimal("0.5"),
        employer_match_limit_pct=Decimal("0.04"),
        existing_emergency_fund_cents=500_00,
        monthly_expenses_cents=3_000_00,
        high_interest_debt_cents=5_000_00,
        high_interest_apr=Decimal("0.22"),
        tax_year=2026,
    )

    assert isinstance(result[0], PriorityStep)
    assert [step.order for step in result] == [1, 2, 3, 4, 5, 6]
    assert [step.account for step in result] == [
        "starter_emergency_fund",
        "401k_match",
        "high_interest_debt",
        "emergency_fund",
        "roth_ira",
        "max_401k",
    ]
    assert [step.annual_amount_cents for step in result] == [
        500_00,
        2_800_00,
        0,
        8_500_00,
        7_500_00,
        21_700_00,
    ]
    assert result[1].monthly_equivalent_cents == 23_333
    assert result[2].priority_rank == "P0_required"
    assert "22% APR balance" in result[2].reason


def test_prime_directive_high_income_phaseout() -> None:
    result = contribution_priority(
        taxable_income_cents=180_000_00,
        filing_status="single",
        modified_agi_cents=180_000_00,
        annual_salary_cents=200_000_00,
        employer_match_pct=Decimal("1"),
        employer_match_limit_pct=Decimal("0.06"),
        has_hsa_eligible_hdhp=True,
        existing_emergency_fund_cents=50_000_00,
        age=40,
        tax_year=2026,
    )

    assert [step.account for step in result] == [
        "401k_match",
        "hsa",
        "roth_ira",
        "max_401k",
    ]
    assert result[1].annual_amount_cents == 4_400_00
    assert result[2].annual_amount_cents == 0
    assert (
        result[2].reason
        == "MAGI $180,000.00 exceeds 2026 Roth IRA phaseout end of $168,000.00 for single filers, so Roth IRA room is $0.00. Your current marginal federal rate is 24%."
    )
    assert result[3].annual_amount_cents == 12_500_00


def test_prime_directive_no_debt_no_match_at_target() -> None:
    result = contribution_priority(
        taxable_income_cents=50_000_00,
        filing_status="single",
        modified_agi_cents=50_000_00,
        annual_salary_cents=50_000_00,
        existing_emergency_fund_cents=10_000_00,
        monthly_expenses_cents=3_000_00,
        age=40,
        tax_year=2026,
    )

    assert [step.account for step in result] == ["roth_ira"]
    assert result[0].annual_amount_cents == 7_500_00


def test_all_zero_inputs_return_zero_cap_roth_step() -> None:
    result = contribution_priority(
        taxable_income_cents=0,
        filing_status="single",
        modified_agi_cents=0,
        tax_year=2026,
    )

    assert [step.account for step in result] == ["roth_ira"]
    assert result[0].annual_amount_cents == 0
    assert "No earned compensation reported" in result[0].reason


def test_other_ira_contributions_reduce_roth_room() -> None:
    result = contribution_priority(
        taxable_income_cents=50_000_00,
        filing_status="single",
        modified_agi_cents=50_000_00,
        annual_salary_cents=100_000_00,
        other_ira_contributions_cents=3_000_00,
        tax_year=2026,
    )

    assert [step.account for step in result] == ["roth_ira"]
    assert result[0].annual_amount_cents == 4_500_00
    assert "already contributed $3,000.00" in result[0].reason
    assert "remaining Roth IRA room is $4,500.00" in result[0].reason


@pytest.mark.parametrize(
    ("age", "expected_max_401k_cents"),
    [
        (40, 20_500_00),
        (50, 28_500_00),
        (60, 31_750_00),
        (64, 28_500_00),
    ],
)
def test_age_variants_change_max_401k_catchup(age: int, expected_max_401k_cents: int) -> None:
    result = contribution_priority(
        taxable_income_cents=50_000_00,
        filing_status="single",
        modified_agi_cents=50_000_00,
        annual_salary_cents=100_000_00,
        employer_match_pct=Decimal("0.5"),
        employer_match_limit_pct=Decimal("0.04"),
        age=age,
        tax_year=2026,
    )

    max_401k_step = next(step for step in result if step.account == "max_401k")
    assert max_401k_step.annual_amount_cents == expected_max_401k_cents


def test_docstring_mentions_self_employed_scope_note() -> None:
    assert contribution_priority.__doc__ is not None
    assert "self-employed" in contribution_priority.__doc__.lower()


def test_unsupported_tax_year_raises_value_error() -> None:
    with pytest.raises(
        ValueError,
        match=r"Tax year 2027 not yet supported\. Supported: \[2025, 2026\]\.",
    ):
        contribution_priority(
            taxable_income_cents=50_000_00,
            filing_status="single",
            modified_agi_cents=50_000_00,
            tax_year=2027,
        )
