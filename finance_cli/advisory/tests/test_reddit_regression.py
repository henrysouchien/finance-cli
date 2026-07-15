from decimal import Decimal

from finance_cli.advisory import (
    contribution_priority,
    debt_vs_invest,
    federal_tax,
    fee_impact,
    runway_projection,
    time_to_goal,
)


# Keep this file focused on Reddit-style integration scenarios. Exact known-answer
# coverage for the newer FM-1b single-module helpers lives in:
# - test_account_comparator.py
# - test_product_evaluator.py
# - test_allocation.py
# - test_retirement_limits_data.py


def test_car_loan_invest_regression() -> None:
    result = debt_vs_invest(
        debt_balance_cents=20_000_00,
        debt_apr=Decimal("0.096"),
        monthly_extra_payment_cents=500_00,
        debt_minimum_payment_cents=200_00,
        expected_market_return=Decimal("0.08"),
    )

    assert result.recommendation == "pay_debt"
    assert result.debt_payoff_months == 33
    assert result.debt_interest_saved_cents == 219_446
    assert result.investment_value_at_debt_payoff_cents == 1_831_717
    assert result.difference_cents == 37_729


def test_student_loan_deductible_regression() -> None:
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
    assert result.debt_payoff_months == 27
    assert result.debt_interest_saved_cents == 80_836
    assert result.investment_value_at_debt_payoff_cents == 1_175_369
    assert result.difference_cents == -14_533


def test_fee_drag_1_75pct_regression() -> None:
    result = fee_impact(
        balance_cents=500_000_00,
        current_fee_pct=Decimal("0.0175"),
        proposed_fee_pct=Decimal("0.0003"),
        years=30,
        annual_return=Decimal("0.08"),
        monthly_contribution_cents=0,
    )

    assert result.current_total_cents == 308_203_926
    assert result.proposed_total_cents == 498_956_915
    assert result.savings_cents == 190_752_989


def test_time_to_1m_regression() -> None:
    assert time_to_goal(50_000_00, 1_000_000_00, 1_000_00, Decimal("0.08")) == 270


def test_marginal_rate_lookup_22pct_regression() -> None:
    result = federal_tax(90_000_00, "single", tax_year=2026)

    assert result.tax_owed_cents == 1_451_200
    assert result.marginal_rate_pct == Decimal("22")


def test_runway_4pct_regression() -> None:
    assert runway_projection(100_000_00, 4_000_00, Decimal("0.04")) == 27


def test_prime_directive_high_interest_debt_reddit_regression() -> None:
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


def test_prime_directive_high_income_phaseout_reddit_regression() -> None:
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
    assert (
        result[2].reason
        == "MAGI $180,000.00 exceeds 2026 Roth IRA phaseout end of $168,000.00 for single filers, so Roth IRA room is $0.00. Your current marginal federal rate is 24%."
    )


def test_prime_directive_no_debt_no_match_at_target_reddit_regression() -> None:
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
