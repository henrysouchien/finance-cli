from decimal import Decimal

import pytest

from finance_cli import mcp_server
from finance_cli.advisory import (
    AnnuitySurrenderAnalysis,
    FundComparisonResult,
    PriorityStep,
    RothConversionAnalysis,
    RothTraditionalComparison,
    annuity_surrender_analysis,
    contribution_priority,
    fund_fee_comparison,
    roth_conversion_analysis,
    roth_vs_traditional,
    target_allocation,
)


def test_advisory_future_value_tool_returns_json_safe_envelope() -> None:
    result = mcp_server.advisory_future_value(
        principal_cents=100_000_00,
        annual_rate_pct=8.0,
        years=10,
    )

    assert result == {
        "data": {
            "future_value_cents": 21_589_250,
            "principal_cents": 10_000_000,
            "annual_rate_pct": "8.0",
            "years": 10,
            "monthly_contribution_cents": 0,
        },
        "summary": {
            "text": "Projected value after 10 years at 8.0% is $215,892.50.",
            "future_value_cents": 21_589_250,
        },
    }


def test_advisory_taxable_income_from_gross_tool_reports_deduction_choice() -> None:
    result = mcp_server.advisory_taxable_income_from_gross(
        gross_income_cents=90_000_00,
        filing_status="single",
        tax_year=2026,
    )

    assert result["data"]["agi_cents"] == 9_000_000
    assert result["data"]["deduction_applied_cents"] == 1_610_000
    assert result["data"]["deduction_type"] == "standard"
    assert result["data"]["taxable_income_cents"] == 7_390_000


def test_advisory_federal_tax_tool_can_include_fica() -> None:
    result = mcp_server.advisory_federal_tax(
        taxable_income_cents=90_000_00,
        filing_status="single",
        tax_year=2026,
        include_fica=True,
        gross_wages_cents=90_000_00,
    )

    assert result["data"]["tax_owed_cents"] == 1_451_200
    assert result["data"]["marginal_rate_pct"] == "22"
    assert result["data"]["bracket_room_cents"] == 15_700_00
    assert result["data"]["fica"]["total_cents"] == 688_500
    assert result["data"]["total_tax_cents"] == 2_139_700
    assert result["summary"]["total_tax_cents"] == 2_139_700


def test_advisory_roth_vs_traditional_tool_converts_expected_return_pct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_roth_vs_traditional(**kwargs):
        captured.update(kwargs)
        return RothTraditionalComparison(
            contribution_cents=kwargs["contribution_cents"],
            current_marginal_rate_pct=kwargs["current_marginal_rate_pct"],
            estimated_retirement_marginal_rate_pct=kwargs["estimated_retirement_marginal_rate_pct"],
            years_to_retirement=kwargs["years_to_retirement"],
            expected_annual_return=kwargs["expected_annual_return"],
            roth_after_tax_cents=1,
            traditional_after_tax_cents=0,
            winner="roth",
            advantage_cents=1,
            reason="test",
        )

    monkeypatch.setattr(mcp_server, "roth_vs_traditional", fake_roth_vs_traditional)

    result = mcp_server.advisory_roth_vs_traditional(
        contribution_cents=700_000,
        current_marginal_rate_pct=22.0,
        estimated_retirement_marginal_rate_pct=12.0,
        years_to_retirement=30,
        expected_annual_return_pct=8.0,
    )

    assert captured["current_marginal_rate_pct"] == Decimal("22.0")
    assert captured["estimated_retirement_marginal_rate_pct"] == Decimal("12.0")
    assert captured["expected_annual_return"] == Decimal("0.08")
    assert set(result) == {"data", "summary"}
    assert isinstance(result["data"], dict)
    assert isinstance(result["summary"]["text"], str)


def test_advisory_roth_vs_traditional_tool_matches_helper() -> None:
    expected = roth_vs_traditional(
        contribution_cents=700_000,
        current_marginal_rate_pct=Decimal("22"),
        estimated_retirement_marginal_rate_pct=Decimal("12"),
        years_to_retirement=30,
        expected_annual_return=Decimal("0.08"),
    )

    result = mcp_server.advisory_roth_vs_traditional(
        contribution_cents=700_000,
        current_marginal_rate_pct=22.0,
        estimated_retirement_marginal_rate_pct=12.0,
        years_to_retirement=30,
        expected_annual_return_pct=8.0,
    )

    assert result["data"]["winner"] == expected.winner
    assert result["data"]["advantage_cents"] == expected.advantage_cents
    assert result["data"]["expected_annual_return"] == "0.08"


def test_advisory_roth_conversion_analysis_tool_converts_expected_return_pct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_roth_conversion_analysis(**kwargs):
        captured.update(kwargs)
        return RothConversionAnalysis(
            conversion_amount_cents=kwargs["conversion_amount_cents"],
            tax_cost_now_cents=1,
            current_marginal_rate_pct=kwargs["current_marginal_rate_pct"],
            estimated_retirement_marginal_rate_pct=kwargs["estimated_retirement_marginal_rate_pct"],
            years_to_retirement=kwargs["years_to_retirement"],
            expected_annual_return=kwargs["expected_annual_return"],
            total_wealth_if_converted_cents=2,
            total_wealth_if_not_converted_cents=1,
            net_advantage_cents=1,
            breakeven_years=0,
            recommendation="convert",
            reason="test",
        )

    monkeypatch.setattr(mcp_server, "roth_conversion_analysis", fake_roth_conversion_analysis)

    result = mcp_server.advisory_roth_conversion_analysis(
        conversion_amount_cents=5_000_000,
        current_marginal_rate_pct=12.0,
        estimated_retirement_marginal_rate_pct=22.0,
        years_to_retirement=25,
        expected_annual_return_pct=8.0,
    )

    assert captured["current_marginal_rate_pct"] == Decimal("12.0")
    assert captured["estimated_retirement_marginal_rate_pct"] == Decimal("22.0")
    assert captured["expected_annual_return"] == Decimal("0.08")
    assert set(result) == {"data", "summary"}
    assert isinstance(result["data"], dict)
    assert isinstance(result["summary"]["text"], str)


def test_advisory_roth_conversion_analysis_tool_matches_helper() -> None:
    expected = roth_conversion_analysis(
        conversion_amount_cents=5_000_000,
        current_marginal_rate_pct=Decimal("12"),
        estimated_retirement_marginal_rate_pct=Decimal("22"),
        years_to_retirement=25,
        expected_annual_return=Decimal("0.08"),
    )

    result = mcp_server.advisory_roth_conversion_analysis(
        conversion_amount_cents=5_000_000,
        current_marginal_rate_pct=12.0,
        estimated_retirement_marginal_rate_pct=22.0,
        years_to_retirement=25,
        expected_annual_return_pct=8.0,
    )

    assert result["data"]["recommendation"] == expected.recommendation
    assert result["data"]["net_advantage_cents"] == expected.net_advantage_cents
    assert result["data"]["expected_annual_return"] == "0.08"


def test_advisory_contribution_priority_tool_converts_percentage_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_contribution_priority(**kwargs):
        captured.update(kwargs)
        return [
            PriorityStep(
                order=1,
                account="roth_ira",
                action="Fund Roth IRA",
                annual_amount_cents=750_000,
                monthly_equivalent_cents=62_500,
                priority_rank="P1_high",
                reason="test",
            )
        ]

    monkeypatch.setattr(mcp_server, "contribution_priority", fake_contribution_priority)

    result = mcp_server.advisory_contribution_priority(
        taxable_income_cents=60_000_00,
        filing_status="single",
        modified_agi_cents=60_000_00,
        annual_salary_cents=70_000_00,
        employer_match_pct=50.0,
        employer_match_limit_pct=4.0,
        high_interest_debt_cents=5_000_00,
        high_interest_apr_pct=22.0,
        high_interest_threshold_pct=8.0,
        expected_market_return_pct=8.0,
    )

    assert captured["employer_match_pct"] == Decimal("0.5")
    assert captured["employer_match_limit_pct"] == Decimal("0.04")
    assert captured["high_interest_apr"] == Decimal("0.22")
    assert captured["high_interest_threshold"] == Decimal("0.08")
    assert captured["expected_market_return"] == Decimal("0.08")
    assert set(result) == {"data", "summary"}
    assert isinstance(result["data"], dict)
    assert isinstance(result["summary"]["text"], str)
    assert result["data"]["source_tax_year"] == 2026
    assert result["data"]["supported_tax_years"] == [2025, 2026]
    assert result["data"]["limits_source"] == {
        "retirement_limits": "IRS Notice 2025-67",
        "hsa_limits": "IRS Rev. Proc. 2025-19",
        "roth_ira_worksheet": "IRS Pub. 590-A Worksheet 2-2",
    }
    assert result["data"]["unsupported_year"] is False


def test_advisory_contribution_priority_tool_matches_helper() -> None:
    expected = contribution_priority(
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

    result = mcp_server.advisory_contribution_priority(
        taxable_income_cents=60_000_00,
        filing_status="single",
        modified_agi_cents=60_000_00,
        annual_salary_cents=70_000_00,
        employer_match_pct=50.0,
        employer_match_limit_pct=4.0,
        existing_emergency_fund_cents=500_00,
        monthly_expenses_cents=3_000_00,
        high_interest_debt_cents=5_000_00,
        high_interest_apr_pct=22.0,
        tax_year=2026,
    )

    assert result["data"]["steps"][0]["account"] == expected[0].account
    assert result["data"]["steps"][-1]["annual_amount_cents"] == expected[-1].annual_amount_cents
    assert result["summary"]["step_count"] == len(expected)
    assert result["summary"]["source_tax_year"] == 2026
    assert result["summary"]["supported_tax_years"] == [2025, 2026]
    assert result["summary"]["unsupported_year"] is False


def test_advisory_contribution_priority_unsupported_tax_year_returns_data_needed() -> None:
    result = mcp_server.advisory_contribution_priority(
        taxable_income_cents=60_000_00,
        filing_status="single",
        modified_agi_cents=60_000_00,
        tax_year=2027,
    )

    assert result["data"]["steps"] == []
    assert result["data"]["source_tax_year"] == 2027
    assert result["data"]["supported_tax_years"] == [2025, 2026]
    assert result["data"]["limits_source"] == {}
    assert result["data"]["unsupported_year"] is True
    assert result["data"]["data_needed"] == [
        "Use a supported tax year or gather current plan/payroll/provider contribution figures.",
        "Do not estimate annual retirement, IRA, or HSA contribution limits from memory.",
    ]
    assert result["summary"]["step_count"] == 0
    assert result["summary"]["next_account"] is None
    assert result["summary"]["unsupported_year"] is True
    assert "Supported years: [2025, 2026]" in result["summary"]["text"]


def test_advisory_contribution_priority_supported_years_include_source_metadata() -> None:
    supported_years = sorted(mcp_server.SUPPORTED_LIMIT_YEARS)
    for tax_year in supported_years:
        result = mcp_server.advisory_contribution_priority(
            taxable_income_cents=60_000_00,
            filing_status="single",
            modified_agi_cents=60_000_00,
            annual_salary_cents=70_000_00,
            tax_year=tax_year,
        )

        assert result["data"]["unsupported_year"] is False
        assert result["data"]["data_needed"] == []
        assert result["data"]["source_tax_year"] == tax_year
        assert result["data"]["supported_tax_years"] == supported_years
        assert set(result["data"]["limits_source"]) == {
            "retirement_limits",
            "hsa_limits",
            "roth_ira_worksheet",
        }
        assert all(result["data"]["limits_source"].values())


def test_advisory_annuity_surrender_analysis_tool_converts_percentage_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_annuity_surrender_analysis(**kwargs):
        captured.update(kwargs)
        return AnnuitySurrenderAnalysis(
            current_value_cents=kwargs["current_value_cents"],
            surrender_charge_cents=1,
            net_after_surrender_cents=2,
            guaranteed_annual_rate=kwargs["guaranteed_annual_rate"],
            years_remaining_guarantee=kwargs["years_remaining_guarantee"],
            alternative_annual_return=kwargs["alternative_annual_return"],
            value_if_kept_cents=3,
            value_if_surrendered_cents=4,
            advantage_cents=1,
            recommendation="surrender",
            reason="test",
        )

    monkeypatch.setattr(mcp_server, "annuity_surrender_analysis", fake_annuity_surrender_analysis)

    result = mcp_server.advisory_annuity_surrender_analysis(
        current_value_cents=10_000_000,
        surrender_charge_pct=2.0,
        guaranteed_annual_rate_pct=5.0,
        years_remaining_guarantee=10,
        alternative_annual_return_pct=8.0,
    )

    assert captured["surrender_charge_pct"] == Decimal("0.02")
    assert captured["guaranteed_annual_rate"] == Decimal("0.05")
    assert captured["alternative_annual_return"] == Decimal("0.08")
    assert set(result) == {"data", "summary"}
    assert isinstance(result["data"], dict)
    assert isinstance(result["summary"]["text"], str)


def test_advisory_annuity_surrender_analysis_tool_matches_helper() -> None:
    expected = annuity_surrender_analysis(
        current_value_cents=10_000_000,
        surrender_charge_pct=Decimal("0.02"),
        guaranteed_annual_rate=Decimal("0.05"),
        years_remaining_guarantee=10,
        alternative_annual_return=Decimal("0.08"),
    )

    result = mcp_server.advisory_annuity_surrender_analysis(
        current_value_cents=10_000_000,
        surrender_charge_pct=2.0,
        guaranteed_annual_rate_pct=5.0,
        years_remaining_guarantee=10,
        alternative_annual_return_pct=8.0,
    )

    assert result["data"]["recommendation"] == expected.recommendation
    assert result["data"]["advantage_cents"] == expected.advantage_cents
    assert result["data"]["guaranteed_annual_rate"] == "0.05"


def test_advisory_fund_fee_comparison_tool_converts_percentage_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_fund_fee_comparison(**kwargs):
        captured.update(kwargs)
        return FundComparisonResult(
            balance_cents=kwargs["balance_cents"],
            current_expense_ratio=kwargs["current_expense_ratio"],
            proposed_expense_ratio=kwargs["proposed_expense_ratio"],
            years=kwargs["years"],
            annual_return_gross=kwargs["annual_return_gross"],
            value_in_current_cents=1,
            value_in_proposed_cents=2,
            total_savings_cents=1,
            capital_gains_tax_cents=0,
            net_savings_cents=1,
            breakeven_years=Decimal("0"),
            recommendation="switch",
            reason="test",
        )

    monkeypatch.setattr(mcp_server, "fund_fee_comparison", fake_fund_fee_comparison)

    result = mcp_server.advisory_fund_fee_comparison(
        balance_cents=50_000_000,
        current_expense_ratio_pct=0.75,
        proposed_expense_ratio_pct=0.03,
        years=25,
        annual_return_gross_pct=8.0,
        capital_gains_tax_rate_pct=15.0,
    )

    assert captured["current_expense_ratio"] == Decimal("0.0075")
    assert captured["proposed_expense_ratio"] == Decimal("0.0003")
    assert captured["annual_return_gross"] == Decimal("0.08")
    assert captured["capital_gains_tax_rate"] == Decimal("0.15")
    assert set(result) == {"data", "summary"}
    assert isinstance(result["data"], dict)
    assert isinstance(result["summary"]["text"], str)


def test_advisory_fund_fee_comparison_tool_matches_helper() -> None:
    expected = fund_fee_comparison(
        balance_cents=50_000_000,
        current_expense_ratio=Decimal("0.0075"),
        proposed_expense_ratio=Decimal("0.0003"),
        years=25,
        annual_return_gross=Decimal("0.08"),
        unrealized_gain_cents=15_000_000,
        capital_gains_tax_rate=Decimal("0.15"),
    )

    result = mcp_server.advisory_fund_fee_comparison(
        balance_cents=50_000_000,
        current_expense_ratio_pct=0.75,
        proposed_expense_ratio_pct=0.03,
        years=25,
        annual_return_gross_pct=8.0,
        unrealized_gain_cents=15_000_000,
        capital_gains_tax_rate_pct=15.0,
    )

    assert result["data"]["recommendation"] == expected.recommendation
    assert result["data"]["net_savings_cents"] == expected.net_savings_cents
    assert result["data"]["current_expense_ratio"] == "0.0075"


def test_advisory_target_allocation_tool_returns_helper_result() -> None:
    expected = target_allocation(age=35, retirement_age=65, risk_tolerance="moderate")

    result = mcp_server.advisory_target_allocation(
        age=35,
        retirement_age=65,
        risk_tolerance="moderate",
    )

    assert set(result) == {"data", "summary"}
    assert isinstance(result["data"], dict)
    assert isinstance(result["summary"]["text"], str)
    assert result["data"]["total_equities_pct"] == str(expected.total_equities_pct)
    assert result["data"]["us_stocks_pct"] == str(expected.us_stocks_pct)


def test_advisory_home_affordability_returns_deterministic_scenario() -> None:
    result = mcp_server.advisory_home_affordability(
        home_price_cents=42_000_000,
        down_payment_cents=4_200_000,
        annual_interest_rate_pct=6.75,
        term_years=30,
        property_tax_monthly_cents=50_000,
        insurance_monthly_cents=18_000,
        hoa_monthly_cents=0,
        pmi_monthly_cents=22_000,
        maintenance_reserve_monthly_cents=35_000,
        closing_cost_estimate_cents=1_260_000,
        moving_cost_estimate_cents=250_000,
        liquid_cash_cents=6_800_000,
        reserve_target_cents=1_800_000,
        other_monthly_debt_payments_cents=76_000,
        gross_monthly_income_cents=900_000,
    )

    assert result["data"]["loan_amount_cents"] == 37_800_000
    assert result["data"]["monthly_principal_interest_cents"] == 245_170
    assert result["data"]["monthly_housing_payment_cents"] == 335_170
    assert result["data"]["monthly_homeownership_cost_cents"] == 370_170
    assert result["data"]["cash_to_close"] == {
        "down_payment_cents": 4_200_000,
        "closing_cost_estimate_cents": 1_260_000,
        "moving_cost_estimate_cents": 250_000,
        "cash_to_close_total_cents": 5_710_000,
        "liquid_cash_cents": 6_800_000,
        "reserve_after_close_cents": 1_090_000,
        "reserve_target_cents": 1_800_000,
        "reserve_gap_cents": 710_000,
    }
    assert result["data"]["ratios"] == {
        "front_end_ratio_pct": "37.2",
        "back_end_ratio_pct": "45.7",
        "full_homeownership_cost_ratio_pct": "41.1",
        "other_monthly_debt_payments_cents": 76_000,
        "ratio_notes": ["Ratios use supplied gross monthly income."],
    }
    assert result["summary"]["reserve_gap_cents"] == 710_000
    assert "$3,351.70" in result["summary"]["text"]


def test_advisory_home_affordability_missing_income_omits_dti_context() -> None:
    result = mcp_server.advisory_home_affordability(
        home_price_cents=300_000_00,
        down_payment_cents=60_000_00,
        annual_interest_rate_pct=0.0,
        term_years=30,
        liquid_cash_cents=70_000_00,
        reserve_target_cents=20_000_00,
    )

    assert result["data"]["monthly_principal_interest_cents"] == 66_667
    assert result["data"]["ratios"] == {
        "front_end_ratio_pct": None,
        "back_end_ratio_pct": None,
        "full_homeownership_cost_ratio_pct": None,
        "other_monthly_debt_payments_cents": 0,
        "ratio_notes": [
            "Gross monthly income is missing or zero, so DTI ratio context is omitted."
        ],
    }

    zero_income = mcp_server.advisory_home_affordability(
        home_price_cents=300_000_00,
        down_payment_cents=60_000_00,
        annual_interest_rate_pct=0.0,
        term_years=30,
        gross_monthly_income_cents=0,
    )
    assert zero_income["data"]["ratios"]["front_end_ratio_pct"] is None
    assert "missing or zero" in zero_income["data"]["ratios"]["ratio_notes"][0]


def test_advisory_home_affordability_validates_impossible_down_payment() -> None:
    result = mcp_server.advisory_home_affordability(
        home_price_cents=10_000_00,
        down_payment_cents=11_000_00,
        annual_interest_rate_pct=6.0,
    )

    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert "down_payment_cents cannot exceed" in result["message"]


@pytest.mark.parametrize(
    ("kwargs", "expected_message"),
    [
        ({"annual_interest_rate_pct": -0.1}, "annual_interest_rate_pct"),
        ({"term_years": 0}, "term_years must be positive"),
        ({"term_years": True}, "term_years must be a non-negative integer"),
    ],
)
def test_advisory_home_affordability_validates_rate_and_term(
    kwargs: dict[str, object],
    expected_message: str,
) -> None:
    params: dict[str, object] = {
        "home_price_cents": 10_000_00,
        "down_payment_cents": 2_000_00,
        "annual_interest_rate_pct": 6.0,
    }
    params.update(kwargs)
    result = mcp_server.advisory_home_affordability(**params)

    assert result["status"] == "error"
    assert result["error_class"] == "ValueError"
    assert expected_message in result["message"]


def test_advisory_home_affordability_is_read_only_tool() -> None:
    from finance_cli.gateway import tools

    assert "advisory_home_affordability" in tools.READ_ONLY_TOOLS
    assert "advisory_home_affordability" not in tools.APPROVAL_REQUIRED_TOOLS
