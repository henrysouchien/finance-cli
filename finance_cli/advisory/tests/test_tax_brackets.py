from decimal import Decimal

import pytest

from finance_cli.advisory import (
    bracket_room,
    federal_tax,
    fica_tax,
    marginal_rate,
    taxable_income_from_gross,
)
from finance_cli.advisory._types import FicaResult, TaxResult
from finance_cli.advisory.tax_brackets_data import FICA_CONSTANTS


BREAKPOINT_CASES_2025 = [
    ("single", 11_925_00, 1_192_50, Decimal("12")),
    ("single", 48_475_00, 5_578_50, Decimal("22")),
    ("single", 103_350_00, 17_651_00, Decimal("24")),
    ("single", 197_300_00, 40_199_00, Decimal("32")),
    ("single", 250_525_00, 57_231_00, Decimal("35")),
    ("single", 626_350_00, 188_769_75, Decimal("37")),
    ("married_filing_jointly", 23_850_00, 2_385_00, Decimal("12")),
    ("married_filing_jointly", 96_950_00, 11_157_00, Decimal("22")),
    ("married_filing_jointly", 206_700_00, 35_302_00, Decimal("24")),
    ("married_filing_jointly", 394_600_00, 80_398_00, Decimal("32")),
    ("married_filing_jointly", 501_050_00, 114_462_00, Decimal("35")),
    ("married_filing_jointly", 751_600_00, 202_154_50, Decimal("37")),
    ("married_filing_separately", 11_925_00, 1_192_50, Decimal("12")),
    ("married_filing_separately", 48_475_00, 5_578_50, Decimal("22")),
    ("married_filing_separately", 103_350_00, 17_651_00, Decimal("24")),
    ("married_filing_separately", 197_300_00, 40_199_00, Decimal("32")),
    ("married_filing_separately", 250_525_00, 57_231_00, Decimal("35")),
    ("married_filing_separately", 375_800_00, 101_077_25, Decimal("37")),
    ("head_of_household", 17_000_00, 1_700_00, Decimal("12")),
    ("head_of_household", 64_850_00, 7_442_00, Decimal("22")),
    ("head_of_household", 103_350_00, 15_912_00, Decimal("24")),
    ("head_of_household", 197_300_00, 38_460_00, Decimal("32")),
    ("head_of_household", 250_500_00, 55_484_00, Decimal("35")),
    ("head_of_household", 626_350_00, 187_031_50, Decimal("37")),
]


BREAKPOINT_CASES_2026 = [
    ("single", 12_400_00, 1_240_00, Decimal("12")),
    ("single", 50_400_00, 5_800_00, Decimal("22")),
    ("single", 105_700_00, 17_966_00, Decimal("24")),
    ("single", 201_775_00, 41_024_00, Decimal("32")),
    ("single", 256_225_00, 58_448_00, Decimal("35")),
    ("single", 640_600_00, 192_979_25, Decimal("37")),
    ("married_filing_jointly", 24_800_00, 2_480_00, Decimal("12")),
    ("married_filing_jointly", 100_800_00, 11_600_00, Decimal("22")),
    ("married_filing_jointly", 211_400_00, 35_932_00, Decimal("24")),
    ("married_filing_jointly", 403_550_00, 82_048_00, Decimal("32")),
    ("married_filing_jointly", 512_450_00, 116_896_00, Decimal("35")),
    ("married_filing_jointly", 768_700_00, 206_583_50, Decimal("37")),
    ("married_filing_separately", 12_400_00, 1_240_00, Decimal("12")),
    ("married_filing_separately", 50_400_00, 5_800_00, Decimal("22")),
    ("married_filing_separately", 105_700_00, 17_966_00, Decimal("24")),
    ("married_filing_separately", 201_775_00, 41_024_00, Decimal("32")),
    ("married_filing_separately", 256_225_00, 58_448_00, Decimal("35")),
    ("married_filing_separately", 384_350_00, 103_291_75, Decimal("37")),
    ("head_of_household", 17_700_00, 1_770_00, Decimal("12")),
    ("head_of_household", 67_450_00, 7_740_00, Decimal("22")),
    ("head_of_household", 105_700_00, 16_155_00, Decimal("24")),
    ("head_of_household", 201_750_00, 39_207_00, Decimal("32")),
    ("head_of_household", 256_200_00, 56_631_00, Decimal("35")),
    ("head_of_household", 640_600_00, 191_171_00, Decimal("37")),
]


@pytest.mark.parametrize(
    ("filing_status", "taxable_income_cents", "expected_tax_cents", "expected_marginal_rate"),
    BREAKPOINT_CASES_2025,
)
def test_2025_breakpoint_known_answers(
    filing_status: str,
    taxable_income_cents: int,
    expected_tax_cents: int,
    expected_marginal_rate: Decimal,
) -> None:
    result = federal_tax(taxable_income_cents, filing_status, tax_year=2025)

    assert isinstance(result, TaxResult)
    assert result.tax_owed_cents == expected_tax_cents
    assert result.marginal_rate_pct == expected_marginal_rate
    assert marginal_rate(taxable_income_cents, filing_status, tax_year=2025) == expected_marginal_rate


@pytest.mark.parametrize(
    ("filing_status", "taxable_income_cents", "expected_tax_cents", "expected_marginal_rate"),
    BREAKPOINT_CASES_2026,
)
def test_2026_breakpoint_known_answers(
    filing_status: str,
    taxable_income_cents: int,
    expected_tax_cents: int,
    expected_marginal_rate: Decimal,
) -> None:
    result = federal_tax(taxable_income_cents, filing_status, tax_year=2026)

    assert isinstance(result, TaxResult)
    assert result.tax_owed_cents == expected_tax_cents
    assert result.marginal_rate_pct == expected_marginal_rate
    assert marginal_rate(taxable_income_cents, filing_status, tax_year=2026) == expected_marginal_rate


def test_zero_income_tax_result() -> None:
    result = federal_tax(0, "single")

    assert result.tax_owed_cents == 0
    assert result.marginal_rate_pct == Decimal("10")
    assert result.effective_rate_pct == Decimal("0")


def test_bracket_room_exact() -> None:
    assert bracket_room(50_000_00, "single", tax_year=2025) == 53_350_00


def test_fica_2025_social_security_wage_base_cap() -> None:
    at_wage_base = fica_tax(gross_wages_cents=17_610_000, tax_year=2025)
    above_wage_base = fica_tax(gross_wages_cents=17_610_100, tax_year=2025)

    assert at_wage_base.social_security_cents == 1_091_820
    assert above_wage_base.social_security_cents == at_wage_base.social_security_cents


@pytest.mark.parametrize(
    ("filing_status", "threshold_cents"),
    [
        ("single", 20_000_000),
        ("married_filing_jointly", 25_000_000),
        ("married_filing_separately", 12_500_000),
        ("head_of_household", 20_000_000),
    ],
)
def test_additional_medicare_thresholds_apply_on_one_dollar(
    filing_status: str,
    threshold_cents: int,
) -> None:
    result = fica_tax(
        gross_wages_cents=threshold_cents + 100,
        filing_status=filing_status,
        tax_year=2025,
    )

    assert result.additional_medicare_cents == 1


def test_self_employment_fica_known_answer() -> None:
    result = fica_tax(net_se_earnings_cents=10_000_000, tax_year=2025)

    assert isinstance(result, FicaResult)
    assert result.se_earnings_base_cents == 9_235_000
    assert result.social_security_cents == 1_145_140
    assert result.medicare_cents == 267_815
    assert result.additional_medicare_cents == 0
    assert result.total_cents == 1_412_955


def test_mixed_w2_and_self_employment_fica() -> None:
    result = fica_tax(
        gross_wages_cents=15_000_000,
        net_se_earnings_cents=5_000_000,
        filing_status="single",
        tax_year=2025,
    )

    assert result.w2_wages_applied_cents == 15_000_000
    assert result.se_earnings_base_cents == 4_617_500
    assert result.social_security_cents == 1_253_640
    assert result.medicare_cents == 351_408
    assert result.additional_medicare_cents == 0
    assert result.total_cents == 1_605_048


def test_taxable_income_from_gross_uses_itemized_when_larger() -> None:
    result = taxable_income_from_gross(
        gross_income_cents=10_000_000,
        filing_status="single",
        tax_year=2026,
        itemized_deductions_cents=2_000_000,
        above_the_line_adjustments_cents=500_000,
    )

    assert result == 7_500_000


def test_taxable_income_from_gross_uses_standard_when_larger() -> None:
    result = taxable_income_from_gross(
        gross_income_cents=10_000_000,
        filing_status="single",
        tax_year=2026,
        itemized_deductions_cents=1_000_000,
        above_the_line_adjustments_cents=500_000,
    )

    assert result == 7_890_000


def test_unsupported_tax_year_message_mentions_supported_years() -> None:
    with pytest.raises(
        ValueError,
        match=r"Tax year 2027 not yet supported\. Supported: \[2025, 2026\]\.",
    ):
        federal_tax(1_000_00, "single", tax_year=2027)


def test_additional_medicare_threshold_constants_match_expected_values() -> None:
    thresholds = FICA_CONSTANTS[2026]["additional_medicare_thresholds_cents"]

    assert thresholds == {
        "single": 20_000_000,
        "married_filing_jointly": 25_000_000,
        "married_filing_separately": 12_500_000,
        "head_of_household": 20_000_000,
    }
