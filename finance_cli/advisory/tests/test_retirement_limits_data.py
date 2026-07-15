from __future__ import annotations

import pytest

from finance_cli.advisory import (
    RETIREMENT_LIMITS,
    RETIREMENT_LIMITS_2025,
    RETIREMENT_LIMITS_2026,
    SUPPORTED_LIMIT_YEARS,
    roth_ira_allowed_contribution_cents,
)


EXPECTED_LIMIT_KEYS = {
    "ira_contribution_cents",
    "ira_catchup_cents",
    "401k_contribution_cents",
    "401k_catchup_cents",
    "401k_supercatchup_cents",
    "401k_total_limit_cents",
    "hsa_single_cents",
    "hsa_family_cents",
    "hsa_catchup_cents",
    "roth_ira_phaseout_single_cents",
    "roth_ira_phaseout_mfj_cents",
    "roth_ira_phaseout_mfs_cents",
    "roth_ira_phaseout_hoh_cents",
}


def test_2025_and_2026_limits_include_all_expected_keys() -> None:
    assert RETIREMENT_LIMITS[2025] is RETIREMENT_LIMITS_2025
    assert RETIREMENT_LIMITS[2026] is RETIREMENT_LIMITS_2026
    assert set(RETIREMENT_LIMITS_2025) == EXPECTED_LIMIT_KEYS
    assert set(RETIREMENT_LIMITS_2026) == EXPECTED_LIMIT_KEYS


def test_supported_limit_years_exact() -> None:
    assert SUPPORTED_LIMIT_YEARS == {2025, 2026}


def test_unsupported_year_raises_value_error() -> None:
    with pytest.raises(
        ValueError,
        match=r"Tax year 2027 not yet supported\. Supported: \[2025, 2026\]\.",
    ):
        roth_ira_allowed_contribution_cents(1_000_00, "single", age=40, tax_year=2027)


def test_roth_ira_age_50_plus_2026_full_limit() -> None:
    result = roth_ira_allowed_contribution_cents(
        modified_agi_cents=5_000_000,
        filing_status="single",
        age=55,
        tax_year=2026,
        taxable_compensation_cents=10_000_000,
    )

    assert result == 860_000


def test_roth_ira_reduced_phaseout_floor() -> None:
    result = roth_ira_allowed_contribution_cents(
        modified_agi_cents=16_499_000,
        filing_status="single",
        age=40,
        tax_year=2025,
    )

    assert result == 20_000


def test_roth_ira_reduced_rounds_up_to_10() -> None:
    result = roth_ira_allowed_contribution_cents(
        modified_agi_cents=15_526_500,
        filing_status="single",
        age=40,
        tax_year=2025,
    )

    assert result == 455_000


def test_roth_ira_compensation_cap_before_phaseout() -> None:
    result = roth_ira_allowed_contribution_cents(
        modified_agi_cents=15_300_000,
        filing_status="single",
        age=40,
        tax_year=2025,
        taxable_compensation_cents=500_000,
    )

    assert result == 400_000


def test_roth_ira_other_contribs_min_rule() -> None:
    result = roth_ira_allowed_contribution_cents(
        modified_agi_cents=15_579_070,
        filing_status="single",
        age=55,
        tax_year=2026,
        other_ira_contributions_cents=200_000,
    )

    assert result == 660_000


def test_roth_ira_phaseout_boundaries() -> None:
    at_start = roth_ira_allowed_contribution_cents(
        modified_agi_cents=15_000_000,
        filing_status="single",
        age=40,
        tax_year=2025,
    )
    at_end = roth_ira_allowed_contribution_cents(
        modified_agi_cents=16_500_000,
        filing_status="single",
        age=40,
        tax_year=2025,
    )

    assert at_start == 700_000
    assert at_end == 0


def test_roth_ira_unknown_filing_status_raises() -> None:
    with pytest.raises(ValueError, match=r"Unsupported filing status: qualifying_surviving_spouse\."):
        roth_ira_allowed_contribution_cents(
            modified_agi_cents=1_000_00,
            filing_status="qualifying_surviving_spouse",  # type: ignore[arg-type]
            age=40,
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"modified_agi_cents": -1, "filing_status": "single", "age": 40}, "modified_agi_cents"),
        ({"modified_agi_cents": 0, "filing_status": "single", "age": -1}, "age"),
        (
            {
                "modified_agi_cents": 0,
                "filing_status": "single",
                "age": 40,
                "taxable_compensation_cents": -1,
            },
            "taxable_compensation_cents",
        ),
        (
            {
                "modified_agi_cents": 0,
                "filing_status": "single",
                "age": 40,
                "other_ira_contributions_cents": -1,
            },
            "other_ira_contributions_cents",
        ),
    ],
)
def test_roth_ira_negative_inputs_raise(kwargs: dict[str, int | str], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        roth_ira_allowed_contribution_cents(**kwargs)
