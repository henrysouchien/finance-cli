from __future__ import annotations

import dataclasses

import pytest

from finance_cli.debt_calculator import (
    DebtCard,
    compare_strategies,
    compute_dashboard,
    monthly_interest_cents,
    project_interest,
    simulate_paydown,
)


def _card(
    card_id: str,
    label: str,
    balance_cents: int,
    apr: float | None,
    min_payment_cents: int,
    limit_cents: int | None = None,
) -> DebtCard:
    return DebtCard(
        card_id=card_id,
        label=label,
        balance_cents=balance_cents,
        apr=apr,
        min_payment_cents=min_payment_cents,
        limit_cents=limit_cents,
    )


def test_monthly_interest_cents_basic_rounding() -> None:
    assert monthly_interest_cents(100_000, 24.0) == 2000


def test_monthly_interest_cents_zero_apr_and_zero_balance() -> None:
    assert monthly_interest_cents(100_000, 0.0) == 0
    assert monthly_interest_cents(0, 22.0) == 0


def test_compute_dashboard_totals_weighted_avg_unknown_apr() -> None:
    cards = [
        _card("a", "A", 10_000, 10.0, 1_000, limit_cents=20_000),
        _card("b", "B", 20_000, None, 1_500),
        _card("c", "C", 30_000, 20.0, 2_000, limit_cents=60_000),
    ]

    result = compute_dashboard(cards)

    assert result["total_balance_cents"] == 60_000
    assert result["total_min_payment_cents"] == 4_500
    assert result["total_monthly_interest_cents"] == 583
    assert result["weighted_avg_apr"] == pytest.approx(17.5)
    assert result["apr_unknown_cards"] == ["B"]
    assert [row["label"] for row in result["cards"]] == ["C", "B", "A"]


def test_compute_dashboard_utilization_and_empty() -> None:
    result = compute_dashboard(
        [
            _card("a", "A", 5_000, 10.0, 300, limit_cents=10_000),
            _card("b", "B", 5_000, 10.0, 300, limit_cents=0),
            _card("c", "C", 5_000, 10.0, 300, limit_cents=None),
        ]
    )
    by_label = {row["label"]: row for row in result["cards"]}
    assert by_label["A"]["utilization_pct"] == pytest.approx(50.0)
    assert by_label["B"]["utilization_pct"] is None
    assert by_label["C"]["utilization_pct"] is None

    empty = compute_dashboard([])
    assert empty["cards"] == []
    assert empty["weighted_avg_apr"] is None


def test_project_interest_one_month_and_unknown_apr() -> None:
    cards = [
        _card("a", "Known", 120_000, 12.0, 10_000),
        _card("b", "Unknown", 50_000, None, 5_000),
    ]

    result = project_interest(cards, months=1)

    assert result["total_interest_cents"] == 1200
    assert result["apr_unknown_count"] == 1
    assert result["apr_unknown_balance_cents"] == 50_000

    by_label = {row["label"]: row for row in result["schedule"][0]["cards"]}
    assert by_label["Known"]["interest_cents"] == 1200
    assert by_label["Known"]["end_balance_cents"] == 111_200
    assert by_label["Unknown"]["interest_cents"] == 0
    assert by_label["Unknown"]["end_balance_cents"] == 45_000


def test_project_interest_card_pays_off_mid_projection() -> None:
    result = project_interest([_card("a", "Solo", 10_000, 12.0, 6_000)], months=3)

    assert result["total_interest_cents"] == 141
    assert result["schedule"][1]["remaining_balance_cents"] == 0
    assert result["schedule"][2]["interest_cents"] == 0


def test_simulate_paydown_avalanche_targets_highest_apr_first() -> None:
    cards = [
        _card("high", "High APR", 10_000, 24.0, 1_000),
        _card("low", "Low APR", 10_000, 12.0, 1_000),
    ]

    result = simulate_paydown(cards, extra_cents=1_000, strategy="avalanche")

    first_month = result["schedule"][0]
    by_label = {row["label"]: row for row in first_month["cards"]}
    assert by_label["High APR"]["extra_payment_cents"] > 0
    assert by_label["Low APR"]["extra_payment_cents"] == 0
    assert result["payoff_order"][0] == "High APR"


def test_simulate_paydown_snowball_targets_lowest_balance_first() -> None:
    cards = [
        _card("small", "Small", 5_000, 10.0, 500),
        _card("large", "Large", 20_000, 25.0, 500),
    ]

    result = simulate_paydown(cards, extra_cents=1_000, strategy="snowball")

    first_month = result["schedule"][0]
    by_label = {row["label"]: row for row in first_month["cards"]}
    assert by_label["Small"]["extra_payment_cents"] > 0
    assert by_label["Large"]["extra_payment_cents"] == 0
    assert result["payoff_order"][0] == "Small"


def test_simulate_paydown_tiebreaker_same_apr_and_balance() -> None:
    cards = [
        _card("a", "Card A", 10_000, 20.0, 0),
        _card("b", "Card B", 10_000, 20.0, 0),
    ]

    result = simulate_paydown(cards, extra_cents=1_000, strategy="avalanche")
    first_month = result["schedule"][0]
    by_id = {row["card_id"]: row for row in first_month["cards"]}
    assert by_id["a"]["extra_payment_cents"] == 1_000
    assert by_id["b"]["extra_payment_cents"] == 0


def test_simulate_paydown_single_card_and_extra_zero() -> None:
    result = simulate_paydown([_card("solo", "Solo", 10_000, 0.0, 1_000)], extra_cents=0, strategy="avalanche")
    assert result["months_to_payoff"] == 10
    assert result["total_interest_cents"] == 0
    assert result["fully_paid_off"] is True


def test_compare_strategies_includes_baseline_and_horizon() -> None:
    cards = [
        _card("high", "High APR", 20_000, 30.0, 400),
        _card("low", "Low APR", 5_000, 5.0, 200),
    ]

    result = compare_strategies(cards, extra_cents=500)

    assert "baseline" in result
    avalanche = result["avalanche"]
    snowball = result["snowball"]
    baseline = result["baseline"]
    assert baseline["months"] == max(avalanche["months_to_payoff"], snowball["months_to_payoff"])
    assert avalanche["total_interest_cents"] < snowball["total_interest_cents"]


def test_simulate_all_unknown_apr_lower_bound_mode() -> None:
    cards = [
        _card("u1", "Unknown 1", 5_000, None, 500),
        _card("u2", "Unknown 2", 8_000, None, 500),
    ]

    result = simulate_paydown(cards, extra_cents=200, strategy="avalanche")

    assert result["months_to_payoff"] > 0
    assert result["fully_paid_off"] is True
    assert result["assumptions"] == ["all_apr_unknown_zero_interest"]


def test_simulate_cap_hit_returns_not_paid_off_and_capped_cards() -> None:
    cards = [_card("cap", "Cap Card", 10_000, 1200.0, 1)]

    result = simulate_paydown(cards, extra_cents=0, strategy="avalanche")

    assert result["months_to_payoff"] == 360
    assert result["fully_paid_off"] is False
    assert result["capped_cards"] == ["Cap Card"]


def test_simulate_mixed_ranked_and_unranked_spillover() -> None:
    cards = [
        _card("ranked", "Ranked", 5_000, 20.0, 100),
        _card("unknown", "Unknown", 2_000, None, 100),
    ]

    result = simulate_paydown(cards, extra_cents=300, strategy="avalanche")

    assert result["unranked_cards"] == ["Unknown"]
    assert result["assumptions"] == ["unknown_apr_zero_interest_optimistic"]
    assert result["unranked_received_extra"] is True

    ranked_paid_month = None
    for row in result["schedule"]:
        if "Ranked" in row["paid_off_cards"]:
            ranked_paid_month = row["month"]
            break
    assert ranked_paid_month is not None

    pre_spill_extras = []
    post_spill_extras = []
    for row in result["schedule"]:
        unknown_row = next(card for card in row["cards"] if card["label"] == "Unknown")
        if row["month"] < ranked_paid_month:
            pre_spill_extras.append(unknown_row["extra_payment_cents"])
        else:
            post_spill_extras.append(unknown_row["extra_payment_cents"])

    assert all(value == 0 for value in pre_spill_extras)
    assert any(value > 0 for value in post_spill_extras)


def test_simulate_budget_conservation_per_month() -> None:
    cards = [
        _card("a", "A", 3_000, 18.0, 200),
        _card("b", "B", 2_000, 12.0, 150),
    ]

    result = simulate_paydown(cards, extra_cents=250, strategy="avalanche")

    for row in result["schedule"]:
        assert int(row["total_payment_cents"]) <= int(row["payment_budget_cents"])


def test_simulate_empty_input_and_compare_empty() -> None:
    sim = simulate_paydown([], extra_cents=500, strategy="snowball")
    assert sim["months_to_payoff"] == 0
    assert sim["fully_paid_off"] is True

    compare = compare_strategies([], extra_cents=500)
    assert compare["baseline"]["months"] == 0
    assert compare["avalanche"]["months_to_payoff"] == 0


def test_mutation_safety_and_compare_deterministic() -> None:
    cards = [
        _card("a", "A", 9_000, 15.0, 300),
        _card("b", "B", 4_000, None, 150),
    ]
    before = [dataclasses.asdict(card) for card in cards]

    simulate_paydown(cards, extra_cents=200, strategy="snowball")
    project_interest(cards, months=6)
    first = compare_strategies(cards, extra_cents=200)
    second = compare_strategies(cards, extra_cents=200)

    assert [dataclasses.asdict(card) for card in cards] == before
    assert first == second


def test_validation_errors() -> None:
    cards = [_card("a", "A", 1_000, 12.0, 100)]

    with pytest.raises(ValueError):
        project_interest(cards, months=0)
    with pytest.raises(ValueError):
        project_interest(cards, months=-1)
    with pytest.raises(ValueError):
        simulate_paydown(cards, extra_cents=-100, strategy="avalanche")
    with pytest.raises(ValueError):
        simulate_paydown(cards, extra_cents=100, strategy="invalid")


# ---------------------------------------------------------------------------
# Lump sum tests
# ---------------------------------------------------------------------------


def test_lump_sum_month1_reduces_interest() -> None:
    """Lump sum at month 1 reduces total interest vs no lump sum."""
    cards = [
        _card("a", "A", 50_000, 24.0, 1_000),
        _card("b", "B", 30_000, 18.0, 500),
    ]
    without = simulate_paydown(cards, extra_cents=500, strategy="avalanche")
    with_lump = simulate_paydown(
        cards, extra_cents=500, strategy="avalanche",
        lump_sum_cents=10_000, lump_sum_month=1,
    )
    assert with_lump["total_interest_cents"] < without["total_interest_cents"]
    assert with_lump["months_to_payoff"] <= without["months_to_payoff"]


def test_lump_sum_month3_works() -> None:
    """Lump sum applied at month 3 still reduces interest."""
    cards = [_card("a", "A", 50_000, 24.0, 1_000)]
    without = simulate_paydown(cards, extra_cents=500, strategy="avalanche")
    with_lump = simulate_paydown(
        cards, extra_cents=500, strategy="avalanche",
        lump_sum_cents=10_000, lump_sum_month=3,
    )
    assert with_lump["total_interest_cents"] < without["total_interest_cents"]
    # Verify lump sum applied field is set on month 3
    assert with_lump["schedule"][2]["lump_sum_applied_cents"] == 10_000
    assert with_lump["schedule"][0]["lump_sum_applied_cents"] == 0


def test_lump_sum_zero_no_change() -> None:
    """Lump sum of 0 produces identical results."""
    cards = [_card("a", "A", 20_000, 18.0, 500)]
    without = simulate_paydown(cards, extra_cents=300, strategy="snowball")
    with_zero = simulate_paydown(
        cards, extra_cents=300, strategy="snowball",
        lump_sum_cents=0, lump_sum_month=1,
    )
    assert without["total_interest_cents"] == with_zero["total_interest_cents"]
    assert without["months_to_payoff"] == with_zero["months_to_payoff"]


def test_lump_sum_exceeds_total_debt_pays_off_month1() -> None:
    """Lump sum exceeding total debt pays everything off quickly."""
    cards = [_card("a", "A", 5_000, 12.0, 500)]
    result = simulate_paydown(
        cards, extra_cents=0, strategy="avalanche",
        lump_sum_cents=100_000, lump_sum_month=1,
    )
    assert result["months_to_payoff"] == 1
    assert result["fully_paid_off"] is True


def test_lump_sum_month_less_than_1_raises() -> None:
    """lump_sum_month < 1 raises ValueError."""
    cards = [_card("a", "A", 5_000, 12.0, 500)]
    with pytest.raises(ValueError, match="lump_sum_month must be >= 1"):
        simulate_paydown(
            cards, extra_cents=0, strategy="avalanche",
            lump_sum_cents=1_000, lump_sum_month=0,
        )
