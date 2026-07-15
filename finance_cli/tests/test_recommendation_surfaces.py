from __future__ import annotations

import pytest

from finance_cli.recommendation_surfaces import (
    enrich_budget_alerts_surface,
    enrich_budget_status_surface,
    enrich_debt_dashboard_surface,
    enrich_goal_status_surface,
    enrich_net_worth_history_surface,
    enrich_net_worth_surface,
    enrich_onboarding_state_surface,
    enrich_plaid_items_surface,
    enrich_spending_trends_surface,
    enrich_summary_surface,
    enrich_subscription_audit_surface,
    enrich_subscription_list_surface,
    format_cents,
    interpret_surface_field,
    living_margin,
    recommendation_annotation,
    recommendation_insight,
)


def test_recommendation_annotation_requires_text_and_normalizes_tone() -> None:
    assert recommendation_annotation(
        source=" debt_dashboard ",
        text="  Start   with the highest APR card. ",
        tone="warn",
    ) == {
        "source": "debt_dashboard",
        "text": "Start with the highest APR card.",
        "tone": "warn",
    }
    assert recommendation_annotation(
        source="x", text="use fallback tone", tone="loud"
    ) == {
        "source": "x",
        "text": "use fallback tone",
        "tone": "pattern",
    }

    with pytest.raises(ValueError, match="text must be non-empty"):
        recommendation_annotation(source="x", text="  ", tone="coach")
    with pytest.raises(ValueError, match="source must be non-empty"):
        recommendation_annotation(source="", text="Valid text", tone="coach")


def test_recommendation_insight_filters_bullets_and_blank_meta() -> None:
    assert recommendation_insight(
        source="spending_trends",
        text="Review Dining.",
        tone="unknown",
        meta="  ",
        bullets=("  Dining is above average.  ", "", None, "Keep it deliberate."),
    ) == {
        "source": "spending_trends",
        "text": "Review Dining.",
        "tone": "diagnose",
        "meta": None,
        "bullets": ["Dining is above average.", "Keep it deliberate."],
    }


def test_living_margin_requires_text_and_normalizes_tone() -> None:
    assert living_margin(
        source="goals_status", text="Push surplus here.", tone="positive"
    ) == {
        "source": "goals_status",
        "text": "Push surplus here.",
        "tone": "positive",
    }
    assert living_margin(
        source="goals_status", text="No immediate move.", tone="coach"
    ) == {
        "source": "goals_status",
        "text": "No immediate move.",
        "tone": "neutral",
    }

    with pytest.raises(ValueError, match="text must be non-empty"):
        living_margin(source="goals_status", text=None, tone="positive")


def test_interpret_surface_field_preserves_missing_null_and_invalid_semantics() -> None:
    missing = interpret_surface_field()
    assert missing.state == "missing"
    assert missing.should_use_legacy_fallback
    assert not missing.should_render

    explicit_null = interpret_surface_field(None)
    assert explicit_null.state == "null"
    assert not explicit_null.should_use_legacy_fallback
    assert not explicit_null.should_render

    blank = interpret_surface_field(
        {"source": "budget_status", "text": "   ", "tone": "coach"}
    )
    assert blank.state == "invalid"
    assert blank.should_use_legacy_fallback
    assert not blank.should_render

    invalid_shape = interpret_surface_field("not a mapping")
    assert invalid_shape.state == "invalid"
    assert invalid_shape.should_use_legacy_fallback


def test_interpret_surface_field_normalizes_valid_payloads() -> None:
    field = interpret_surface_field(
        {
            "source": " spending_trends ",
            "text": "  Review Dining.  ",
            "tone": "warn",
            "meta": " 3 months of history ",
            "bullets": ["  Dining is $200 high. ", "", 42],
        }
    )

    assert field.state == "valid"
    assert field.should_render
    assert not field.should_use_legacy_fallback
    assert field.payload == {
        "source": "spending_trends",
        "text": "Review Dining.",
        "tone": "warn",
        "meta": "3 months of history",
        "bullets": ["Dining is $200 high.", "42"],
    }


def test_format_cents_uses_cents_inputs_and_handles_bad_values() -> None:
    assert format_cents(4367) == "$44"
    assert format_cents(4367, places=2) == "$43.67"
    assert format_cents(-12_000, absolute=False) == "-$120"
    assert format_cents(float("nan")) == "$0"
    assert format_cents("not-a-number") == "$0"


def test_enrich_budget_status_surface_owns_status_recommendations() -> None:
    source = {
        "month": "2026-05",
        "status": [
            {
                "actual_cents": -20_000,
                "budget_cents": 50_000,
                "category_name": "Dining",
                "remaining_cents": 30_000,
                "utilization": 0.4,
            },
            {
                "actual_cents": -30_000,
                "budget_cents": 30_000,
                "category_name": "Travel",
                "remaining_cents": 0,
                "utilization": 1.0,
            },
        ],
    }

    enriched = enrich_budget_status_surface(source)

    assert "living_margin" not in source["status"][0]
    assert enriched["stat_annotations"] == {
        "planned": {
            "source": "budget_status",
            "text": "$800 planned across 2 budgets; move dollars before categories crowd the month.",
            "tone": "pattern",
        },
        "committed": {
            "source": "budget_status",
            "text": "$300 still uncommitted; keep new spending attached to daily room.",
            "tone": "coach",
        },
        "room_used": {
            "source": "budget_status",
            "text": "$300 left; 62.5% of the monthly plan is used.",
            "tone": "coach",
        },
    }
    assert enriched["status"][0]["living_margin"] == {
        "source": "budget_status",
        "text": "$300 room left. Dining is not crowding the month right now.",
        "tone": "positive",
    }
    assert enriched["status"][1]["living_margin"] == {
        "source": "budget_status",
        "text": "$0 left. Keep Travel close to plan for the rest of the month.",
        "tone": "concern",
    }


def test_enrich_budget_alerts_surface_owns_alert_recommendations() -> None:
    source = {
        "alerts": [
            {
                "actual_cents": -20_000,
                "budget_cents": 50_000,
                "category_name": "Dining",
                "forecast_utilization": 0.92,
                "remaining_daily_budget": 15,
                "severity": "alert",
                "use_type": "personal",
            },
            {
                "actual_cents": -35_000,
                "budget_cents": 30_000,
                "category_name": "Travel",
                "forecast_utilization": 1.4,
                "remaining_daily_budget": -2,
                "severity": "over",
                "use_type": "personal",
            },
        ],
    }

    enriched = enrich_budget_alerts_surface(source)

    assert "budget_insight" not in source
    assert enriched["budget_insight"] == {
        "source": "budget_alerts",
        "text": (
            "Pause Travel (Personal): it is $50 over budget. Hold new spending at $0/day until next month. "
            "Steer Dining (Personal): keep new spending near $15/day to finish near 92% of budget."
        ),
        "bullets": [
            "Travel (Personal) is $50 over; pause new spending here first.",
            "Dining (Personal) has $15/day left to land near plan.",
        ],
        "tone": "warn",
    }


def test_enrich_summary_surface_owns_dashboard_summary_recommendations() -> None:
    source = {
        "budget_alerts": [
            {
                "actual_cents": 12_000,
                "budget_cents": 10_000,
                "category_name": "Dining",
                "remaining_daily_budget_cents": -500,
                "severity": "over",
            }
        ],
        "expense_30d": 210,
        "goals": [
            {
                "current_cents": 900_000,
                "estimated_months": 6,
                "name": "Emergency Fund",
                "progress_pct": 50,
                "target_cents": 1_500_000,
            }
        ],
        "subscriptions": 89,
        "uncategorized": 3,
        "unreviewed": 2,
    }

    enriched = enrich_summary_surface(
        source,
        categorization_rate=0.8,
        transaction_count=20,
    )

    assert "living_margin" not in source["budget_alerts"][0]
    assert enriched["budget_alerts"][0]["living_margin"] == {
        "source": "summary",
        "text": "$20 over plan. The next move is a pause, not a bigger budget.",
        "tone": "concern",
    }
    assert enriched["stat_annotations"] == {
        "handled_automatically": {
            "source": "summary",
            "text": "About 8 min of sorting handled across 16 transactions.",
            "tone": "coach",
        },
        "money_position": {
            "source": "summary",
            "text": "$6,000 from Emergency Fund; ~6 mos left.",
            "tone": "coach",
        },
        "spend_to_steer": {
            "source": "summary",
            "text": "$20 over Dining; hold new spending at $0/day this month.",
            "tone": "warn",
        },
        "subscription_drag": {
            "source": "summary",
            "text": "$89/mo in recurring spend; review fixed charges before they disappear into the baseline.",
            "tone": "pattern",
        },
    }
    assert enriched["transaction_annotations"]["insight"] == {
        "source": "summary",
        "text": "Clear 5 transaction checks: categorize 3 and review 2 so budgets and recommendations stay actionable.",
        "tone": "warn",
    }
    assert enriched["transaction_annotations"]["stat_annotations"][
        "needs_category"
    ] == {
        "source": "summary",
        "text": "Categorize 3 transactions before trusting category-level recommendations.",
        "tone": "warn",
    }


def test_enrich_subscription_audit_surface_owns_subscription_recommendations() -> None:
    source = {
        "baseline": {"total_debt_cents": 270_000},
        "discretionary_count": 3,
        "discretionary_monthly_cents": 7_500,
        "essential_count": 1,
        "essential_monthly_cents": 300,
        "scenarios": [
            {
                "interest_saved_cents": 12_000,
                "monthly_savings_cents": 7_500,
                "months_shaved": 2,
                "name": "Cut top 3",
                "subs_affected": ["Gym", "Hulu", "Spotify"],
            },
            {
                "interest_saved_cents": 0,
                "monthly_savings_cents": 0,
                "months_shaved": 0,
                "name": "No-op",
                "subs_affected": [],
            },
        ],
    }

    enriched = enrich_subscription_audit_surface(source)

    assert "living_margin" not in source["scenarios"][0]
    assert enriched["subscription_insight"] == {
        "source": "subs_audit",
        "text": "Cut the top 3 discretionary subscriptions to free $75/mo and save $120 in interest.",
        "meta": "4 active subscriptions tracked",
        "bullets": [
            "$900/yr of discretionary spend is available to review.",
            "Tie the cash freed to debt paydown before it disappears into the baseline.",
        ],
        "tone": "warn",
    }
    assert enriched["stat_annotations"]["recurring"] == {
        "source": "subs_audit",
        "text": "$78/mo recurring baseline across 4 subscriptions.",
        "tone": "pattern",
    }
    assert enriched["scenarios"][0]["living_margin"] == {
        "source": "subs_audit",
        "text": "$75/mo can be redirected immediately; that is $900/yr before interest effects.",
        "tone": "positive",
    }
    assert enriched["scenarios"][1]["living_margin"] is None


def test_enrich_subscription_list_surface_owns_subscription_row_margins() -> None:
    source = {
        "subscriptions": [
            {"category_name": "Streaming", "monthly_amount": 20, "vendor_name": "Hulu"},
            {"category_name": "Utilities", "monthly_amount": 80, "vendor_name": "Power"},
            {"category_name": None, "monthly_amount": 12, "vendor_name": "Unknown"},
            {"category_name": "Music", "monthly_amount": 0, "vendor_name": "Free trial"},
        ],
    }

    enriched = enrich_subscription_list_surface(
        source,
        essential_categories=frozenset({"utilities"}),
    )

    assert "living_margin" not in source["subscriptions"][0]
    assert enriched["subscriptions"][0]["living_margin"] == {
        "source": "subs_list",
        "text": "$20/mo cut candidate. Confirm use before it stays in the baseline.",
        "tone": "concern",
    }
    assert enriched["subscriptions"][1]["living_margin"] == {
        "source": "subs_list",
        "text": "$80/mo essential baseline. Keep unless duplicate coverage exists.",
        "tone": "positive",
    }
    assert enriched["subscriptions"][2]["living_margin"] == {
        "source": "subs_list",
        "text": "$12/mo recurring charge. Classify keep or cut before the next review.",
        "tone": "neutral",
    }
    assert enriched["subscriptions"][3]["living_margin"] is None


def test_enrich_net_worth_surface_owns_current_net_worth_recommendations() -> None:
    source = {
        "assets": 200_000,
        "assets_cents": 20_000_000,
        "breakdown": [
            {
                "account_type": "brokerage",
                "balance": 120_000,
                "balance_cents": 12_000_000,
            },
            {
                "account_type": "checking",
                "balance": 80_000,
                "balance_cents": 8_000_000,
            },
            {
                "account_type": "credit_card",
                "balance": -50_000,
                "balance_cents": -5_000_000,
            },
        ],
        "liabilities": 50_000,
        "liabilities_cents": 5_000_000,
        "net_worth": 150_000,
        "net_worth_cents": 15_000_000,
    }

    enriched = enrich_net_worth_surface(source)

    assert "living_margin" not in source["breakdown"][0]
    assert enriched["net_worth_insight"] is None
    assert enriched["stat_annotations"] == {
        "assets": {
            "source": "net_worth",
            "text": "$200,000 available on the asset side of the plan.",
            "tone": "pattern",
        },
        "liabilities": {
            "source": "net_worth",
            "text": "$50,000 in debt drag to route through a payoff order.",
            "tone": "pattern",
        },
        "net_worth": {
            "source": "net_worth",
            "text": "$150,000 current position; use account mix to choose the next move.",
            "tone": "pattern",
        },
    }
    assert enriched["breakdown"][0]["living_margin"] == {
        "source": "net_worth",
        "text": "$120,000 is 60% of assets. Keep this account tied to a job or goal.",
        "tone": "neutral",
    }
    assert enriched["breakdown"][1]["living_margin"] == {
        "source": "net_worth",
        "text": "$80,000 supports the asset side of the plan.",
        "tone": "positive",
    }
    assert enriched["breakdown"][2]["living_margin"] == {
        "source": "net_worth",
        "text": "$50,000 debt here is 100% of liabilities. Prioritize the highest-rate balance first.",
        "tone": "concern",
    }


def test_enrich_net_worth_history_surface_owns_history_recommendations() -> None:
    source = {
        "points": [
            {
                "snapshot_date": "2026-05-01",
                "assets_cents": 18_500_000,
                "liabilities_cents": 5_500_000,
                "net_worth_cents": 13_000_000,
            },
            {
                "snapshot_date": "2026-06-01",
                "assets_cents": 20_000_000,
                "liabilities_cents": 5_000_000,
                "net_worth_cents": 15_000_000,
            },
        ],
        "days": 10000,
    }

    enriched = enrich_net_worth_history_surface(source)

    assert enriched["points"] == source["points"]
    assert enriched["points"] is not source["points"]
    assert enriched["net_worth_insight"] == {
        "source": "net_worth",
        "text": (
            "Protect the momentum: net worth is up $20,000 over the last 1 month. "
            "Keep surplus aimed at the next goal."
        ),
        "meta": "2 balance snapshots tracked",
        "bullets": [
            "$20,000 higher than the starting snapshot.",
            "Keep surplus assigned before it drifts back into unplanned spending.",
        ],
        "tone": "coach",
    }
    assert enriched["stat_annotations"] == {
        "assets": {
            "source": "net_worth",
            "text": "$15,000 more assets than prior snapshot; keep new cash assigned.",
            "tone": "coach",
        },
        "liabilities": {
            "source": "net_worth",
            "text": "$5,000 less debt than prior snapshot; keep paydown pointed at high-rate balances.",
            "tone": "coach",
        },
        "net_worth": {
            "source": "net_worth",
            "text": "$20,000 higher than prior snapshot; keep the surplus attached to a goal.",
            "tone": "coach",
        },
    }

    suppressed = enrich_net_worth_history_surface(
        source,
        suppress_recommendations=True,
    )

    assert suppressed["net_worth_insight"] is None
    assert suppressed["stat_annotations"] == {
        "assets": None,
        "liabilities": None,
        "net_worth": None,
    }


def test_enrich_plaid_items_surface_owns_settings_annotations() -> None:
    source = {
        "items": [
            {"plaid_item_id": "item-active", "status": "active", "needs_reauth": 0},
            {"plaid_item_id": "item-error", "status": "error", "needs_reauth": 1},
        ]
    }

    enriched = enrich_plaid_items_surface(source)

    assert "settings_annotations" not in source
    assert enriched["items"] == source["items"]
    assert enriched["items"] is not source["items"]
    assert enriched["settings_annotations"] == {
        "connection_insight": {
            "meta": "Recommendation data health across 2 institutions",
            "source": "plaid_items",
            "text": "Reauthorize 1 connection so recommendations use current data.",
        },
        "stat_notes": {
            "latest_data": {
                "source": "plaid_items",
                "text": "2 connections powering recommendations",
            },
            "ready_connections": {
                "source": "plaid_items",
                "text": "1 needs reauthorization",
            },
        },
    }

    mixed_attention = enrich_plaid_items_surface(
        {
            "items": [
                {"plaid_item_id": "item-reauth", "status": "active", "needs_reauth": 1},
                {"plaid_item_id": "item-disconnected", "status": "disconnected", "needs_reauth": 0},
            ]
        }
    )
    assert mixed_attention["settings_annotations"]["connection_insight"] == {
        "meta": "Recommendation data health across 2 institutions",
        "source": "plaid_items",
        "text": "Reauthorize or reconnect 2 connections so recommendations use current data.",
    }
    assert mixed_attention["settings_annotations"]["stat_notes"]["ready_connections"] == {
        "source": "plaid_items",
        "text": "2 need attention",
    }

    pending = enrich_plaid_items_surface(
        {"items": [{"plaid_item_id": "item-pending", "status": "pending"}]}
    )
    assert pending["settings_annotations"]["connection_insight"] == {
        "meta": "Recommendation data health across 1 institution",
        "source": "plaid_items",
        "text": "1 connection is still initializing before it can power recommendations.",
    }

    empty = enrich_plaid_items_surface({"items": []})
    assert empty["settings_annotations"] == {
        "connection_insight": None,
        "stat_notes": {
            "latest_data": {"source": "plaid_items", "text": "Connect a bank to start"},
            "ready_connections": {"source": "plaid_items", "text": "0 total"},
        },
    }


def test_enrich_onboarding_state_surface_owns_onboarding_insight() -> None:
    source = {
        "current_phase": "connect",
        "is_demo_mode": False,
        "is_fully_onboarded": False,
        "is_gate_open": False,
        "phases": [
            {
                "id": "connect",
                "missing": ["transaction_history_or_acknowledgment"],
                "status": "in_progress",
            }
        ],
        "progress": {"required_done": 0, "required_total": 4},
    }

    enriched = enrich_onboarding_state_surface(
        source,
        insight_phases=[
            {
                "id": "connect",
                "missing": ["one_month_history_or_acknowledgment"],
                "status": "in_progress",
            }
        ],
    )

    assert "onboarding_insight" not in source
    assert enriched["phases"] == source["phases"]
    assert enriched["onboarding_insight"] == {
        "source": "onboarding_state",
        "text": "Add a month of transactions or acknowledge limited history before CashNerd starts steering moves.",
        "tone": "diagnose",
        "meta": "Step 1 of 4",
        "bullets": ["The chat can help import a CSV or continue with a thinner first recommendation."],
        "phase": "connect",
    }

    demo = enrich_onboarding_state_surface(
        {
            "current_phase": "profile",
            "is_demo_mode": True,
            "is_fully_onboarded": False,
            "is_gate_open": True,
            "phases": [],
            "progress": {"required_done": 1, "required_total": 4},
        }
    )
    assert demo["onboarding_insight"] == {
        "source": "onboarding_state",
        "text": "Sample data opened the dashboard; finish setup before relying on real recommendations.",
        "tone": "coach",
        "meta": "Dashboard open",
        "bullets": ["CashNerd can keep coaching from here without blocking dashboard access."],
        "phase": "profile",
    }


def test_enrich_goal_status_surface_owns_goal_recommendations() -> None:
    source = {
        "goals": [
            {
                "current_cents": 900_000,
                "direction": "up",
                "estimated_months": 6,
                "metric": "liquid_cash",
                "name": "Emergency Fund",
                "progress_pct": 50,
                "starting_cents": 300_000,
                "target_cents": 1_500_000,
            },
            {
                "current_cents": 270_000,
                "direction": "down",
                "estimated_months": None,
                "metric": "total_debt",
                "name": "Debt Free",
                "progress_pct": 46,
                "starting_cents": 500_000,
                "target_cents": 0,
            },
        ]
    }

    enriched = enrich_goal_status_surface(source)

    assert "living_margin" not in source["goals"][0]
    assert enriched["goal_insight"] == {
        "source": "goals_status",
        "text": (
            "Keep momentum on 1 of 2 active goals. "
            "Push surplus toward Emergency Fund: 50% complete with about 6 months to go."
        ),
        "meta": "2 active goals across 2 metrics",
        "bullets": [
            "$6,000 left for Emergency Fund.",
            "1 goal needs a new monthly move before CashNerd can project the finish line.",
        ],
        "tone": "warn",
    }
    assert enriched["stat_annotations"]["goals_moving"] == {
        "source": "goals_status",
        "text": "1 goal needs a new monthly move before it can get back on track.",
        "tone": "warn",
    }
    assert enriched["goals"][0]["living_margin"] == {
        "source": "goals_status",
        "text": "$6,000 left. At this pace, target is about 6 months away.",
        "tone": "positive",
    }
    assert enriched["goals"][1]["living_margin"] == {
        "source": "goals_status",
        "text": "$2,700 left. Add a monthly move so CashNerd can project the finish line.",
        "tone": "concern",
    }


def test_enrich_goal_status_surface_handles_savings_rate_without_eta() -> None:
    enriched = enrich_goal_status_surface(
        {
            "goals": [
                {
                    "current_pct": 12,
                    "direction": "up",
                    "estimated_months": None,
                    "metric": "savings_rate",
                    "name": "Savings Rate",
                    "progress_pct": 46,
                    "starting_pct": 5,
                    "target_pct": 20,
                }
            ]
        }
    )

    assert enriched["goal_insight"] == {
        "source": "goals_status",
        "text": "Keep momentum on 1 of 1 active goal. Push surplus toward Savings Rate: 46% complete.",
        "meta": "1 active goal across 1 metric",
        "bullets": [
            "8 percentage points left for Savings Rate.",
            "Keep surplus pointed at the closest win before adding a new target.",
        ],
        "tone": "coach",
    }
    assert enriched["stat_annotations"]["closest_win"] == {
        "source": "goals_status",
        "text": "8 percentage points left for Savings Rate at the current pace.",
        "tone": "coach",
    }
    assert enriched["goals"][0]["living_margin"] == {
        "source": "goals_status",
        "text": "8 percentage points left. Keep the next surplus move assigned so the savings-rate gain keeps compounding.",
        "tone": "positive",
    }


def test_enrich_debt_dashboard_surface_owns_debt_recommendations() -> None:
    source = {
        "cards": [
            {
                "apr": 24.99,
                "card_id": "card-1",
                "intro_apr_end_date": None,
                "label": "Chase 1234",
                "monthly_interest_cents": 3_750,
                "utilization_pct": 62,
            },
            {
                "apr": 0,
                "card_id": "card-2",
                "intro_apr_end_date": "2026-12-31",
                "label": "Promo Card",
                "monthly_interest_cents": 0,
                "utilization_pct": 20,
            },
        ],
        "total_balance_cents": 230_000,
        "total_monthly_interest_cents": 3_750,
        "weighted_avg_apr": 18.4,
    }

    enriched = enrich_debt_dashboard_surface(source)

    assert "living_margin" not in source["cards"][0]
    assert enriched["stat_annotations"]["apr"] == {
        "source": "debt_dashboard",
        "text": "18.4% weighted APR; compare avalanche vs snowball before splitting extra cash.",
        "tone": "pattern",
    }
    assert enriched["cards"][0]["living_margin"] == {
        "source": "debt_dashboard",
        "text": "$38/mo interest here. Put extra dollars here before lower-rate balances.",
        "tone": "concern",
    }
    assert enriched["debt_insight"]["tone"] == "warn"
    assert (
        enriched["debt_insight"]["bullets"][0]
        == "Chase 1234 carries the highest APR pressure."
    )


def test_enrich_spending_trends_surface_owns_spending_recommendations() -> None:
    source = {
        "categories": [
            {
                "average_cents": 20_000,
                "category": "Dining",
                "months_cents": {"2026-04": 20_000, "2026-05": 40_000},
                "trend": "\u2191",
            },
            {
                "average_cents": 15_000,
                "category": "Groceries",
                "months_cents": {"2026-04": 16_000, "2026-05": 11_000},
                "trend": "\u2193",
            },
        ],
        "grand_average": 0,
        "months": ["2026-04", "2026-05"],
        "totals_cents": {"2026-04": 50_000, "2026-05": 62_000},
    }

    enriched = enrich_spending_trends_surface(source)

    assert "living_margin" not in source["categories"][0]
    assert enriched["stat_annotations"]["current_spend"] == {
        "source": "spending_trends",
        "text": "$120 more than last month; start with the biggest category driver.",
        "tone": "warn",
    }
    assert enriched["categories"][0]["living_margin"] == {
        "source": "spending_trends",
        "text": "$200 above typical. Check Dining before it becomes the baseline.",
        "tone": "concern",
    }
    assert enriched["spending_insight"] == {
        "source": "spending_trends",
        "text": "Review Dining: it is trending up and averaging $200/mo across tracked months.",
        "meta": "2 months of spending history",
        "bullets": [
            "Dining is $200 above its tracked average.",
            "Start there before the higher run rate becomes the new baseline.",
        ],
        "tone": "warn",
    }
