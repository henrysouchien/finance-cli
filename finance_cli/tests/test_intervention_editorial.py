from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from finance_cli.intervention_editorial import (
    attach_cached_editorials,
    build_financial_profile,
    clear_editorial_cache,
    warm_editorial_cache,
)


NOW = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)


def _intervention(**overrides: Any) -> dict[str, Any]:
    item = {
        "action": {"build_stub": False, "label": "Run the move", "params": {}, "tool": "test_tool"},
        "detail_bullets": ["The deterministic detail stays available."],
        "dollar_impact": 250,
        "dollar_impact_cents": 25_000,
        "fired_at": "2026-05-26T12:00:00",
        "goal_link": None,
        "headline": "Move $250 to the highest-impact account.",
        "last_fired_at": None,
        "log_id": None,
        "move": "prescribe",
        "pattern_id": "D-1",
        "priority_rank": 0,
        "tier4_is_fallback": False,
        "tier4_ladder": "That's about 2 weeks faster to Emergency fund.",
        "tiers": [1, 4],
    }
    item.update(overrides)
    return item


def test_editorial_cache_warms_and_attaches(monkeypatch) -> None:
    clear_editorial_cache()
    monkeypatch.setenv("CASHNERD_EDITORIAL_API_KEY", "test-key")
    profile = {"months_of_history": 8, "budget_alert_count": 1}
    surfaces = {"dashboard": [_intervention()]}

    enriched, misses = attach_cached_editorials(
        surfaces,
        user_id="user-1",
        profile=profile,
        now=NOW,
    )

    assert enriched["dashboard"][0]["editorial"] is None
    assert len(misses) == 1

    def fake_send(provider: str, **kwargs):
        assert provider == "claude"
        assert kwargs["api_key"] == "test-key"
        prompt = kwargs["user_prompt"]
        assert "Move $250" in prompt
        return (
            '{"items":[{"key":"%s","headline":"Move $250 before Friday.","detail_bullets":["It keeps the payoff plan ahead."]}]}'
            % misses[0].key,
            {"input_tokens": 100, "output_tokens": 30},
        )

    result = warm_editorial_cache(
        misses,
        provider="claude",
        model="claude-test",
        send_fn=fake_send,
        now=NOW,
    )

    assert result == {"warmed": 1, "skipped": None}

    enriched_again, second_misses = attach_cached_editorials(
        surfaces,
        user_id="user-1",
        profile=profile,
        now=NOW,
    )

    assert second_misses == []
    assert enriched_again["dashboard"][0]["editorial"]["headline"] == "Move $250 before Friday."
    assert enriched_again["dashboard"][0]["editorial"]["detail_bullets"] == ["It keeps the payoff plan ahead."]
    assert enriched_again["dashboard"][0]["editorial"]["provider"] == "claude"
    assert enriched_again["dashboard"][0]["editorial"]["model"] == "claude-test"


def test_editorial_cache_key_changes_when_profile_changes(monkeypatch) -> None:
    clear_editorial_cache()
    monkeypatch.setenv("CASHNERD_EDITORIAL_API_KEY", "test-key")
    surfaces = {"dashboard": [_intervention()]}
    first_profile = {"months_of_history": 8}
    second_profile = {"months_of_history": 9}
    _, misses = attach_cached_editorials(surfaces, user_id="user-1", profile=first_profile, now=NOW)

    def fake_send(provider: str, **kwargs):
        return (
            '{"items":[{"key":"%s","headline":"Cached first profile.","detail_bullets":[]}]}'
            % misses[0].key,
            {"input_tokens": 1, "output_tokens": 1},
        )

    warm_editorial_cache(misses, provider="claude", model="claude-test", send_fn=fake_send, now=NOW)

    enriched, second_misses = attach_cached_editorials(
        surfaces,
        user_id="user-1",
        profile=second_profile,
        now=NOW,
    )

    assert enriched["dashboard"][0]["editorial"] is None
    assert len(second_misses) == 1
    assert second_misses[0].key != misses[0].key


def test_build_financial_profile_uses_summary_shape() -> None:
    profile = build_financial_profile(
        {
            "budget_alerts": [{"category_name": "Dining"}],
            "goals": [{"name": "Emergency fund"}],
            "onboarding_state": {
                "categorization_rate": 0.9381,
                "months_of_history": 8,
                "transaction_count": 42,
                "vendor_memory_count": 12,
            },
            "stat_annotations": {
                "handled_automatically": {"text": "done"},
                "spend_to_steer": None,
            },
        }
    )

    assert profile == {
        "active_goal_count": 1,
        "budget_alert_count": 1,
        "categorization_rate": 0.938,
        "months_of_history": 8,
        "stat_annotation_slots": ["handled_automatically"],
        "transaction_count": 42,
        "vendor_memory_count": 12,
    }
