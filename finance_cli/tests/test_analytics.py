from __future__ import annotations

import json
from pathlib import Path

from finance_cli.analytics import (
    KNOWN_EVENTS,
    KNOWN_PROPERTIES,
    PropType,
    _filter_properties,
    log_event,
    prune_analytics,
)
from finance_cli.db import connect, initialize_database


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def test_log_event_writes_to_analytics_events_table(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    log_event(
        db_path,
        "import.csv_completed",
        properties={"row_count": 12, "account_type": "checking", "ignored": "x"},
        source="cli",
    )

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT event, domain, outcome, properties, source
            FROM analytics_events
            """
        ).fetchone()

    assert row is not None
    assert row["event"] == "import.csv_completed"
    assert row["domain"] == "import"
    assert row["outcome"] == "succeeded"
    assert row["source"] == "cli"
    assert json.loads(row["properties"]) == {
        "account_type": "checking",
        "row_count": 12,
    }


def test_log_event_never_raises_on_db_errors(tmp_path: Path) -> None:
    broken_db_path = tmp_path / "missing" / "finance.db"
    log_event(broken_db_path, "feature.budget_set")


def test_abandoned_signal_events_require_explicit_abandoned_outcome(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    log_event(
        db_path,
        "feature.goal_abandoned",
        properties={"goal_id": "goal-1", "goal_name": "Debt sprint"},
    )
    log_event(
        db_path,
        "feature.plan_abandoned",
        properties={"month": "2026-04"},
    )

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT event, outcome
              FROM analytics_events
             ORDER BY event
            """
        ).fetchall()

    assert [(row["event"], row["outcome"]) for row in rows] == [
        ("feature.goal_abandoned", "succeeded"),
        ("feature.plan_abandoned", "succeeded"),
    ]


def test_filter_properties_strips_unknown_keys() -> None:
    filtered = _filter_properties(
        "import.plaid_synced",
        {"txn_count": 10, "account_count": 2, "other": "ignored"},
    )

    assert filtered == {"txn_count": 10, "account_count": 2}


def test_filter_properties_keeps_abandoned_signal_keys() -> None:
    assert _filter_properties(
        "feature.goal_abandoned",
        {"goal_id": "goal-1", "goal_name": "Debt sprint", "ignored": "x"},
    ) == {"goal_id": "goal-1", "goal_name": "Debt sprint"}
    assert _filter_properties(
        "feature.plan_abandoned",
        {"month": "2026-04", "ignored": "x"},
    ) == {"month": "2026-04"}


def test_filter_properties_validates_int_enum_and_bool(monkeypatch) -> None:
    monkeypatch.setitem(
        KNOWN_PROPERTIES,
        "test.props",
        {
            "count": (PropType.INT, None),
            "flag": (PropType.BOOL, None),
            "kind": (PropType.ENUM, {"alpha"}),
        },
    )

    filtered = _filter_properties(
        "test.props",
        {
            "count": "12",
            "flag": 1,
            "kind": "alpha",
        },
    )

    assert filtered == {"kind": "alpha"}

    filtered = _filter_properties(
        "test.props",
        {
            "count": 12,
            "flag": True,
            "kind": "alpha",
        },
    )

    assert filtered == {"count": 12, "flag": True, "kind": "alpha"}


def test_filter_properties_replaces_unknown_enum_values() -> None:
    filtered = _filter_properties(
        "feature.export_generated",
        {"format": "xlsx"},
    )

    assert filtered == {"format": "unknown"}


def test_onboarding_wizard_properties_are_allowlisted() -> None:
    filtered = _filter_properties(
        "onboarding.wizard",
        {"step": "begin_setup", "context": "dashboard", "ignored": "x"},
    )

    assert filtered == {"step": "begin_setup", "context": "dashboard"}


def test_prune_analytics_deletes_old_records(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO analytics_events (event, domain, created_at)
            VALUES ('feature.budget_set', 'feature', datetime('now', '-120 days'))
            """
        )
        conn.execute(
            """
            INSERT INTO analytics_events (event, domain, created_at)
            VALUES ('feature.goal_set', 'feature', datetime('now'))
            """
        )
        prune_analytics(conn, retention_days=90)
        rows = conn.execute(
            "SELECT event FROM analytics_events ORDER BY event"
        ).fetchall()

    assert [row["event"] for row in rows] == ["feature.goal_set"]


def test_known_events_contains_full_taxonomy() -> None:
    assert KNOWN_EVENTS == {
        "onboarding.wizard",
        "onboarding.plaid_link",
        "onboarding.csv_import",
        "onboarding.first_categorization",
        "onboarding.profile_captured",
        "onboarding.focus_selected",
        "onboarding.setup_acknowledged",
        "onboarding.complete",
        "chat.session",
        "feature.budget_set",
        "feature.goal_set",
        "feature.goal_abandoned",
        "feature.subscription_detected",
        "feature.spending_trends_viewed",
        "feature.debt_simulated",
        "feature.export_generated",
        "feature.plan_created",
        "feature.plan_abandoned",
        "import.csv_completed",
        "import.pdf_completed",
        "import.plaid_synced",
        "import.stripe_synced",
        "cost.limit_warning",
        "account.deletion_scheduled",
        "account.deletion_immediate",
        "account.deletion_cancelled",
        "account.deletion_completed",
    }


def test_log_event_extracts_domain_from_event_name(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    log_event(db_path, "feature.plan_created")

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT domain FROM analytics_events WHERE event = 'feature.plan_created'"
        ).fetchone()

    assert row is not None
    assert row["domain"] == "feature"
