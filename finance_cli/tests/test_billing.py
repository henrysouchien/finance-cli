from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from finance_cli.billing import (
    DEFAULT_CLAUDE_MONTHLY_CAP_USD6,
    LIFETIME_CLAUDE_MONTHLY_CAP_USD6,
    MCP_READ_ONLY_ALLOWLIST,
    PLAN_CONFIGS,
    RequestResolution,
    TRIAL_CLAUDE_MONTHLY_CAP_USD6,
    apply_lifetime_cost_cap,
    apply_tier_transition,
    apply_trial_cost_cap,
    days_remaining_trial,
    effective_plan,
    effective_tier,
    has_active_engagement,
    has_paid_rate,
    is_active_subscriber,
    is_read_only,
    mcp_tool_allowed_for_user,
    resolve_request,
    restore_default_cost_cap,
    seed_plan_caps,
)
from finance_cli.commands.ops_cmd import normalize_phase1_sentinels
from finance_cli.db import connect, initialize_database


class FakeCursor:
    def __init__(self, row: dict | None = None, rowcount: int = 1) -> None:
        self._row = row
        self.rowcount = rowcount
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, query: str, params: tuple = ()) -> None:
        self.statements.append((query, params))

    def fetchone(self) -> dict | None:
        return self._row


class FakeConn:
    def __init__(self, row: dict | None = None, rowcount: int = 1) -> None:
        self.cursor_obj = FakeCursor(row=row, rowcount=rowcount)
        self.commit = Mock()

    def cursor(self) -> FakeCursor:
        return self.cursor_obj


def _future(days: int = 5) -> datetime:
    return datetime.now(timezone.utc) + timedelta(days=days)


def _past(days: int = 1) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


def test_effective_tier_all_known_tiers_and_trial_expiry() -> None:
    assert effective_tier({"tier": "registered"}) == "registered"
    assert effective_tier({"tier": "trial", "trial_ends_at": _future()}) == "trial"
    assert effective_tier({"tier": "trial", "trial_ends_at": _past()}) == "expired"
    assert effective_tier({"tier": "trial", "trial_ends_at": None}) == "expired"
    assert effective_tier({"tier": "paid"}) == "paid"
    assert effective_tier({"tier": "lifetime"}) == "lifetime"
    assert effective_tier({"tier": "past_due"}) == "past_due"
    assert effective_tier({"tier": "business"}) == "business"
    assert effective_tier({"tier": "cancelled"}) == "cancelled"
    assert effective_tier({"tier": "expired"}) == "expired"
    assert effective_tier({"tier": "pro"}) == "pro"
    assert effective_tier({"tier": "premium"}) == "premium"


def test_unknown_corrupt_tiers_fall_back_to_registered_and_inactive() -> None:
    for tier in ("", None, "free", "anonymous", "nonsense"):
        user = {"tier": tier}
        assert effective_tier(user) == "registered"
        assert is_active_subscriber(user) is False
        assert is_read_only(user) is False
        assert has_paid_rate(user) is False


def test_is_active_subscriber_for_all_tiers_including_business() -> None:
    active = {"trial", "paid", "lifetime", "past_due", "business"}
    inactive = {"registered", "cancelled", "expired", "pro", "premium"}
    for tier in active:
        assert is_active_subscriber({"tier": tier, "trial_ends_at": _future()}) is True
    for tier in inactive:
        assert is_active_subscriber({"tier": tier}) is False


def test_has_active_engagement_accepts_productized_entitlement() -> None:
    assert has_active_engagement({"tier": "registered", "has_active_engagement": True}) is True
    assert has_active_engagement({"tier": "registered", "has_active_engagement": "true"}) is True
    assert has_active_engagement({"tier": "registered", "has_active_engagement": False}) is False


def test_has_paid_rate_includes_legacy_and_business() -> None:
    for tier in ("trial", "paid", "lifetime", "past_due", "business", "pro", "premium"):
        assert has_paid_rate({"tier": tier, "trial_ends_at": _future()}) is True
    for tier in ("registered", "cancelled", "expired"):
        assert has_paid_rate({"tier": tier}) is False


def test_days_remaining_trial() -> None:
    assert days_remaining_trial({"tier": "paid", "trial_ends_at": _future()}) is None
    assert days_remaining_trial({"tier": "trial", "trial_ends_at": None}) is None
    assert days_remaining_trial({"tier": "trial", "trial_ends_at": _past()}) == 0
    assert days_remaining_trial({"tier": "trial", "trial_ends_at": _future(3)}) in {2, 3}
    assert days_remaining_trial({"tier": "trial", "trial_ends_at": _future(4).isoformat()}) in {3, 4}


def test_mcp_tool_allowed_for_user_by_tier() -> None:
    assert mcp_tool_allowed_for_user("plaid_sync", {"tier": "paid"}) is True
    assert mcp_tool_allowed_for_user("txn_list", {"tier": "cancelled"}) is True
    assert mcp_tool_allowed_for_user("plaid_sync", {"tier": "cancelled"}) is False
    assert mcp_tool_allowed_for_user("txn_list", {"tier": "registered"}) is False


def test_apply_tier_transition_updates_expected_fields_without_commit() -> None:
    conn = FakeConn(row={"tier": "paid", "lifetime_deal": False})

    updated = apply_tier_transition(
        "user-1",
        "cancelled",
        conn,
        stripe_subscription_id="sub_123",
        subscription_status="canceled",
    )

    assert updated is True
    assert len(conn.cursor_obj.statements) == 2
    update_query, update_params = conn.cursor_obj.statements[1]
    assert "UPDATE users SET tier = %s" in update_query
    assert "stripe_subscription_id = %s" in update_query
    assert "subscription_status = %s" in update_query
    assert "updated_at = NOW()" in update_query
    assert update_params == ("cancelled", "sub_123", "canceled", "user-1")
    conn.commit.assert_not_called()


def test_apply_tier_transition_lifetime_guard_and_override() -> None:
    guarded = FakeConn(row={"tier": "lifetime", "lifetime_deal": True})

    assert apply_tier_transition("user-1", "cancelled", guarded) is False
    assert len(guarded.cursor_obj.statements) == 1
    guarded.commit.assert_not_called()

    override = FakeConn(row={"tier": "lifetime", "lifetime_deal": True})
    assert apply_tier_transition(
        "user-1",
        "cancelled",
        override,
        allow_lifetime_override=True,
    ) is True
    assert len(override.cursor_obj.statements) == 1
    override.commit.assert_not_called()


def test_apply_tier_transition_returns_false_when_no_row_updated() -> None:
    conn = FakeConn(row={"tier": "paid", "lifetime_deal": False}, rowcount=0)
    assert apply_tier_transition("missing", "expired", conn) is False
    conn.commit.assert_not_called()


def test_mcp_read_only_allowlist_drift_guard() -> None:
    assert {"txn_list", "txn_categorize", "rules_add_keyword", "export_csv"} <= MCP_READ_ONLY_ALLOWLIST
    assert {
        "biz_pl",
        "interventions_get",
        "plaid_sync",
        "budget_set",
        "cat_auto_categorize",
    }.isdisjoint(MCP_READ_ONLY_ALLOWLIST)


def _create_cost_limit_db(db_path: Path, *, limit_usd6: int, action: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE cost_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                period TEXT NOT NULL,
                limit_usd6 INTEGER NOT NULL,
                action TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                UNIQUE(provider, period)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO cost_limits (provider, period, limit_usd6, action)
            VALUES ('claude', 'monthly', ?, ?)
            """,
            (limit_usd6, action),
        )


def _cost_limit_row(db_path: Path) -> tuple[int, str]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT limit_usd6, action
            FROM cost_limits
            WHERE provider = 'claude' AND period = 'monthly'
            """
        ).fetchone()
    return int(row[0]), str(row[1])


def test_apply_trial_cost_cap_updates_default_row(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _create_cost_limit_db(db_path, limit_usd6=DEFAULT_CLAUDE_MONTHLY_CAP_USD6, action="warn")

    apply_trial_cost_cap(db_path)

    assert _cost_limit_row(db_path) == (TRIAL_CLAUDE_MONTHLY_CAP_USD6, "block")


def test_apply_trial_cost_cap_preserves_user_set_and_is_idempotent(tmp_path: Path) -> None:
    user_set = tmp_path / "user-set.db"
    _create_cost_limit_db(user_set, limit_usd6=1_000_000, action="block")
    apply_trial_cost_cap(user_set)
    assert _cost_limit_row(user_set) == (1_000_000, "block")

    already_applied = tmp_path / "already-applied.db"
    _create_cost_limit_db(
        already_applied,
        limit_usd6=TRIAL_CLAUDE_MONTHLY_CAP_USD6,
        action="block",
    )
    apply_trial_cost_cap(already_applied)
    assert _cost_limit_row(already_applied) == (TRIAL_CLAUDE_MONTHLY_CAP_USD6, "block")


def test_restore_default_cost_cap_restores_bridge_row_only(tmp_path: Path) -> None:
    bridge = tmp_path / "bridge.db"
    _create_cost_limit_db(bridge, limit_usd6=TRIAL_CLAUDE_MONTHLY_CAP_USD6, action="block")
    restore_default_cost_cap(bridge)
    assert _cost_limit_row(bridge) == (DEFAULT_CLAUDE_MONTHLY_CAP_USD6, "warn")

    user_set = tmp_path / "user-set.db"
    _create_cost_limit_db(user_set, limit_usd6=1_000_000, action="block")
    restore_default_cost_cap(user_set)
    assert _cost_limit_row(user_set) == (1_000_000, "block")


def test_apply_lifetime_cost_cap_updates_default_row(tmp_path: Path) -> None:
    db_path = tmp_path / "default.db"
    _create_cost_limit_db(db_path, limit_usd6=DEFAULT_CLAUDE_MONTHLY_CAP_USD6, action="warn")

    apply_lifetime_cost_cap(db_path)

    assert _cost_limit_row(db_path) == (LIFETIME_CLAUDE_MONTHLY_CAP_USD6, "block")


def test_apply_lifetime_cost_cap_updates_trial_bridge_row(tmp_path: Path) -> None:
    db_path = tmp_path / "trial-bridge.db"
    _create_cost_limit_db(db_path, limit_usd6=TRIAL_CLAUDE_MONTHLY_CAP_USD6, action="block")

    apply_lifetime_cost_cap(db_path)

    assert _cost_limit_row(db_path) == (LIFETIME_CLAUDE_MONTHLY_CAP_USD6, "block")


@pytest.mark.parametrize(
    ("limit_usd6", "action"),
    [
        (1_000_000, "block"),
        (20_000_000, "warn"),
        (LIFETIME_CLAUDE_MONTHLY_CAP_USD6, "block"),
    ],
)
def test_apply_lifetime_cost_cap_preserves_user_set_and_is_idempotent(
    tmp_path: Path,
    limit_usd6: int,
    action: str,
) -> None:
    db_path = tmp_path / f"{limit_usd6}-{action}.db"
    _create_cost_limit_db(db_path, limit_usd6=limit_usd6, action=action)

    apply_lifetime_cost_cap(db_path)

    assert _cost_limit_row(db_path) == (limit_usd6, action)


def test_lifetime_cost_cap_drift_guard() -> None:
    assert LIFETIME_CLAUDE_MONTHLY_CAP_USD6 == 10_000_000


@pytest.mark.parametrize(
    ("user", "expected_code"),
    [
        ({"tier": "trial", "stripe_price_id": "price_lite"}, "standard"),
        ({"tier": "paid", "lifetime_deal": True, "stripe_price_id": "price_lite"}, "lifetime"),
        ({"tier": "lifetime", "lifetime_deal": False}, "lifetime"),
        ({"tier": "paid", "stripe_price_id": "price_lite"}, "lite"),
        ({"tier": "paid", "stripe_price_id": "price_standard"}, "standard"),
        ({"tier": "registered", "stripe_price_id": None}, "standard"),
    ],
)
def test_effective_plan_precedence(user: dict, expected_code: str) -> None:
    settings = SimpleNamespace(stripe_price_lite="price_lite")
    assert effective_plan(user, settings).code == expected_code


def _init_phase2_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _set_claude_monthly_cap(db_path: Path, *, limit_usd6: int | None, system_limit_usd6: int | None) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = ?,
                   system_limit_usd6 = ?,
                   action = 'warn'
             WHERE provider = 'claude'
               AND period = 'monthly'
            """,
            (limit_usd6, system_limit_usd6),
        )
        conn.commit()


def test_resolve_request_byok_skips_capacity_checks(tmp_path: Path) -> None:
    missing_db = tmp_path / "missing.db"
    settings = SimpleNamespace(stripe_price_lite="price_lite")

    resolution = resolve_request(
        {"tier": "paid", "billing_mode": "byok"},
        missing_db,
        settings,
        explicit_model="claude-opus-4-6",
    )

    assert resolution == RequestResolution(
        mode="byok",
        action="allow",
        effective_model="claude-opus-4-6",
        warn_threshold_hit=False,
        credits_available=0,
    )


def test_resolve_request_has_capacity_honors_explicit_model(tmp_path: Path) -> None:
    db_path = _init_phase2_db(tmp_path)
    _set_claude_monthly_cap(db_path, limit_usd6=None, system_limit_usd6=1_000_000)

    resolution = resolve_request(
        {"tier": "paid"},
        db_path,
        SimpleNamespace(stripe_price_lite="price_lite"),
        explicit_model="claude-opus-4-6",
    )

    assert resolution.mode == "subscription"
    assert resolution.action == "allow"
    assert resolution.effective_model == "claude-opus-4-6"
    assert resolution.warn_threshold_hit is False
    assert resolution.credits_available == 0


def test_resolve_request_no_capacity_standard_downgrades(tmp_path: Path) -> None:
    db_path = _init_phase2_db(tmp_path)
    _set_claude_monthly_cap(db_path, limit_usd6=None, system_limit_usd6=1_000)
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO cost_ledger (provider, operation, cost_usd6) VALUES ('claude', 'chat', 1000)"
        )
        conn.commit()

    resolution = resolve_request({"tier": "paid"}, db_path, SimpleNamespace(stripe_price_lite="price_lite"))

    assert resolution.action == "downgrade"
    assert resolution.effective_model == PLAN_CONFIGS["lite"].default_model
    assert resolution.warn_threshold_hit is True


def test_resolve_request_no_capacity_lite_blocks(tmp_path: Path) -> None:
    db_path = _init_phase2_db(tmp_path)
    _set_claude_monthly_cap(db_path, limit_usd6=None, system_limit_usd6=1_000)
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO cost_ledger (provider, operation, cost_usd6) VALUES ('claude', 'chat', 1000)"
        )
        conn.commit()

    resolution = resolve_request(
        {"tier": "paid", "stripe_price_id": "price_lite"},
        db_path,
        SimpleNamespace(stripe_price_lite="price_lite"),
    )

    assert resolution.action == "block"
    assert resolution.effective_model == PLAN_CONFIGS["lite"].default_model


def test_seed_plan_caps_preserves_user_limit_and_is_idempotent(tmp_path: Path) -> None:
    db_path = _init_phase2_db(tmp_path)
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = ?,
                   system_limit_usd6 = NULL,
                   action = 'block'
             WHERE provider = 'claude'
               AND period = 'monthly'
            """,
            (2_000_000,),
        )
        seed_plan_caps(conn, PLAN_CONFIGS["standard"])
        seed_plan_caps(conn, PLAN_CONFIGS["standard"])
        row = conn.execute(
            """
            SELECT limit_usd6, system_limit_usd6, action
            FROM cost_limits
            WHERE provider = 'claude' AND period = 'monthly'
            """
        ).fetchone()

    assert row["limit_usd6"] == 2_000_000
    assert row["system_limit_usd6"] == PLAN_CONFIGS["standard"].monthly_cap_usd6
    assert row["action"] == "block"


def _create_phase2_cost_limit_db(db_path: Path, *, limit_usd6: int, action: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE cost_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                period TEXT NOT NULL,
                limit_usd6 INTEGER,
                system_limit_usd6 INTEGER,
                action TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                UNIQUE(provider, period)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO cost_limits (provider, period, limit_usd6, system_limit_usd6, action)
            VALUES ('claude', 'monthly', ?, NULL, ?)
            """,
            (limit_usd6, action),
        )


def _phase2_limit_row(db_path: Path) -> tuple[int | None, str]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT limit_usd6, action
            FROM cost_limits
            WHERE provider = 'claude' AND period = 'monthly'
            """
        ).fetchone()
    return row[0], str(row[1])


@pytest.mark.parametrize(
    ("user", "limit_usd6", "expected"),
    [
        ({"tier": "paid", "lifetime_deal": True}, LIFETIME_CLAUDE_MONTHLY_CAP_USD6, (None, "warn")),
        ({"tier": "paid", "lifetime_deal": False}, LIFETIME_CLAUDE_MONTHLY_CAP_USD6, (10_000_000, "block")),
        ({"tier": "trial", "lifetime_deal": False}, TRIAL_CLAUDE_MONTHLY_CAP_USD6, (None, "warn")),
        ({"tier": "paid", "lifetime_deal": False}, TRIAL_CLAUDE_MONTHLY_CAP_USD6, (3_000_000, "block")),
    ],
)
def test_plan_caps_reseed_sentinel_normalization(
    tmp_path: Path,
    user: dict,
    limit_usd6: int,
    expected: tuple[int | None, str],
) -> None:
    db_path = tmp_path / "sentinel.db"
    _create_phase2_cost_limit_db(db_path, limit_usd6=limit_usd6, action="block")

    with sqlite3.connect(db_path) as conn:
        normalize_phase1_sentinels(conn, user)

    assert _phase2_limit_row(db_path) == expected
