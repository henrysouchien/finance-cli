"""Shared billing and tier helpers."""

from __future__ import annotations

from collections.abc import Iterator, Set as AbstractSet
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cache
from pathlib import Path
import sqlite3
from typing import Any, Literal, Mapping

from finance_cli.db import connect

ACTIVE_TIERS = frozenset({"trial", "paid", "lifetime", "past_due", "business"})
READ_ONLY_TIERS = frozenset({"cancelled", "expired"})
PAID_RATE_TIERS = ACTIVE_TIERS | frozenset({"pro", "premium"})

_EXCLUDED_IN_READ_ONLY = frozenset(
    {
        "biz_pl",
        "biz_cashflow",
        "biz_tax",
        "biz_tax_detail",
        "biz_estimated_tax",
        "biz_forecast",
        "biz_runway",
        "biz_seasonal",
        "biz_budget_status",
        "biz_mileage_list",
        "biz_mileage_summary",
        "biz_contractor_list",
        "biz_1099_report",
        "interventions_get",
    }
)

_READ_ONLY_EXTRAS = frozenset(
    {
        "txn_categorize",
        "txn_bulk_categorize",
        "txn_review",
        "txn_tag",
        "txn_bulk_tag",
        "cat_memory_add",
        "cat_memory_confirm",
        "cat_memory_delete",
        "cat_memory_delete_bulk",
        "cat_memory_disable",
        "cat_memory_disable_bulk",
        "cat_memory_restore",
        "cat_memory_undo",
        "rules_add_keyword",
        "rules_add_keywords",
        "rules_remove_keyword",
        "rules_update_priority",
        "rules_add_split",
        "export_csv",
        "export_summary",
        "export_wave",
        "db_export_preferences",
    }
)


@cache
def _mcp_read_only_allowlist() -> frozenset[str]:
    from finance_cli.gateway.tools import READ_ONLY_TOOLS

    return (READ_ONLY_TOOLS - _EXCLUDED_IN_READ_ONLY) | _READ_ONLY_EXTRAS


class _LazyMcpReadOnlyAllowlist(AbstractSet[str]):
    def __contains__(self, item: object) -> bool:
        return item in _mcp_read_only_allowlist()

    def __iter__(self) -> Iterator[str]:
        return iter(_mcp_read_only_allowlist())

    def __len__(self) -> int:
        return len(_mcp_read_only_allowlist())

    def __repr__(self) -> str:
        return repr(_mcp_read_only_allowlist())


MCP_READ_ONLY_ALLOWLIST = _LazyMcpReadOnlyAllowlist()

TRIAL_CLAUDE_MONTHLY_CAP_USD6 = 3_000_000
DEFAULT_CLAUDE_MONTHLY_CAP_USD6 = 50_000_000
LIFETIME_CLAUDE_MONTHLY_CAP_USD6 = 10_000_000


@dataclass(frozen=True)
class PlanConfig:
    code: str
    default_model: str
    cap_action: Literal["downgrade", "block"]
    monthly_cap_usd6: int


PLAN_CONFIGS: dict[str, PlanConfig] = {
    "lite": PlanConfig("lite", "claude-haiku-4-5", "block", 1_250_000),
    "standard": PlanConfig("standard", "claude-sonnet-4-6", "downgrade", 3_500_000),
    "lifetime": PlanConfig("lifetime", "claude-sonnet-4-6", "downgrade", 10_000_000),
}


@dataclass(frozen=True)
class RequestResolution:
    mode: Literal["subscription", "byok"]
    action: Literal["allow", "downgrade", "block"]
    effective_model: str
    warn_threshold_hit: bool
    credits_available: int


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def effective_tier(user: Mapping[str, Any]) -> str:
    raw = str(user.get("tier") or "registered").strip().lower() or "registered"
    if raw == "trial":
        ends = _parse_ts(user.get("trial_ends_at"))
        if ends is None or ends < datetime.now(timezone.utc):
            return "expired"
    if raw in ACTIVE_TIERS or raw in READ_ONLY_TIERS or raw in PAID_RATE_TIERS:
        return raw
    return "registered"


def effective_plan(user: Mapping[str, Any], settings: Any) -> PlanConfig:
    """Resolve the active plan with trial-first precedence."""
    tier = str(user.get("tier") or "").strip().lower()
    if tier == "trial":
        return PLAN_CONFIGS["standard"]
    if bool(user.get("lifetime_deal")) or tier == "lifetime":
        return PLAN_CONFIGS["lifetime"]

    stripe_price_id = str(user.get("stripe_price_id") or "").strip()
    lite_price = str(getattr(settings, "stripe_price_lite", "") or "").strip()
    if lite_price and stripe_price_id == lite_price:
        return PLAN_CONFIGS["lite"]
    return PLAN_CONFIGS["standard"]


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "byok"}
    return bool(value)


def _byok_active(user: Mapping[str, Any]) -> bool:
    if str(user.get("billing_mode") or "").strip().lower() == "byok":
        return True
    for key in (
        "byok",
        "is_byok",
        "has_byok",
        "has_byok_key",
        "has_anthropic_key",
        "anthropic_key_active",
    ):
        if _truthy(user.get(key)):
            return True
    return bool(user.get("anthropic_api_key_secret_ref") or user.get("anthropic_api_key_enc"))


def _effective_cap(limit_usd6: Any, system_limit_usd6: Any) -> int | None:
    candidates: list[int] = []
    for value in (limit_usd6, system_limit_usd6):
        if value is None:
            continue
        candidates.append(max(int(value), 0))
    return min(candidates) if candidates else None


def _monthly_non_byok_spend(conn: sqlite3.Connection, provider: str) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(cost_usd6), 0) AS spent
        FROM cost_ledger
        WHERE provider = ?
          AND COALESCE(is_byok, 0) = 0
          AND strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')
        """,
        (provider,),
    ).fetchone()
    return int(row["spent"] if row is not None else 0)


def _monthly_cap(conn: sqlite3.Connection, provider: str) -> int | None:
    row = conn.execute(
        """
        SELECT limit_usd6, system_limit_usd6
        FROM cost_limits
        WHERE provider = ?
          AND period = 'monthly'
          AND is_active = 1
        """,
        (provider,),
    ).fetchone()
    if row is None:
        return None
    return _effective_cap(row["limit_usd6"], row["system_limit_usd6"])


def _credit_balance(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT balance_usd6 FROM credit_balance WHERE id = 1").fetchone()
    return max(int(row["balance_usd6"] if row is not None else 0), 0)


def resolve_request(
    user: Mapping[str, Any],
    db_path: Path,
    settings: Any,
    *,
    explicit_model: str | None = None,
) -> RequestResolution:
    """Resolve AI request routing.

    PRECONDITION: caller has already done auth and tier gating. Anonymous,
    expired, and cancelled users should be rejected before calling this helper.
    """
    plan = effective_plan(user, settings)
    requested_model = str(explicit_model or "").strip() or None

    if _byok_active(user):
        return RequestResolution(
            mode="byok",
            action="allow",
            effective_model=requested_model or plan.default_model,
            warn_threshold_hit=False,
            credits_available=0,
        )

    with connect(Path(db_path)) as conn:
        cap_usd6 = _monthly_cap(conn, "claude")
        mtd_spend = _monthly_non_byok_spend(conn, "claude")
        credits_available = _credit_balance(conn)

    if cap_usd6 is None:
        return RequestResolution(
            mode="subscription",
            action="allow",
            effective_model=requested_model or plan.default_model,
            warn_threshold_hit=False,
            credits_available=credits_available,
        )

    remaining_allowance = max(cap_usd6 - mtd_spend, 0)
    has_capacity = remaining_allowance > 0 or credits_available > 0
    warn_threshold_hit = cap_usd6 > 0 and mtd_spend >= int(cap_usd6 * 0.8)

    if has_capacity:
        effective_model = requested_model or plan.default_model
        if requested_model is None and plan.code == "lite" and credits_available > 0:
            effective_model = PLAN_CONFIGS["standard"].default_model
        return RequestResolution(
            mode="subscription",
            action="allow",
            effective_model=effective_model,
            warn_threshold_hit=warn_threshold_hit,
            credits_available=credits_available,
        )

    if plan.cap_action == "downgrade":
        return RequestResolution(
            mode="subscription",
            action="downgrade",
            effective_model=PLAN_CONFIGS["lite"].default_model,
            warn_threshold_hit=True,
            credits_available=credits_available,
        )

    return RequestResolution(
        mode="subscription",
        action="block",
        effective_model=plan.default_model,
        warn_threshold_hit=True,
        credits_available=credits_available,
    )


def seed_plan_caps(conn: sqlite3.Connection, plan: PlanConfig) -> None:
    """Upsert system-owned plan caps while preserving user-set limit_usd6."""
    conn.execute(
        """
        INSERT INTO cost_limits (provider, period, limit_usd6, system_limit_usd6, action)
        VALUES ('claude', 'monthly', NULL, ?, 'warn')
        ON CONFLICT(provider, period) DO UPDATE SET
            system_limit_usd6 = excluded.system_limit_usd6
        """,
        (plan.monthly_cap_usd6,),
    )


def is_active_subscriber(user: Mapping[str, Any]) -> bool:
    return effective_tier(user) in ACTIVE_TIERS


def has_active_engagement(user: Mapping[str, Any]) -> bool:
    return is_active_subscriber(user) or _truthy(user.get("has_active_engagement"))


def is_read_only(user: Mapping[str, Any]) -> bool:
    return effective_tier(user) in READ_ONLY_TIERS


def has_paid_rate(user: Mapping[str, Any]) -> bool:
    return effective_tier(user) in PAID_RATE_TIERS


def days_remaining_trial(user: Mapping[str, Any]) -> int | None:
    if str(user.get("tier") or "").lower() != "trial":
        return None
    ends = _parse_ts(user.get("trial_ends_at"))
    if ends is None:
        return None
    delta = ends - datetime.now(timezone.utc)
    return max(0, delta.days)


def mcp_tool_allowed_for_user(tool_name: str, user: Mapping[str, Any]) -> bool:
    eff = effective_tier(user)
    if eff in ACTIVE_TIERS:
        return True
    if eff in READ_ONLY_TIERS:
        return tool_name in MCP_READ_ONLY_ALLOWLIST
    return False


def apply_tier_transition(
    user_id: Any,
    new_tier: str,
    conn,
    *,
    allow_lifetime_override: bool = False,
    **stripe_fields: Any,
) -> bool:
    cursor = conn.cursor()
    if not allow_lifetime_override:
        cursor.execute(
            "SELECT tier, lifetime_deal FROM users WHERE id = %s",
            (user_id,),
        )
        row = cursor.fetchone()
        if row and (row["tier"] == "lifetime" or row["lifetime_deal"]) and new_tier != "lifetime":
            return False

    fields = ["tier = %s"]
    values: list[Any] = [new_tier]
    for key, val in stripe_fields.items():
        fields.append(f"{key} = %s")
        values.append(val)
    fields.append("updated_at = NOW()")
    values.append(user_id)
    cursor.execute(
        f"UPDATE users SET {', '.join(fields)} WHERE id = %s",
        tuple(values),
    )
    return cursor.rowcount > 0


def apply_trial_cost_cap(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = ?,
                   action = 'block'
             WHERE provider = 'claude'
               AND period = 'monthly'
               AND limit_usd6 = ?
               AND action = 'warn'
            """,
            (TRIAL_CLAUDE_MONTHLY_CAP_USD6, DEFAULT_CLAUDE_MONTHLY_CAP_USD6),
        )
        conn.commit()


def restore_default_cost_cap(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = ?,
                   action = 'warn'
             WHERE provider = 'claude'
               AND period = 'monthly'
               AND limit_usd6 = ?
               AND action = 'block'
            """,
            (DEFAULT_CLAUDE_MONTHLY_CAP_USD6, TRIAL_CLAUDE_MONTHLY_CAP_USD6),
        )
        conn.commit()


def apply_lifetime_cost_cap(db_path: Path) -> None:
    """Set the LTD cap only from known default or trial sentinel states."""
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = ?,
                   action = 'block'
             WHERE provider = 'claude'
               AND period = 'monthly'
               AND (
                    (limit_usd6 = ? AND action = 'warn')
                 OR (limit_usd6 = ? AND action = 'block')
               )
            """,
            (
                LIFETIME_CLAUDE_MONTHLY_CAP_USD6,
                DEFAULT_CLAUDE_MONTHLY_CAP_USD6,
                TRIAL_CLAUDE_MONTHLY_CAP_USD6,
            ),
        )
        conn.commit()
