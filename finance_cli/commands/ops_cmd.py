"""Operational commands for billing and AI cost controls."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any, Mapping

from ..billing import (
    LIFETIME_CLAUDE_MONTHLY_CAP_USD6,
    TRIAL_CLAUDE_MONTHLY_CAP_USD6,
    effective_plan,
    seed_plan_caps,
)
from ..db import connect
from ..exceptions import TenantMismatchError
from ..storage_lease import LeaseQueuedError, optional_lease_scope
from ..user_provisioning import user_db_path

_MICRODOLLARS_PER_DOLLAR = Decimal("1000000")


@dataclass(frozen=True)
class OpsSettings:
    data_root: Path
    database_url: str
    stripe_price_lite: str = ""


def load_ops_settings() -> OpsSettings:
    repo_root = Path(__file__).resolve().parents[2]
    default_data_root = repo_root / "finance-web" / "data" / "users"
    raw_data_root = (
        os.getenv("FINANCE_WEB_DATA_ROOT")
        or os.getenv("FINANCE_GATEWAY_DATA_ROOT")
        or str(default_data_root)
    )
    return OpsSettings(
        data_root=Path(raw_data_root).expanduser().resolve(),
        database_url=str(os.getenv("DATABASE_URL") or "").strip(),
        stripe_price_lite=str(os.getenv("STRIPE_PRICE_LITE") or "").strip(),
    )


def usd_to_usd6(raw_value: Any) -> int:
    try:
        parsed = Decimal(str(raw_value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("amount-usd must be a valid decimal dollar amount") from exc
    if parsed < 0:
        raise ValueError("amount-usd must be >= 0")
    return int((parsed * _MICRODOLLARS_PER_DOLLAR).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def normalize_phase1_sentinels(conn: sqlite3.Connection, user: Mapping[str, Any]) -> list[str]:
    """Reset only Phase 1 system-owned sentinel rows for confirmed user tiers."""
    normalized: list[str] = []
    tier = str(user.get("tier") or "").strip().lower()
    lifetime_deal = bool(user.get("lifetime_deal"))

    if lifetime_deal:
        cursor = conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = NULL,
                   action = 'warn'
             WHERE provider = 'claude'
               AND period = 'monthly'
               AND limit_usd6 = ?
               AND action = 'block'
            """,
            (LIFETIME_CLAUDE_MONTHLY_CAP_USD6,),
        )
        if int(cursor.rowcount or 0) > 0:
            normalized.append("lifetime")

    if tier == "trial":
        cursor = conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = NULL,
                   action = 'warn'
             WHERE provider = 'claude'
               AND period = 'monthly'
               AND limit_usd6 = ?
               AND action = 'block'
            """,
            (TRIAL_CLAUDE_MONTHLY_CAP_USD6,),
        )
        if int(cursor.rowcount or 0) > 0:
            normalized.append("trial")

    return normalized


def reseed_user_plan_caps(conn: sqlite3.Connection, user: Mapping[str, Any], settings: OpsSettings) -> dict[str, Any]:
    normalized = normalize_phase1_sentinels(conn, user)
    plan = effective_plan(user, settings)
    seed_plan_caps(conn, plan)
    return {
        "user_id": str(user.get("id") or user.get("user_id") or ""),
        "plan_code": plan.code,
        "system_limit_usd6": plan.monthly_cap_usd6,
        "normalized": normalized,
    }


def _postgres_connect(database_url: str):
    if not database_url:
        raise ValueError("DATABASE_URL is required for ops plan-caps-reseed")
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise ValueError("psycopg2 is required for ops plan-caps-reseed") from exc
    return psycopg2.connect(database_url, cursor_factory=psycopg2.extras.RealDictCursor)


def _fetch_user(settings: OpsSettings, user_id: str) -> dict[str, Any]:
    with _postgres_connect(settings.database_url) as pg_conn:
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            SELECT id, tier, lifetime_deal, stripe_price_id
                 , storage_mode
              FROM users
             WHERE id = %s
            """,
            (user_id,),
        )
        row = cursor.fetchone()
    if not row:
        raise ValueError(f"User not found: {user_id}")
    return dict(row)


def _fetch_all_users(settings: OpsSettings) -> list[dict[str, Any]]:
    with _postgres_connect(settings.database_url) as pg_conn:
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            SELECT id, tier, lifetime_deal, stripe_price_id
                 , storage_mode
              FROM users
             WHERE deleted_at IS NULL
             ORDER BY id
            """
        )
        rows = cursor.fetchall()
    return [dict(row) for row in rows]


@contextlib.contextmanager
def _connect_user_db(settings: OpsSettings, user_id: str | int) -> Iterator[Any]:
    user_id_str = str(user_id)
    lease_context = (
        optional_lease_scope(
            user_id_str,
            operation="ops_user_db",
            metadata={"source": "ops_cmd"},
            heartbeat=True,
        )
        if settings.database_url
        else contextlib.nullcontext()
    )
    with lease_context:
        with connect(
            user_db_path(settings.data_root, user_id_str),
            expected_user_id=user_id_str,
            busy_timeout=5000,
        ) as conn:
            yield conn


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("ops", parents=[format_parent], help="Operational billing tools")
    ops_sub = parser.add_subparsers(dest="ops_command", required=True)

    p_credit = ops_sub.add_parser("credit-grant", parents=[format_parent], help="Grant user credit balance")
    p_credit.add_argument("--user", required=True, help="User id")
    p_credit.add_argument("--amount-usd", required=True, help="Dollar amount to grant")
    p_credit.add_argument("--reason", required=True, help="Audit reason")
    p_credit.set_defaults(func=handle_credit_grant, command_name="ops.credit-grant", uses_default_db=False)

    p_cap = ops_sub.add_parser("plan-cap-set", parents=[format_parent], help="Set a user-owned AI cost cap")
    p_cap.add_argument("--user", required=True, help="User id")
    p_cap.add_argument("--amount-usd", required=True, help="Dollar cap amount")
    p_cap.add_argument("--reason", required=True, help="Audit reason")
    p_cap.add_argument("--yes", action="store_true", help="Apply without interactive confirmation")
    p_cap.set_defaults(func=handle_plan_cap_set, command_name="ops.plan-cap-set", uses_default_db=False)

    p_reseed = ops_sub.add_parser("plan-caps-reseed", parents=[format_parent], help="Reseed system plan caps")
    group = p_reseed.add_mutually_exclusive_group(required=True)
    group.add_argument("--all-users", action="store_true", help="Reseed every active user")
    group.add_argument("--user", help="Reseed one user id")
    p_reseed.set_defaults(func=handle_plan_caps_reseed, command_name="ops.plan-caps-reseed", uses_default_db=False)


def handle_credit_grant(args, _conn) -> dict[str, Any]:
    settings = load_ops_settings()
    user_id = str(args.user)
    amount_usd6 = usd_to_usd6(args.amount_usd)
    if amount_usd6 <= 0:
        raise ValueError("amount-usd must be > 0")

    with _connect_user_db(settings, user_id) as user_conn:
        user_conn.execute("BEGIN IMMEDIATE")
        user_conn.execute(
            """
            INSERT INTO credit_ledger (source, amount_usd6, notes)
            VALUES ('adjustment', ?, ?)
            """,
            (amount_usd6, str(args.reason)),
        )
        user_conn.execute(
            """
            UPDATE credit_balance
               SET balance_usd6 = balance_usd6 + ?,
                   updated_at = datetime('now')
             WHERE id = 1
            """,
            (amount_usd6,),
        )
        balance = user_conn.execute("SELECT balance_usd6 FROM credit_balance WHERE id = 1").fetchone()
        user_conn.commit()

    balance_usd6 = int(balance["balance_usd6"]) if balance is not None else 0
    return {
        "data": {"user_id": user_id, "amount_usd6": amount_usd6, "balance_usd6": balance_usd6},
        "summary": {"amount_usd6": amount_usd6, "balance_usd6": balance_usd6},
        "cli_report": f"Granted ${amount_usd6 / 1_000_000:.2f} credits to user {user_id}",
    }


def _confirm_plan_cap(args, amount_usd6: int) -> None:
    if getattr(args, "yes", False):
        return
    if not sys.stdin.isatty():
        raise ValueError("plan-cap-set requires --yes when not running interactively")
    prompt = (
        f"Set user {args.user} claude monthly user cap to "
        f"${amount_usd6 / 1_000_000:.2f}? Type SET to continue: "
    )
    if input(prompt).strip() != "SET":
        raise ValueError("plan-cap-set aborted")


def handle_plan_cap_set(args, _conn) -> dict[str, Any]:
    settings = load_ops_settings()
    user_id = str(args.user)
    amount_usd6 = usd_to_usd6(args.amount_usd)
    _confirm_plan_cap(args, amount_usd6)

    with _connect_user_db(settings, user_id) as user_conn:
        user_conn.execute("BEGIN IMMEDIATE")
        user_conn.execute(
            """
            INSERT INTO cost_limits (provider, period, limit_usd6, action, is_active)
            VALUES ('claude', 'monthly', ?, 'warn', 1)
            ON CONFLICT(provider, period) DO UPDATE SET
                limit_usd6 = excluded.limit_usd6,
                is_active = 1
            """,
            (amount_usd6,),
        )
        row = user_conn.execute(
            """
            SELECT limit_usd6, system_limit_usd6, action
            FROM cost_limits
            WHERE provider = 'claude' AND period = 'monthly'
            """
        ).fetchone()
        user_conn.commit()

    return {
        "data": {
            "user_id": user_id,
            "limit_usd6": int(row["limit_usd6"]),
            "system_limit_usd6": row["system_limit_usd6"],
            "action": row["action"],
            "reason": str(args.reason),
        },
        "summary": {"limit_usd6": int(row["limit_usd6"])},
        "cli_report": f"Set user {user_id} monthly cap to ${amount_usd6 / 1_000_000:.2f}",
    }


def handle_plan_caps_reseed(args, _conn) -> dict[str, Any]:
    settings = load_ops_settings()
    users = _fetch_all_users(settings) if getattr(args, "all_users", False) else [_fetch_user(settings, str(args.user))]
    results: list[dict[str, Any]] = []

    for user in users:
        user_id = str(user.get("id"))
        db_path = user_db_path(settings.data_root, user_id)
        storage_mode = str(user.get("storage_mode") or "local").strip().lower()
        if storage_mode == "local" and not db_path.exists():
            results.append({"user_id": user_id, "status": "missing_db", "db_path": str(db_path)})
            continue
        try:
            with _connect_user_db(settings, user_id) as user_conn:
                user_conn.execute("BEGIN IMMEDIATE")
                result = reseed_user_plan_caps(user_conn, user, settings)
                user_conn.commit()
        except LeaseQueuedError as exc:
            results.append({"user_id": user_id, "status": "queued", "reason": str(exc)})
            continue
        except TenantMismatchError as exc:
            if getattr(exc, "reason", "") != "missing_file":
                raise
            results.append({"user_id": user_id, "status": "missing_db", "db_path": str(db_path)})
            continue
        result["status"] = "reseeded"
        results.append(result)

    reseeded = sum(1 for result in results if result.get("status") == "reseeded")
    return {
        "data": {"results": results},
        "summary": {"users": len(results), "reseeded": reseeded},
        "cli_report": f"Reseeded {reseeded}/{len(results)} user plan cap row(s)",
    }
