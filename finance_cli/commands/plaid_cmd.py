"""Plaid commands."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
import sqlite3
import webbrowser
from pathlib import Path
from typing import Any

from ..analytics import log_event
from ..cost_tracking import PLAID_ITEM_MONTHLY_USD6, check_cost_limit
from ..db import backup_database
from ..db import _connected_main_db_path
from ..error_capture import capture_error

log = logging.getLogger(__name__)
_MICRODOLLARS_PER_DOLLAR = 1_000_000


def _plaid_client_attr(name: str):
    from .. import plaid_client

    return getattr(plaid_client, name)


def _plaid_unavailable_error() -> type[Exception]:
    return _plaid_client_attr("PlaidUnavailableError")


def backfill_item_products(*args, **kwargs):
    return _plaid_client_attr("backfill_item_products")(*args, **kwargs)


def complete_link_session(*args, **kwargs):
    return _plaid_client_attr("complete_link_session")(*args, **kwargs)


def config_status(*args, **kwargs):
    return _plaid_client_attr("config_status")(*args, **kwargs)


def create_hosted_link_session(*args, **kwargs):
    return _plaid_client_attr("create_hosted_link_session")(*args, **kwargs)


def fetch_liabilities(*args, **kwargs):
    return _plaid_client_attr("fetch_liabilities")(*args, **kwargs)


def list_plaid_items(*args, **kwargs):
    return _plaid_client_attr("list_plaid_items")(*args, **kwargs)


def refresh_balances(*args, **kwargs):
    return _plaid_client_attr("refresh_balances")(*args, **kwargs)


def run_sync(*args, **kwargs):
    return _plaid_client_attr("run_sync")(*args, **kwargs)


def sanitize_client_user_id(*args, **kwargs):
    return _plaid_client_attr("sanitize_client_user_id")(*args, **kwargs)


def unlink_item(*args, **kwargs):
    return _plaid_client_attr("unlink_item")(*args, **kwargs)


def _plaid_cost_db_path(conn):
    return _plaid_client_attr("resolve_plaid_cost_db_path")(conn)


def _plaid_cost_path_unavailable_reason() -> str:
    return _plaid_client_attr("PLAID_COST_DB_PATH_UNAVAILABLE_REASON")


def _format_error_lines(errors: list[dict]) -> str:
    """Format Plaid item errors as newline-separated detail lines."""
    lines = []
    for err in errors:
        name = err.get("institution_name") or err.get("plaid_item_id", "unknown")
        message = err.get("error", "unknown error")
        # Truncate long Plaid error messages for CLI readability.
        if len(message) > 120:
            message = message[:117] + "..."
        lines.append(f"  FAILED: {name} — {message}")
    return "\n".join(lines)


def _sync_cli_report(result: dict) -> str:
    mode = " backfill=true" if result.get("backfill") else ""
    lines = [
        (
        f"items_synced={result['items_synced']} items_skipped={result.get('items_skipped', 0)} (cooldown) "
        f"items_failed={result['items_failed']} "
        f"added={result['added']} modified={result['modified']} removed={result['removed']} "
        f"elapsed={int(result.get('total_elapsed_ms', 0))}ms{mode}"
        )
    ]
    for item in result.get("items") or []:
        if "investment_added" in item or "investment_modified" in item:
            lines.append(
                f"  Investment: +{int(item.get('investment_added', 0))} ~{int(item.get('investment_modified', 0))}"
            )
        investment_error = str(item.get("investment_error") or "").strip()
        if investment_error:
            lines.append(f"  Investment sync failed: {investment_error}")
    errors = result.get("errors") or []
    if errors:
        lines.append(_format_error_lines(errors))
    return "\n".join(lines)


def _balance_cli_report(result: dict) -> str:
    line = (
        f"items_refreshed={result['items_refreshed']} items_skipped={result.get('items_skipped', 0)} (cooldown) "
        f"items_failed={result['items_failed']} "
        f"accounts_updated={result['accounts_updated']} snapshots_updated={result['snapshots_updated']}"
    )
    errors = result.get("errors") or []
    if errors:
        line += "\n" + _format_error_lines(errors)
    return line


def _liabilities_cli_report(result: dict) -> str:
    line = (
        f"items_synced={result['items_synced']} items_skipped={result.get('items_skipped', 0)} (cooldown) "
        f"items_failed={result['items_failed']} "
        f"liabilities_upserted={result['liabilities_upserted']} "
        f"liabilities_deactivated={result['liabilities_deactivated']}"
    )
    errors = result.get("errors") or []
    if errors:
        line += "\n" + _format_error_lines(errors)
    return line


def _truncate_error(value: Any, max_len: int = 80) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


_TOKEN_REQUIRED_STATUSES = {"active", "pending", "error"}


def _token_missing(item: dict[str, Any]) -> bool:
    item_status = str(item.get("status") or "").lower()
    return item_status in _TOKEN_REQUIRED_STATUSES and not bool(item.get("has_token_ref"))


def _status_cli_report(
    items: list[dict[str, Any]],
    *,
    configured: bool,
    has_sdk: bool,
    webhook_url_configured: bool | None = None,
) -> str:
    active_count = sum(1 for item in items if str(item.get("status") or "") == "active")
    error_count = sum(1 for item in items if str(item.get("status") or "") == "error")
    reauth_count = sum(1 for item in items if bool(item.get("needs_reauth")))
    token_missing_count = sum(1 for item in items if _token_missing(item))
    if webhook_url_configured is None:
        webhook_url_configured = bool(str(os.getenv("PLAID_WEBHOOK_URL") or "").strip())
    lines = [
        (
            f"items={len(items)} active={active_count} errors={error_count} "
            f"configured={configured} sdk={has_sdk} webhook_url={webhook_url_configured} "
            f"reauth={reauth_count} token_missing={token_missing_count}"
        )
    ]
    for item in items:
        institution_name = str(item.get("institution_name") or item.get("plaid_item_id") or "unknown")
        item_status = str(item.get("status") or "unknown")
        last_sync = str(item.get("last_sync_at") or "never")
        last_webhook = str(item.get("last_webhook_at") or "never")
        needs_reauth = bool(item.get("needs_reauth"))
        token_missing = _token_missing(item)
        item_line = f"  {institution_name}: status={item_status} last_sync={last_sync}"
        if item_status == "error":
            error_message = _truncate_error(item.get("error_code"))
            item_line += f" error={error_message}"
        item_line += f" last_webhook={last_webhook} needs_reauth={'yes' if needs_reauth else 'no'}"
        if token_missing:
            item_line += " token=missing"
        lines.append(item_line)

        plaid_item_id = str(item.get("plaid_item_id") or "").strip()
        if token_missing and plaid_item_id:
            lines.append(
                f"    -> Fix: finance plaid unlink --item {plaid_item_id}; then reconnect with finance setup connect"
            )
        elif (item_status == "error" or needs_reauth) and plaid_item_id:
            lines.append(
                f"    -> Fix: finance plaid link --update --item {plaid_item_id} --wait --user-id default"
            )
    return "\n".join(lines)


def _sanitize_status_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized_items: list[dict[str, Any]] = []
    for item in items:
        sanitized = {
            key: value
            for key, value in item.items()
            if key not in {"access_token_ref", "sync_cursor"}
        }
        sanitized.setdefault("has_token_ref", bool(item.get("access_token_ref")))
        sanitized["token_missing"] = _token_missing(sanitized)
        sanitized_items.append(sanitized)
    return sanitized_items


def _format_usd6(value: int) -> str:
    return f"{max(int(value or 0), 0) / _MICRODOLLARS_PER_DOLLAR:.2f}"


def _pct_used(spent_usd6: int, limit_usd6: int) -> float:
    if limit_usd6 <= 0:
        return 0.0
    return round((max(int(spent_usd6), 0) / int(limit_usd6)) * 100.0, 1)


def _plaid_usage_period_payload(
    conn: sqlite3.Connection,
    *,
    period: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if period == "day":
        start_sql = "datetime('now', 'start of day')"
        start = conn.execute(
            "SELECT strftime('%Y-%m-%dT00:00:00Z', 'now', 'start of day') AS start"
        ).fetchone()["start"]
        limit_period = "daily"
    else:
        start_sql = "datetime('now', 'start of month')"
        start = conn.execute(
            "SELECT strftime('%Y-%m-01T00:00:00Z', 'now') AS start"
        ).fetchone()["start"]
        limit_period = "monthly"

    rows = conn.execute(
        f"""
        SELECT operation, COUNT(*) AS calls, COALESCE(SUM(cost_usd6), 0) AS spent_usd6
        FROM cost_ledger
        WHERE provider = 'plaid'
          AND created_at >= {start_sql}
        GROUP BY operation
        ORDER BY spent_usd6 DESC, calls DESC
        """
    ).fetchall()
    by_operation = [
        {
            "operation": str(row["operation"]),
            "calls": int(row["calls"] or 0),
            "cost_usd6": int(row["spent_usd6"] or 0),
        }
        for row in rows
    ]
    total_calls = sum(int(row["calls"]) for row in by_operation)
    total_cost_usd6 = sum(int(row["cost_usd6"]) for row in by_operation)
    limit_row = conn.execute(
        """
        SELECT limit_usd6
        FROM cost_limits
        WHERE provider = 'plaid'
          AND period = ?
          AND is_active = 1
        """,
        (limit_period,),
    ).fetchone()
    limit_usd6 = int(limit_row["limit_usd6"]) if limit_row is not None else 0
    data = {
        "period": period,
        "start": str(start),
        "totals": {
            "calls": total_calls,
            "cost_usd6": total_cost_usd6,
        },
        "by_operation": by_operation,
    }
    summary = {
        "calls": total_calls,
        "cost_usd": _format_usd6(total_cost_usd6),
        "limit_usd": _format_usd6(limit_usd6),
        "pct_used": _pct_used(total_cost_usd6, limit_usd6),
    }
    return data, summary


def _plaid_usage_section(title: str, total_label: str, data: dict[str, Any], summary: dict[str, Any]) -> list[str]:
    lines = [f"  {title:<28}calls    est. cost"]
    for row in data["by_operation"]:
        lines.append(
            f"  {row['operation']:<28}{int(row['calls']):>5}      ${_format_usd6(int(row['cost_usd6']))}"
        )
    lines.append("  ---")
    limit_label = "daily" if data["period"] == "day" else "monthly"
    lines.append(
        f"  {total_label:<28}{int(data['totals']['calls']):>5}      ${_format_usd6(int(data['totals']['cost_usd6']))}"
        f"   ({limit_label} limit ${summary['limit_usd']}, {float(summary['pct_used']):.1f}% used)"
    )
    return lines


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("plaid", parents=[format_parent], help="Plaid integration")
    plaid_sub = parser.add_subparsers(dest="plaid_command", required=True)

    p_link = plaid_sub.add_parser("link", parents=[format_parent], help="Create hosted Plaid Link session")
    p_link.add_argument("--user-id")
    p_link.add_argument("--update", action="store_true")
    p_link.add_argument("--item", help="Plaid item id for update-mode link")
    p_link.add_argument(
        "--product",
        action="append",
        choices=["transactions", "balance", "liabilities", "investments"],
    )
    p_link.add_argument("--include-balance", action="store_true", help="Balance is implicit; does not change Link products")
    p_link.add_argument("--include-liabilities", action="store_true")
    p_link.add_argument("--wait", action="store_true", help="Poll completion and exchange token")
    p_link.add_argument(
        "--allow-duplicate",
        action="store_true",
        help="Allow linking a second active item for the same institution",
    )
    p_link.add_argument("--timeout", type=int, default=300)
    p_link.add_argument("--poll-seconds", type=int, default=10)
    p_link.add_argument("--open-browser", action="store_true")
    p_link.set_defaults(func=handle_link, command_name="plaid.link")

    p_sync = plaid_sub.add_parser("sync", parents=[format_parent], help="Sync transactions")
    p_sync.add_argument("--days", type=int)
    p_sync.add_argument("--item", help="Sync only a specific plaid item")
    p_sync.add_argument("--force", "-f", action="store_true", help="Bypass cooldown and force API calls")
    p_sync.add_argument("--backfill", action="store_true", help="Request historical transactions by ignoring the stored cursor")
    p_sync.set_defaults(func=handle_sync, command_name="plaid.sync")

    p_refresh = plaid_sub.add_parser("balance-refresh", parents=[format_parent], help="Refresh account balances")
    p_refresh.add_argument("--item", help="Refresh only a specific plaid item")
    p_refresh.add_argument("--force", "-f", action="store_true", help="Bypass cooldown and force API calls")
    p_refresh.set_defaults(func=handle_balance_refresh, command_name="plaid.balance_refresh")

    p_liab = plaid_sub.add_parser("liabilities-sync", parents=[format_parent], help="Fetch liabilities")
    p_liab.add_argument("--item", help="Sync only a specific plaid item")
    p_liab.add_argument("--force", "-f", action="store_true", help="Bypass cooldown and force API calls")
    p_liab.set_defaults(func=handle_liabilities_sync, command_name="plaid.liabilities_sync")

    p_usage = plaid_sub.add_parser("usage", parents=[format_parent], help="Show Plaid API usage by endpoint")
    usage_period_group = p_usage.add_mutually_exclusive_group()
    usage_period_group.add_argument("--day", action="store_true", help="Show today only")
    usage_period_group.add_argument("--month", action="store_true", help="Show this month only")
    p_usage.set_defaults(func=handle_usage, command_name="plaid.usage")

    p_status = plaid_sub.add_parser("status", parents=[format_parent], help="Plaid item status")
    p_status.set_defaults(func=handle_status, command_name="plaid.status")

    p_unlink = plaid_sub.add_parser("unlink", parents=[format_parent], help="Unlink a plaid item")
    p_unlink.add_argument("--item", required=True)
    p_unlink.set_defaults(func=handle_unlink, command_name="plaid.unlink")

    p_products_backfill = plaid_sub.add_parser(
        "products-backfill",
        parents=[format_parent],
        help="Refresh consented products metadata from Plaid item data",
    )
    p_products_backfill.add_argument("--item", help="Refresh only a specific plaid item")
    p_products_backfill.set_defaults(func=handle_products_backfill, command_name="plaid.products_backfill")


def handle_link(args, conn) -> dict[str, Any]:
    """Create a hosted Link session and optionally complete token exchange."""
    status = config_status()
    ready = status.configured and status.has_sdk

    user_id = str(args.user_id or "").strip() or None
    if args.wait and not user_id:
        raise ValueError("--user-id is required when using --wait")
    client_user_id = sanitize_client_user_id(user_id or "finance-cli-user")

    update_item = args.item if args.update else None
    if args.update and not update_item:
        raise ValueError("--update requires --item <item_id>")

    if not ready:
        data = {
            "ready": ready,
            "has_sdk": status.has_sdk,
            "configured": status.configured,
            "missing_env": status.missing_env,
            "plaid_env": status.env,
            "message": "Plaid not fully configured; set env vars and install plaid-python.",
        }
        return {
            "data": data,
            "summary": {"ready": ready},
            "cli_report": data["message"],
        }

    if not args.update:
        db_path = _plaid_cost_db_path(conn)
        if db_path is None:
            reason = _plaid_cost_path_unavailable_reason()
            message = f"Plaid link blocked by cost guardrail: {reason}"
            log.warning("Plaid link blocked reason=%s", reason)
            return {
                "data": {"blocked": True, "reason": reason},
                "summary": {"ready": True, "blocked": 1},
                "cli_report": message,
            }

        try:
            allowed, reason = check_cost_limit(
                db_path,
                "plaid",
                projected_cost_usd6=PLAID_ITEM_MONTHLY_USD6,
                source="cli",
            )
        except Exception as exc:
            capture_error(
                exc,
                source="cli",
                endpoint="plaid.link",
                db_path=db_path,
            )
            log.warning("Plaid cost guardrail check failed: %s", exc)
            allowed, reason = True, None

        if not allowed:
            message = f"Plaid link blocked by cost guardrail: {reason or 'cost limit reached'}"
            log.warning("Plaid link blocked reason=%s", reason)
            return {
                "data": {"blocked": True, "reason": reason},
                "summary": {"ready": True, "blocked": 1},
                "cli_report": message,
            }

    try:
        session = create_hosted_link_session(
            conn,
            user_id=client_user_id,
            update_item_id=update_item,
            include_balance=args.include_balance,
            include_liabilities=args.include_liabilities,
            requested_products=args.product,
        )
    except _plaid_unavailable_error() as exc:
        raise ValueError(str(exc)) from exc

    if args.open_browser and session.get("hosted_link_url"):
        webbrowser.open(str(session["hosted_link_url"]))

    data: dict[str, Any] = {
        "ready": True,
        "session": session,
    }
    if user_id and user_id != client_user_id:
        data["user_id_sanitized"] = True
        data["client_user_id"] = client_user_id

    if args.wait:
        linked_item = complete_link_session(
            conn,
            user_id=client_user_id,
            link_token=str(session["link_token"]),
            timeout_seconds=args.timeout,
            poll_seconds=args.poll_seconds,
            requested_products=session.get("requested_products"),
            allow_duplicate_institution=bool(args.allow_duplicate),
        )
        data["linked_item"] = linked_item
        try:
            data["backup_path"] = str(backup_database(conn=conn))
        except Exception as exc:
            data["backup_warning"] = f"Post-link backup failed: {exc}"

    message = "Plaid hosted link session created"
    if args.wait:
        message = "Plaid link completed and item stored"

    return {
        "data": data,
        "summary": {"ready": True, "waited": bool(args.wait)},
        "cli_report": message,
    }


def handle_plaid_exchange(args, conn) -> dict[str, Any]:
    """Complete a previously created hosted Link session."""
    linked_item = complete_link_session(
        conn,
        user_id=sanitize_client_user_id("finance-cli-user"),
        link_token=str(args.link_token),
        requested_products=args.requested_products,
        timeout_seconds=int(getattr(args, "timeout", 300) or 300),
        poll_seconds=int(getattr(args, "poll_seconds", 10) or 10),
        allow_duplicate_institution=bool(
            getattr(args, "allow_duplicate_institution", False)
            or getattr(args, "allow_duplicate", False)
        ),
    )

    return {
        "data": linked_item,
        "summary": {
            "plaid_item_id": linked_item.get("plaid_item_id"),
            "status": linked_item.get("status"),
        },
        "cli_report": "Plaid link completed and item stored",
    }


def handle_sync(args, conn, rules_path: Path | None = None) -> dict[str, Any]:
    """Run transactions sync and return summarized mutation counts."""
    db_path = _connected_main_db_path(conn)
    try:
        result = run_sync(
            conn,
            days=args.days,
            item_id=args.item,
            force_refresh=args.force,
            backfill=bool(getattr(args, "backfill", False)),
            rules_path=rules_path,
        )
    except _plaid_unavailable_error() as exc:
        log_event(db_path, "import.plaid_synced", outcome="failed")
        raise ValueError(str(exc)) from exc

    log_event(
        db_path,
        "import.plaid_synced",
        properties={
            "txn_count": int(result.get("added", 0)) + int(result.get("modified", 0)) + int(result.get("removed", 0)),
            "account_count": int(result.get("items_synced", 0)),
        },
    )

    return {
        "data": result,
        "summary": {
            "items_requested": result["items_requested"],
            "items_synced": result["items_synced"],
            "items_skipped": result.get("items_skipped", 0),
            "items_failed": result["items_failed"],
            "added": result["added"],
            "modified": result["modified"],
            "removed": result["removed"],
            "backfill": bool(getattr(args, "backfill", False)),
            "total_elapsed_ms": int(result.get("total_elapsed_ms", 0)),
        },
        "cli_report": _sync_cli_report(result),
    }


def handle_status(args, conn) -> dict[str, Any]:
    """Return Plaid client readiness plus locally tracked item registry."""
    status = config_status()
    items = _sanitize_status_items(list_plaid_items(conn))
    active_count = sum(1 for item in items if str(item.get("status") or "") == "active")
    error_count = sum(1 for item in items if str(item.get("status") or "") == "error")
    reauth_count = sum(1 for item in items if bool(item.get("needs_reauth")))
    token_missing_count = sum(1 for item in items if _token_missing(item))
    webhook_url_configured = bool(str(os.getenv("PLAID_WEBHOOK_URL") or "").strip())

    data = {
        "configured": status.configured,
        "has_sdk": status.has_sdk,
        "missing_env": status.missing_env,
        "plaid_env": status.env,
        "webhook_url_configured": webhook_url_configured,
        "items": items,
        "active_count": active_count,
        "error_count": error_count,
        "reauth_count": reauth_count,
        "token_missing_count": token_missing_count,
    }
    return {
        "data": data,
        "summary": {
            "total_items": len(items),
            "active_count": active_count,
            "error_count": error_count,
            "reauth_count": reauth_count,
            "token_missing_count": token_missing_count,
            "webhook_url_configured": webhook_url_configured,
        },
        "cli_report": _status_cli_report(
            items,
            configured=status.configured,
            has_sdk=status.has_sdk,
            webhook_url_configured=webhook_url_configured,
        ),
    }


def handle_balance_refresh(args, conn) -> dict[str, Any]:
    """Run real-time balance refresh across one or more Plaid items."""
    try:
        result = refresh_balances(conn, item_id=args.item, force_refresh=args.force)
    except _plaid_unavailable_error() as exc:
        raise ValueError(str(exc)) from exc

    return {
        "data": result,
        "summary": {
            "items_requested": result["items_requested"],
            "items_refreshed": result["items_refreshed"],
            "items_skipped": result.get("items_skipped", 0),
            "items_failed": result["items_failed"],
            "accounts_updated": result["accounts_updated"],
            "snapshots_updated": result["snapshots_updated"],
        },
        "cli_report": _balance_cli_report(result),
    }


def handle_liabilities_sync(args, conn) -> dict[str, Any]:
    """Sync liabilities for items that have liabilities product consent."""
    try:
        result = fetch_liabilities(conn, item_id=args.item, force_refresh=args.force)
    except _plaid_unavailable_error() as exc:
        raise ValueError(str(exc)) from exc

    return {
        "data": result,
        "summary": {
            "items_requested": result["items_requested"],
            "items_synced": result["items_synced"],
            "items_skipped": result.get("items_skipped", 0),
            "items_failed": result["items_failed"],
            "liabilities_upserted": result["liabilities_upserted"],
            "liabilities_deactivated": result["liabilities_deactivated"],
        },
        "cli_report": _liabilities_cli_report(result),
    }


def handle_usage(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Return Plaid API usage grouped by endpoint for day/month windows."""
    conn.execute("PRAGMA query_only = 1")

    if bool(args.day) and bool(args.month):
        raise ValueError("--day and --month are mutually exclusive")

    day_data, day_summary = _plaid_usage_period_payload(conn, period="day")
    month_data, month_summary = _plaid_usage_period_payload(conn, period="month")

    if args.day:
        data: dict[str, Any] = day_data
        summary: dict[str, Any] = day_summary
    elif args.month:
        data = month_data
        summary = month_summary
    else:
        data = {"day": day_data, "month": month_data}
        summary = {"day": day_summary, "month": month_summary}

    header_date = datetime.now(timezone.utc).date().isoformat()
    month_label = datetime.now(timezone.utc).strftime("%b %Y")
    cli_lines = [f"Plaid usage — {header_date}", ""]
    if args.day:
        cli_lines.extend(_plaid_usage_section("Today", "day total", day_data, day_summary))
    elif args.month:
        cli_lines.extend(
            _plaid_usage_section(f"This month ({month_label})", "month total", month_data, month_summary)
        )
    else:
        cli_lines.extend(_plaid_usage_section("Today", "day total", day_data, day_summary))
        cli_lines.append("")
        cli_lines.extend(
            _plaid_usage_section(f"This month ({month_label})", "month total", month_data, month_summary)
        )

    return {
        "data": data,
        "summary": summary,
        "cli_report": "\n".join(cli_lines),
    }


def handle_unlink(args, conn) -> dict[str, Any]:
    """Disconnect a local Plaid item and best-effort remove remote token/item."""
    row = conn.execute("SELECT 1 FROM plaid_items WHERE plaid_item_id = ?", (args.item,)).fetchone()
    if not row:
        raise ValueError(f"Plaid item {args.item} not found")

    backup_path = str(backup_database(conn=conn))
    ok = unlink_item(conn, args.item)
    if not ok:
        raise ValueError(f"Plaid item {args.item} not found")

    return {
        "data": {"item_id": args.item, "status": "disconnected", "backup_path": backup_path},
        "summary": {"total_items": 1},
        "cli_report": f"Unlinked item {args.item}",
    }


def handle_products_backfill(args, conn) -> dict[str, Any]:
    """Backfill local consented-products metadata from Plaid item data."""
    try:
        result = backfill_item_products(conn, item_id=args.item)
    except _plaid_unavailable_error() as exc:
        raise ValueError(str(exc)) from exc

    return {
        "data": result,
        "summary": {
            "items_requested": result["items_requested"],
            "items_updated": result["items_updated"],
            "items_failed": result["items_failed"],
        },
        "cli_report": (
            f"items_updated={result['items_updated']} items_failed={result['items_failed']}"
        ),
    }
