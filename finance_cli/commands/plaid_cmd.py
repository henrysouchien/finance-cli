"""Plaid commands."""

from __future__ import annotations

import webbrowser
from typing import Any

from ..db import backup_database
from ..plaid_client import (
    PlaidUnavailableError,
    backfill_item_products,
    complete_link_session,
    config_status,
    create_hosted_link_session,
    fetch_liabilities,
    list_plaid_items,
    refresh_balances,
    run_sync,
    sanitize_client_user_id,
    unlink_item,
)


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
    lines = [
        (
        f"items_synced={result['items_synced']} items_skipped={result.get('items_skipped', 0)} (cooldown) "
        f"items_failed={result['items_failed']} "
        f"added={result['added']} modified={result['modified']} removed={result['removed']} "
        f"elapsed={int(result.get('total_elapsed_ms', 0))}ms"
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


def _status_cli_report(items: list[dict[str, Any]], *, configured: bool, has_sdk: bool) -> str:
    active_count = sum(1 for item in items if str(item.get("status") or "") == "active")
    error_count = sum(1 for item in items if str(item.get("status") or "") == "error")
    lines = [
        f"items={len(items)} active={active_count} errors={error_count} configured={configured} sdk={has_sdk}"
    ]
    for item in items:
        institution_name = str(item.get("institution_name") or item.get("plaid_item_id") or "unknown")
        item_status = str(item.get("status") or "unknown")
        last_sync = str(item.get("last_sync_at") or "never")
        item_line = f"  {institution_name}: status={item_status} last_sync={last_sync}"
        if item_status == "error":
            error_message = _truncate_error(item.get("error_code"))
            item_line += f" error={error_message}"
        lines.append(item_line)

        plaid_item_id = str(item.get("plaid_item_id") or "").strip()
        if item_status == "error" and plaid_item_id:
            lines.append(
                f"    -> Fix: finance plaid link --update --item {plaid_item_id} --wait --user-id default"
            )
    return "\n".join(lines)


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
    p_sync.set_defaults(func=handle_sync, command_name="plaid.sync")

    p_refresh = plaid_sub.add_parser("balance-refresh", parents=[format_parent], help="Refresh account balances")
    p_refresh.add_argument("--item", help="Refresh only a specific plaid item")
    p_refresh.add_argument("--force", "-f", action="store_true", help="Bypass cooldown and force API calls")
    p_refresh.set_defaults(func=handle_balance_refresh, command_name="plaid.balance_refresh")

    p_liab = plaid_sub.add_parser("liabilities-sync", parents=[format_parent], help="Fetch liabilities")
    p_liab.add_argument("--item", help="Sync only a specific plaid item")
    p_liab.add_argument("--force", "-f", action="store_true", help="Bypass cooldown and force API calls")
    p_liab.set_defaults(func=handle_liabilities_sync, command_name="plaid.liabilities_sync")

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

    try:
        session = create_hosted_link_session(
            conn,
            user_id=client_user_id,
            update_item_id=update_item,
            include_balance=args.include_balance,
            include_liabilities=args.include_liabilities,
            requested_products=args.product,
        )
    except PlaidUnavailableError as exc:
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


def handle_sync(args, conn) -> dict[str, Any]:
    """Run transactions sync and return summarized mutation counts."""
    try:
        result = run_sync(conn, days=args.days, item_id=args.item, force_refresh=args.force)
    except PlaidUnavailableError as exc:
        raise ValueError(str(exc)) from exc

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
            "total_elapsed_ms": int(result.get("total_elapsed_ms", 0)),
        },
        "cli_report": _sync_cli_report(result),
    }


def handle_status(args, conn) -> dict[str, Any]:
    """Return Plaid client readiness plus locally tracked item registry."""
    status = config_status()
    items = list_plaid_items(conn)
    active_count = sum(1 for item in items if str(item.get("status") or "") == "active")
    error_count = sum(1 for item in items if str(item.get("status") or "") == "error")

    data = {
        "configured": status.configured,
        "has_sdk": status.has_sdk,
        "missing_env": status.missing_env,
        "plaid_env": status.env,
        "items": items,
        "active_count": active_count,
        "error_count": error_count,
    }
    return {
        "data": data,
        "summary": {
            "total_items": len(items),
            "active_count": active_count,
            "error_count": error_count,
        },
        "cli_report": _status_cli_report(items, configured=status.configured, has_sdk=status.has_sdk),
    }


def handle_balance_refresh(args, conn) -> dict[str, Any]:
    """Run real-time balance refresh across one or more Plaid items."""
    try:
        result = refresh_balances(conn, item_id=args.item, force_refresh=args.force)
    except PlaidUnavailableError as exc:
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
    except PlaidUnavailableError as exc:
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
    except PlaidUnavailableError as exc:
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
