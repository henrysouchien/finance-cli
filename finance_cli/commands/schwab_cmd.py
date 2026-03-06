"""Schwab brokerage commands."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..models import cents_to_dollars
from ..schwab_client import check_token_health, config_status, sync_schwab_balances


def _sync_cli_report(result: dict[str, Any]) -> str:
    lines = [
        (
            f"accounts_synced={result['accounts_synced']} "
            f"snapshots_upserted={result['snapshots_upserted']} "
            f"accounts_failed={result['accounts_failed']}"
        )
    ]
    for account in result.get("accounts", []):
        masked = str(account.get("account_masked") or "****")
        balance_cents = int(account.get("balance_current_cents") or 0)
        lines.append(f"  {masked}: ${cents_to_dollars(balance_cents):,.2f}")
    for err in result.get("errors", []):
        masked = str(err.get("account") or "****")
        message = str(err.get("error") or "unknown")
        lines.append(f"  FAILED: {masked} - {message}")
    return "\n".join(lines)


def _status_cli_report(data: dict[str, Any]) -> str:
    refresh_days_remaining = data["token_health"].get("refresh_token_days_remaining")
    if refresh_days_remaining is None:
        refresh_text = "unknown"
    else:
        refresh_text = f"{float(refresh_days_remaining):.2f}"

    lines = [
        (
            f"configured={data['configured']} sdk={data['has_sdk']} "
            f"token_exists={data['token_exists']} refresh_days_remaining={refresh_text}"
        )
    ]
    stale_accounts = [a for a in data["accounts"] if bool(a.get("is_stale"))]
    if stale_accounts:
        lines.append(f"WARNING: stale_accounts={len(stale_accounts)} (>7 days since update)")
    for warning in data["token_health"].get("warnings", []):
        lines.append(f"WARNING: {warning}")
    return "\n".join(lines)


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("schwab", parents=[format_parent], help="Schwab brokerage integration")
    schwab_sub = parser.add_subparsers(dest="schwab_command", required=True)

    p_sync = schwab_sub.add_parser("sync", parents=[format_parent], help="Sync Schwab balances")
    p_sync.set_defaults(func=handle_sync, command_name="schwab.sync")

    p_status = schwab_sub.add_parser("status", parents=[format_parent], help="Show Schwab config and account status")
    p_status.set_defaults(func=handle_status, command_name="schwab.status")


def handle_sync(args, conn: sqlite3.Connection) -> dict[str, Any]:
    del args
    result = sync_schwab_balances(conn)
    return {
        "data": result,
        "summary": {
            "accounts_requested": result["accounts_requested"],
            "accounts_synced": result["accounts_synced"],
            "snapshots_upserted": result["snapshots_upserted"],
            "accounts_failed": result["accounts_failed"],
            "total_value_cents": result["total_value_cents"],
        },
        "cli_report": _sync_cli_report(result),
    }


def handle_status(args, conn: sqlite3.Connection) -> dict[str, Any]:
    del args
    status = config_status()
    token_health = check_token_health()
    accounts = conn.execute(
        """
        SELECT
            id,
            institution_name,
            account_name,
            account_type,
            source,
            balance_current_cents,
            iso_currency_code,
            balance_updated_at,
            is_active,
            CASE
                WHEN balance_updated_at IS NULL THEN 1
                WHEN julianday('now') - julianday(balance_updated_at) > 7 THEN 1
                ELSE 0
            END AS is_stale
          FROM accounts
         WHERE source = 'schwab'
         ORDER BY account_name
        """
    ).fetchall()

    data = {
        "configured": status.configured,
        "has_sdk": status.has_sdk,
        "missing_env": status.missing_env,
        "token_path": status.token_path,
        "token_exists": status.token_exists,
        "token_health": token_health,
        "accounts": [dict(row) for row in accounts],
    }
    stale_count = sum(1 for row in accounts if bool(row["is_stale"]))
    return {
        "data": data,
        "summary": {
            "total_accounts": len(accounts),
            "stale_accounts": stale_count,
            "configured": status.configured,
        },
        "cli_report": _status_cli_report(data),
    }

