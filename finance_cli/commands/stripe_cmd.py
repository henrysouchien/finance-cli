"""Stripe commands."""

from __future__ import annotations

import re
import sqlite3
from datetime import date
from typing import Any

from ..models import cents_to_dollars
from ..stripe_client import (
    StripeSyncError,
    StripeUnavailableError,
    balance_status,
    config_status,
    link_connection,
    run_sync,
    unlink_connection,
)

_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")
_QUARTER_RE = re.compile(r"^(\d{4})-Q([1-4])$")
_YEAR_RE = re.compile(r"^\d{4}$")


def _sync_cli_report(result: dict[str, Any]) -> str:
    if result.get("skipped_cooldown"):
        return f"Skipped by cooldown (last_sync_at={result.get('last_sync_at') or 'unknown'})"

    return (
        f"charges_added={result['charges_added']} "
        f"fees_added={result['fees_added']} "
        f"refunds_added={result['refunds_added']} "
        f"adjustments_added={result['adjustments_added']} "
        f"payouts_matched={result['payouts_matched']} "
        f"payouts_ambiguous={result['payouts_ambiguous']} "
        f"payouts_unmatched={result['payouts_unmatched']} "
        f"skipped_existing={result['skipped_existing']} "
        f"skipped_non_usd={result['skipped_non_usd']} "
        f"skipped_unknown_type={result['skipped_unknown_type']}"
    )


def _status_cli_report(data: dict[str, Any]) -> str:
    line = (
        f"configured={data['configured']} "
        f"sdk={data['has_sdk']} "
        f"connections={data['connection_count']} "
        f"stripe_txns={data['transaction_count']}"
    )
    connection_status = str(data.get("connection_status") or "not_linked")
    line += f" status={connection_status}"
    if data.get("balance"):
        available = int(data["balance"].get("available_cents") or 0)
        pending = int(data["balance"].get("pending_cents") or 0)
        line += f" available=${cents_to_dollars(available):,.2f} pending=${cents_to_dollars(pending):,.2f}"
    return line


def _quarter_bounds(quarter: str) -> tuple[str, str]:
    match = _QUARTER_RE.match(quarter)
    if not match:
        raise ValueError("Quarter must be in YYYY-QN format (e.g., 2026-Q1)")
    year = int(match.group(1))
    quarter_num = int(match.group(2))
    start_month = ((quarter_num - 1) * 3) + 1
    start = date(year, start_month, 1)
    if start_month == 10:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, start_month + 3, 1)
    return start.isoformat(), end.isoformat()


def _month_bounds(month: str) -> tuple[str, str]:
    match = _MONTH_RE.match(month)
    if not match:
        raise ValueError("Month must be in YYYY-MM format")
    year = int(match.group(1))
    month_num = int(match.group(2))
    if month_num < 1 or month_num > 12:
        raise ValueError("Month must be between 01 and 12")
    start = date(year, month_num, 1)
    if month_num == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month_num + 1, 1)
    return start.isoformat(), end.isoformat()


def _year_bounds(year: str) -> tuple[str, str]:
    if not _YEAR_RE.match(year):
        raise ValueError("Year must be in YYYY format")
    start_year = int(year)
    return date(start_year, 1, 1).isoformat(), date(start_year + 1, 1, 1).isoformat()


def _resolve_period(args) -> tuple[str | None, str | None, str]:
    period_args = [
        bool(getattr(args, "month", None)),
        bool(getattr(args, "quarter", None)),
        bool(getattr(args, "year", None)),
    ]
    if sum(period_args) > 1:
        raise ValueError("Use only one of --month, --quarter, or --year")

    if args.month:
        start, end = _month_bounds(str(args.month))
        return start, end, str(args.month)
    if args.quarter:
        start, end = _quarter_bounds(str(args.quarter))
        return start, end, str(args.quarter)
    if args.year:
        start, end = _year_bounds(str(args.year))
        return start, end, str(args.year)
    return None, None, "all"


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("stripe", parents=[format_parent], help="Stripe integration")
    stripe_sub = parser.add_subparsers(dest="stripe_command", required=True)

    p_link = stripe_sub.add_parser("link", parents=[format_parent], help="Connect Stripe account")
    p_link.set_defaults(func=handle_link, command_name="stripe.link")

    p_sync = stripe_sub.add_parser("sync", parents=[format_parent], help="Sync Stripe balance transactions")
    p_sync.add_argument("--days", type=int, help="Sync last N days (overrides cursor)")
    p_sync.add_argument("--force", "-f", action="store_true", help="Bypass cooldown")
    p_sync.add_argument("--backfill", action="store_true", help="Full history sync (ignores cursor)")
    p_sync.set_defaults(func=handle_sync, command_name="stripe.sync")

    p_status = stripe_sub.add_parser("status", parents=[format_parent], help="Stripe connection status")
    p_status.set_defaults(func=handle_status, command_name="stripe.status")

    p_revenue = stripe_sub.add_parser("revenue", parents=[format_parent], help="Revenue breakdown by period")
    p_revenue.add_argument("--month", help="YYYY-MM")
    p_revenue.add_argument("--quarter", help="YYYY-QN")
    p_revenue.add_argument("--year", help="YYYY")
    p_revenue.set_defaults(func=handle_revenue, command_name="stripe.revenue")

    p_unlink = stripe_sub.add_parser("unlink", parents=[format_parent], help="Disconnect Stripe")
    p_unlink.set_defaults(func=handle_unlink, command_name="stripe.unlink")


def handle_link(args, conn: sqlite3.Connection) -> dict[str, Any]:
    del args
    status = config_status(conn)
    ready = status.has_sdk and status.configured
    if not status.has_sdk:
        return {
            "data": {
                "ready": False,
                "has_sdk": False,
                "configured": status.configured,
                "missing_env": status.missing_env,
            },
            "summary": {"ready": False},
            "cli_report": "stripe package not installed. Run: pip install stripe",
        }
    if not status.configured:
        return {
            "data": {
                "ready": False,
                "has_sdk": True,
                "configured": False,
                "missing_env": status.missing_env,
            },
            "summary": {"ready": False},
            "cli_report": "Set STRIPE_API_KEY environment variable",
        }

    try:
        linked = link_connection(conn)
    except StripeUnavailableError as exc:
        raise ValueError(str(exc)) from exc

    data = {
        "ready": ready,
        "stripe_account_id": linked.get("stripe_account_id"),
        "account_name": linked.get("account_name"),
        "local_account_id": linked.get("local_account_id"),
        "api_key_ref": linked.get("api_key_ref"),
    }

    return {
        "data": data,
        "summary": {"ready": True, "connections": config_status(conn).connection_count},
        "cli_report": f"Connected to {linked.get('account_name')}",
    }


def handle_sync(args, conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        result = run_sync(
            conn,
            days=getattr(args, "days", None),
            force=bool(getattr(args, "force", False)),
            backfill=bool(getattr(args, "backfill", False)),
        )
    except (StripeUnavailableError, StripeSyncError) as exc:
        raise ValueError(str(exc)) from exc

    return {
        "data": result,
        "summary": {
            "charges_added": result["charges_added"],
            "fees_added": result["fees_added"],
            "refunds_added": result["refunds_added"],
            "adjustments_added": result["adjustments_added"],
            "payouts_matched": result["payouts_matched"],
            "payouts_ambiguous": result["payouts_ambiguous"],
            "payouts_unmatched": result["payouts_unmatched"],
            "skipped_existing": result["skipped_existing"],
            "skipped_non_usd": result["skipped_non_usd"],
            "skipped_unknown_type": result["skipped_unknown_type"],
            "skipped_cooldown": bool(result.get("skipped_cooldown")),
        },
        "cli_report": _sync_cli_report(result),
    }


def handle_status(args, conn: sqlite3.Connection) -> dict[str, Any]:
    del args
    status = config_status(conn)
    try:
        connection_row = conn.execute(
            """
            SELECT id, account_id, account_name, api_key_ref, sync_cursor, last_sync_at, status
              FROM stripe_connections
             ORDER BY created_at ASC
             LIMIT 1
            """
        ).fetchone()
    except sqlite3.OperationalError:
        connection_row = None

    txn_row = conn.execute(
        "SELECT COUNT(*) AS n FROM transactions WHERE source = 'stripe'"
    ).fetchone()

    data: dict[str, Any] = {
        "configured": status.configured,
        "has_sdk": status.has_sdk,
        "missing_env": status.missing_env,
        "connection_count": status.connection_count,
        "account_name": status.account_name,
        "transaction_count": int(txn_row["n"] or 0),
        "connection_status": str(connection_row["status"] or "not_linked") if connection_row else "not_linked",
        "connection": dict(connection_row) if connection_row else None,
        "balance": None,
    }

    if status.configured and status.has_sdk:
        try:
            data["balance"] = balance_status()
        except Exception:
            data["balance"] = None

    return {
        "data": data,
        "summary": {
            "configured": status.configured,
            "connection_count": status.connection_count,
            "transaction_count": data["transaction_count"],
        },
        "cli_report": _status_cli_report(data),
    }


def handle_revenue(args, conn: sqlite3.Connection) -> dict[str, Any]:
    start, end, period_label = _resolve_period(args)

    where = ["source = 'stripe'", "is_active = 1"]
    params: list[Any] = []
    if start is not None and end is not None:
        where.append("date >= ?")
        where.append("date < ?")
        params.extend([start, end])

    rows = conn.execute(
        f"""
        SELECT substr(date, 1, 7) AS month,
               COALESCE(SUM(CASE
                   WHEN source_category = 'charge' THEN amount_cents
                   WHEN source_category = 'dispute_reversal' AND amount_cents > 0 THEN amount_cents
                   ELSE 0
               END), 0) AS gross_cents,
               ABS(COALESCE(SUM(CASE
                   WHEN source_category = 'fee' THEN amount_cents
                   ELSE 0
               END), 0)) AS fees_cents,
               ABS(COALESCE(SUM(CASE
                   WHEN source_category IN ('refund', 'partial_capture_reversal', 'dispute') THEN amount_cents
                   ELSE 0
               END), 0)) AS refunds_cents,
               COALESCE(SUM(amount_cents), 0) AS net_cents,
               COUNT(*) AS txn_count
          FROM transactions
         WHERE {' AND '.join(where)}
         GROUP BY substr(date, 1, 7)
         ORDER BY month ASC
        """,
        tuple(params),
    ).fetchall()

    monthly: list[dict[str, Any]] = []
    for row in rows:
        gross_cents = int(row["gross_cents"] or 0)
        fees_cents = int(row["fees_cents"] or 0)
        refunds_cents = int(row["refunds_cents"] or 0)
        fee_pct = None
        if gross_cents > 0:
            fee_pct = round((fees_cents / gross_cents) * 100.0, 2)

        monthly.append(
            {
                "month": str(row["month"]),
                "gross_cents": gross_cents,
                "fees_cents": fees_cents,
                "refunds_cents": refunds_cents,
                "net_cents": int(row["net_cents"] or 0),
                "txn_count": int(row["txn_count"] or 0),
                "fee_pct": fee_pct,
            }
        )

    totals = {
        "gross_cents": sum(item["gross_cents"] for item in monthly),
        "fees_cents": sum(item["fees_cents"] for item in monthly),
        "refunds_cents": sum(item["refunds_cents"] for item in monthly),
        "net_cents": sum(item["net_cents"] for item in monthly),
    }

    if not monthly:
        cli_report = f"No Stripe revenue data for period={period_label}"
    else:
        cli_lines = [
            (
                f"period={period_label} months={len(monthly)} "
                f"gross=${cents_to_dollars(totals['gross_cents']):,.2f} "
                f"fees=${cents_to_dollars(totals['fees_cents']):,.2f} "
                f"refunds=${cents_to_dollars(totals['refunds_cents']):,.2f} "
                f"net=${cents_to_dollars(totals['net_cents']):,.2f}"
            )
        ]
        for item in monthly:
            cli_lines.append(
                (
                    f"  {item['month']}: gross=${cents_to_dollars(item['gross_cents']):,.2f} "
                    f"fees=${cents_to_dollars(item['fees_cents']):,.2f} "
                    f"refunds=${cents_to_dollars(item['refunds_cents']):,.2f} "
                    f"net=${cents_to_dollars(item['net_cents']):,.2f}"
                )
            )
        cli_report = "\n".join(cli_lines)

    data = {
        "period": period_label,
        "rows": monthly,
        "totals": totals,
    }

    return {
        "data": data,
        "summary": {
            "period": period_label,
            "months": len(monthly),
            "gross_cents": totals["gross_cents"],
            "fees_cents": totals["fees_cents"],
            "refunds_cents": totals["refunds_cents"],
            "net_cents": totals["net_cents"],
        },
        "cli_report": cli_report,
    }


def handle_unlink(args, conn: sqlite3.Connection) -> dict[str, Any]:
    del args
    result = unlink_connection(conn)
    return {
        "data": result,
        "summary": {"updated": int(result.get("updated") or 0)},
        "cli_report": "Stripe disconnected" if result.get("updated") else "No Stripe connection found",
    }
