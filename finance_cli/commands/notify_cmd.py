"""Notification command handlers."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

try:
    import notify

    _HAS_NOTIFY = True
except ImportError:
    notify = None  # type: ignore[assignment]
    _HAS_NOTIFY = False

from ..budget_engine import budget_alerts
from ..models import cents_to_dollars


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("notify", parents=[format_parent], help="Send notifications")
    notify_sub = parser.add_subparsers(dest="notify_command", required=True)

    p_alerts = notify_sub.add_parser("budget-alerts", parents=[format_parent], help="Send budget alerts")
    p_alerts.add_argument("--channel", choices=["telegram", "imessage"], default="telegram")
    p_alerts.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_alerts.add_argument("--month")
    p_alerts.add_argument("--dry-run", action="store_true")
    p_alerts.set_defaults(func=handle_budget_alerts, command_name="notify.budget_alerts")

    p_test = notify_sub.add_parser("test", parents=[format_parent], help="Send test notification")
    p_test.add_argument("--channel", choices=["telegram", "imessage"], default="telegram")
    p_test.add_argument("--dry-run", action="store_true")
    p_test.set_defaults(func=handle_test, command_name="notify.test")


def _fmt_currency(cents: int) -> str:
    return f"${cents_to_dollars(int(cents)):,.2f}".replace(".00", "")


def format_budget_alert(alert_data: dict[str, Any]) -> str:
    month_raw = str(alert_data.get("month", ""))
    try:
        month_label = datetime.strptime(month_raw, "%Y-%m").strftime("%b %Y")
    except ValueError:
        month_label = month_raw

    lines = [
        f"Budget Alerts - {month_label} "
        f"(day {int(alert_data.get('days_elapsed', 0))}/{int(alert_data.get('days_in_month', 0))})"
    ]

    over_rows = [row for row in alert_data.get("alerts", []) if row.get("severity") == "over"]
    risk_rows = [row for row in alert_data.get("alerts", []) if row.get("severity") == "alert"]
    warn_rows = [row for row in alert_data.get("alerts", []) if row.get("severity") == "warn"]

    if over_rows:
        lines.extend(["", "OVER BUDGET:"])
        for row in over_rows:
            spent = abs(int(row["actual_cents"]))
            budget_cents = int(row["budget_cents"])
            over_cents = max(spent - budget_cents, 0)
            utilization = float(row.get("utilization", 0.0)) * 100.0
            lines.append(
                f"  {row['category_name']}: {_fmt_currency(spent)}/{_fmt_currency(budget_cents)} "
                f"({utilization:.0f}%) - {_fmt_currency(over_cents)} over"
            )

    if risk_rows:
        lines.extend(["", "AT RISK:"])
        for row in risk_rows:
            spent = abs(int(row["actual_cents"]))
            budget_cents = int(row["budget_cents"])
            forecast_cents = int(row["forecast_cents"])
            lines.append(
                f"  {row['category_name']}: {_fmt_currency(spent)}/{_fmt_currency(budget_cents)} "
                f"-> pace {_fmt_currency(forecast_cents)}"
            )

    if warn_rows:
        lines.extend(["", "WARNING:"])
        for row in warn_rows:
            spent = abs(int(row["actual_cents"]))
            budget_cents = int(row["budget_cents"])
            forecast_cents = int(row["forecast_cents"])
            lines.append(
                f"  {row['category_name']}: {_fmt_currency(spent)}/{_fmt_currency(budget_cents)} "
                f"-> pace {_fmt_currency(forecast_cents)}"
            )

    lines.extend(["", f"{int(alert_data.get('ok_count', 0))} on track"])
    return "\n".join(lines)


def handle_budget_alerts(args, conn: sqlite3.Connection) -> dict[str, Any]:
    alert_data = budget_alerts(conn, month=getattr(args, "month", None), view=getattr(args, "view", "all"))
    message = format_budget_alert(alert_data)

    dry_run = bool(getattr(args, "dry_run", False))
    channel = str(getattr(args, "channel", "telegram"))
    if dry_run:
        delivery = {"ok": True, "channel": channel, "dry_run": True}
    else:
        if not _HAS_NOTIFY:
            raise ValueError(
                "notify module not installed. Run: pip install -e notify"
            )
        delivery = notify.send(message, channel=channel)

    cli_lines = [
        f"channel={channel}",
        f"dry_run={dry_run}",
        f"alerts={len(alert_data.get('alerts', []))}",
        "",
        message,
    ]

    return {
        "data": {
            "channel": channel,
            "dry_run": dry_run,
            "message": message,
            "delivery": delivery,
            "alert_data": alert_data,
        },
        "summary": {
            "total_alerts": int(len(alert_data.get("alerts", []))),
            "ok_count": int(alert_data.get("ok_count", 0)),
            "over_count": int(alert_data.get("over_count", 0)),
            "alert_count": int(alert_data.get("alert_count", 0)),
            "warn_count": int(alert_data.get("warn_count", 0)),
        },
        "cli_report": "\n".join(cli_lines),
    }


def handle_test(args, _conn: sqlite3.Connection) -> dict[str, Any]:
    message = "finance_cli notification test - connection OK"
    dry_run = bool(getattr(args, "dry_run", False))
    channel = str(getattr(args, "channel", "telegram"))

    if dry_run:
        delivery = {"ok": True, "channel": channel, "dry_run": True}
    else:
        if not _HAS_NOTIFY:
            raise ValueError(
                "notify module not installed. Run: pip install -e notify"
            )
        delivery = notify.send(message, channel=channel)

    return {
        "data": {
            "channel": channel,
            "dry_run": dry_run,
            "message": message,
            "delivery": delivery,
        },
        "summary": {"sent": int(not dry_run), "channel": channel},
        "cli_report": (
            f"channel={channel}\n"
            f"dry_run={dry_run}\n"
            f"sent={not dry_run}\n\n"
            f"{message}"
        ),
    }
