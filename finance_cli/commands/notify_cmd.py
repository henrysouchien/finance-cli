"""Notification command handlers."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import alerts

    _HAS_ALERTS = True
except ImportError:
    alerts = None  # type: ignore[assignment]
    _HAS_ALERTS = False

from ..budget_engine import budget_alerts
from ..models import cents_to_dollars
from ..notification_utils import resolve_notification_creds

_VALID_CHANNELS = {"telegram", "imessage"}
_REQUIRED_CONFIG_KEYS = {
    "telegram": "chat_id",
    "imessage": "target",
}


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

    p_channel_set = notify_sub.add_parser(
        "channel-set",
        parents=[format_parent],
        help="Create or update a notification channel config",
    )
    p_channel_set.add_argument("channel", choices=["telegram", "imessage"])
    p_channel_set.add_argument("config")
    p_channel_set.add_argument("--label", default="")
    p_channel_set.set_defaults(func=handle_channel_set, command_name="notify.channel_set")

    p_channel_list = notify_sub.add_parser(
        "channel-list",
        parents=[format_parent],
        help="List notification channel configs",
    )
    p_channel_list.set_defaults(func=handle_channel_list, command_name="notify.channel_list")

    p_channel_remove = notify_sub.add_parser(
        "channel-remove",
        parents=[format_parent],
        help="Remove a notification channel config",
    )
    p_channel_remove.add_argument("channel", choices=["telegram", "imessage"])
    p_channel_remove.set_defaults(func=handle_channel_remove, command_name="notify.channel_remove")


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


def _validate_channel(channel: str) -> str:
    normalized = str(channel or "").strip().lower()
    if normalized not in _VALID_CHANNELS:
        raise ValueError(f"Unsupported notification channel: {channel}")
    return normalized


def _parse_channel_config(channel: str, raw_config: str) -> dict[str, Any]:
    parsed = json.loads(raw_config)
    if not isinstance(parsed, dict):
        raise ValueError("Notification channel config must be a JSON object")
    required_key = _REQUIRED_CONFIG_KEYS[channel]
    if required_key not in parsed or parsed[required_key] in (None, ""):
        raise ValueError(f"Notification channel config must include '{required_key}'")
    return parsed


def handle_budget_alerts(
    args,
    conn: sqlite3.Connection,
    data_dir: Path | None = None,
) -> dict[str, Any]:
    alert_data = budget_alerts(conn, month=getattr(args, "month", None), view=getattr(args, "view", "all"))
    message = format_budget_alert(alert_data)

    dry_run = bool(getattr(args, "dry_run", False))
    channel = _validate_channel(str(getattr(args, "channel", "telegram")))
    require_config = data_dir is not None
    creds = resolve_notification_creds(conn, channel, require=require_config)
    if dry_run:
        delivery = {"ok": True, "channel": channel, "dry_run": True, "resolved_creds": creds}
    else:
        if not _HAS_ALERTS:
            raise ValueError(
                "alerts module not installed. Run: pip install -e alerts"
            )
        delivery = alerts.send(message, channel=channel, **creds)

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


def handle_test(
    args,
    conn: sqlite3.Connection,
    data_dir: Path | None = None,
) -> dict[str, Any]:
    message = "finance_cli notification test - connection OK"
    dry_run = bool(getattr(args, "dry_run", False))
    channel = _validate_channel(str(getattr(args, "channel", "telegram")))
    require_config = data_dir is not None
    creds = resolve_notification_creds(conn, channel, require=require_config)

    if dry_run:
        delivery = {"ok": True, "channel": channel, "dry_run": True, "resolved_creds": creds}
    else:
        if not _HAS_ALERTS:
            raise ValueError(
                "alerts module not installed. Run: pip install -e alerts"
            )
        delivery = alerts.send(message, channel=channel, **creds)

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


def handle_channel_set(args, conn: sqlite3.Connection) -> dict[str, Any]:
    channel = _validate_channel(str(getattr(args, "channel", "")))
    raw_config = str(getattr(args, "config", ""))
    label = str(getattr(args, "label", "") or "")
    parsed = _parse_channel_config(channel, raw_config)
    stored_config = json.dumps(parsed, sort_keys=True)

    conn.execute(
        """
        INSERT INTO notification_channels (channel, config, label)
        VALUES (?, ?, ?)
        ON CONFLICT(channel) DO UPDATE SET
            config = excluded.config,
            label = excluded.label
        """,
        (channel, stored_config, label),
    )
    conn.commit()

    return {
        "data": {
            "channel": channel,
            "config": parsed,
            "label": label,
            "updated": True,
        },
        "summary": {"channel": channel, "updated": True},
        "cli_report": f"channel={channel}\nlabel={label}\nupdated=True",
    }


def handle_channel_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    del args
    rows = conn.execute(
        """
        SELECT channel, config, label, created_at, updated_at
        FROM notification_channels
        ORDER BY channel
        """
    ).fetchall()
    channels = [
        {
            "channel": str(row["channel"] if isinstance(row, sqlite3.Row) else row[0]),
            "config": json.loads(str(row["config"] if isinstance(row, sqlite3.Row) else row[1])),
            "label": str(
                (row["label"] if isinstance(row, sqlite3.Row) else row[2]) or ""
            ),
            "created_at": str(row["created_at"] if isinstance(row, sqlite3.Row) else row[3]),
            "updated_at": str(row["updated_at"] if isinstance(row, sqlite3.Row) else row[4]),
        }
        for row in rows
    ]

    telegram_chat_id: str | None = None
    try:
        tg_row = conn.execute(
            "SELECT chat_id FROM telegram_config WHERE id = 1 AND chat_id IS NOT NULL"
        ).fetchone()
        if tg_row and tg_row[0]:
            telegram_chat_id = str(tg_row[0])
    except sqlite3.OperationalError:
        telegram_chat_id = None

    cli_lines = [f"channels={len(channels)}", f"telegram_fallback={bool(telegram_chat_id)}"]
    for item in channels:
        cli_lines.append(
            f"{item['channel']}: label={item['label'] or '-'} config={json.dumps(item['config'], sort_keys=True)}"
        )

    return {
        "data": {
            "channels": channels,
            "telegram_fallback_configured": bool(telegram_chat_id),
            "telegram_fallback_chat_id": telegram_chat_id,
        },
        "summary": {
            "count": len(channels),
            "telegram_fallback_configured": bool(telegram_chat_id),
        },
        "cli_report": "\n".join(cli_lines),
    }


def handle_channel_remove(args, conn: sqlite3.Connection) -> dict[str, Any]:
    channel = _validate_channel(str(getattr(args, "channel", "")))
    dry_run = bool(getattr(args, "dry_run", False))
    row = conn.execute(
        "SELECT channel, label, updated_at FROM notification_channels WHERE channel = ?",
        (channel,),
    ).fetchone()
    if dry_run:
        return {
            "data": {
                "dry_run": True,
                "channel": channel,
                "would_delete": row is not None,
                "current_channel": dict(row) if row else None,
            },
            "summary": {"dry_run": True, "would_delete": row is not None},
            "cli_report": f"channel={channel}\ndry_run=True\nwould_delete={row is not None}",
        }
    cursor = conn.execute("DELETE FROM notification_channels WHERE channel = ?", (channel,))
    conn.commit()
    deleted = bool(cursor.rowcount)
    return {
        "data": {"channel": channel, "deleted": deleted},
        "summary": {"deleted": deleted},
        "cli_report": f"channel={channel}\ndeleted={deleted}",
    }
