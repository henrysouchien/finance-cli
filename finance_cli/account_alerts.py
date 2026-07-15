"""Persistent account alert rule helpers."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any
import uuid

try:
    import alerts

    _HAS_ALERTS = True
except ImportError:
    alerts = None  # type: ignore[assignment]
    _HAS_ALERTS = False

from .models import cents_to_dollars
from .notification_utils import resolve_notification_creds

_VALID_CHANNELS = {"telegram", "imessage"}
_VALID_STATUSES = {"active", "paused", "cancelled"}
_LOW_BALANCE_RULE_TYPE = "low_balance"


def _sqlite_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_now(raw_value: datetime | str | None = None) -> datetime:
    if raw_value is None:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    if isinstance(raw_value, datetime):
        parsed = raw_value
    else:
        raw = str(raw_value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError("now must be an ISO datetime") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _fmt_currency(cents: int) -> str:
    return f"${cents_to_dollars(int(cents)):,.2f}".replace(".00", "")


def _validate_channel(channel: str | None) -> str | None:
    if channel in (None, ""):
        return None
    normalized = str(channel or "").strip().lower()
    if normalized not in _VALID_CHANNELS:
        raise ValueError(f"Unsupported notification channel: {channel}")
    return normalized


def _normalize_status(status: str | None) -> str:
    normalized = str(status or "active").strip().lower()
    if normalized not in _VALID_STATUSES:
        expected = ", ".join(sorted(_VALID_STATUSES))
        raise ValueError(f"status must be one of: {expected}")
    return normalized


def _coerce_threshold_cents(value: Any) -> int:
    threshold = int(value)
    if threshold <= 0:
        raise ValueError("threshold_cents must be greater than 0")
    return threshold


def _coerce_cooldown_hours(value: Any) -> int:
    cooldown_hours = int(value)
    if cooldown_hours < 1 or cooldown_hours > 720:
        raise ValueError("cooldown_hours must be between 1 and 720")
    return cooldown_hours


def _account_label(row: sqlite3.Row) -> str:
    institution = str(row["institution_name"] or "").strip()
    account_name = str(row["account_name"] or "").strip()
    label = " ".join(part for part in (institution, account_name) if part).strip()
    return label or str(row["id"])


def _load_alert_account(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT id, institution_name, account_name, account_type, balance_current_cents, is_active
          FROM accounts
         WHERE id = ?
        """,
        (account_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Account not found: {account_id}")
    if int(row["is_active"] or 0) != 1:
        raise ValueError("account must be active")
    account_type = str(row["account_type"] or "")
    if account_type not in {"checking", "savings"}:
        raise ValueError("low-balance alerts require a checking or savings account")
    return row


def _rule_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload_raw = str(row["payload_json"] or "{}")
    payload = json.loads(payload_raw) if payload_raw else {}
    return {
        "id": str(row["id"]),
        "rule_type": str(row["rule_type"]),
        "account_id": str(row["account_id"]),
        "threshold_cents": int(row["threshold_cents"]),
        "channel": row["channel"],
        "label": row["label"],
        "status": str(row["status"]),
        "cooldown_hours": int(row["cooldown_hours"]),
        "last_triggered_at": row["last_triggered_at"],
        "last_error": row["last_error"],
        "payload": payload if isinstance(payload, dict) else {},
        "idempotency_key": str(row["idempotency_key"]),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _select_rule_by_idempotency(conn: sqlite3.Connection, idempotency_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT id, rule_type, account_id, threshold_cents, channel, label, status,
               cooldown_hours, last_triggered_at, last_error, payload_json,
               idempotency_key, created_at, updated_at
          FROM account_alert_rules
         WHERE idempotency_key = ?
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("account alert rule was not written")
    return _rule_row_to_dict(row)


def set_low_balance_alert(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    threshold_cents: int,
    channel: str | None = "telegram",
    cooldown_hours: int = 24,
    label: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Create or update an active low-balance alert rule."""
    normalized_account_id = str(account_id or "").strip()
    if not normalized_account_id:
        raise ValueError("account_id is required")
    threshold = _coerce_threshold_cents(threshold_cents)
    normalized_channel = _validate_channel(channel)
    cooldown = _coerce_cooldown_hours(cooldown_hours)
    account = _load_alert_account(conn, normalized_account_id)
    account_label = _account_label(account)
    label_value = str(label or "").strip() or f"{account_label} low-balance alert"
    idempotency_key = f"low_balance:{normalized_account_id}:{normalized_channel or 'default'}"
    current_balance_cents = (
        None if account["balance_current_cents"] is None else int(account["balance_current_cents"])
    )
    payload = {
        "account_label": account_label,
        "current_balance_cents": current_balance_cents,
    }
    preview = {
        "id": None,
        "rule_type": _LOW_BALANCE_RULE_TYPE,
        "account_id": normalized_account_id,
        "threshold_cents": threshold,
        "channel": normalized_channel,
        "label": label_value,
        "status": "active",
        "cooldown_hours": cooldown,
        "payload": payload,
        "idempotency_key": idempotency_key,
    }
    if dry_run:
        return {
            "data": {"rule": preview, "dry_run": True},
            "summary": {"configured": 0, "dry_run": True},
        }

    conn.execute(
        """
        INSERT INTO account_alert_rules (
            id, rule_type, account_id, threshold_cents, channel, label, status,
            cooldown_hours, payload_json, idempotency_key
        )
        VALUES (?, 'low_balance', ?, ?, ?, ?, 'active', ?, ?, ?)
        ON CONFLICT(idempotency_key) DO UPDATE SET
            threshold_cents = excluded.threshold_cents,
            channel = excluded.channel,
            label = excluded.label,
            status = 'active',
            cooldown_hours = excluded.cooldown_hours,
            payload_json = excluded.payload_json,
            last_error = NULL,
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            normalized_account_id,
            threshold,
            normalized_channel,
            label_value,
            cooldown,
            json.dumps(payload, sort_keys=True),
            idempotency_key,
        ),
    )
    conn.commit()
    rule = _select_rule_by_idempotency(conn, idempotency_key)
    return {
        "data": {"rule": rule, "dry_run": False},
        "summary": {"configured": 1, "dry_run": False, "id": rule["id"]},
    }


def list_account_alert_rules(
    conn: sqlite3.Connection,
    *,
    status: str | None = "active",
    limit: int = 100,
) -> dict[str, Any]:
    """List configured account alert rules."""
    normalized_status = None if status in (None, "", "all") else _normalize_status(status)
    limit = max(1, min(500, int(limit or 100)))
    if normalized_status is None:
        rows = conn.execute(
            """
            SELECT id, rule_type, account_id, threshold_cents, channel, label, status,
                   cooldown_hours, last_triggered_at, last_error, payload_json,
                   idempotency_key, created_at, updated_at
              FROM account_alert_rules
             ORDER BY status, created_at
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, rule_type, account_id, threshold_cents, channel, label, status,
                   cooldown_hours, last_triggered_at, last_error, payload_json,
                   idempotency_key, created_at, updated_at
              FROM account_alert_rules
             WHERE status = ?
             ORDER BY created_at
             LIMIT ?
            """,
            (normalized_status, limit),
        ).fetchall()
    rules = [_rule_row_to_dict(row) for row in rows]
    return {
        "data": {"rules": rules},
        "summary": {"count": len(rules), "status": normalized_status or "all"},
    }


def _last_triggered_within_cooldown(row: sqlite3.Row, *, now: datetime) -> bool:
    raw = row["last_triggered_at"]
    if not raw:
        return False
    try:
        last = datetime.fromisoformat(str(raw))
    except ValueError:
        return False
    cooldown_hours = int(row["cooldown_hours"] or 24)
    return last > now - timedelta(hours=cooldown_hours)


def _low_balance_message(row: sqlite3.Row, *, current_balance_cents: int) -> str:
    label = str(row["label"] or row["account_label"] or "Account")
    threshold_cents = int(row["threshold_cents"])
    return (
        f"{label}: current balance is {_fmt_currency(current_balance_cents)}, "
        f"below your {_fmt_currency(threshold_cents)} alert threshold."
    )


def _send_notification(
    conn: sqlite3.Connection,
    *,
    channel: str,
    message: str,
    dry_run: bool,
) -> Any:
    creds = resolve_notification_creds(conn, channel, require=not dry_run)
    if dry_run:
        return {"ok": True, "channel": channel, "dry_run": True, "resolved_creds": creds}
    if not _HAS_ALERTS:
        raise ValueError("alerts module not installed. Run: pip install -e alerts")
    return alerts.send(message, channel=channel, **creds)


def evaluate_account_alert_rules(
    conn: sqlite3.Connection,
    *,
    now: datetime | str | None = None,
    limit: int = 50,
    dry_run: bool = False,
    channel_override: str | None = None,
) -> dict[str, Any]:
    """Evaluate active account alert rules and send notifications for triggered rules."""
    now_dt = _parse_now(now)
    now_sql = _sqlite_datetime(now_dt)
    limit = max(1, min(500, int(limit or 50)))
    channel_override = _validate_channel(channel_override)
    rows = conn.execute(
        """
        SELECT r.id, r.rule_type, r.account_id, r.threshold_cents, r.channel, r.label,
               r.status, r.cooldown_hours, r.last_triggered_at, r.last_error,
               r.payload_json, r.idempotency_key, r.created_at, r.updated_at,
               a.institution_name, a.account_name, a.account_type, a.balance_current_cents,
               a.is_active,
               TRIM(COALESCE(a.institution_name, '') || ' ' || COALESCE(a.account_name, '')) AS account_label
          FROM account_alert_rules r
          JOIN accounts a ON a.id = r.account_id
         WHERE r.status = 'active'
           AND r.rule_type = 'low_balance'
           AND a.is_active = 1
         ORDER BY r.created_at
         LIMIT ?
        """,
        (limit,),
    ).fetchall()

    sent: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    previews: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in rows:
        if row["balance_current_cents"] is None:
            skipped.append({"id": row["id"], "reason": "missing_balance"})
            continue
        current_balance_cents = int(row["balance_current_cents"])
        threshold_cents = int(row["threshold_cents"])
        if current_balance_cents > threshold_cents:
            continue
        if _last_triggered_within_cooldown(row, now=now_dt):
            skipped.append({"id": row["id"], "reason": "cooldown"})
            continue

        channel = channel_override or row["channel"] or "telegram"
        message = _low_balance_message(row, current_balance_cents=current_balance_cents)
        event = {
            "id": str(row["id"]),
            "rule_type": str(row["rule_type"]),
            "account_id": str(row["account_id"]),
            "threshold_cents": threshold_cents,
            "current_balance_cents": current_balance_cents,
            "channel": channel,
            "message": message,
        }
        try:
            delivery = _send_notification(
                conn,
                channel=str(channel),
                message=message,
                dry_run=dry_run,
            )
        except Exception as exc:
            error = str(exc)
            if not dry_run:
                conn.execute(
                    """
                    UPDATE account_alert_rules
                       SET last_error = ?,
                           updated_at = datetime('now')
                     WHERE id = ?
                    """,
                    (error, row["id"]),
                )
                conn.commit()
            failed.append({**event, "error": error})
            continue

        delivered = {**event, "delivery": delivery}
        if dry_run:
            previews.append(delivered)
            continue
        conn.execute(
            """
            UPDATE account_alert_rules
               SET last_triggered_at = ?,
                   last_error = NULL,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (now_sql, row["id"]),
        )
        conn.commit()
        sent.append(delivered)

    return {
        "now": now_sql,
        "checked_count": len(rows),
        "sent": sent,
        "failed": failed,
        "previews": previews,
        "skipped": skipped,
        "dry_run": dry_run,
    }
