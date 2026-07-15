"""Reminder command handlers."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

try:
    import alerts

    _HAS_ALERTS = True
except ImportError:
    alerts = None  # type: ignore[assignment]
    _HAS_ALERTS = False

from ..models import cents_to_dollars
from ..notification_utils import resolve_notification_creds

_VALID_CHANNELS = {"telegram", "imessage"}
_DEFAULT_CARD_ROTATION_DAYS_BEFORE = 7
_DEFAULT_BALANCE_TRANSFER_FEE_PERCENT = Decimal("3.0")
_BALANCE_TRANSFER_MIN_BALANCE_CENTS = 200_000
_BALANCE_TRANSFER_MIN_APR = Decimal("18.0")


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("reminders", parents=[format_parent], help="Manage reminders")
    reminder_sub = parser.add_subparsers(dest="reminder_command", required=True)

    p_card = reminder_sub.add_parser(
        "set-card-rotation",
        parents=[format_parent],
        help="Schedule a 0%% APR card rotation reminder",
    )
    p_card.add_argument("--zero-apr-account-id", required=True)
    p_card.add_argument("--paydown-account-id", required=True)
    p_card.add_argument("--intro-apr-end-date", required=True)
    p_card.add_argument("--avg-monthly-spend-cents", type=int, default=0)
    p_card.add_argument("--estimated-interest-saved-cents", type=int, default=0)
    p_card.add_argument("--channel", choices=sorted(_VALID_CHANNELS), default="telegram")
    p_card.add_argument("--days-before", type=int, default=_DEFAULT_CARD_ROTATION_DAYS_BEFORE)
    p_card.add_argument("--dry-run", action="store_true")
    p_card.set_defaults(
        func=handle_card_rotation_set,
        command_name="reminders.set_card_rotation",
    )

    p_transfer = reminder_sub.add_parser(
        "set-balance-transfer",
        parents=[format_parent],
        help="Schedule a balance-transfer opportunity reminder",
    )
    p_transfer.add_argument("--account-id", required=True)
    p_transfer.add_argument("--remind-on", required=True, help="Reminder date in YYYY-MM-DD format")
    p_transfer.add_argument(
        "--balance-transfer-fee-percent",
        type=float,
        default=float(_DEFAULT_BALANCE_TRANSFER_FEE_PERCENT),
        help="Expected transfer fee percentage, e.g. 3.0 for 3%%",
    )
    p_transfer.add_argument("--channel", choices=sorted(_VALID_CHANNELS), default="telegram")
    p_transfer.add_argument("--note", default="")
    p_transfer.add_argument("--dry-run", action="store_true")
    p_transfer.set_defaults(
        func=handle_balance_transfer_set,
        command_name="reminders.set_balance_transfer",
    )

    p_list = reminder_sub.add_parser("list", parents=[format_parent], help="List reminders")
    p_list.add_argument("--status", choices=["pending", "sent", "cancelled", "failed"], default="pending")
    p_list.add_argument("--all", action="store_true", dest="show_all")
    p_list.add_argument("--limit", type=int, default=50)
    p_list.set_defaults(func=handle_list, command_name="reminders.list")

    p_cancel = reminder_sub.add_parser("cancel", parents=[format_parent], help="Cancel a reminder")
    p_cancel.add_argument("id")
    p_cancel.set_defaults(func=handle_cancel, command_name="reminders.cancel")

    p_send = reminder_sub.add_parser(
        "send-due",
        parents=[format_parent],
        help="Send reminders that are due",
    )
    p_send.add_argument("--channel", choices=sorted(_VALID_CHANNELS))
    p_send.add_argument("--now", help="Override current time in ISO format")
    p_send.add_argument("--limit", type=int, default=50)
    p_send.add_argument("--dry-run", action="store_true")
    p_send.set_defaults(func=handle_send_due, command_name="reminders.send_due")


def _validate_channel(channel: str | None) -> str | None:
    if channel is None:
        return None
    normalized = str(channel or "").strip().lower()
    if normalized not in _VALID_CHANNELS:
        raise ValueError(f"Unsupported notification channel: {channel}")
    return normalized


def _parse_date(raw_value: object, *, field_name: str) -> date:
    try:
        return date.fromisoformat(str(raw_value))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc


def _parse_now(raw_value: object | None) -> datetime:
    if raw_value in (None, ""):
        return datetime.now(timezone.utc).replace(tzinfo=None)
    raw = str(raw_value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("--now must be an ISO datetime") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _sqlite_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _due_at_for_intro_end(intro_end_date: date, days_before: int) -> str:
    if days_before < 0 or days_before > 120:
        raise ValueError("--days-before must be between 0 and 120")
    due_date = intro_end_date - timedelta(days=days_before)
    return _sqlite_datetime(datetime.combine(due_date, time(hour=9)))


def _due_at_for_date(remind_on: date) -> str:
    return _sqlite_datetime(datetime.combine(remind_on, time(hour=9)))


def _fmt_currency(cents: int) -> str:
    return f"${cents_to_dollars(int(cents)):,.2f}".replace(".00", "")


def _fmt_percent(value: Decimal) -> str:
    normalized = value.quantize(Decimal("0.01")).normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _parse_percent(raw_value: object, *, field_name: str) -> Decimal:
    if raw_value in (None, ""):
        return _DEFAULT_BALANCE_TRANSFER_FEE_PERCENT
    try:
        value = Decimal(str(raw_value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a percentage number") from exc
    if value < 0 or value > 20:
        raise ValueError(f"{field_name} must be between 0 and 20")
    return value.quantize(Decimal("0.01"))


def _cents_for_percent(amount_cents: int, percent: Decimal) -> int:
    value = (Decimal(int(amount_cents)) * percent / Decimal("100")).quantize(
        Decimal("1"),
        rounding=ROUND_HALF_UP,
    )
    return int(value)


def _row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, sqlite3.Row):
        return row[key]
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    try:
        return row[index]
    except Exception:
        return default


def _account_label(conn: sqlite3.Connection, account_id: str) -> str:
    row = conn.execute(
        """
        SELECT institution_name, account_name, card_ending
        FROM accounts
        WHERE id = ?
        """,
        (account_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Account not found: {account_id}")

    institution = str(_row_get(row, "institution_name", 0, "") or "").strip()
    account_name = str(_row_get(row, "account_name", 1, "") or "").strip()
    card_ending = str(_row_get(row, "card_ending", 2, "") or "").strip()
    label = " ".join(part for part in (institution, account_name) if part).strip()
    if card_ending:
        label = f"{label} ending {card_ending}" if label else f"card ending {card_ending}"
    return label or account_id


def _card_rotation_payload(
    *,
    zero_apr_account_id: str,
    paydown_account_id: str,
    intro_apr_end_date: date,
    avg_monthly_spend_cents: int,
    estimated_interest_saved_cents: int,
    days_before: int,
) -> dict[str, Any]:
    return {
        "zero_apr_account_id": zero_apr_account_id,
        "paydown_account_id": paydown_account_id,
        "intro_apr_end_date": intro_apr_end_date.isoformat(),
        "avg_monthly_spend_cents": int(avg_monthly_spend_cents),
        "estimated_interest_saved_cents": int(estimated_interest_saved_cents),
        "days_before": int(days_before),
    }


def _card_rotation_message(
    *,
    zero_label: str,
    paydown_label: str,
    intro_apr_end_date: date,
    avg_monthly_spend_cents: int,
    estimated_interest_saved_cents: int,
) -> tuple[str, str]:
    title = "0% APR card rotation reminder"
    body_parts = [
        f"{zero_label}'s 0% APR period ends {intro_apr_end_date.isoformat()}.",
        "Stop routing new daily spend there before the promo expires and confirm the promo balance is paid off.",
        f"Keep sending freed cash to {paydown_label}.",
    ]
    if avg_monthly_spend_cents > 0:
        body_parts.append(f"Recent spend estimate: {_fmt_currency(avg_monthly_spend_cents)}/mo.")
    if estimated_interest_saved_cents > 0:
        body_parts.append(
            f"Estimated interest avoided by rotating spend: {_fmt_currency(estimated_interest_saved_cents)}."
        )
    return title, " ".join(body_parts)


def _balance_transfer_card(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
    normalized = str(account_id or "").strip()
    if not normalized:
        raise ValueError("account_id is required")
    row = conn.execute(
        """
        SELECT a.id, a.institution_name, a.account_name, a.card_ending,
               a.account_type, a.balance_current_cents, a.balance_limit_cents,
               a.is_active, l.id AS liability_id, l.apr_purchase,
               l.minimum_payment_cents, l.next_monthly_payment_cents,
               l.last_statement_issue_date, l.next_payment_due_date,
               a.is_business
          FROM accounts a
          LEFT JOIN liabilities l
            ON l.account_id = a.id
           AND l.is_active = 1
           AND l.liability_type = 'credit'
         WHERE a.id = ?
         LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        raise ValueError(f"account not found: {normalized}")
    if int(_row_get(row, "is_active", 7, 0) or 0) != 1:
        raise ValueError("balance-transfer reminders require an active account")
    if str(_row_get(row, "account_type", 4, "") or "") != "credit_card":
        raise ValueError("balance-transfer reminders require a credit_card account")
    if int(_row_get(row, "is_business", 14, 0) or 0) == 1:
        raise ValueError("balance-transfer reminders require a personal credit_card account")
    alias_row = conn.execute(
        """
        SELECT 1
          FROM account_aliases
         WHERE hash_account_id = ?
         LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if alias_row is not None:
        raise ValueError("balance-transfer reminders require a canonical account")
    balance_cents = abs(int(_row_get(row, "balance_current_cents", 5, 0) or 0))
    if balance_cents <= 0:
        raise ValueError("balance-transfer reminders require a card with a positive balance")
    if balance_cents < _BALANCE_TRANSFER_MIN_BALANCE_CENTS:
        raise ValueError("balance-transfer reminders require at least $2,000 of card balance")
    apr_raw = _row_get(row, "apr_purchase", 9)
    if apr_raw in (None, ""):
        raise ValueError(
            "balance-transfer reminders require apr_purchase on an active credit liability"
        )
    try:
        apr = Decimal(str(apr_raw))
    except InvalidOperation as exc:
        raise ValueError("apr_purchase must be a percentage number") from exc
    if apr <= 0:
        raise ValueError("balance-transfer reminders require apr_purchase greater than 0")
    if apr < _BALANCE_TRANSFER_MIN_APR:
        raise ValueError("balance-transfer reminders require apr_purchase of at least 18%")
    return row


def _account_alias_group(conn: sqlite3.Connection, account_id: str) -> tuple[str, ...]:
    rows = conn.execute(
        """
        SELECT hash_account_id
          FROM account_aliases
         WHERE canonical_id = ?
         ORDER BY hash_account_id
        """,
        (account_id,),
    ).fetchall()
    aliases = tuple(str(_row_get(row, "hash_account_id", 0)) for row in rows)
    return (account_id, *aliases)


def _pending_balance_transfer_idempotency_key(
    conn: sqlite3.Connection,
    *,
    account_ids: tuple[str, ...],
) -> str | None:
    if not account_ids:
        return None
    placeholders = ",".join("?" for _ in account_ids)
    row = conn.execute(
        f"""
        SELECT idempotency_key
          FROM reminders
         WHERE kind = 'balance_transfer'
           AND status = 'pending'
           AND json_extract(payload_json, '$.account_id') IN ({placeholders})
         ORDER BY datetime(created_at) DESC, id DESC
         LIMIT 1
        """,
        account_ids,
    ).fetchone()
    if row is None:
        return None
    return str(_row_get(row, "idempotency_key", 0))


def _balance_transfer_label(row: Any) -> str:
    institution = str(_row_get(row, "institution_name", 1, "") or "").strip()
    account_name = str(_row_get(row, "account_name", 2, "") or "").strip()
    card_ending = str(_row_get(row, "card_ending", 3, "") or "").strip()
    label = " ".join(part for part in (institution, account_name) if part).strip()
    if card_ending:
        label = f"{label} ending {card_ending}" if label else f"card ending {card_ending}"
    return label or str(_row_get(row, "id", 0))


def _balance_transfer_payload(
    *,
    card: sqlite3.Row,
    account_label: str,
    remind_on: date,
    fee_percent: Decimal,
    note: str,
) -> dict[str, Any]:
    balance_cents = abs(int(_row_get(card, "balance_current_cents", 5, 0) or 0))
    apr = Decimal(str(_row_get(card, "apr_purchase", 9)))
    fee_cents = _cents_for_percent(balance_cents, fee_percent)
    interest_avoided_cents = _cents_for_percent(balance_cents, apr)
    net_savings_cents = interest_avoided_cents - fee_cents
    minimum_payment = _row_get(card, "minimum_payment_cents", 10)
    if minimum_payment is None:
        minimum_payment = _row_get(card, "next_monthly_payment_cents", 11)
    return {
        "account_id": str(_row_get(card, "id", 0)),
        "account_label": account_label,
        "liability_id": _row_get(card, "liability_id", 8),
        "balance_cents": balance_cents,
        "balance_current_cents": int(_row_get(card, "balance_current_cents", 5, 0) or 0),
        "balance_limit_cents": _row_get(card, "balance_limit_cents", 6),
        "apr_purchase": float(apr),
        "minimum_payment_cents": minimum_payment,
        "last_statement_issue_date": _row_get(card, "last_statement_issue_date", 12),
        "next_payment_due_date": _row_get(card, "next_payment_due_date", 13),
        "remind_on": remind_on.isoformat(),
        "balance_transfer_fee_percent": float(fee_percent),
        "balance_transfer_fee_cents": fee_cents,
        "interest_avoided_12mo_cents": interest_avoided_cents,
        "net_savings_12mo_cents": net_savings_cents,
        "meets_playbook_trigger": balance_cents >= 200_000 and apr >= Decimal("18.0"),
        "note": note,
    }


def _balance_transfer_message(
    *,
    account_label: str,
    payload: dict[str, Any],
    fee_percent: Decimal,
    note: str,
) -> tuple[str, str]:
    title = f"Balance transfer check: {account_label}"
    body_parts = [
        f"Check 0% balance-transfer options for {account_label}.",
        (
            f"Current balance {_fmt_currency(int(payload['balance_cents']))} "
            f"at {_fmt_percent(Decimal(str(payload['apr_purchase'])))}% APR."
        ),
        (
            f"Estimated 12-month interest avoided: "
            f"{_fmt_currency(int(payload['interest_avoided_12mo_cents']))}; "
            f"typical {_fmt_percent(fee_percent)}% transfer fee: "
            f"{_fmt_currency(int(payload['balance_transfer_fee_cents']))}; "
            f"net estimate: {_fmt_currency(int(payload['net_savings_12mo_cents']))}."
        ),
        "Confirm eligibility, transfer terms, and payoff plan before applying.",
    ]
    if note:
        body_parts.append(note)
    return title, " ".join(body_parts)


def _reminder_to_dict(row: Any) -> dict[str, Any]:
    payload_raw = str(_row_get(row, "payload_json", 7, "{}") or "{}")
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        payload = {}
    return {
        "id": str(_row_get(row, "id", 0)),
        "kind": str(_row_get(row, "kind", 1)),
        "title": str(_row_get(row, "title", 2)),
        "body": str(_row_get(row, "body", 3)),
        "due_at": str(_row_get(row, "due_at", 4)),
        "channel": _row_get(row, "channel", 5),
        "status": str(_row_get(row, "status", 6)),
        "payload": payload,
        "idempotency_key": _row_get(row, "idempotency_key", 8),
        "sent_at": _row_get(row, "sent_at", 9),
        "cancelled_at": _row_get(row, "cancelled_at", 10),
        "last_error": _row_get(row, "last_error", 11),
        "created_at": str(_row_get(row, "created_at", 12)),
        "updated_at": str(_row_get(row, "updated_at", 13)),
    }


def _upsert_reminder(
    conn: sqlite3.Connection,
    *,
    kind: str,
    title: str,
    body: str,
    due_at: str,
    channel: str | None,
    payload: dict[str, Any],
    idempotency_key: str,
) -> dict[str, Any]:
    conn.execute(
        """
        INSERT INTO reminders (
            id, kind, title, body, due_at, channel, status, payload_json, idempotency_key
        )
        VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        ON CONFLICT(idempotency_key) DO UPDATE SET
            kind = excluded.kind,
            title = excluded.title,
            body = excluded.body,
            due_at = excluded.due_at,
            channel = excluded.channel,
            status = 'pending',
            payload_json = excluded.payload_json,
            sent_at = NULL,
            cancelled_at = NULL,
            last_error = NULL,
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            kind,
            title,
            body,
            due_at,
            channel,
            json.dumps(payload, sort_keys=True),
            idempotency_key,
        ),
    )
    conn.commit()
    row = conn.execute(
        """
        SELECT id, kind, title, body, due_at, channel, status, payload_json,
               idempotency_key, sent_at, cancelled_at, last_error, created_at, updated_at
        FROM reminders
        WHERE idempotency_key = ?
        """,
        (idempotency_key,),
    ).fetchone()
    return _reminder_to_dict(row)


def handle_card_rotation_set(args, conn: sqlite3.Connection) -> dict[str, Any]:
    zero_apr_account_id = str(getattr(args, "zero_apr_account_id", "") or "").strip()
    paydown_account_id = str(getattr(args, "paydown_account_id", "") or "").strip()
    if not zero_apr_account_id or not paydown_account_id:
        raise ValueError("Both card account ids are required")
    if zero_apr_account_id == paydown_account_id:
        raise ValueError("zero_apr_account_id and paydown_account_id must be different")

    intro_end_date = _parse_date(
        getattr(args, "intro_apr_end_date", ""),
        field_name="intro_apr_end_date",
    )
    days_before = int(getattr(args, "days_before", _DEFAULT_CARD_ROTATION_DAYS_BEFORE) or 0)
    avg_monthly_spend_cents = max(
        0,
        int(getattr(args, "avg_monthly_spend_cents", 0) or 0),
    )
    estimated_interest_saved_cents = max(
        0,
        int(getattr(args, "estimated_interest_saved_cents", 0) or 0),
    )
    channel = _validate_channel(str(getattr(args, "channel", "telegram") or "telegram"))
    due_at = _due_at_for_intro_end(intro_end_date, days_before)
    zero_label = _account_label(conn, zero_apr_account_id)
    paydown_label = _account_label(conn, paydown_account_id)
    title, body = _card_rotation_message(
        zero_label=zero_label,
        paydown_label=paydown_label,
        intro_apr_end_date=intro_end_date,
        avg_monthly_spend_cents=avg_monthly_spend_cents,
        estimated_interest_saved_cents=estimated_interest_saved_cents,
    )
    payload = _card_rotation_payload(
        zero_apr_account_id=zero_apr_account_id,
        paydown_account_id=paydown_account_id,
        intro_apr_end_date=intro_end_date,
        avg_monthly_spend_cents=avg_monthly_spend_cents,
        estimated_interest_saved_cents=estimated_interest_saved_cents,
        days_before=days_before,
    )
    idempotency_key = (
        "card_rotation:"
        f"{zero_apr_account_id}:{paydown_account_id}:{intro_end_date.isoformat()}"
    )
    dry_run = bool(getattr(args, "dry_run", False))
    preview = {
        "id": None,
        "kind": "card_rotation",
        "title": title,
        "body": body,
        "due_at": due_at,
        "channel": channel,
        "status": "pending",
        "payload": payload,
        "idempotency_key": idempotency_key,
    }
    if dry_run:
        return {
            "data": {"reminder": preview, "dry_run": True},
            "summary": {"scheduled": 0, "dry_run": True},
            "cli_report": (
                f"dry_run=True\nkind=card_rotation\ndue_at={due_at}\n"
                f"channel={channel}\n\n{title}\n{body}"
            ),
        }

    reminder = _upsert_reminder(
        conn,
        kind="card_rotation",
        title=title,
        body=body,
        due_at=due_at,
        channel=channel,
        payload=payload,
        idempotency_key=idempotency_key,
    )
    return {
        "data": {"reminder": reminder, "dry_run": False},
        "summary": {"scheduled": 1, "dry_run": False, "id": reminder["id"]},
        "cli_report": (
            f"id={reminder['id']}\nkind=card_rotation\ndue_at={due_at}\n"
            f"channel={channel}\n\n{title}\n{body}"
        ),
    }


def handle_balance_transfer_set(args, conn: sqlite3.Connection) -> dict[str, Any]:
    account_id = str(getattr(args, "account_id", "") or "").strip()
    remind_on = _parse_date(getattr(args, "remind_on", ""), field_name="remind_on")
    fee_percent = _parse_percent(
        getattr(args, "balance_transfer_fee_percent", _DEFAULT_BALANCE_TRANSFER_FEE_PERCENT),
        field_name="balance_transfer_fee_percent",
    )
    channel = _validate_channel(str(getattr(args, "channel", "telegram") or "telegram"))
    note = " ".join(str(getattr(args, "note", "") or "").split())[:240]

    card = _balance_transfer_card(conn, account_id)
    account_label = _balance_transfer_label(card)
    due_at = _due_at_for_date(remind_on)
    payload = _balance_transfer_payload(
        card=card,
        account_label=account_label,
        remind_on=remind_on,
        fee_percent=fee_percent,
        note=note,
    )
    if int(payload["net_savings_12mo_cents"]) <= 0:
        raise ValueError("balance-transfer reminders require positive estimated net savings")
    title, body = _balance_transfer_message(
        account_label=account_label,
        payload=payload,
        fee_percent=fee_percent,
        note=note,
    )
    account_ids = _account_alias_group(conn, account_id)
    idempotency_key = _pending_balance_transfer_idempotency_key(
        conn,
        account_ids=account_ids,
    ) or f"balance_transfer:{account_id}"
    dry_run = bool(getattr(args, "dry_run", False))
    preview = {
        "id": None,
        "kind": "balance_transfer",
        "title": title,
        "body": body,
        "due_at": due_at,
        "channel": channel,
        "status": "pending",
        "payload": payload,
        "idempotency_key": idempotency_key,
    }
    summary = {
        "scheduled": 0 if dry_run else 1,
        "dry_run": dry_run,
        "balance_transfer_fee_cents": payload["balance_transfer_fee_cents"],
        "interest_avoided_12mo_cents": payload["interest_avoided_12mo_cents"],
        "net_savings_12mo_cents": payload["net_savings_12mo_cents"],
        "meets_playbook_trigger": payload["meets_playbook_trigger"],
    }
    if dry_run:
        return {
            "data": {"reminder": preview, "dry_run": True},
            "summary": summary,
            "cli_report": (
                f"dry_run=True\nkind=balance_transfer\ndue_at={due_at}\n"
                f"channel={channel}\n\n{title}\n{body}"
            ),
        }

    reminder = _upsert_reminder(
        conn,
        kind="balance_transfer",
        title=title,
        body=body,
        due_at=due_at,
        channel=channel,
        payload=payload,
        idempotency_key=idempotency_key,
    )
    summary["id"] = reminder["id"]
    return {
        "data": {"reminder": reminder, "dry_run": False},
        "summary": summary,
        "cli_report": (
            f"id={reminder['id']}\nkind=balance_transfer\ndue_at={due_at}\n"
            f"channel={channel}\n\n{title}\n{body}"
        ),
    }


def _select_reminders_sql(where_clause: str) -> str:
    return f"""
        SELECT id, kind, title, body, due_at, channel, status, payload_json,
               idempotency_key, sent_at, cancelled_at, last_error, created_at, updated_at
        FROM reminders
        {where_clause}
        ORDER BY datetime(due_at) ASC, created_at ASC
        LIMIT ?
    """


def handle_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    limit = max(1, min(500, int(getattr(args, "limit", 50) or 50)))
    if bool(getattr(args, "show_all", False)):
        rows = conn.execute(_select_reminders_sql(""), (limit,)).fetchall()
    else:
        status = str(getattr(args, "status", "pending") or "pending")
        rows = conn.execute(_select_reminders_sql("WHERE status = ?"), (status, limit)).fetchall()
    reminders = [_reminder_to_dict(row) for row in rows]
    cli_lines = [f"reminders={len(reminders)}"]
    for item in reminders:
        cli_lines.append(
            f"{item['id']}: {item['status']} {item['due_at']} {item['kind']} - {item['title']}"
        )
    return {
        "data": {"reminders": reminders},
        "summary": {"count": len(reminders)},
        "cli_report": "\n".join(cli_lines),
    }


def handle_cancel(args, conn: sqlite3.Connection) -> dict[str, Any]:
    reminder_id = str(getattr(args, "id", "") or "").strip()
    if not reminder_id:
        raise ValueError("Reminder id is required")
    cursor = conn.execute(
        """
        UPDATE reminders
           SET status = 'cancelled',
               cancelled_at = datetime('now'),
               updated_at = datetime('now')
         WHERE id = ?
           AND status IN ('pending', 'failed')
        """,
        (reminder_id,),
    )
    conn.commit()
    cancelled = bool(cursor.rowcount)
    return {
        "data": {"id": reminder_id, "cancelled": cancelled},
        "summary": {"cancelled": cancelled},
        "cli_report": f"id={reminder_id}\ncancelled={cancelled}",
    }


def _notification_message(row: Any) -> str:
    title = str(_row_get(row, "title", 2, "") or "").strip()
    body = str(_row_get(row, "body", 3, "") or "").strip()
    return f"{title}\n\n{body}" if body else title


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


def send_due_reminders(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
    limit: int = 50,
    dry_run: bool = False,
    channel_override: str | None = None,
) -> dict[str, Any]:
    now_dt = now or datetime.now(timezone.utc).replace(tzinfo=None)
    now_sql = _sqlite_datetime(now_dt)
    limit = max(1, min(500, int(limit or 50)))
    channel_override = _validate_channel(channel_override)
    rows = conn.execute(
        _select_reminders_sql("WHERE status = 'pending' AND datetime(due_at) <= datetime(?)"),
        (now_sql, limit),
    ).fetchall()

    sent: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    previews: list[dict[str, Any]] = []
    for row in rows:
        reminder = _reminder_to_dict(row)
        channel = channel_override or reminder["channel"] or "telegram"
        try:
            delivery = _send_notification(
                conn,
                channel=str(channel),
                message=_notification_message(row),
                dry_run=dry_run,
            )
        except Exception as exc:
            error = str(exc)
            if not dry_run:
                conn.execute(
                    """
                    UPDATE reminders
                       SET status = 'failed',
                           last_error = ?,
                           updated_at = datetime('now')
                     WHERE id = ?
                    """,
                    (error, reminder["id"]),
                )
                conn.commit()
            failed.append({**reminder, "error": error})
            continue

        delivered = {**reminder, "delivery": delivery, "channel": channel}
        if dry_run:
            previews.append(delivered)
            continue
        conn.execute(
            """
            UPDATE reminders
               SET status = 'sent',
                   sent_at = ?,
                   last_error = NULL,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (now_sql, reminder["id"]),
        )
        conn.commit()
        sent.append(delivered)

    return {
        "now": now_sql,
        "due_count": len(rows),
        "sent": sent,
        "failed": failed,
        "previews": previews,
        "dry_run": dry_run,
    }


def handle_send_due(
    args,
    conn: sqlite3.Connection,
    data_dir: Path | None = None,
) -> dict[str, Any]:
    del data_dir
    now = _parse_now(getattr(args, "now", None))
    result = send_due_reminders(
        conn,
        now=now,
        limit=int(getattr(args, "limit", 50) or 50),
        dry_run=bool(getattr(args, "dry_run", False)),
        channel_override=getattr(args, "channel", None),
    )
    sent_count = len(result["sent"])
    failed_count = len(result["failed"])
    preview_count = len(result["previews"])
    cli_lines = [
        f"now={result['now']}",
        f"dry_run={result['dry_run']}",
        f"due={result['due_count']}",
        f"sent={sent_count}",
        f"failed={failed_count}",
        f"previews={preview_count}",
    ]
    for item in result["sent"]:
        cli_lines.append(f"sent {item['id']}: {item['title']}")
    for item in result["previews"]:
        cli_lines.append(f"preview {item['id']}: {item['title']}")
    for item in result["failed"]:
        cli_lines.append(f"failed {item['id']}: {item['error']}")
    return {
        "data": result,
        "summary": {
            "due": int(result["due_count"]),
            "sent": sent_count,
            "failed": failed_count,
            "previews": preview_count,
        },
        "cli_report": "\n".join(cli_lines),
    }
