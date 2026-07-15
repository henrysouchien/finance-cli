"""Durable spending freeze flag helpers."""

from __future__ import annotations

from datetime import date, timedelta
import json
import re
import sqlite3
from typing import Any
import uuid

_VALID_SCOPES = {"discretionary", "all_nonessential", "category", "account"}
_VALID_STATUSES = {"active", "resolved", "cancelled"}
_TERMINAL_STATUSES = {"resolved", "cancelled"}
_VALID_SOURCES = {"user", "agent", "system"}
_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _normalize_scope(scope: str | None) -> str:
    normalized = str(scope or "discretionary").strip().lower().replace("-", "_")
    if normalized not in _VALID_SCOPES:
        expected = ", ".join(sorted(_VALID_SCOPES))
        raise ValueError(f"scope must be one of: {expected}")
    return normalized


def _normalize_status(status: str | None) -> str:
    normalized = str(status or "active").strip().lower()
    if normalized not in _VALID_STATUSES:
        expected = ", ".join(sorted(_VALID_STATUSES))
        raise ValueError(f"status must be one of: {expected}")
    return normalized


def _normalize_terminal_status(status: str | None) -> str:
    normalized = _normalize_status(status)
    if normalized not in _TERMINAL_STATUSES:
        expected = ", ".join(sorted(_TERMINAL_STATUSES))
        raise ValueError(f"clear status must be one of: {expected}")
    return normalized


def _normalize_source(source: str | None) -> str:
    normalized = str(source or "agent").strip().lower()
    if normalized not in _VALID_SOURCES:
        expected = ", ".join(sorted(_VALID_SOURCES))
        raise ValueError(f"source must be one of: {expected}")
    return normalized


def _parse_date(raw_value: str | date | None, *, field_name: str) -> date | None:
    if raw_value in (None, ""):
        return None
    if isinstance(raw_value, date):
        return raw_value
    try:
        return date.fromisoformat(str(raw_value).strip())
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date (YYYY-MM-DD)") from exc


def _coerce_optional_cents(
    value: Any,
    *,
    field_name: str,
    allow_negative: bool = False,
) -> int | None:
    if value in (None, ""):
        return None
    cents = int(value)
    if cents < 0 and not allow_negative:
        raise ValueError(f"{field_name} must be greater than or equal to 0")
    return cents


def _normalize_text(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _slug(value: str | None) -> str:
    raw = str(value or "general").strip().lower()
    slug = _SLUG_RE.sub("-", raw).strip("-")
    return slug or "general"


def _account_row(conn: sqlite3.Connection, account_id: str | None) -> sqlite3.Row | None:
    normalized = str(account_id or "").strip()
    if not normalized:
        return None
    row = conn.execute(
        """
        SELECT id, institution_name, account_name, account_type, is_active
          FROM accounts
         WHERE id = ?
         LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        raise ValueError(f"account not found: {normalized}")
    if int(row["is_active"] or 0) != 1:
        raise ValueError("account must be active")
    return row


def _category_row(conn: sqlite3.Connection, category_id: str | None) -> sqlite3.Row | None:
    normalized = str(category_id or "").strip()
    if not normalized:
        return None
    row = conn.execute(
        """
        SELECT id, name, is_system
          FROM categories
         WHERE id = ?
         LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        raise ValueError(f"category not found: {normalized}")
    return row


def _account_label(row: sqlite3.Row | None) -> str | None:
    if row is None:
        return None
    institution = str(row["institution_name"] or "").strip()
    account_name = str(row["account_name"] or "").strip()
    label = " ".join(part for part in (institution, account_name) if part).strip()
    return label or str(row["id"])


def _default_reason(*, bill_name: str | None, due_date: date | None) -> str:
    if bill_name and due_date:
        return f"Hold nonessential spending until {bill_name} clears on {due_date.isoformat()}."
    if bill_name:
        return f"Hold nonessential spending until {bill_name} clears."
    return "Temporary spending freeze."


def _idempotency_key(
    *,
    scope: str,
    account_id: str | None,
    category_id: str | None,
    hold_until: date,
    bill_name: str | None,
) -> str:
    return ":".join(
        (
            "spending_freeze",
            scope,
            account_id or "-",
            category_id or "-",
            hold_until.isoformat(),
            _slug(bill_name),
        )
    )


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload_raw = row["payload_json"]
    payload = json.loads(payload_raw) if payload_raw else {}
    hold_until = str(row["hold_until"])
    try:
        is_expired = date.fromisoformat(hold_until) < date.today()
    except ValueError:
        is_expired = False
    return {
        "id": str(row["id"]),
        "scope": str(row["scope"]),
        "status": str(row["status"]),
        "account_id": row["account_id"],
        "category_id": row["category_id"],
        "category_name": row["category_name"],
        "reason": str(row["reason"]),
        "bill_name": row["bill_name"],
        "bill_amount_cents": row["bill_amount_cents"],
        "due_date": row["due_date"],
        "hold_until": hold_until,
        "target_balance_after_cents": row["target_balance_after_cents"],
        "source": str(row["source"]),
        "payload": payload if isinstance(payload, dict) else {},
        "idempotency_key": str(row["idempotency_key"]),
        "is_expired": is_expired,
        "resolved_at": row["resolved_at"],
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _select_flag_by_idempotency(conn: sqlite3.Connection, idempotency_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT f.id, f.scope, f.status, f.account_id, f.category_id, c.name AS category_name,
               f.reason, f.bill_name, f.bill_amount_cents, f.due_date, f.hold_until,
               f.target_balance_after_cents, f.source, f.payload_json, f.idempotency_key,
               f.resolved_at, f.created_at, f.updated_at
          FROM spending_freeze_flags f
          LEFT JOIN categories c ON c.id = f.category_id
         WHERE f.idempotency_key = ?
         LIMIT 1
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("spending freeze flag was not written")
    return _row_to_dict(row)


def _select_flag_by_id(conn: sqlite3.Connection, flag_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT f.id, f.scope, f.status, f.account_id, f.category_id, c.name AS category_name,
               f.reason, f.bill_name, f.bill_amount_cents, f.due_date, f.hold_until,
               f.target_balance_after_cents, f.source, f.payload_json, f.idempotency_key,
               f.resolved_at, f.created_at, f.updated_at
          FROM spending_freeze_flags f
          LEFT JOIN categories c ON c.id = f.category_id
         WHERE f.id = ?
         LIMIT 1
        """,
        (flag_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"spending freeze flag not found: {flag_id}")
    return _row_to_dict(row)


def set_spending_freeze_flag(
    conn: sqlite3.Connection,
    *,
    scope: str = "discretionary",
    hold_until: str | date | None = None,
    reason: str = "",
    account_id: str | None = None,
    category_id: str | None = None,
    bill_name: str = "",
    bill_amount_cents: int | None = None,
    due_date: str | date | None = None,
    target_balance_after_cents: int | None = None,
    source: str = "agent",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Create or reactivate a temporary spending freeze flag."""
    normalized_account_id = _normalize_text(account_id)
    normalized_category_id = _normalize_text(category_id)
    normalized_scope = _normalize_scope(scope)
    if normalized_category_id and normalized_scope == "discretionary":
        normalized_scope = "category"
    if normalized_scope == "category" and not normalized_category_id:
        raise ValueError("category_id is required when scope=category")
    if normalized_scope == "account" and not normalized_account_id:
        raise ValueError("account_id is required when scope=account")

    account = _account_row(conn, normalized_account_id)
    category = _category_row(conn, normalized_category_id)
    normalized_source = _normalize_source(source)
    due = _parse_date(due_date, field_name="due_date")
    hold = _parse_date(hold_until, field_name="hold_until")
    if hold is None:
        hold = due if due is not None else date.today() + timedelta(days=7)
    if hold < date.today():
        raise ValueError("hold_until must not be in the past")
    normalized_bill_name = _normalize_text(bill_name)
    bill_amount = _coerce_optional_cents(bill_amount_cents, field_name="bill_amount_cents")
    target_balance = _coerce_optional_cents(
        target_balance_after_cents,
        field_name="target_balance_after_cents",
        allow_negative=True,
    )
    reason_value = str(reason or "").strip() or _default_reason(
        bill_name=normalized_bill_name,
        due_date=due,
    )
    payload = {
        "account_label": _account_label(account),
        "account_type": None if account is None else account["account_type"],
        "category_name": None if category is None else category["name"],
    }
    idempotency_key = _idempotency_key(
        scope=normalized_scope,
        account_id=normalized_account_id,
        category_id=normalized_category_id,
        hold_until=hold,
        bill_name=normalized_bill_name,
    )
    preview = {
        "id": None,
        "scope": normalized_scope,
        "status": "active",
        "account_id": normalized_account_id,
        "category_id": normalized_category_id,
        "category_name": payload["category_name"],
        "reason": reason_value,
        "bill_name": normalized_bill_name,
        "bill_amount_cents": bill_amount,
        "due_date": due.isoformat() if due is not None else None,
        "hold_until": hold.isoformat(),
        "target_balance_after_cents": target_balance,
        "source": normalized_source,
        "payload": payload,
        "idempotency_key": idempotency_key,
        "is_expired": False,
        "resolved_at": None,
    }
    if dry_run:
        return {
            "data": {"flag": preview, "dry_run": True},
            "summary": {
                "configured": 0,
                "dry_run": True,
                "scope": normalized_scope,
                "hold_until": hold.isoformat(),
            },
        }

    conn.execute(
        """
        INSERT INTO spending_freeze_flags (
            id, scope, status, account_id, category_id, reason, bill_name,
            bill_amount_cents, due_date, hold_until, target_balance_after_cents,
            source, payload_json, idempotency_key, resolved_at
        )
        VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(idempotency_key) DO UPDATE SET
            scope = excluded.scope,
            status = 'active',
            account_id = excluded.account_id,
            category_id = excluded.category_id,
            reason = excluded.reason,
            bill_name = excluded.bill_name,
            bill_amount_cents = excluded.bill_amount_cents,
            due_date = excluded.due_date,
            hold_until = excluded.hold_until,
            target_balance_after_cents = excluded.target_balance_after_cents,
            source = excluded.source,
            payload_json = excluded.payload_json,
            resolved_at = NULL,
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            normalized_scope,
            normalized_account_id,
            normalized_category_id,
            reason_value,
            normalized_bill_name,
            bill_amount,
            due.isoformat() if due is not None else None,
            hold.isoformat(),
            target_balance,
            normalized_source,
            json.dumps(payload, sort_keys=True),
            idempotency_key,
        ),
    )
    conn.commit()
    flag = _select_flag_by_idempotency(conn, idempotency_key)
    return {
        "data": {"flag": flag, "dry_run": False},
        "summary": {
            "configured": 1,
            "dry_run": False,
            "scope": normalized_scope,
            "hold_until": hold.isoformat(),
            "id": flag["id"],
        },
    }


def list_spending_freeze_flags(
    conn: sqlite3.Connection,
    *,
    status: str | None = "active",
    limit: int = 100,
) -> dict[str, Any]:
    """List spending freeze flags."""
    normalized_status = None if status in (None, "", "all") else _normalize_status(status)
    limit = max(1, min(500, int(limit or 100)))
    if normalized_status is None:
        rows = conn.execute(
            """
            SELECT f.id, f.scope, f.status, f.account_id, f.category_id, c.name AS category_name,
                   f.reason, f.bill_name, f.bill_amount_cents, f.due_date, f.hold_until,
                   f.target_balance_after_cents, f.source, f.payload_json, f.idempotency_key,
                   f.resolved_at, f.created_at, f.updated_at
              FROM spending_freeze_flags f
              LEFT JOIN categories c ON c.id = f.category_id
             ORDER BY f.status ASC, f.hold_until ASC, f.created_at DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT f.id, f.scope, f.status, f.account_id, f.category_id, c.name AS category_name,
                   f.reason, f.bill_name, f.bill_amount_cents, f.due_date, f.hold_until,
                   f.target_balance_after_cents, f.source, f.payload_json, f.idempotency_key,
                   f.resolved_at, f.created_at, f.updated_at
              FROM spending_freeze_flags f
              LEFT JOIN categories c ON c.id = f.category_id
             WHERE f.status = ?
             ORDER BY f.hold_until ASC, f.created_at DESC
             LIMIT ?
            """,
            (normalized_status, limit),
        ).fetchall()
    flags = [_row_to_dict(row) for row in rows]
    return {
        "data": {"flags": flags},
        "summary": {"count": len(flags), "status": normalized_status or "all"},
    }


def clear_spending_freeze_flag(
    conn: sqlite3.Connection,
    *,
    flag_id: str,
    status: str = "resolved",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Resolve or cancel an active spending freeze flag."""
    normalized_flag_id = str(flag_id or "").strip()
    if not normalized_flag_id:
        raise ValueError("flag_id is required")
    normalized_status = _normalize_terminal_status(status)
    existing = _select_flag_by_id(conn, normalized_flag_id)
    if dry_run:
        preview = dict(existing)
        preview["status"] = normalized_status
        preview["resolved_at"] = None
        return {
            "data": {"flag": preview, "dry_run": True},
            "summary": {"cleared": 0, "dry_run": True, "id": normalized_flag_id},
        }

    conn.execute(
        """
        UPDATE spending_freeze_flags
           SET status = ?,
               resolved_at = datetime('now'),
               updated_at = datetime('now')
         WHERE id = ?
        """,
        (normalized_status, normalized_flag_id),
    )
    conn.commit()
    flag = _select_flag_by_id(conn, normalized_flag_id)
    return {
        "data": {"flag": flag, "dry_run": False},
        "summary": {"cleared": 1, "dry_run": False, "id": normalized_flag_id},
    }
