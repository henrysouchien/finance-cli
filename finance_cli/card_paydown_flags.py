"""Durable credit-card paydown flag helpers."""

from __future__ import annotations

import json
import sqlite3
from typing import Any
import uuid

from finance_cli.exceptions import ValidationError

_VALID_STATUSES = {"active", "resolved", "cancelled"}
_TERMINAL_STATUSES = {"resolved", "cancelled"}
_VALID_SOURCES = {"user", "agent", "system"}


def _normalize_source(source: str | None) -> str:
    normalized = str(source or "agent").strip().lower()
    if normalized not in _VALID_SOURCES:
        expected = ", ".join(sorted(_VALID_SOURCES))
        raise ValidationError(f"source must be one of: {expected}")
    return normalized


def _normalize_status(status: str | None) -> str:
    normalized = str(status or "active").strip().lower()
    if normalized not in _VALID_STATUSES:
        expected = ", ".join(sorted(_VALID_STATUSES))
        raise ValidationError(f"status must be one of: {expected}")
    return normalized


def _normalize_terminal_status(status: str | None) -> str:
    normalized = _normalize_status(status)
    if normalized not in _TERMINAL_STATUSES:
        expected = ", ".join(sorted(_TERMINAL_STATUSES))
        raise ValidationError(f"clear status must be one of: {expected}")
    return normalized


def _coerce_nonnegative_cents(value: Any, *, field_name: str) -> int:
    if value in (None, ""):
        return 0
    cents = int(value)
    if cents < 0:
        raise ValidationError(f"{field_name} must be greater than or equal to 0")
    return cents


def _coerce_optional_int(value: Any, *, field_name: str) -> int | None:
    if value in (None, ""):
        return None
    cents = int(value)
    if cents < 0:
        raise ValidationError(f"{field_name} must be greater than or equal to 0")
    return cents


def _account_label(row: sqlite3.Row) -> str:
    institution = str(row["institution_name"] or "").strip()
    account_name = str(row["account_name"] or "").strip()
    ending = str(row["card_ending"] or "").strip()
    label = " ".join(part for part in (institution, account_name) if part).strip()
    if ending:
        return f"{label} {ending}".strip()
    return label or str(row["id"])


def _load_card(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
    normalized = str(account_id or "").strip()
    if not normalized:
        raise ValidationError("account_id is required")
    row = conn.execute(
        """
        SELECT a.id, a.institution_name, a.account_name, a.card_ending,
               a.account_type, a.balance_current_cents, a.balance_limit_cents,
               a.is_active, l.id AS liability_id, l.apr_purchase,
               l.minimum_payment_cents, l.next_monthly_payment_cents,
               l.intro_apr_end_date
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
        raise ValidationError(f"account not found: {normalized}")
    if int(row["is_active"] or 0) != 1:
        raise ValidationError("account must be active")
    if str(row["account_type"] or "") != "credit_card":
        raise ValidationError("paydown flags require a credit_card account")
    balance_cents = abs(int(row["balance_current_cents"] or 0))
    if balance_cents <= 0:
        raise ValidationError("paydown flags require a card with a positive balance")
    return row


def _load_cash_source(conn: sqlite3.Connection, account_id: str | None) -> sqlite3.Row | None:
    normalized = str(account_id or "").strip()
    if not normalized:
        return None
    row = conn.execute(
        """
        SELECT id, institution_name, account_name, NULL AS card_ending,
               account_type, balance_current_cents, is_active
          FROM accounts
         WHERE id = ?
         LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"cash source account not found: {normalized}")
    if int(row["is_active"] or 0) != 1:
        raise ValidationError("cash source account must be active")
    if str(row["account_type"] or "") not in {"checking", "savings"}:
        raise ValidationError("cash_source_account_id must be checking or savings")
    return row


def _snapshot(
    *,
    card: sqlite3.Row,
    cash_source: sqlite3.Row | None,
    suggested_payment_cents: int,
    interest_saved_annual_cents: int | None,
) -> dict[str, Any]:
    minimum_payment = card["minimum_payment_cents"]
    if minimum_payment is None:
        minimum_payment = card["next_monthly_payment_cents"]
    return {
        "account_id": str(card["id"]),
        "account_label": _account_label(card),
        "balance_cents": abs(int(card["balance_current_cents"] or 0)),
        "balance_current_cents": int(card["balance_current_cents"] or 0),
        "balance_limit_cents": card["balance_limit_cents"],
        "liability_id": card["liability_id"],
        "apr_purchase": card["apr_purchase"],
        "minimum_payment_cents": minimum_payment,
        "intro_apr_end_date": card["intro_apr_end_date"],
        "cash_source_account_id": None if cash_source is None else str(cash_source["id"]),
        "cash_source_label": None if cash_source is None else _account_label(cash_source),
        "cash_source_balance_cents": (
            None if cash_source is None else cash_source["balance_current_cents"]
        ),
        "suggested_payment_cents": suggested_payment_cents,
        "interest_saved_annual_cents": interest_saved_annual_cents,
    }


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    snapshot_raw = row["snapshot_json"]
    snapshot = json.loads(snapshot_raw) if snapshot_raw else {}
    return {
        "id": str(row["id"]),
        "account_id": str(row["account_id"]),
        "account_name": row["account_name"],
        "institution_name": row["institution_name"],
        "liability_id": row["liability_id"],
        "status": str(row["status"]),
        "reason": row["reason"],
        "suggested_payment_cents": int(row["suggested_payment_cents"] or 0),
        "cash_source_account_id": row["cash_source_account_id"],
        "interest_saved_annual_cents": row["interest_saved_annual_cents"],
        "source": str(row["source"]),
        "snapshot": snapshot if isinstance(snapshot, dict) else {},
        "idempotency_key": str(row["idempotency_key"]),
        "resolved_at": row["resolved_at"],
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _select_flag_by_idempotency(conn: sqlite3.Connection, idempotency_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT f.id, f.account_id, a.account_name, a.institution_name, f.liability_id,
               f.status, f.reason, f.suggested_payment_cents, f.cash_source_account_id,
               f.interest_saved_annual_cents, f.source, f.snapshot_json,
               f.idempotency_key, f.resolved_at, f.created_at, f.updated_at
          FROM card_paydown_flags f
          JOIN accounts a ON a.id = f.account_id
         WHERE f.idempotency_key = ?
         LIMIT 1
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("card paydown flag was not written")
    return _row_to_dict(row)


def _select_flag_by_id(conn: sqlite3.Connection, flag_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT f.id, f.account_id, a.account_name, a.institution_name, f.liability_id,
               f.status, f.reason, f.suggested_payment_cents, f.cash_source_account_id,
               f.interest_saved_annual_cents, f.source, f.snapshot_json,
               f.idempotency_key, f.resolved_at, f.created_at, f.updated_at
          FROM card_paydown_flags f
          JOIN accounts a ON a.id = f.account_id
         WHERE f.id = ?
         LIMIT 1
        """,
        (flag_id,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"card paydown flag not found: {flag_id}")
    return _row_to_dict(row)


def flag_card_for_paydown(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    suggested_payment_cents: int = 0,
    cash_source_account_id: str | None = None,
    interest_saved_annual_cents: int | None = None,
    reason: str = "",
    source: str = "agent",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Create or reactivate a credit-card paydown flag."""
    card = _load_card(conn, account_id)
    if cash_source_account_id and str(cash_source_account_id).strip() == str(card["id"]):
        raise ValidationError("cash_source_account_id must be different from account_id")
    cash_source = _load_cash_source(conn, cash_source_account_id)
    suggested_payment = _coerce_nonnegative_cents(
        suggested_payment_cents,
        field_name="suggested_payment_cents",
    )
    interest_saved = _coerce_optional_int(
        interest_saved_annual_cents,
        field_name="interest_saved_annual_cents",
    )
    normalized_source = _normalize_source(source)
    snapshot = _snapshot(
        card=card,
        cash_source=cash_source,
        suggested_payment_cents=suggested_payment,
        interest_saved_annual_cents=interest_saved,
    )
    reason_value = str(reason or "").strip() or f"Flag {_account_label(card)} for the next paydown."
    idempotency_key = f"card_paydown:{card['id']}"
    preview = {
        "id": None,
        "account_id": str(card["id"]),
        "liability_id": card["liability_id"],
        "status": "active",
        "reason": reason_value,
        "suggested_payment_cents": suggested_payment,
        "cash_source_account_id": None if cash_source is None else str(cash_source["id"]),
        "interest_saved_annual_cents": interest_saved,
        "source": normalized_source,
        "snapshot": snapshot,
        "idempotency_key": idempotency_key,
        "resolved_at": None,
    }
    if dry_run:
        return {
            "data": {"flag": preview, "dry_run": True},
            "summary": {
                "flagged": 0,
                "dry_run": True,
                "account_id": str(card["id"]),
                "balance_cents": int(snapshot["balance_cents"]),
                "suggested_payment_cents": suggested_payment,
            },
        }

    conn.execute(
        """
        INSERT INTO card_paydown_flags (
            id, account_id, liability_id, status, reason, suggested_payment_cents,
            cash_source_account_id, interest_saved_annual_cents, source,
            snapshot_json, idempotency_key, resolved_at
        )
        VALUES (?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(idempotency_key) DO UPDATE SET
            liability_id = excluded.liability_id,
            status = 'active',
            reason = excluded.reason,
            suggested_payment_cents = excluded.suggested_payment_cents,
            cash_source_account_id = excluded.cash_source_account_id,
            interest_saved_annual_cents = excluded.interest_saved_annual_cents,
            source = excluded.source,
            snapshot_json = excluded.snapshot_json,
            resolved_at = NULL,
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            str(card["id"]),
            card["liability_id"],
            reason_value,
            suggested_payment,
            None if cash_source is None else str(cash_source["id"]),
            interest_saved,
            normalized_source,
            json.dumps(snapshot, sort_keys=True),
            idempotency_key,
        ),
    )
    conn.commit()
    flag = _select_flag_by_idempotency(conn, idempotency_key)
    return {
        "data": {"flag": flag, "dry_run": False},
        "summary": {
            "flagged": 1,
            "dry_run": False,
            "account_id": str(card["id"]),
            "balance_cents": int(snapshot["balance_cents"]),
            "suggested_payment_cents": suggested_payment,
            "id": flag["id"],
        },
    }


def list_card_paydown_flags(
    conn: sqlite3.Connection,
    *,
    status: str | None = "active",
    limit: int = 100,
) -> dict[str, Any]:
    """List credit-card paydown flags."""
    normalized_status = None if status in (None, "", "all") else _normalize_status(status)
    limit = max(1, min(500, int(limit or 100)))
    where = "" if normalized_status is None else "WHERE f.status = ?"
    params: tuple[Any, ...] = (limit,) if normalized_status is None else (normalized_status, limit)
    rows = conn.execute(
        f"""
        SELECT f.id, f.account_id, a.account_name, a.institution_name, f.liability_id,
               f.status, f.reason, f.suggested_payment_cents, f.cash_source_account_id,
               f.interest_saved_annual_cents, f.source, f.snapshot_json,
               f.idempotency_key, f.resolved_at, f.created_at, f.updated_at
          FROM card_paydown_flags f
          JOIN accounts a ON a.id = f.account_id
          {where}
         ORDER BY f.status ASC, f.updated_at DESC
         LIMIT ?
        """,
        params,
    ).fetchall()
    flags = [_row_to_dict(row) for row in rows]
    return {
        "data": {"flags": flags},
        "summary": {"count": len(flags), "status": normalized_status or "all"},
    }


def clear_card_paydown_flag(
    conn: sqlite3.Connection,
    *,
    flag_id: str,
    status: str = "resolved",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Resolve or cancel a credit-card paydown flag."""
    normalized_flag_id = str(flag_id or "").strip()
    if not normalized_flag_id:
        raise ValidationError("flag_id is required")
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
        UPDATE card_paydown_flags
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
