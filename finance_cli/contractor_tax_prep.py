"""Durable contractor tax-prep flag helpers."""

from __future__ import annotations

from datetime import date
import json
import re
import sqlite3
from typing import Any
import uuid

_YEAR_RE = re.compile(r"^\d{4}$")
_FLAG_TYPE = "january_1099_prep"
_VALID_STATUSES = {"active", "resolved", "cancelled"}
_VALID_SOURCES = {"user", "agent", "system"}
_APPROACHING_1099_THRESHOLD_CENTS = 50_000
_REQUIRES_1099_THRESHOLD_CENTS = 60_000


def _parse_tax_year(raw_value: int | str | None) -> int:
    if raw_value in (None, ""):
        return date.today().year
    value = str(raw_value).strip()
    if not _YEAR_RE.fullmatch(value):
        raise ValueError("tax_year must be in YYYY format")
    tax_year = int(value)
    if tax_year < 2000 or tax_year > 2100:
        raise ValueError("tax_year must be between 2000 and 2100")
    return tax_year


def _normalize_status(status: str | None) -> str:
    normalized = str(status or "active").strip().lower()
    if normalized not in _VALID_STATUSES:
        expected = ", ".join(sorted(_VALID_STATUSES))
        raise ValueError(f"status must be one of: {expected}")
    return normalized


def _normalize_source(source: str | None) -> str:
    normalized = str(source or "agent").strip().lower()
    if normalized not in _VALID_SOURCES:
        expected = ", ".join(sorted(_VALID_SOURCES))
        raise ValueError(f"source must be one of: {expected}")
    return normalized


def _load_contractor(conn: sqlite3.Connection, contractor_id: str) -> sqlite3.Row:
    normalized_id = str(contractor_id or "").strip()
    if not normalized_id:
        raise ValueError("contractor_id is required")
    row = conn.execute(
        """
        SELECT id, name, tin_last4, entity_type, is_active, notes, created_at
          FROM contractors
         WHERE id = ?
         LIMIT 1
        """,
        (normalized_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"contractor not found: {normalized_id}")
    if int(row["is_active"] or 0) != 1:
        raise ValueError("contractor must be active")
    return row


def _contractor_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    tin_last4 = row["tin_last4"]
    return {
        "id": str(row["id"]),
        "name": str(row["name"] or ""),
        "tin_last4": str(tin_last4) if tin_last4 is not None else None,
        "tin_on_file": bool(tin_last4),
        "entity_type": str(row["entity_type"] or "individual"),
        "is_active": int(row["is_active"] or 0),
        "notes": row["notes"],
        "created_at": str(row["created_at"]),
    }


def _payment_snapshot(
    conn: sqlite3.Connection,
    *,
    contractor_id: str,
    tax_year: int,
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT COUNT(CASE WHEN t.is_active = 1 THEN 1 END) AS payment_count,
               COALESCE(SUM(CASE WHEN t.is_active = 1 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS total_paid_cents,
               COALESCE(SUM(CASE WHEN t.is_active = 1 AND cp.paid_via_card = 0 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS non_card_paid_cents,
               COALESCE(SUM(CASE WHEN t.is_active = 1 AND cp.paid_via_card = 1 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS card_paid_cents
          FROM contractor_payments cp
          JOIN transactions t ON t.id = cp.transaction_id
         WHERE cp.contractor_id = ?
           AND cp.tax_year = ?
        """,
        (contractor_id, tax_year),
    ).fetchone()
    total_paid_cents = int(row["total_paid_cents"] or 0)
    non_card_paid_cents = int(row["non_card_paid_cents"] or 0)
    card_paid_cents = int(row["card_paid_cents"] or 0)
    payment_count = int(row["payment_count"] or 0)
    return {
        "tax_year": int(tax_year),
        "payment_count": payment_count,
        "total_paid_cents": total_paid_cents,
        "non_card_paid_cents": non_card_paid_cents,
        "card_paid_cents": card_paid_cents,
    }


def _snapshot_with_contractor(
    conn: sqlite3.Connection,
    *,
    contractor: sqlite3.Row,
    tax_year: int,
) -> dict[str, Any]:
    contractor_payload = _contractor_to_dict(contractor)
    snapshot = _payment_snapshot(conn, contractor_id=contractor_payload["id"], tax_year=tax_year)
    entity_type = contractor_payload["entity_type"]
    corporation_exempt = entity_type == "corporation"
    non_card_paid_cents = int(snapshot["non_card_paid_cents"])
    snapshot.update(
        {
            "contractor_id": contractor_payload["id"],
            "contractor_name": contractor_payload["name"],
            "entity_type": entity_type,
            "tin_on_file": bool(contractor_payload["tin_on_file"]),
            "corporation_exempt": corporation_exempt,
            "approaching_1099_threshold": (
                non_card_paid_cents >= _APPROACHING_1099_THRESHOLD_CENTS
                and not corporation_exempt
            ),
            "requires_1099": (
                non_card_paid_cents >= _REQUIRES_1099_THRESHOLD_CENTS
                and not corporation_exempt
            ),
            "w9_collection_recommended": (
                not contractor_payload["tin_on_file"]
                and non_card_paid_cents >= _APPROACHING_1099_THRESHOLD_CENTS
                and not corporation_exempt
            ),
        }
    )
    return snapshot


def _default_reason(snapshot: dict[str, Any]) -> str:
    if bool(snapshot["requires_1099"]):
        return "Contractor has crossed the non-card 1099-NEC reporting threshold."
    if bool(snapshot["approaching_1099_threshold"]):
        return "Contractor is approaching the non-card 1099-NEC reporting threshold."
    return "Contractor flagged for January 1099 prep."


def _flag_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    snapshot_raw = row["payment_snapshot_json"]
    snapshot = json.loads(snapshot_raw) if snapshot_raw else {}
    return {
        "id": str(row["id"]),
        "contractor_id": str(row["contractor_id"]),
        "contractor_name": row["contractor_name"],
        "tax_year": int(row["tax_year"]),
        "flag_type": str(row["flag_type"]),
        "status": str(row["status"]),
        "reason": row["reason"],
        "source": str(row["source"]),
        "payment_snapshot": snapshot if isinstance(snapshot, dict) else {},
        "resolved_at": row["resolved_at"],
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _select_flag(
    conn: sqlite3.Connection,
    *,
    contractor_id: str,
    tax_year: int,
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT f.id, f.contractor_id, c.name AS contractor_name, f.tax_year,
               f.flag_type, f.status, f.reason, f.source, f.payment_snapshot_json,
               f.resolved_at, f.created_at, f.updated_at
          FROM contractor_tax_prep_flags f
          JOIN contractors c ON c.id = f.contractor_id
         WHERE f.contractor_id = ?
           AND f.tax_year = ?
           AND f.flag_type = ?
         LIMIT 1
        """,
        (contractor_id, tax_year, _FLAG_TYPE),
    ).fetchone()
    if row is None:
        raise RuntimeError("contractor tax-prep flag was not written")
    return _flag_row_to_dict(row)


def flag_contractor_january_prep(
    conn: sqlite3.Connection,
    *,
    contractor_id: str,
    tax_year: int | str | None = None,
    reason: str = "",
    source: str = "agent",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Create or reactivate a January 1099 prep flag for a contractor."""
    normalized_tax_year = _parse_tax_year(tax_year)
    normalized_source = _normalize_source(source)
    contractor = _load_contractor(conn, contractor_id)
    contractor_payload = _contractor_to_dict(contractor)
    snapshot = _snapshot_with_contractor(
        conn,
        contractor=contractor,
        tax_year=normalized_tax_year,
    )
    reason_value = str(reason or "").strip() or _default_reason(snapshot)
    preview = {
        "id": None,
        "contractor_id": contractor_payload["id"],
        "contractor_name": contractor_payload["name"],
        "tax_year": normalized_tax_year,
        "flag_type": _FLAG_TYPE,
        "status": "active",
        "reason": reason_value,
        "source": normalized_source,
        "payment_snapshot": snapshot,
        "resolved_at": None,
    }
    if dry_run:
        return {
            "data": {
                "flag": preview,
                "contractor": contractor_payload,
                "payment_snapshot": snapshot,
                "dry_run": True,
            },
            "summary": {
                "flagged": 0,
                "dry_run": True,
                "tax_year": normalized_tax_year,
                "contractor_id": contractor_payload["id"],
                "contractor_name": contractor_payload["name"],
                "non_card_paid_cents": int(snapshot["non_card_paid_cents"]),
                "requires_1099": bool(snapshot["requires_1099"]),
                "approaching_1099_threshold": bool(snapshot["approaching_1099_threshold"]),
            },
        }

    conn.execute(
        """
        INSERT INTO contractor_tax_prep_flags (
            id, contractor_id, tax_year, flag_type, status, reason, source,
            payment_snapshot_json, resolved_at
        )
        VALUES (?, ?, ?, ?, 'active', ?, ?, ?, NULL)
        ON CONFLICT(contractor_id, tax_year, flag_type) DO UPDATE SET
            status = 'active',
            reason = excluded.reason,
            source = excluded.source,
            payment_snapshot_json = excluded.payment_snapshot_json,
            resolved_at = NULL,
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            contractor_payload["id"],
            normalized_tax_year,
            _FLAG_TYPE,
            reason_value,
            normalized_source,
            json.dumps(snapshot, sort_keys=True),
        ),
    )
    conn.commit()
    flag = _select_flag(
        conn,
        contractor_id=contractor_payload["id"],
        tax_year=normalized_tax_year,
    )
    return {
        "data": {
            "flag": flag,
            "contractor": contractor_payload,
            "payment_snapshot": snapshot,
            "dry_run": False,
        },
        "summary": {
            "flagged": 1,
            "dry_run": False,
            "tax_year": normalized_tax_year,
            "contractor_id": contractor_payload["id"],
            "contractor_name": contractor_payload["name"],
            "non_card_paid_cents": int(snapshot["non_card_paid_cents"]),
            "requires_1099": bool(snapshot["requires_1099"]),
            "approaching_1099_threshold": bool(snapshot["approaching_1099_threshold"]),
            "id": flag["id"],
        },
    }


def list_contractor_january_prep_flags(
    conn: sqlite3.Connection,
    *,
    tax_year: int | str | None = None,
    status: str | None = "active",
    limit: int = 100,
) -> dict[str, Any]:
    """List January 1099 prep flags."""
    normalized_status = None if status in (None, "", "all") else _normalize_status(status)
    normalized_tax_year = None if tax_year in (None, "") else _parse_tax_year(tax_year)
    limit = max(1, min(500, int(limit or 100)))
    clauses = ["f.flag_type = ?"]
    params: list[Any] = [_FLAG_TYPE]
    if normalized_status is not None:
        clauses.append("f.status = ?")
        params.append(normalized_status)
    if normalized_tax_year is not None:
        clauses.append("f.tax_year = ?")
        params.append(normalized_tax_year)
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT f.id, f.contractor_id, c.name AS contractor_name, f.tax_year,
               f.flag_type, f.status, f.reason, f.source, f.payment_snapshot_json,
               f.resolved_at, f.created_at, f.updated_at
          FROM contractor_tax_prep_flags f
          JOIN contractors c ON c.id = f.contractor_id
         WHERE {' AND '.join(clauses)}
         ORDER BY f.tax_year DESC, f.status ASC, f.updated_at DESC
         LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    flags = [_flag_row_to_dict(row) for row in rows]
    return {
        "data": {"flags": flags},
        "summary": {
            "count": len(flags),
            "tax_year": normalized_tax_year,
            "status": normalized_status or "all",
        },
    }
