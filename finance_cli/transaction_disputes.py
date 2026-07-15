"""Transaction dispute workflow helpers."""

from __future__ import annotations

from datetime import date
import json
import sqlite3
from typing import Any
import uuid

from finance_cli.exceptions import ValidationError
from finance_cli.models import cents_to_dollars

_VALID_REASONS = {
    "duplicate_charge",
    "unrecognized_merchant",
    "incorrect_amount",
    "unauthorized",
    "other",
}
_VALID_SOURCES = {"user", "agent", "system"}


def _normalize_choice(value: Any, *, field_name: str, expected: set[str]) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    normalized = " ".join(normalized.split()).replace(" ", "_")
    if normalized not in expected:
        choices = ", ".join(sorted(expected))
        raise ValidationError(f"{field_name} must be one of: {choices}")
    return normalized


def _normalize_source(value: str | None) -> str:
    return _normalize_choice(value or "agent", field_name="source", expected=_VALID_SOURCES)


def _parse_txn_date(value: Any, *, field_name: str) -> date:
    raw = str(value or "").strip()
    try:
        return date.fromisoformat(raw[:10])
    except ValueError as exc:
        raise ValidationError(f"{field_name} must be in YYYY-MM-DD format") from exc


def _load_transaction(conn: sqlite3.Connection, transaction_id: str) -> sqlite3.Row:
    normalized = str(transaction_id or "").strip()
    if not normalized:
        raise ValidationError("transaction_id is required")
    row = conn.execute(
        """
        SELECT t.id, t.account_id, t.date, t.description, t.amount_cents,
               t.is_active, a.account_type, a.institution_name, a.account_name
          FROM transactions t
          LEFT JOIN accounts a ON a.id = t.account_id
         WHERE t.id = ?
         LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"transaction not found: {normalized}")
    if int(row["is_active"] or 0) != 1:
        raise ValidationError("transaction must be active")
    if int(row["amount_cents"] or 0) >= 0:
        raise ValidationError("dispute workflows require an expense transaction")
    return row


def _validate_duplicate(primary: sqlite3.Row, duplicate: sqlite3.Row | None) -> None:
    if duplicate is None:
        raise ValidationError("duplicate_transaction_id is required for duplicate_charge")
    if str(primary["id"]) == str(duplicate["id"]):
        raise ValidationError("duplicate_transaction_id must be different from transaction_id")
    if str(primary["account_id"] or "") != str(duplicate["account_id"] or ""):
        raise ValidationError("duplicate transactions must be on the same account")
    primary_amount = abs(int(primary["amount_cents"] or 0))
    duplicate_amount = abs(int(duplicate["amount_cents"] or 0))
    if primary_amount != duplicate_amount:
        raise ValidationError("duplicate transactions must have the same amount")
    primary_date = _parse_txn_date(primary["date"], field_name="transaction.date")
    duplicate_date = _parse_txn_date(duplicate["date"], field_name="duplicate.date")
    if abs((primary_date - duplicate_date).days) > 7:
        raise ValidationError("duplicate transactions must be within 7 days")


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    snapshot_raw = row["snapshot_json"] or "{}"
    try:
        snapshot = json.loads(snapshot_raw)
    except json.JSONDecodeError:
        snapshot = {}
    return {
        "id": str(row["id"]),
        "transaction_id": str(row["transaction_id"]),
        "duplicate_transaction_id": row["duplicate_transaction_id"],
        "account_id": row["account_id"],
        "status": str(row["status"]),
        "dispute_reason": str(row["dispute_reason"]),
        "amount_cents": int(row["amount_cents"]),
        "merchant_name": str(row["merchant_name"]),
        "transaction_date": str(row["transaction_date"]),
        "duplicate_date": row["duplicate_date"],
        "note": row["note"],
        "source": str(row["source"]),
        "snapshot": snapshot if isinstance(snapshot, dict) else {},
        "idempotency_key": str(row["idempotency_key"]),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _select_workflow(conn: sqlite3.Connection, idempotency_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT id, transaction_id, duplicate_transaction_id, account_id, status,
               dispute_reason, amount_cents, merchant_name, transaction_date,
               duplicate_date, note, source, snapshot_json, idempotency_key,
               created_at, updated_at
          FROM transaction_dispute_workflows
         WHERE idempotency_key = ?
         LIMIT 1
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("transaction dispute workflow was not written")
    return _row_to_dict(row)


def txn_dispute_workflow(
    conn: sqlite3.Connection,
    *,
    transaction_id: str,
    dispute_reason: str = "duplicate_charge",
    duplicate_transaction_id: str | None = None,
    note: str = "",
    source: str = "agent",
    dry_run: bool = False,
) -> dict[str, Any]:
    reason = _normalize_choice(
        dispute_reason,
        field_name="dispute_reason",
        expected=_VALID_REASONS,
    )
    transaction = _load_transaction(conn, transaction_id)
    duplicate = (
        _load_transaction(conn, duplicate_transaction_id)
        if duplicate_transaction_id
        else None
    )
    if reason == "duplicate_charge":
        _validate_duplicate(transaction, duplicate)
    normalized_source = _normalize_source(source)
    normalized_note = " ".join(str(note or "").split())[:240]
    amount_cents = abs(int(transaction["amount_cents"] or 0))
    transaction_day = _parse_txn_date(transaction["date"], field_name="transaction.date")
    duplicate_day = (
        _parse_txn_date(duplicate["date"], field_name="duplicate.date")
        if duplicate is not None
        else None
    )
    snapshot = {
        "transaction": {
            "id": str(transaction["id"]),
            "date": transaction_day.isoformat(),
            "description": str(transaction["description"] or ""),
            "amount_cents": amount_cents,
            "account_id": transaction["account_id"],
            "account_type": transaction["account_type"],
        },
        "duplicate": (
            {
                "id": str(duplicate["id"]),
                "date": duplicate_day.isoformat() if duplicate_day else None,
                "description": str(duplicate["description"] or ""),
                "amount_cents": abs(int(duplicate["amount_cents"] or 0)),
            }
            if duplicate is not None
            else None
        ),
        "next_steps": [
            "Review the transaction details with the user.",
            "Use the card or bank issuer's dispute process if the user confirms the charge is wrong.",
            "Attach receipts or merchant communication when available.",
        ],
    }
    duplicate_key = str(duplicate["id"]) if duplicate is not None else "-"
    idempotency_key = f"txn_dispute:{transaction['id']}:{duplicate_key}:{reason}"
    preview = {
        "id": None,
        "transaction_id": str(transaction["id"]),
        "duplicate_transaction_id": str(duplicate["id"]) if duplicate is not None else None,
        "account_id": transaction["account_id"],
        "status": "active",
        "dispute_reason": reason,
        "amount_cents": amount_cents,
        "merchant_name": str(transaction["description"] or ""),
        "transaction_date": transaction_day.isoformat(),
        "duplicate_date": duplicate_day.isoformat() if duplicate_day else None,
        "note": normalized_note,
        "source": normalized_source,
        "snapshot": snapshot,
        "idempotency_key": idempotency_key,
    }
    summary = {
        "prepared": 0 if dry_run else 1,
        "dry_run": bool(dry_run),
        "transaction_id": str(transaction["id"]),
        "duplicate_transaction_id": str(duplicate["id"]) if duplicate is not None else None,
        "dispute_reason": reason,
        "amount_cents": amount_cents,
    }
    if dry_run:
        return {
            "data": {"workflow": preview, "dry_run": True},
            "summary": summary,
            "cli_report": (
                f"[DRY RUN] Would prepare dispute workflow for "
                f"${cents_to_dollars(amount_cents):,.2f} charge"
            ),
        }

    conn.execute(
        """
        INSERT INTO transaction_dispute_workflows (
            id, transaction_id, duplicate_transaction_id, account_id, status,
            dispute_reason, amount_cents, merchant_name, transaction_date,
            duplicate_date, note, source, snapshot_json, idempotency_key
        ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(idempotency_key) DO UPDATE SET
            transaction_id = excluded.transaction_id,
            duplicate_transaction_id = excluded.duplicate_transaction_id,
            account_id = excluded.account_id,
            status = 'active',
            dispute_reason = excluded.dispute_reason,
            amount_cents = excluded.amount_cents,
            merchant_name = excluded.merchant_name,
            transaction_date = excluded.transaction_date,
            duplicate_date = excluded.duplicate_date,
            note = excluded.note,
            source = excluded.source,
            snapshot_json = excluded.snapshot_json,
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            str(transaction["id"]),
            str(duplicate["id"]) if duplicate is not None else None,
            transaction["account_id"],
            reason,
            amount_cents,
            str(transaction["description"] or ""),
            transaction_day.isoformat(),
            duplicate_day.isoformat() if duplicate_day else None,
            normalized_note,
            normalized_source,
            json.dumps(snapshot, sort_keys=True),
            idempotency_key,
        ),
    )
    conn.commit()
    workflow = _select_workflow(conn, idempotency_key)
    summary["id"] = workflow["id"]
    return {
        "data": {"workflow": workflow, "dry_run": False},
        "summary": summary,
        "cli_report": (
            f"Prepared dispute workflow for ${cents_to_dollars(amount_cents):,.2f} charge"
        ),
    }


def handle_workflow(args, conn: sqlite3.Connection) -> dict[str, Any]:
    return txn_dispute_workflow(
        conn,
        transaction_id=getattr(args, "transaction_id", ""),
        dispute_reason=getattr(args, "dispute_reason", "duplicate_charge"),
        duplicate_transaction_id=getattr(args, "duplicate_transaction_id", None),
        note=getattr(args, "note", ""),
        source=getattr(args, "source", "agent"),
        dry_run=bool(getattr(args, "dry_run", False)),
    )
