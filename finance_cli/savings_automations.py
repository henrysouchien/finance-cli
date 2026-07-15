"""Durable savings automation plan helpers."""

from __future__ import annotations

from datetime import date
import json
import sqlite3
from typing import Any
import uuid

from finance_cli.exceptions import ValidationError
from finance_cli.models import cents_to_dollars

_VALID_FUNDING_METHODS = {
    "auto_transfer",
    "paycheck_split",
    "percentage_of_paycheck",
    "windfall_capture",
    "hybrid",
}
_VALID_CADENCES = {"weekly", "biweekly", "monthly", "paycheck"}
_VALID_SOURCES = {"user", "agent", "system"}
_SOURCE_ACCOUNT_TYPES = {"checking"}
_DESTINATION_ACCOUNT_TYPES = {"savings", "investment"}


def _coerce_positive_cents(value: Any, *, field_name: str) -> int:
    try:
        cents = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer number of cents") from exc
    if cents <= 0:
        raise ValidationError(f"{field_name} must be greater than 0")
    return cents


def _coerce_optional_nonnegative_cents(value: Any, *, field_name: str) -> int | None:
    if value in (None, ""):
        return None
    try:
        cents = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer number of cents") from exc
    if cents < 0:
        raise ValidationError(f"{field_name} must be greater than or equal to 0")
    return cents


def _parse_date(value: Any, *, field_name: str, required: bool = True) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        if required:
            raise ValidationError(f"{field_name} is required")
        return None
    try:
        parsed = date.fromisoformat(raw[:10])
    except ValueError as exc:
        raise ValidationError(f"{field_name} must be in YYYY-MM-DD format") from exc
    return parsed.isoformat()


def _normalize_choice(value: Any, *, field_name: str, expected: set[str]) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    normalized = " ".join(normalized.split()).replace(" ", "_")
    if normalized not in expected:
        choices = ", ".join(sorted(expected))
        raise ValidationError(f"{field_name} must be one of: {choices}")
    return normalized


def _normalize_source(value: str | None) -> str:
    return _normalize_choice(value or "agent", field_name="source", expected=_VALID_SOURCES)


def _coerce_day_of_month(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        day = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError("day_of_month must be an integer") from exc
    if day < 1 or day > 31:
        raise ValidationError("day_of_month must be between 1 and 31")
    return day


def _load_goal(conn: sqlite3.Connection, goal_id: str) -> sqlite3.Row:
    normalized = str(goal_id or "").strip()
    if not normalized:
        raise ValidationError("goal_id is required")
    row = conn.execute(
        """
        SELECT id, name, metric, target_cents, target_pct, deadline, is_active
          FROM goals
         WHERE id = ?
         LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"goal not found: {normalized}")
    if int(row["is_active"] or 0) != 1:
        raise ValidationError("goal must be active")
    return row


def _load_account(
    conn: sqlite3.Connection,
    account_id: str | None,
    *,
    field_name: str,
    allowed_types: set[str],
) -> sqlite3.Row | None:
    normalized = str(account_id or "").strip()
    if not normalized:
        return None
    row = conn.execute(
        """
        SELECT id, institution_name, account_name, account_type,
               balance_current_cents, is_active
          FROM accounts
         WHERE id = ?
         LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"{field_name} not found: {normalized}")
    if int(row["is_active"] or 0) != 1:
        raise ValidationError(f"{field_name} must be active")
    account_type = str(row["account_type"] or "")
    if account_type not in allowed_types:
        choices = ", ".join(sorted(allowed_types))
        raise ValidationError(f"{field_name} must be one of account types: {choices}")
    return row


def _account_label(row: sqlite3.Row | None) -> str | None:
    if row is None:
        return None
    institution = str(row["institution_name"] or "").strip()
    account_name = str(row["account_name"] or "").strip()
    label = " ".join(part for part in (institution, account_name) if part).strip()
    return label or str(row["id"])


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    snapshot_raw = row["snapshot_json"] or "{}"
    try:
        snapshot = json.loads(snapshot_raw)
    except json.JSONDecodeError:
        snapshot = {}
    return {
        "id": str(row["id"]),
        "goal_id": str(row["goal_id"]),
        "status": str(row["status"]),
        "funding_method": str(row["funding_method"]),
        "cadence": str(row["cadence"]),
        "amount_cents": int(row["amount_cents"]),
        "start_date": str(row["start_date"]),
        "day_of_month": row["day_of_month"],
        "source_account_id": row["source_account_id"],
        "destination_account_id": row["destination_account_id"],
        "target_amount_cents": row["target_amount_cents"],
        "projected_end_balance_cents": row["projected_end_balance_cents"],
        "goal_date": row["goal_date"],
        "reason": row["reason"],
        "source": str(row["source"]),
        "snapshot": snapshot if isinstance(snapshot, dict) else {},
        "idempotency_key": str(row["idempotency_key"]),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _select_automation(conn: sqlite3.Connection, idempotency_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT id, goal_id, status, funding_method, cadence, amount_cents,
               start_date, day_of_month, source_account_id, destination_account_id,
               target_amount_cents, projected_end_balance_cents, goal_date,
               reason, source, snapshot_json, idempotency_key, created_at, updated_at
          FROM savings_automations
         WHERE idempotency_key = ?
         LIMIT 1
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("savings automation was not written")
    return _row_to_dict(row)


def setup_savings_automation(
    conn: sqlite3.Connection,
    *,
    goal_id: str,
    amount_cents: Any,
    start_date: Any,
    cadence: str = "monthly",
    funding_method: str = "auto_transfer",
    day_of_month: Any = None,
    source_account_id: str | None = None,
    destination_account_id: str | None = None,
    target_amount_cents: Any = None,
    projected_end_balance_cents: Any = None,
    goal_date: Any = None,
    reason: str = "",
    source: str = "agent",
    dry_run: bool = False,
) -> dict[str, Any]:
    goal = _load_goal(conn, goal_id)
    amount = _coerce_positive_cents(amount_cents, field_name="amount_cents")
    normalized_start = _parse_date(start_date, field_name="start_date")
    normalized_goal_date = _parse_date(goal_date, field_name="goal_date", required=False)
    if normalized_goal_date is None and goal["deadline"]:
        normalized_goal_date = _parse_date(goal["deadline"], field_name="goal.deadline", required=False)
    normalized_cadence = _normalize_choice(
        cadence,
        field_name="cadence",
        expected=_VALID_CADENCES,
    )
    normalized_method = _normalize_choice(
        funding_method,
        field_name="funding_method",
        expected=_VALID_FUNDING_METHODS,
    )
    day = _coerce_day_of_month(day_of_month)
    if normalized_cadence == "monthly" and day is None:
        day = date.fromisoformat(normalized_start).day
    target_amount = _coerce_optional_nonnegative_cents(
        target_amount_cents if target_amount_cents not in (None, "") else goal["target_cents"],
        field_name="target_amount_cents",
    )
    projected_end_balance = _coerce_optional_nonnegative_cents(
        projected_end_balance_cents,
        field_name="projected_end_balance_cents",
    )
    source_account = _load_account(
        conn,
        source_account_id,
        field_name="source_account_id",
        allowed_types=_SOURCE_ACCOUNT_TYPES,
    )
    destination_account = _load_account(
        conn,
        destination_account_id,
        field_name="destination_account_id",
        allowed_types=_DESTINATION_ACCOUNT_TYPES,
    )
    normalized_source = _normalize_source(source)
    normalized_reason = " ".join(str(reason or "").split())[:240]
    snapshot = {
        "goal_name": str(goal["name"] or ""),
        "goal_metric": str(goal["metric"] or ""),
        "goal_deadline": goal["deadline"],
        "amount_cents": amount,
        "cadence": normalized_cadence,
        "funding_method": normalized_method,
        "source_account_label": _account_label(source_account),
        "destination_account_label": _account_label(destination_account),
        "target_amount_cents": target_amount,
        "projected_end_balance_cents": projected_end_balance,
        "goal_date": normalized_goal_date,
    }
    idempotency_key = f"savings_automation:{goal['id']}"
    preview = {
        "id": None,
        "goal_id": str(goal["id"]),
        "status": "active",
        "funding_method": normalized_method,
        "cadence": normalized_cadence,
        "amount_cents": amount,
        "start_date": normalized_start,
        "day_of_month": day,
        "source_account_id": source_account_id or None,
        "destination_account_id": destination_account_id or None,
        "target_amount_cents": target_amount,
        "projected_end_balance_cents": projected_end_balance,
        "goal_date": normalized_goal_date,
        "reason": normalized_reason,
        "source": normalized_source,
        "snapshot": snapshot,
        "idempotency_key": idempotency_key,
    }
    summary = {
        "configured": 0 if dry_run else 1,
        "dry_run": bool(dry_run),
        "goal_id": str(goal["id"]),
        "amount_cents": amount,
        "cadence": normalized_cadence,
        "funding_method": normalized_method,
    }
    if dry_run:
        return {
            "data": {"automation": preview, "dry_run": True},
            "summary": summary,
            "cli_report": (
                f"[DRY RUN] Would set savings automation for {goal['name']}: "
                f"${cents_to_dollars(amount):,.2f} {normalized_cadence}"
            ),
        }

    conn.execute(
        """
        INSERT INTO savings_automations (
            id, goal_id, status, funding_method, cadence, amount_cents,
            start_date, day_of_month, source_account_id, destination_account_id,
            target_amount_cents, projected_end_balance_cents, goal_date,
            reason, source, snapshot_json, idempotency_key
        ) VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(idempotency_key) DO UPDATE SET
            goal_id = excluded.goal_id,
            status = 'active',
            funding_method = excluded.funding_method,
            cadence = excluded.cadence,
            amount_cents = excluded.amount_cents,
            start_date = excluded.start_date,
            day_of_month = excluded.day_of_month,
            source_account_id = excluded.source_account_id,
            destination_account_id = excluded.destination_account_id,
            target_amount_cents = excluded.target_amount_cents,
            projected_end_balance_cents = excluded.projected_end_balance_cents,
            goal_date = excluded.goal_date,
            reason = excluded.reason,
            source = excluded.source,
            snapshot_json = excluded.snapshot_json,
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            str(goal["id"]),
            normalized_method,
            normalized_cadence,
            amount,
            normalized_start,
            day,
            str(source_account["id"]) if source_account is not None else None,
            str(destination_account["id"]) if destination_account is not None else None,
            target_amount,
            projected_end_balance,
            normalized_goal_date,
            normalized_reason,
            normalized_source,
            json.dumps(snapshot, sort_keys=True),
            idempotency_key,
        ),
    )
    conn.commit()
    automation = _select_automation(conn, idempotency_key)
    summary["id"] = automation["id"]
    return {
        "data": {"automation": automation, "dry_run": False},
        "summary": summary,
        "cli_report": (
            f"Set savings automation for {goal['name']}: "
            f"${cents_to_dollars(amount):,.2f} {normalized_cadence}"
        ),
    }


def handle_setup(args, conn: sqlite3.Connection) -> dict[str, Any]:
    return setup_savings_automation(
        conn,
        goal_id=getattr(args, "goal_id", ""),
        amount_cents=getattr(args, "amount_cents", None),
        start_date=getattr(args, "start_date", None),
        cadence=getattr(args, "cadence", "monthly"),
        funding_method=getattr(args, "funding_method", "auto_transfer"),
        day_of_month=getattr(args, "day_of_month", None),
        source_account_id=getattr(args, "source_account_id", None),
        destination_account_id=getattr(args, "destination_account_id", None),
        target_amount_cents=getattr(args, "target_amount_cents", None),
        projected_end_balance_cents=getattr(args, "projected_end_balance_cents", None),
        goal_date=getattr(args, "goal_date", None),
        reason=getattr(args, "reason", ""),
        source=getattr(args, "source", "agent"),
        dry_run=bool(getattr(args, "dry_run", False)),
    )
