"""Retirement contribution target helpers."""

from __future__ import annotations

from datetime import date
import json
import re
import sqlite3
from typing import Any
import uuid

from finance_cli.exceptions import ValidationError
from finance_cli.models import cents_to_dollars

_YEAR_RE = re.compile(r"^\d{4}$")
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_VALID_SOURCES = {"user", "agent", "system"}
_ACCOUNT_ALIASES = {
    "roth": "roth_ira",
    "roth_ira": "roth_ira",
    "roth ira": "roth_ira",
    "traditional": "traditional_ira",
    "traditional_ira": "traditional_ira",
    "traditional ira": "traditional_ira",
    "sep": "sep_ira",
    "sep_ira": "sep_ira",
    "sep ira": "sep_ira",
    "solo_401k": "solo_401k",
    "solo 401k": "solo_401k",
    "solo 401(k)": "solo_401k",
    "401k": "employer_401k",
    "401(k)": "employer_401k",
    "employer_401k": "employer_401k",
    "employer 401k": "employer_401k",
    "employer 401(k)": "employer_401k",
    "other": "other_retirement",
    "other_retirement": "other_retirement",
}


def _validate_year(value: Any) -> int:
    raw = str(value or "").strip()
    if not _YEAR_RE.match(raw):
        raise ValidationError("tax_year must be in YYYY format")
    year = int(raw)
    if year < 2000 or year > 2100:
        raise ValidationError("tax_year must be between 2000 and 2100")
    return year


def _normalize_account_type(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    normalized = " ".join(normalized.split())
    try:
        return _ACCOUNT_ALIASES[normalized]
    except KeyError as exc:
        expected = ", ".join(
            ["roth_ira", "traditional_ira", "sep_ira", "solo_401k", "employer_401k", "other_retirement"]
        )
        raise ValidationError(f"account_type must be one of: {expected}") from exc


def _coerce_positive_cents(value: Any, *, field_name: str) -> int:
    try:
        cents = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer number of cents") from exc
    if cents <= 0:
        raise ValidationError(f"{field_name} must be greater than 0")
    return cents


def _coerce_nonnegative_optional_cents(value: Any, *, field_name: str) -> int | None:
    if value in (None, ""):
        return None
    try:
        cents = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer number of cents") from exc
    if cents < 0:
        raise ValidationError(f"{field_name} must be greater than or equal to 0")
    return cents


def _validate_month(value: Any, *, field_name: str, tax_year: int) -> str:
    raw = str(value or "").strip()
    if not _MONTH_RE.match(raw):
        raise ValidationError(f"{field_name} must be in YYYY-MM format")
    year_str, month_str = raw.split("-")
    month = int(month_str)
    if month < 1 or month > 12:
        raise ValidationError(f"{field_name} must have a month between 01 and 12")
    if int(year_str) != tax_year:
        raise ValidationError(f"{field_name} must be in tax_year {tax_year}")
    return raw


def _month_range(start_month: str, end_month: str) -> list[str]:
    start_year, start = [int(part) for part in start_month.split("-")]
    end_year, end = [int(part) for part in end_month.split("-")]
    start_index = start_year * 12 + start - 1
    end_index = end_year * 12 + end - 1
    if end_index < start_index:
        raise ValidationError("end_month must be greater than or equal to start_month")
    months: list[str] = []
    for index in range(start_index, end_index + 1):
        year, month_zero = divmod(index, 12)
        months.append(f"{year:04d}-{month_zero + 1:02d}")
    return months


def _normalize_source(source: str | None) -> str:
    normalized = str(source or "agent").strip().lower()
    if normalized not in _VALID_SOURCES:
        expected = ", ".join(sorted(_VALID_SOURCES))
        raise ValidationError(f"source must be one of: {expected}")
    return normalized


def _validate_optional_deadline(value: str | None, *, tax_year: int) -> str:
    raw = str(value or "").strip() or f"{tax_year}-12-31"
    try:
        date.fromisoformat(raw)
    except ValueError as exc:
        raise ValidationError("deadline must be in YYYY-MM-DD format") from exc
    return raw


def _validate_contribution_room(
    *,
    total_planned_cents: int,
    room_remaining_cents: int | None,
    annual_limit_cents: int | None,
    contributed_ytd_cents: int | None,
) -> None:
    if (
        room_remaining_cents is not None
        and total_planned_cents > room_remaining_cents
    ):
        raise ValidationError(
            "monthly target exceeds room_remaining_cents for the selected month range"
        )
    if (
        annual_limit_cents is not None
        and room_remaining_cents is not None
        and room_remaining_cents > annual_limit_cents
    ):
        raise ValidationError("room_remaining_cents cannot exceed annual_limit_cents")
    if (
        annual_limit_cents is not None
        and contributed_ytd_cents is not None
        and contributed_ytd_cents > annual_limit_cents
    ):
        raise ValidationError("contributed_ytd_cents cannot exceed annual_limit_cents")
    if (
        annual_limit_cents is not None
        and contributed_ytd_cents is not None
        and room_remaining_cents is not None
        and contributed_ytd_cents + room_remaining_cents > annual_limit_cents
    ):
        raise ValidationError(
            "contributed_ytd_cents plus room_remaining_cents cannot exceed annual_limit_cents"
        )


def _select_target_by_key(conn: sqlite3.Connection, idempotency_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT id, tax_year, account_type, status, monthly_target_cents,
               start_month, end_month, room_remaining_cents, annual_limit_cents,
               contributed_ytd_cents, estimated_tax_savings_cents, deadline,
               reason, source, payload_json, idempotency_key, resolved_at,
               created_at, updated_at
          FROM retirement_contribution_targets
         WHERE idempotency_key = ?
         LIMIT 1
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("retirement contribution target was not written")
    return _row_to_dict(row)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload_raw = row["payload_json"] or "{}"
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        payload = {}
    return {
        "id": str(row["id"]),
        "tax_year": int(row["tax_year"]),
        "account_type": str(row["account_type"]),
        "status": str(row["status"]),
        "monthly_target_cents": int(row["monthly_target_cents"]),
        "start_month": str(row["start_month"]),
        "end_month": str(row["end_month"]),
        "room_remaining_cents": row["room_remaining_cents"],
        "annual_limit_cents": row["annual_limit_cents"],
        "contributed_ytd_cents": row["contributed_ytd_cents"],
        "estimated_tax_savings_cents": row["estimated_tax_savings_cents"],
        "deadline": row["deadline"],
        "reason": row["reason"],
        "source": str(row["source"]),
        "payload": payload if isinstance(payload, dict) else {},
        "idempotency_key": str(row["idempotency_key"]),
        "resolved_at": row["resolved_at"],
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _upsert_monthly_plan_targets(
    conn: sqlite3.Connection,
    *,
    months: list[str],
    monthly_target_cents: int,
) -> None:
    for month in months:
        conn.execute(
            """
            INSERT INTO monthly_plans (id, month, investment_target_cents)
            VALUES (?, ?, ?)
            ON CONFLICT(month) DO UPDATE SET
                investment_target_cents = excluded.investment_target_cents
            """,
            (uuid.uuid4().hex, month, monthly_target_cents),
        )


def set_monthly_retirement_target(
    conn: sqlite3.Connection,
    *,
    tax_year: Any,
    account_type: Any,
    monthly_target_cents: Any,
    start_month: Any,
    end_month: Any,
    room_remaining_cents: Any = None,
    annual_limit_cents: Any = None,
    contributed_ytd_cents: Any = None,
    estimated_tax_savings_cents: Any = None,
    deadline: str | None = None,
    reason: str = "",
    source: str = "agent",
    update_monthly_plans: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    year = _validate_year(tax_year)
    account = _normalize_account_type(account_type)
    monthly_target = _coerce_positive_cents(
        monthly_target_cents,
        field_name="monthly_target_cents",
    )
    start = _validate_month(start_month, field_name="start_month", tax_year=year)
    end = _validate_month(end_month, field_name="end_month", tax_year=year)
    months = _month_range(start, end)
    room_remaining = _coerce_nonnegative_optional_cents(
        room_remaining_cents,
        field_name="room_remaining_cents",
    )
    annual_limit = _coerce_nonnegative_optional_cents(
        annual_limit_cents,
        field_name="annual_limit_cents",
    )
    contributed_ytd = _coerce_nonnegative_optional_cents(
        contributed_ytd_cents,
        field_name="contributed_ytd_cents",
    )
    estimated_tax_savings = _coerce_nonnegative_optional_cents(
        estimated_tax_savings_cents,
        field_name="estimated_tax_savings_cents",
    )
    total_planned_cents = monthly_target * len(months)
    _validate_contribution_room(
        total_planned_cents=total_planned_cents,
        room_remaining_cents=room_remaining,
        annual_limit_cents=annual_limit,
        contributed_ytd_cents=contributed_ytd,
    )
    normalized_source = _normalize_source(source)
    normalized_reason = " ".join(str(reason or "").split())[:240]
    normalized_deadline = _validate_optional_deadline(deadline, tax_year=year)
    payload = {
        "months": months,
        "months_count": len(months),
        "total_planned_cents": total_planned_cents,
        "update_monthly_plans": bool(update_monthly_plans),
        "created_on": date.today().isoformat(),
    }
    idempotency_key = f"retirement_target:{year}:{account}:{start}:{end}"
    preview = {
        "id": None,
        "tax_year": year,
        "account_type": account,
        "status": "active",
        "monthly_target_cents": monthly_target,
        "start_month": start,
        "end_month": end,
        "room_remaining_cents": room_remaining,
        "annual_limit_cents": annual_limit,
        "contributed_ytd_cents": contributed_ytd,
        "estimated_tax_savings_cents": estimated_tax_savings,
        "deadline": normalized_deadline,
        "reason": normalized_reason,
        "source": normalized_source,
        "payload": payload,
        "idempotency_key": idempotency_key,
    }
    summary = {
        "set": 0 if dry_run else 1,
        "dry_run": bool(dry_run),
        "monthly_target_cents": monthly_target,
        "months_count": len(months),
        "total_planned_cents": total_planned_cents,
        "monthly_plans_updated": 0 if dry_run or not update_monthly_plans else len(months),
    }
    if dry_run:
        return {
            "data": {"target": preview, "dry_run": True},
            "summary": summary,
            "cli_report": (
                f"[DRY RUN] Would set {account} retirement target for {year}: "
                f"${cents_to_dollars(monthly_target):,.2f}/mo from {start} to {end}"
            ),
        }

    conn.execute(
        """
        INSERT INTO retirement_contribution_targets (
            id, tax_year, account_type, status, monthly_target_cents,
            start_month, end_month, room_remaining_cents, annual_limit_cents,
            contributed_ytd_cents, estimated_tax_savings_cents, deadline,
            reason, source, payload_json, idempotency_key
        ) VALUES (?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(idempotency_key) DO UPDATE SET
            tax_year = excluded.tax_year,
            account_type = excluded.account_type,
            status = 'active',
            monthly_target_cents = excluded.monthly_target_cents,
            start_month = excluded.start_month,
            end_month = excluded.end_month,
            room_remaining_cents = excluded.room_remaining_cents,
            annual_limit_cents = excluded.annual_limit_cents,
            contributed_ytd_cents = excluded.contributed_ytd_cents,
            estimated_tax_savings_cents = excluded.estimated_tax_savings_cents,
            deadline = excluded.deadline,
            reason = excluded.reason,
            source = excluded.source,
            payload_json = excluded.payload_json,
            resolved_at = NULL,
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            year,
            account,
            monthly_target,
            start,
            end,
            room_remaining,
            annual_limit,
            contributed_ytd,
            estimated_tax_savings,
            normalized_deadline,
            normalized_reason,
            normalized_source,
            json.dumps(payload, sort_keys=True),
            idempotency_key,
        ),
    )
    if update_monthly_plans:
        _upsert_monthly_plan_targets(
            conn,
            months=months,
            monthly_target_cents=monthly_target,
        )
    conn.commit()
    target = _select_target_by_key(conn, idempotency_key)
    summary["id"] = target["id"]
    return {
        "data": {"target": target, "dry_run": False},
        "summary": summary,
        "cli_report": (
            f"Set {account} retirement target for {year}: "
            f"${cents_to_dollars(monthly_target):,.2f}/mo from {start} to {end}"
        ),
    }


def setup_monthly_transfer_goal(
    conn: sqlite3.Connection,
    *,
    tax_year: Any,
    monthly_transfer_cents: Any,
    room_remaining_cents: Any,
    start_month: Any,
    end_month: Any,
    account_type: Any = "roth_ira",
    annual_limit_cents: Any = None,
    contributed_ytd_cents: Any = None,
    estimated_tax_savings_cents: Any = None,
    reason: str = "",
    source: str = "agent",
    update_monthly_plans: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    normalized_reason = " ".join(str(reason or "").split())
    if not normalized_reason:
        normalized_reason = "Monthly retirement transfer goal."
    return set_monthly_retirement_target(
        conn,
        tax_year=tax_year,
        account_type=account_type,
        monthly_target_cents=monthly_transfer_cents,
        start_month=start_month,
        end_month=end_month,
        room_remaining_cents=room_remaining_cents,
        annual_limit_cents=annual_limit_cents,
        contributed_ytd_cents=contributed_ytd_cents,
        estimated_tax_savings_cents=estimated_tax_savings_cents,
        reason=normalized_reason,
        source=source,
        update_monthly_plans=update_monthly_plans,
        dry_run=dry_run,
    )


def handle_set(args, conn: sqlite3.Connection) -> dict[str, Any]:
    return set_monthly_retirement_target(
        conn,
        tax_year=getattr(args, "tax_year", None),
        account_type=getattr(args, "account_type", None),
        monthly_target_cents=getattr(args, "monthly_target_cents", None),
        start_month=getattr(args, "start_month", None),
        end_month=getattr(args, "end_month", None),
        room_remaining_cents=getattr(args, "room_remaining_cents", None),
        annual_limit_cents=getattr(args, "annual_limit_cents", None),
        contributed_ytd_cents=getattr(args, "contributed_ytd_cents", None),
        estimated_tax_savings_cents=getattr(args, "estimated_tax_savings_cents", None),
        deadline=getattr(args, "deadline", None),
        reason=getattr(args, "reason", ""),
        source=getattr(args, "source", "agent"),
        update_monthly_plans=bool(getattr(args, "update_monthly_plans", True)),
        dry_run=bool(getattr(args, "dry_run", False)),
    )


def handle_transfer_goal(args, conn: sqlite3.Connection) -> dict[str, Any]:
    return setup_monthly_transfer_goal(
        conn,
        tax_year=getattr(args, "tax_year", None),
        monthly_transfer_cents=getattr(args, "monthly_transfer_cents", None),
        room_remaining_cents=getattr(args, "room_remaining_cents", None),
        start_month=getattr(args, "start_month", None),
        end_month=getattr(args, "end_month", None),
        account_type=getattr(args, "account_type", "roth_ira"),
        annual_limit_cents=getattr(args, "annual_limit_cents", None),
        contributed_ytd_cents=getattr(args, "contributed_ytd_cents", None),
        estimated_tax_savings_cents=getattr(args, "estimated_tax_savings_cents", None),
        reason=getattr(args, "reason", ""),
        source=getattr(args, "source", "agent"),
        update_monthly_plans=bool(getattr(args, "update_monthly_plans", True)),
        dry_run=bool(getattr(args, "dry_run", False)),
    )
