"""Durable HYSA transfer flag helpers."""

from __future__ import annotations

from datetime import date, timedelta
import json
import sqlite3
from typing import Any
import uuid

from finance_cli.exceptions import ValidationError
from finance_cli.models import cents_to_dollars

_VALID_SOURCES = {"user", "agent", "system"}
_MAX_SNAPSHOT_STALENESS_DAYS = 7


def _normalize_source(source: str | None) -> str:
    normalized = str(source or "agent").strip().lower()
    if normalized not in _VALID_SOURCES:
        expected = ", ".join(sorted(_VALID_SOURCES))
        raise ValidationError(f"source must be one of: {expected}")
    return normalized


def _coerce_positive_cents(value: Any, *, field_name: str) -> int:
    try:
        cents = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer number of cents") from exc
    if cents <= 0:
        raise ValidationError(f"{field_name} must be greater than 0")
    return cents


def _coerce_nonnegative_cents(value: Any, *, field_name: str) -> int:
    if value in (None, ""):
        return 0
    try:
        cents = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer number of cents") from exc
    if cents < 0:
        raise ValidationError(f"{field_name} must be greater than or equal to 0")
    return cents


def _coerce_positive_int(value: Any, *, field_name: str) -> int:
    try:
        integer = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer") from exc
    if integer <= 0:
        raise ValidationError(f"{field_name} must be greater than 0")
    return integer


def _coerce_nonnegative_int(value: Any, *, field_name: str) -> int:
    if value in (None, ""):
        return 0
    try:
        integer = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{field_name} must be an integer") from exc
    if integer < 0:
        raise ValidationError(f"{field_name} must be greater than or equal to 0")
    return integer


def _parse_as_of(value: str | date | None) -> date:
    if value is None or value == "":
        return date.today()
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    try:
        return date.fromisoformat(raw[:10])
    except ValueError as exc:
        raise ValidationError("as_of must be in YYYY-MM-DD format") from exc


def _parse_snapshot_date(value: Any) -> date:
    raw = str(value or "").strip()
    try:
        return date.fromisoformat(raw[:10])
    except ValueError as exc:
        raise ValidationError("balance_snapshots contain an invalid snapshot_date") from exc


def _account_label(row: sqlite3.Row) -> str:
    institution = str(row["institution_name"] or "").strip()
    account_name = str(row["account_name"] or "").strip()
    label = " ".join(part for part in (institution, account_name) if part).strip()
    return label or str(row["id"])


def _load_checking_account(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
    normalized = str(account_id or "").strip()
    if not normalized:
        raise ValidationError("account_id is required")
    row = conn.execute(
        """
        SELECT id, institution_name, account_name, account_type,
               balance_current_cents, is_active, is_business
          FROM accounts
         WHERE id = ?
         LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"account not found: {normalized}")
    if int(row["is_active"] or 0) != 1:
        raise ValidationError("account must be active")
    if str(row["account_type"] or "") != "checking":
        raise ValidationError("HYSA transfer flags require a checking account")
    if int(row["is_business"] or 0) == 1:
        raise ValidationError("HYSA transfer flags require a personal checking account")
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
        raise ValidationError("HYSA transfer flags require a canonical account")
    return row


def _load_balance_evidence(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    minimum_balance_cents: int,
    lookback_days: int,
    as_of: date,
) -> dict[str, Any]:
    cutoff = as_of - timedelta(days=lookback_days)
    rows = conn.execute(
        """
        SELECT snapshot_date, balance_current_cents, source
          FROM balance_snapshots
         WHERE account_id = ?
           AND snapshot_date <= ?
           AND balance_current_cents IS NOT NULL
         ORDER BY snapshot_date ASC, created_at ASC
        """,
        (account_id, as_of.isoformat()),
    ).fetchall()
    if not rows:
        raise ValidationError("balance_snapshots are required to verify the HYSA transfer flag")

    boundary: sqlite3.Row | None = None
    evidence_rows: list[sqlite3.Row] = []
    for row in rows:
        snapshot_day = _parse_snapshot_date(row["snapshot_date"])
        if snapshot_day <= cutoff:
            boundary = row
            continue
        evidence_rows.append(row)
    if boundary is None:
        raise ValidationError(
            f"balance_snapshots must include a snapshot on or before {cutoff.isoformat()}"
        )
    evidence_rows.insert(0, boundary)
    observed_balances = [int(row["balance_current_cents"] or 0) for row in evidence_rows]
    min_observed_cents = min(observed_balances)
    if min_observed_cents < minimum_balance_cents:
        raise ValidationError(
            "balance history dropped below minimum_balance_cents during the lookback window"
        )
    latest = evidence_rows[-1]
    latest_snapshot_date = _parse_snapshot_date(latest["snapshot_date"])
    freshness_cutoff = as_of - timedelta(days=_MAX_SNAPSHOT_STALENESS_DAYS)
    if latest_snapshot_date < freshness_cutoff:
        raise ValidationError(
            f"balance_snapshots must include a recent snapshot on or after "
            f"{freshness_cutoff.isoformat()}"
        )
    return {
        "observed_since": _parse_snapshot_date(boundary["snapshot_date"]).isoformat(),
        "latest_snapshot_date": latest_snapshot_date.isoformat(),
        "min_observed_balance_cents": min_observed_cents,
        "evidence_points": len(evidence_rows),
    }


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    snapshot_raw = row["snapshot_json"] or "{}"
    try:
        snapshot = json.loads(snapshot_raw)
    except json.JSONDecodeError:
        snapshot = {}
    return {
        "id": str(row["id"]),
        "account_id": str(row["account_id"]),
        "status": str(row["status"]),
        "current_balance_cents": int(row["current_balance_cents"]),
        "suggested_transfer_cents": int(row["suggested_transfer_cents"]),
        "retained_buffer_cents": int(row["retained_buffer_cents"]),
        "minimum_balance_cents": int(row["minimum_balance_cents"]),
        "current_apy_bps": int(row["current_apy_bps"]),
        "hysa_apy_bps": int(row["hysa_apy_bps"]),
        "estimated_annual_yield_cents": int(row["estimated_annual_yield_cents"]),
        "observed_since": str(row["observed_since"]),
        "lookback_days": int(row["lookback_days"]),
        "reason": row["reason"],
        "source": str(row["source"]),
        "snapshot": snapshot if isinstance(snapshot, dict) else {},
        "idempotency_key": str(row["idempotency_key"]),
        "resolved_at": row["resolved_at"],
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _select_flag(conn: sqlite3.Connection, idempotency_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT id, account_id, status, current_balance_cents, suggested_transfer_cents,
               retained_buffer_cents, minimum_balance_cents, current_apy_bps,
               hysa_apy_bps, estimated_annual_yield_cents, observed_since,
               lookback_days, reason, source, snapshot_json, idempotency_key,
               resolved_at, created_at, updated_at
          FROM hysa_transfer_flags
         WHERE idempotency_key = ?
         LIMIT 1
        """,
        (idempotency_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("HYSA transfer flag was not written")
    return _row_to_dict(row)


def flag_account_for_hysa_transfer(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    suggested_transfer_cents: Any,
    hysa_apy_bps: Any,
    current_apy_bps: Any = 0,
    retained_buffer_cents: Any = 0,
    minimum_balance_cents: Any = 200_000,
    lookback_days: Any = 90,
    as_of: str | date | None = None,
    reason: str = "",
    source: str = "agent",
    dry_run: bool = False,
) -> dict[str, Any]:
    account = _load_checking_account(conn, account_id)
    transfer_cents = _coerce_positive_cents(
        suggested_transfer_cents,
        field_name="suggested_transfer_cents",
    )
    retained_cents = _coerce_nonnegative_cents(
        retained_buffer_cents,
        field_name="retained_buffer_cents",
    )
    requested_minimum_cents = _coerce_positive_cents(
        minimum_balance_cents,
        field_name="minimum_balance_cents",
    )
    minimum_cents = max(requested_minimum_cents, retained_cents + transfer_cents)
    lookback = _coerce_positive_int(lookback_days, field_name="lookback_days")
    if lookback > 3650:
        raise ValidationError("lookback_days must be less than or equal to 3650")
    current_bps = _coerce_nonnegative_int(current_apy_bps, field_name="current_apy_bps")
    hysa_bps = _coerce_positive_int(hysa_apy_bps, field_name="hysa_apy_bps")
    if hysa_bps <= current_bps:
        raise ValidationError("hysa_apy_bps must be greater than current_apy_bps")
    as_of_date = _parse_as_of(as_of)
    current_balance_cents = int(account["balance_current_cents"] or 0)
    available_for_transfer = current_balance_cents - retained_cents
    if transfer_cents > available_for_transfer:
        raise ValidationError(
            "suggested_transfer_cents exceeds current balance after retained_buffer_cents"
        )
    if current_balance_cents < minimum_cents:
        raise ValidationError("checking balance is below minimum_balance_cents")

    evidence = _load_balance_evidence(
        conn,
        account_id=str(account["id"]),
        minimum_balance_cents=minimum_cents,
        lookback_days=lookback,
        as_of=as_of_date,
    )
    estimated_yield_cents = (transfer_cents * (hysa_bps - current_bps) + 5_000) // 10_000
    normalized_source = _normalize_source(source)
    normalized_reason = " ".join(str(reason or "").split())[:240]
    account_label = _account_label(account)
    snapshot = {
        "account_label": account_label,
        "account_type": str(account["account_type"]),
        "current_balance_cents": current_balance_cents,
        "suggested_transfer_cents": transfer_cents,
        "retained_buffer_cents": retained_cents,
        "minimum_balance_cents": minimum_cents,
        "current_apy_bps": current_bps,
        "hysa_apy_bps": hysa_bps,
        "estimated_annual_yield_cents": estimated_yield_cents,
        "as_of": as_of_date.isoformat(),
        **evidence,
    }
    idempotency_key = f"hysa_transfer:{account['id']}"
    preview = {
        "id": None,
        "account_id": str(account["id"]),
        "status": "active",
        "current_balance_cents": current_balance_cents,
        "suggested_transfer_cents": transfer_cents,
        "retained_buffer_cents": retained_cents,
        "minimum_balance_cents": minimum_cents,
        "current_apy_bps": current_bps,
        "hysa_apy_bps": hysa_bps,
        "estimated_annual_yield_cents": estimated_yield_cents,
        "observed_since": evidence["observed_since"],
        "lookback_days": lookback,
        "reason": normalized_reason,
        "source": normalized_source,
        "snapshot": snapshot,
        "idempotency_key": idempotency_key,
    }
    summary = {
        "flagged": 0 if dry_run else 1,
        "dry_run": bool(dry_run),
        "account_id": str(account["id"]),
        "suggested_transfer_cents": transfer_cents,
        "estimated_annual_yield_cents": estimated_yield_cents,
        "observed_since": evidence["observed_since"],
        "lookback_days": lookback,
    }
    if dry_run:
        return {
            "data": {"flag": preview, "dry_run": True},
            "summary": summary,
            "cli_report": (
                f"[DRY RUN] Would flag {account_label} for "
                f"${cents_to_dollars(transfer_cents):,.2f} HYSA transfer"
            ),
        }

    conn.execute(
        """
        INSERT INTO hysa_transfer_flags (
            id, account_id, status, current_balance_cents,
            suggested_transfer_cents, retained_buffer_cents, minimum_balance_cents,
            current_apy_bps, hysa_apy_bps, estimated_annual_yield_cents,
            observed_since, lookback_days, reason, source, snapshot_json,
            idempotency_key
        ) VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(idempotency_key) DO UPDATE SET
            account_id = excluded.account_id,
            status = 'active',
            current_balance_cents = excluded.current_balance_cents,
            suggested_transfer_cents = excluded.suggested_transfer_cents,
            retained_buffer_cents = excluded.retained_buffer_cents,
            minimum_balance_cents = excluded.minimum_balance_cents,
            current_apy_bps = excluded.current_apy_bps,
            hysa_apy_bps = excluded.hysa_apy_bps,
            estimated_annual_yield_cents = excluded.estimated_annual_yield_cents,
            observed_since = excluded.observed_since,
            lookback_days = excluded.lookback_days,
            reason = excluded.reason,
            source = excluded.source,
            snapshot_json = excluded.snapshot_json,
            resolved_at = NULL,
            updated_at = datetime('now')
        """,
        (
            uuid.uuid4().hex,
            str(account["id"]),
            current_balance_cents,
            transfer_cents,
            retained_cents,
            minimum_cents,
            current_bps,
            hysa_bps,
            estimated_yield_cents,
            evidence["observed_since"],
            lookback,
            normalized_reason,
            normalized_source,
            json.dumps(snapshot, sort_keys=True),
            idempotency_key,
        ),
    )
    conn.commit()
    flag = _select_flag(conn, idempotency_key)
    summary["id"] = flag["id"]
    return {
        "data": {"flag": flag, "dry_run": False},
        "summary": summary,
        "cli_report": (
            f"Flagged {account_label} for "
            f"${cents_to_dollars(transfer_cents):,.2f} HYSA transfer"
        ),
    }


def handle_flag(args, conn: sqlite3.Connection) -> dict[str, Any]:
    return flag_account_for_hysa_transfer(
        conn,
        account_id=getattr(args, "account_id", ""),
        suggested_transfer_cents=getattr(args, "suggested_transfer_cents", None),
        hysa_apy_bps=getattr(args, "hysa_apy_bps", None),
        current_apy_bps=getattr(args, "current_apy_bps", 0),
        retained_buffer_cents=getattr(args, "retained_buffer_cents", 0),
        minimum_balance_cents=getattr(args, "minimum_balance_cents", 200_000),
        lookback_days=getattr(args, "lookback_days", 90),
        as_of=getattr(args, "as_of", None),
        reason=getattr(args, "reason", ""),
        source=getattr(args, "source", "agent"),
        dry_run=bool(getattr(args, "dry_run", False)),
    )
