"""Stripe integration boundary for finance_cli."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .provider_routing import check_provider_allowed

logger = logging.getLogger(__name__)

_STRIPE_API_KEY_ENV = "STRIPE_API_KEY"
_STRIPE_CONNECTION_ID = "default"
_COOLDOWN_SECONDS = 300


@dataclass(frozen=True)
class StripeConfigStatus:
    configured: bool
    has_sdk: bool
    missing_env: list[str]
    account_name: str | None
    connection_count: int


class StripeUnavailableError(RuntimeError):
    """Raised when Stripe operations are requested but setup is incomplete."""


class StripeSyncError(RuntimeError):
    """Raised when Stripe sync fails."""


def _has_stripe_sdk() -> bool:
    try:
        import stripe  # noqa: F401

        return True
    except Exception:
        return False


def _import_stripe() -> Any:
    import stripe

    return stripe


def _sync_cooldown_seconds() -> int:
    raw = str(os.getenv("STRIPE_SYNC_COOLDOWN", "")).strip()
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            return _COOLDOWN_SECONDS
    return _COOLDOWN_SECONDS


def _stripe_get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    try:
        return getattr(value, key)
    except Exception:
        pass
    try:
        return value[key]
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _utc_date_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=UTC).date().isoformat()


def _category_id_by_name(conn: sqlite3.Connection, name: str) -> str | None:
    row = conn.execute(
        """
        SELECT id
          FROM categories
         WHERE lower(trim(name)) = lower(trim(?))
         ORDER BY rowid ASC
         LIMIT 1
        """,
        (name,),
    ).fetchone()
    if row:
        return str(row["id"])
    return None


def _get_or_create_system_category(conn: sqlite3.Connection, name: str, *, is_income: int = 0) -> str:
    existing = _category_id_by_name(conn, name)
    if existing:
        return existing

    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_income, is_system) VALUES (?, ?, ?, 1)",
        (category_id, name, int(is_income)),
    )
    return category_id


def _ensure_stripe_categories(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        "income_business": _get_or_create_system_category(conn, "Income: Business", is_income=1),
        "cogs": _get_or_create_system_category(conn, "Cost of Goods Sold", is_income=0),
        "payments_transfers": _get_or_create_system_category(conn, "Payments & Transfers", is_income=0),
    }


def _ensure_stripe_balance_account(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT id
          FROM accounts
         WHERE institution_name = 'Stripe'
           AND account_name = 'Stripe Balance'
         ORDER BY rowid ASC
         LIMIT 1
        """
    ).fetchone()
    if row:
        account_id = str(row["id"])
        conn.execute(
            """
            UPDATE accounts
               SET source = 'stripe',
                   is_business = 1,
                   is_active = 1,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (account_id,),
        )
        return account_id

    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id,
            institution_name,
            account_name,
            account_type,
            source,
            is_active,
            is_business,
            created_at,
            updated_at
        ) VALUES (?, 'Stripe', 'Stripe Balance', 'checking', 'stripe', 1, 1, datetime('now'), datetime('now'))
        """,
        (account_id,),
    )
    return account_id


def _connection_row(conn: sqlite3.Connection) -> sqlite3.Row | None:
    try:
        return conn.execute(
            """
            SELECT *
              FROM stripe_connections
             WHERE status != 'disconnected'
             ORDER BY created_at ASC
             LIMIT 1
            """
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            raise StripeUnavailableError("Stripe schema missing. Run database migrations (021).") from exc
        raise


def _upsert_connection(
    conn: sqlite3.Connection,
    *,
    account_id: str | None,
    account_name: str | None,
    api_key_ref: str = _STRIPE_API_KEY_ENV,
) -> None:
    conn.execute(
        """
        INSERT INTO stripe_connections (
            id,
            account_id,
            account_name,
            api_key_ref,
            sync_cursor,
            last_sync_at,
            status,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, NULL, NULL, 'active', datetime('now'), datetime('now'))
        ON CONFLICT(id) DO UPDATE SET
            account_id = COALESCE(excluded.account_id, stripe_connections.account_id),
            account_name = COALESCE(excluded.account_name, stripe_connections.account_name),
            api_key_ref = excluded.api_key_ref,
            status = 'active',
            updated_at = datetime('now')
        """,
        (_STRIPE_CONNECTION_ID, account_id, account_name, api_key_ref),
    )


def _account_name_from_payload(account_payload: Any) -> str:
    business_profile = _stripe_get(account_payload, "business_profile", {})
    if isinstance(business_profile, dict):
        business_name = str(business_profile.get("name") or "").strip()
    else:
        business_name = str(_stripe_get(business_profile, "name", "") or "").strip()
    if business_name:
        return business_name

    settings = _stripe_get(account_payload, "settings", {})
    dashboard = _stripe_get(settings, "dashboard", {}) if settings else {}
    dashboard_name = str(_stripe_get(dashboard, "display_name", "") or "").strip()
    if dashboard_name:
        return dashboard_name

    account_id = str(_stripe_get(account_payload, "id", "") or "").strip()
    if account_id:
        return account_id
    return "Stripe"


def _raw_payload(
    *,
    txn_id: str,
    reporting_category: str,
    txn_type: str,
    amount_cents: int,
    fee_cents: int,
    currency: str,
    created: int,
    source_id: str | None,
) -> str:
    payload = {
        "id": txn_id,
        "reporting_category": reporting_category,
        "type": txn_type,
        "amount": int(amount_cents),
        "fee": int(fee_cents),
        "currency": currency,
        "created": int(created),
        "source_id": source_id,
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _within_cooldown(conn: sqlite3.Connection, last_sync_at: str | None, cooldown_seconds: int) -> bool:
    if not last_sync_at or cooldown_seconds <= 0:
        return False
    row = conn.execute(
        "SELECT datetime(?) > datetime('now', ? || ' seconds') AS is_fresh",
        (last_sync_at, f"-{cooldown_seconds}"),
    ).fetchone()
    return bool(row and row["is_fresh"])


def _insert_transaction(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    stripe_txn_id: str | None,
    dedupe_key: str,
    txn_date: str,
    description: str,
    amount_cents: int,
    category_id: str | None,
    source_category: str,
    raw_json: str,
    is_payment: int = 0,
) -> bool:
    try:
        conn.execute(
            """
            INSERT INTO transactions (
                id,
                account_id,
                stripe_txn_id,
                dedupe_key,
                date,
                description,
                amount_cents,
                category_id,
                source_category,
                category_source,
                use_type,
                is_payment,
                source,
                raw_plaid_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'institution', 'Business', ?, 'stripe', ?, datetime('now'), datetime('now'))
            """,
            (
                uuid.uuid4().hex,
                account_id,
                stripe_txn_id,
                dedupe_key,
                txn_date,
                description,
                int(amount_cents),
                category_id,
                source_category,
                int(is_payment),
                raw_json,
            ),
        )
        return True
    except sqlite3.IntegrityError as exc:
        if "unique" in str(exc).lower():
            return False
        raise


def _mark_connection_error(conn: sqlite3.Connection) -> None:
    try:
        conn.execute(
            """
            UPDATE stripe_connections
               SET status = 'error',
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (_STRIPE_CONNECTION_ID,),
        )
        conn.commit()
    except Exception:
        conn.rollback()


def _iter_balance_transactions(stripe: Any, created_gte: int) -> list[Any]:
    all_rows: list[Any] = []
    starting_after: str | None = None
    while True:
        params: dict[str, Any] = {
            "created": {"gte": int(created_gte)},
            "limit": 100,
            "expand": ["data.source"],
        }
        if starting_after:
            params["starting_after"] = starting_after
        page = stripe.BalanceTransaction.list(**params)
        data = list(_stripe_get(page, "data", []) or [])
        if not data:
            break
        all_rows.extend(data)
        has_more = bool(_stripe_get(page, "has_more", False))
        last_id = str(_stripe_get(data[-1], "id", "") or "").strip()
        if not has_more or not last_id:
            break
        starting_after = last_id
    return all_rows


def config_status(conn: sqlite3.Connection | None = None) -> StripeConfigStatus:
    missing = [name for name in (_STRIPE_API_KEY_ENV,) if not os.getenv(name)]
    has_sdk = _has_stripe_sdk()

    account_name: str | None = None
    connection_count = 0

    if conn is not None:
        try:
            connection_count_row = conn.execute(
                "SELECT COUNT(*) AS n FROM stripe_connections"
            ).fetchone()
            connection_count = int(connection_count_row["n"] or 0) if connection_count_row else 0
            account_row = conn.execute(
                """
                SELECT account_name
                  FROM stripe_connections
                 WHERE status = 'active'
                 ORDER BY updated_at DESC
                 LIMIT 1
                """
            ).fetchone()
            if account_row:
                account_name = str(account_row["account_name"] or "").strip() or None
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise

    return StripeConfigStatus(
        configured=len(missing) == 0,
        has_sdk=has_sdk,
        missing_env=missing,
        account_name=account_name,
        connection_count=connection_count,
    )


def link_connection(conn: sqlite3.Connection) -> dict[str, Any]:
    status = config_status(conn)
    if not status.has_sdk:
        raise StripeUnavailableError("stripe package not installed")
    if not status.configured:
        raise StripeUnavailableError("missing env: " + ",".join(status.missing_env))

    stripe = _import_stripe()
    stripe.api_key = str(os.getenv(_STRIPE_API_KEY_ENV) or "")

    account_payload = stripe.Account.retrieve()
    stripe_account_id = str(_stripe_get(account_payload, "id", "") or "").strip() or None
    account_name = _account_name_from_payload(account_payload)

    _upsert_connection(
        conn,
        account_id=stripe_account_id,
        account_name=account_name,
        api_key_ref=_STRIPE_API_KEY_ENV,
    )
    local_account_id = _ensure_stripe_balance_account(conn)
    conn.commit()

    return {
        "stripe_account_id": stripe_account_id,
        "account_name": account_name,
        "local_account_id": local_account_id,
        "api_key_ref": _STRIPE_API_KEY_ENV,
    }


def unlink_connection(conn: sqlite3.Connection) -> dict[str, Any]:
    row = _connection_row(conn)
    if not row:
        return {"updated": 0, "status": "not_found"}

    conn.execute(
        """
        UPDATE stripe_connections
           SET status = 'disconnected',
               updated_at = datetime('now')
         WHERE id = ?
        """,
        (str(row["id"]),),
    )
    conn.commit()
    return {"updated": 1, "status": "disconnected", "id": str(row["id"])}


def balance_status() -> dict[str, int]:
    """Fetch current Stripe balance (USD cents)."""
    status = config_status()
    if not status.has_sdk:
        raise StripeUnavailableError("stripe package not installed")
    if not status.configured:
        raise StripeUnavailableError("missing env: " + ",".join(status.missing_env))

    stripe = _import_stripe()
    stripe.api_key = str(os.getenv(_STRIPE_API_KEY_ENV) or "")
    payload = stripe.Balance.retrieve()

    available_rows = _stripe_get(payload, "available", []) or []
    pending_rows = _stripe_get(payload, "pending", []) or []

    available_cents = 0
    pending_cents = 0

    for row in available_rows:
        if str(_stripe_get(row, "currency", "") or "").lower() != "usd":
            continue
        available_cents += _as_int(_stripe_get(row, "amount"), 0)

    for row in pending_rows:
        if str(_stripe_get(row, "currency", "") or "").lower() != "usd":
            continue
        pending_cents += _as_int(_stripe_get(row, "amount"), 0)

    return {"available_cents": available_cents, "pending_cents": pending_cents}


def _dedup_payout_against_plaid(
    conn: sqlite3.Connection,
    payout_amount_cents: int,
    payout_date: str,
    payout_id: str,
) -> str:
    target_cents = abs(int(payout_amount_cents))

    candidates = conn.execute(
        """
        SELECT t.id,
               t.notes,
               t.amount_cents,
               t.date
          FROM transactions t
          JOIN accounts a ON a.id = t.account_id
         WHERE t.is_active = 1
           AND t.source = 'plaid'
           AND t.amount_cents > 0
           AND a.is_business = 1
           AND ABS(t.amount_cents) = ?
           AND date(t.date) BETWEEN date(?, '-2 days') AND date(?, '+2 days')
         ORDER BY t.date ASC, t.id ASC
        """,
        (target_cents, payout_date, payout_date),
    ).fetchall()

    if not candidates:
        logger.warning(
            "Stripe payout unmatched payout_id=%s amount_cents=%s payout_date=%s",
            payout_id,
            payout_amount_cents,
            payout_date,
        )
        return "unmatched"

    if len(candidates) > 1:
        logger.warning(
            "Stripe payout ambiguous payout_id=%s amount_cents=%s payout_date=%s candidate_count=%s",
            payout_id,
            payout_amount_cents,
            payout_date,
            len(candidates),
        )
        return "ambiguous"

    candidate = candidates[0]
    dedup_note = f"Deduped: Stripe payout {payout_id}"
    existing_notes = str(candidate["notes"] or "").strip()
    if not existing_notes:
        merged_notes = dedup_note
    elif dedup_note in existing_notes:
        merged_notes = existing_notes
    else:
        merged_notes = f"{existing_notes}\n{dedup_note}"

    conn.execute(
        """
        UPDATE transactions
           SET is_active = 0,
               notes = ?,
               updated_at = datetime('now')
         WHERE id = ?
        """,
        (merged_notes, str(candidate["id"])),
    )
    return "matched"


def run_sync(
    conn: sqlite3.Connection,
    days: int | None = None,
    force: bool = False,
    backfill: bool = False,
) -> dict[str, Any]:
    """Sync Stripe balance transactions into local storage."""
    status = config_status(conn)
    if not status.has_sdk or not status.configured:
        missing_parts: list[str] = []
        if not status.has_sdk:
            missing_parts.append("stripe package not installed")
        if status.missing_env:
            missing_parts.append("missing env: " + ",".join(status.missing_env))
        raise StripeUnavailableError("Stripe sync unavailable: " + "; ".join(missing_parts))

    allowed, designated = check_provider_allowed(conn, "Stripe", "stripe")
    if not allowed:
        return {
            "charges_added": 0,
            "fees_added": 0,
            "refunds_added": 0,
            "adjustments_added": 0,
            "payouts_matched": 0,
            "payouts_ambiguous": 0,
            "payouts_unmatched": 0,
            "skipped_existing": 0,
            "skipped_non_usd": 0,
            "skipped_unknown_type": 0,
            "errors": [],
            "skipped_reason": f"institution routed to {designated}",
            "skipped_cooldown": False,
        }

    result: dict[str, Any] = {
        "charges_added": 0,
        "fees_added": 0,
        "refunds_added": 0,
        "adjustments_added": 0,
        "payouts_matched": 0,
        "payouts_ambiguous": 0,
        "payouts_unmatched": 0,
        "skipped_existing": 0,
        "skipped_non_usd": 0,
        "skipped_unknown_type": 0,
        "errors": [],
        "skipped_cooldown": False,
        "created_gte": 0,
        "max_created": 0,
        "transactions_seen": 0,
    }

    try:
        stripe = _import_stripe()
        stripe.api_key = str(os.getenv(_STRIPE_API_KEY_ENV) or "")

        connection = _connection_row(conn)
        if not connection:
            account_payload = stripe.Account.retrieve()
            _upsert_connection(
                conn,
                account_id=str(_stripe_get(account_payload, "id", "") or "").strip() or None,
                account_name=_account_name_from_payload(account_payload),
            )
            connection = _connection_row(conn)

        if not connection:
            raise StripeSyncError("Unable to initialize stripe connection row")

        cooldown_seconds = _sync_cooldown_seconds()
        last_sync_at = str(connection["last_sync_at"] or "").strip() or None
        if not force and _within_cooldown(conn, last_sync_at, cooldown_seconds):
            result["skipped_cooldown"] = True
            result["last_sync_at"] = last_sync_at
            return result

        if backfill:
            created_gte = 0
        elif days is not None:
            created_gte = max(0, int(time.time()) - (max(0, int(days)) * 86400))
        else:
            created_gte = max(0, _as_int(connection["sync_cursor"], 0))

        result["created_gte"] = created_gte
        max_created = created_gte

        local_account_id = _ensure_stripe_balance_account(conn)
        categories = _ensure_stripe_categories(conn)

        txns = _iter_balance_transactions(stripe, created_gte=created_gte)

        for entry in txns:
            result["transactions_seen"] += 1

            txn_id = str(_stripe_get(entry, "id", "") or "").strip()
            if not txn_id:
                result["skipped_unknown_type"] += 1
                logger.warning("Stripe balance transaction missing id; skipping")
                continue

            created_ts = _as_int(_stripe_get(entry, "created"), 0)
            if created_ts > max_created:
                max_created = created_ts

            currency = str(_stripe_get(entry, "currency", "usd") or "usd").strip().lower()
            if currency != "usd":
                result["skipped_non_usd"] += 1
                logger.warning("Stripe transaction skipped non-USD txn_id=%s currency=%s", txn_id, currency)
                continue

            reporting_category = str(_stripe_get(entry, "reporting_category", "") or "").strip().lower()
            txn_type = str(_stripe_get(entry, "type", "") or "").strip().lower()
            amount_cents = _as_int(_stripe_get(entry, "amount"), 0)
            fee_cents = _as_int(_stripe_get(entry, "fee"), 0)
            txn_date = _utc_date_from_unix(created_ts)

            source_obj = _stripe_get(entry, "source")
            source_id = str(_stripe_get(source_obj, "id", source_obj) or "").strip() or None
            base_description = str(_stripe_get(entry, "description", "") or "").strip()
            if not base_description:
                base_description = f"Stripe {reporting_category or txn_type or 'transaction'}"

            payload_json = _raw_payload(
                txn_id=txn_id,
                reporting_category=reporting_category,
                txn_type=txn_type,
                amount_cents=amount_cents,
                fee_cents=fee_cents,
                currency=currency,
                created=created_ts,
                source_id=source_id,
            )

            if reporting_category == "charge":
                inserted_charge = _insert_transaction(
                    conn,
                    account_id=local_account_id,
                    stripe_txn_id=txn_id,
                    dedupe_key=f"stripe:{txn_id}:charge",
                    txn_date=txn_date,
                    description=base_description,
                    amount_cents=amount_cents,
                    category_id=categories["income_business"],
                    source_category="charge",
                    raw_json=payload_json,
                )
                if inserted_charge:
                    result["charges_added"] += 1
                else:
                    result["skipped_existing"] += 1

                inserted_fee = _insert_transaction(
                    conn,
                    account_id=local_account_id,
                    stripe_txn_id=None,
                    dedupe_key=f"stripe:{txn_id}:fee",
                    txn_date=txn_date,
                    description=f"{base_description} (Stripe fee)",
                    amount_cents=-abs(fee_cents),
                    category_id=categories["cogs"],
                    source_category="fee",
                    raw_json=payload_json,
                )
                if inserted_fee:
                    result["fees_added"] += 1
                else:
                    result["skipped_existing"] += 1
                continue

            if reporting_category == "fee":
                inserted = _insert_transaction(
                    conn,
                    account_id=local_account_id,
                    stripe_txn_id=txn_id,
                    dedupe_key=f"stripe:{txn_id}:fee",
                    txn_date=txn_date,
                    description=base_description,
                    amount_cents=amount_cents if amount_cents <= 0 else -abs(amount_cents),
                    category_id=categories["cogs"],
                    source_category="fee",
                    raw_json=payload_json,
                )
                if inserted:
                    result["fees_added"] += 1
                else:
                    result["skipped_existing"] += 1
                continue

            if reporting_category in {"refund", "partial_capture_reversal"}:
                inserted = _insert_transaction(
                    conn,
                    account_id=local_account_id,
                    stripe_txn_id=txn_id,
                    dedupe_key=f"stripe:{txn_id}",
                    txn_date=txn_date,
                    description=base_description,
                    amount_cents=amount_cents,
                    category_id=categories["income_business"],
                    source_category=reporting_category,
                    raw_json=payload_json,
                )
                if inserted:
                    result["refunds_added"] += 1
                else:
                    result["skipped_existing"] += 1
                continue

            if reporting_category in {"dispute", "dispute_reversal", "other_adjustment"}:
                inserted = _insert_transaction(
                    conn,
                    account_id=local_account_id,
                    stripe_txn_id=txn_id,
                    dedupe_key=f"stripe:{txn_id}",
                    txn_date=txn_date,
                    description=base_description,
                    amount_cents=amount_cents,
                    category_id=categories["income_business"],
                    source_category=reporting_category,
                    raw_json=payload_json,
                )
                if inserted:
                    result["adjustments_added"] += 1
                else:
                    result["skipped_existing"] += 1
                continue

            if reporting_category == "payout":
                payout_id = source_id or txn_id
                match_status = _dedup_payout_against_plaid(
                    conn,
                    payout_amount_cents=amount_cents,
                    payout_date=txn_date,
                    payout_id=payout_id,
                )
                if match_status == "matched":
                    result["payouts_matched"] += 1
                elif match_status == "ambiguous":
                    result["payouts_ambiguous"] += 1
                else:
                    result["payouts_unmatched"] += 1
                continue

            if reporting_category == "payout_reversal":
                inserted = _insert_transaction(
                    conn,
                    account_id=local_account_id,
                    stripe_txn_id=txn_id,
                    dedupe_key=f"stripe:{txn_id}",
                    txn_date=txn_date,
                    description=base_description,
                    amount_cents=amount_cents,
                    category_id=categories["payments_transfers"],
                    source_category=reporting_category,
                    raw_json=payload_json,
                    is_payment=1,
                )
                if inserted:
                    result["adjustments_added"] += 1
                else:
                    result["skipped_existing"] += 1
                continue

            result["skipped_unknown_type"] += 1
            logger.warning(
                "Stripe transaction skipped unknown reporting_category txn_id=%s reporting_category=%s type=%s",
                txn_id,
                reporting_category,
                txn_type,
            )

        conn.execute(
            """
            UPDATE stripe_connections
               SET sync_cursor = ?,
                   last_sync_at = datetime('now'),
                   status = 'active',
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (str(max_created), _STRIPE_CONNECTION_ID),
        )
        conn.commit()
        result["max_created"] = max_created
        return result
    except Exception as exc:
        conn.rollback()
        _mark_connection_error(conn)
        raise StripeSyncError(str(exc)) from exc
