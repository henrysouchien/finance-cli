"""Cost ledger, guardrails, and alert helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import sqlite3
from pathlib import Path
from typing import Any, Literal

try:
    import alerts

    _HAS_ALERTS = True
except ImportError:  # pragma: no cover - optional dependency
    alerts = None  # type: ignore[assignment]
    _HAS_ALERTS = False

from .analytics import log_event
from .config import get_db_path
from .db import _resolve_connection_user_id, connect
from .notification_utils import resolve_notification_creds
from .perf import get_request_id
from . import storage_lease as storage_lease  # noqa: F401

log = logging.getLogger(__name__)

PLAID_ITEM_MONTHLY_USD6 = 300_000
# USD-microdollars per call. Estimates; Plaid pricing varies by plan.
# Bundled endpoints record cost=0 to preserve call-count visibility without
# double-counting (they are gated by PLAID_ITEM_MONTHLY_USD6 at link time).
PLAID_OPERATION_COSTS_USD6: dict[str, int] = {
    "accounts_balance_get": 100_000,
    "liabilities_get": 0,
    "transactions_sync": 0,
    "investments_transactions_get": 0,
    "auth_get": 0,
    "identity_get": 0,
    "item_get": 0,
    "item_remove": 0,
    "link_token_create": 0,
    "item_public_token_exchange": 0,
    "institutions_get_by_id": 0,
}
_MICRODOLLARS_PER_DOLLAR = 1_000_000
_ONE_MILLION_TOKENS = 1_000_000


@dataclass(frozen=True)
class TokenPricing:
    input_usd6_per_million: int
    output_usd6_per_million: int
    cache_creation_usd6_per_million: int = 0
    cache_read_usd6_per_million: int = 0


@dataclass(frozen=True)
class SettlementResult:
    status: Literal["settled", "replay_no_op"]
    ledger_id: int | None
    allowance_debited: int
    credits_debited: int
    overflow_unattributed: int


_OPENAI_MODEL_PRICING: tuple[tuple[str, TokenPricing], ...] = (
    ("gpt-4.1-mini", TokenPricing(400_000, 1_600_000)),
    ("gpt-4.1", TokenPricing(2_000_000, 8_000_000)),
    ("gpt-4o-mini", TokenPricing(150_000, 600_000)),
    ("gpt-4o", TokenPricing(2_500_000, 10_000_000)),
)
_CLAUDE_MODEL_PRICING: tuple[tuple[str, TokenPricing], ...] = (
    ("claude-haiku-4-5", TokenPricing(1_000_000, 5_000_000, 1_250_000, 100_000)),
    ("claude-sonnet-4-6", TokenPricing(3_000_000, 15_000_000, 3_750_000, 300_000)),
    ("claude-sonnet-4-5", TokenPricing(3_000_000, 15_000_000, 3_750_000, 300_000)),
    ("claude-sonnet-4", TokenPricing(3_000_000, 15_000_000, 3_750_000, 300_000)),
    ("claude-3-7-sonnet", TokenPricing(3_000_000, 15_000_000, 3_750_000, 300_000)),
)


def _coerce_db_path(db_path: str | Path | None) -> str | None:
    if db_path is None:
        return None
    if isinstance(db_path, str) and (db_path == ":memory:" or db_path.startswith("file:")):
        return db_path
    return str(Path(db_path).expanduser().resolve())


def _open_db(db_path: str) -> sqlite3.Connection:
    """Open a DB connection, handling file: URIs correctly."""
    if db_path.startswith("file:"):
        conn = sqlite3.connect(db_path, uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    resolved = Path(db_path).expanduser().resolve()
    try:
        user_id = _resolve_connection_user_id(resolved)
    except ValueError:
        return connect(db_path=resolved, check_same_thread=True)
    # Phase 5 Batch A: web hot paths call this under dependencies.get_user_conn,
    # so db.connect reuses the request-scoped storage lease instead of taking one per metric write.
    return connect(
        db_path=resolved,
        check_same_thread=True,
        user_id=user_id,
    )


def dollars_to_usd6(value: Any) -> int:
    try:
        return max(int(round(float(value) * _MICRODOLLARS_PER_DOLLAR)), 0)
    except (TypeError, ValueError):
        return 0


def _tokens_to_usd6(tokens: int | None, usd6_per_million: int) -> int:
    token_count = max(int(tokens or 0), 0)
    if token_count <= 0 or usd6_per_million <= 0:
        return 0
    return int(round((token_count * usd6_per_million) / _ONE_MILLION_TOKENS))


def _pricing_for_model(provider: str, model: str | None) -> TokenPricing | None:
    normalized_provider = str(provider or "").strip().lower()
    normalized_model = str(model or "").strip().lower()
    pricing_map = _OPENAI_MODEL_PRICING if normalized_provider == "openai" else _CLAUDE_MODEL_PRICING

    for prefix, pricing in pricing_map:
        if normalized_model.startswith(prefix):
            return pricing
    return None


def estimate_ai_cost_usd6(
    provider: str,
    *,
    model: str | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cache_creation_tokens: int | None = None,
    cache_read_tokens: int | None = None,
) -> int:
    pricing = _pricing_for_model(provider, model)
    if pricing is None:
        return 0

    return (
        _tokens_to_usd6(input_tokens, pricing.input_usd6_per_million)
        + _tokens_to_usd6(output_tokens, pricing.output_usd6_per_million)
        + _tokens_to_usd6(cache_creation_tokens, pricing.cache_creation_usd6_per_million)
        + _tokens_to_usd6(cache_read_tokens, pricing.cache_read_usd6_per_million)
    )


def _record_cost_strict(
    conn: sqlite3.Connection,
    provider: str,
    operation: str,
    cost_usd6: int,
    *,
    idempotency_key: str | None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cache_creation_tokens: int | None = None,
    cache_read_tokens: int | None = None,
    model: str | None = None,
    request_id: str | None = None,
    is_estimated: bool = False,
    is_byok: bool = False,
    allowance_debit_usd6: int = 0,
    credits_debit_usd6: int = 0,
    overflow_unattributed_usd6: int = 0,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO cost_ledger (
            provider,
            operation,
            cost_usd6,
            input_tokens,
            output_tokens,
            cache_creation_tokens,
            cache_read_tokens,
            model,
            request_id,
            is_estimated,
            idempotency_key,
            is_byok,
            allowance_debit_usd6,
            credits_debit_usd6,
            overflow_unattributed_usd6
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(provider or "").strip().lower(),
            str(operation or "").strip(),
            max(int(cost_usd6), 0),
            input_tokens,
            output_tokens,
            cache_creation_tokens,
            cache_read_tokens,
            model,
            get_request_id() if request_id is None else request_id,
            1 if is_estimated else 0,
            idempotency_key,
            1 if is_byok else 0,
            max(int(allowance_debit_usd6), 0),
            max(int(credits_debit_usd6), 0),
            max(int(overflow_unattributed_usd6), 0),
        ),
    )
    return int(cursor.lastrowid)


def record_cost(
    db_path: str | Path | None,
    provider: str,
    operation: str,
    cost_usd6: int,
    *,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cache_creation_tokens: int | None = None,
    cache_read_tokens: int | None = None,
    model: str | None = None,
    request_id: str | None = None,
    is_estimated: bool = False,
    idempotency_key: str | None = None,
) -> None:
    resolved_db_path = _coerce_db_path(db_path)
    if resolved_db_path is None:
        return

    try:
        with _open_db(resolved_db_path) as conn:
            _record_cost_strict(
                conn,
                provider,
                operation,
                cost_usd6,
                idempotency_key=idempotency_key,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_creation_tokens=cache_creation_tokens,
                cache_read_tokens=cache_read_tokens,
                model=model,
                request_id=request_id,
                is_estimated=is_estimated,
            )
            conn.commit()
    except Exception as exc:
        log.warning("Failed to record cost: %s", exc)


def _effective_limit_usd6(limit_usd6: Any, system_limit_usd6: Any) -> int | None:
    values: list[int] = []
    for value in (limit_usd6, system_limit_usd6):
        if value is None:
            continue
        values.append(max(int(value), 0))
    return min(values) if values else None


def _effective_cost_limit(conn: sqlite3.Connection, provider: str, period: str) -> int | None:
    row = conn.execute(
        """
        SELECT limit_usd6, system_limit_usd6
        FROM cost_limits
        WHERE provider = ?
          AND period = ?
          AND is_active = 1
        """,
        (provider, period),
    ).fetchone()
    if row is None:
        return None
    return _effective_limit_usd6(row["limit_usd6"], row["system_limit_usd6"])


def _credit_balance_usd6(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT balance_usd6 FROM credit_balance WHERE id = 1").fetchone()
    return max(int(row["balance_usd6"] if row is not None else 0), 0)


def record_and_settle_cost(
    db_path: str | Path,
    provider: str,
    operation: str,
    cost_usd6: int,
    idempotency_key: str,
    *,
    is_byok: bool = False,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cache_creation_tokens: int | None = None,
    cache_read_tokens: int | None = None,
    model: str | None = None,
    request_id: str | None = None,
    is_estimated: bool = False,
) -> SettlementResult:
    resolved_db_path = _coerce_db_path(db_path)
    if resolved_db_path is None:
        raise ValueError("db_path is required")
    normalized_key = str(idempotency_key or "").strip()
    if not normalized_key:
        raise ValueError("idempotency_key is required")

    normalized_provider = str(provider or "").strip().lower()
    normalized_cost = max(int(cost_usd6), 0)
    conn = _open_db(resolved_db_path)
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("BEGIN IMMEDIATE")

        if is_byok:
            try:
                ledger_id = _record_cost_strict(
                    conn,
                    normalized_provider,
                    operation,
                    normalized_cost,
                    idempotency_key=normalized_key,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cache_creation_tokens=cache_creation_tokens,
                    cache_read_tokens=cache_read_tokens,
                    model=model,
                    request_id=request_id,
                    is_estimated=is_estimated,
                    is_byok=True,
                )
            except sqlite3.IntegrityError:
                conn.rollback()
                return SettlementResult("replay_no_op", None, 0, 0, 0)
            conn.commit()
            return SettlementResult("settled", ledger_id, 0, 0, 0)

        effective_cap = _effective_cost_limit(conn, normalized_provider, "monthly")
        if effective_cap is None:
            allowance_debit = normalized_cost
        else:
            mtd_prior = _provider_spent_in_period(conn, normalized_provider, "monthly")
            remaining_allowance = max(effective_cap - mtd_prior, 0)
            allowance_debit = min(normalized_cost, remaining_allowance)

        overflow = normalized_cost - allowance_debit
        credit_debit = min(overflow, _credit_balance_usd6(conn)) if overflow > 0 else 0
        overflow_unattributed = overflow - credit_debit

        try:
            ledger_id = _record_cost_strict(
                conn,
                normalized_provider,
                operation,
                normalized_cost,
                idempotency_key=normalized_key,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_creation_tokens=cache_creation_tokens,
                cache_read_tokens=cache_read_tokens,
                model=model,
                request_id=request_id,
                is_estimated=is_estimated,
                allowance_debit_usd6=allowance_debit,
                credits_debit_usd6=credit_debit,
                overflow_unattributed_usd6=overflow_unattributed,
            )
        except sqlite3.IntegrityError:
            conn.rollback()
            return SettlementResult("replay_no_op", None, 0, 0, 0)

        if credit_debit > 0:
            conn.execute(
                """
                INSERT INTO credit_ledger (source, amount_usd6, cost_ledger_idempotency_key, notes)
                VALUES ('consume', ?, ?, ?)
                """,
                (-credit_debit, normalized_key, f"auto: {operation}"),
            )
            conn.execute(
                """
                UPDATE credit_balance
                   SET balance_usd6 = balance_usd6 - ?,
                       updated_at = datetime('now')
                 WHERE id = 1
                """,
                (credit_debit,),
            )

        conn.commit()
        return SettlementResult(
            status="settled",
            ledger_id=ledger_id,
            allowance_debited=allowance_debit,
            credits_debited=credit_debit,
            overflow_unattributed=overflow_unattributed,
        )
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def _period_bucket(period: str) -> str:
    now = datetime.now(timezone.utc)
    if period == "monthly":
        return now.strftime("%Y-%m")
    return now.strftime("%Y-%m-%d")


def _provider_spent_in_period(conn: sqlite3.Connection, provider: str, period: str) -> int:
    if period == "daily":
        row = conn.execute(
            """
            SELECT COALESCE(SUM(cost_usd6), 0) AS spent
            FROM cost_ledger
            WHERE provider = ?
              AND COALESCE(is_byok, 0) = 0
              AND created_at >= datetime('now', 'start of day')
            """,
            (provider,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(cost_usd6), 0) AS spent
            FROM cost_ledger
            WHERE provider = ?
              AND COALESCE(is_byok, 0) = 0
              AND strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')
            """,
            (provider,),
        ).fetchone()
    return int(row["spent"] if row is not None else 0)


def _total_spent_in_period(conn: sqlite3.Connection, period: str) -> int:
    if period == "daily":
        row = conn.execute(
            """
            SELECT COALESCE(SUM(cost_usd6), 0) AS spent
            FROM cost_ledger
            WHERE COALESCE(is_byok, 0) = 0
              AND created_at >= datetime('now', 'start of day')
            """
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(cost_usd6), 0) AS spent
            FROM cost_ledger
            WHERE COALESCE(is_byok, 0) = 0
              AND strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')
            """
        ).fetchone()
    return int(row["spent"] if row is not None else 0)


def _fire_cost_alert(
    db_path: str | Path | None,
    provider: str,
    period: str,
    spent_usd6: int,
    limit_usd6: int,
    source: str = "api",
) -> None:
    resolved_db_path = _coerce_db_path(db_path)
    if resolved_db_path is None:
        return

    period_bucket = _period_bucket(period)
    try:
        with _open_db(resolved_db_path) as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO cost_alert_log (
                    provider,
                    period,
                    threshold,
                    period_bucket
                ) VALUES (?, ?, ?, ?)
                """,
                (provider, period, "100pct", period_bucket),
            )
            conn.commit()
            if cursor.rowcount == 0:
                return

            if _HAS_ALERTS:
                try:
                    _db_str = str(resolved_db_path)
                    if _db_str == ":memory:" or _db_str.startswith("file:"):
                        _is_user_scoped = False
                    else:
                        _is_user_scoped = Path(resolved_db_path).resolve() != get_db_path()
                    creds = resolve_notification_creds(conn, "telegram", require=_is_user_scoped)
                    alerts.send(
                        (
                            f"Cost alert: {provider} {period} spend "
                            f"${spent_usd6 / _MICRODOLLARS_PER_DOLLAR:.2f} "
                            f"reached limit ${limit_usd6 / _MICRODOLLARS_PER_DOLLAR:.2f}"
                        ),
                        channel="telegram",
                        **creds,
                    )
                except Exception as exc:  # pragma: no cover - optional dependency path
                    log.warning("Failed to send cost alert notification: %s", exc)
    except Exception:
        return

    log_event(
        resolved_db_path,
        "cost.limit_warning",
        outcome="succeeded",
        source=source,
        properties={"provider": provider, "period": period, "spent_pct": 100},
    )


def check_cost_limit(
    db_path: str | Path | None,
    provider: str,
    projected_cost_usd6: int = 0,
    source: str = "api",
) -> tuple[bool, str | None]:
    resolved_db_path = _coerce_db_path(db_path)
    if resolved_db_path is None:
        return True, None

    normalized_provider = str(provider or "").strip().lower()
    projected_total = max(int(projected_cost_usd6), 0)
    blocked_reason: str | None = None

    with _open_db(resolved_db_path) as conn:
        # Use COMPAT_ROW_FACTORY so it works with both sqlite3 and sqlcipher connections
        from finance_cli.db import COMPAT_ROW_FACTORY
        conn.row_factory = COMPAT_ROW_FACTORY
        conn.execute("PRAGMA query_only = ON")

        for check_provider in (normalized_provider, "all"):
            for period in ("daily", "monthly"):
                limit_row = conn.execute(
                    """
                    SELECT limit_usd6, system_limit_usd6, action
                    FROM cost_limits
                    WHERE provider = ?
                      AND period = ?
                      AND is_active = 1
                    """,
                    (check_provider, period),
                ).fetchone()
                if limit_row is None:
                    continue
                limit_usd6 = _effective_limit_usd6(
                    limit_row["limit_usd6"],
                    limit_row["system_limit_usd6"],
                )
                if limit_usd6 is None:
                    continue

                if check_provider == "all":
                    spent = _total_spent_in_period(conn, period)
                else:
                    spent = _provider_spent_in_period(conn, normalized_provider, period)

                projected_spend = spent + projected_total
                if projected_spend < limit_usd6:
                    continue

                projected_dollars = projected_spend / _MICRODOLLARS_PER_DOLLAR
                limit_dollars = limit_usd6 / _MICRODOLLARS_PER_DOLLAR
                message = (
                    f"{check_provider} {period} limit reached "
                    f"(${projected_dollars:.2f} / ${limit_dollars:.2f})"
                )

                if str(limit_row["action"] or "warn") == "block" and blocked_reason is None:
                    blocked_reason = message

                _fire_cost_alert(
                    resolved_db_path,
                    check_provider,
                    period,
                    projected_spend,
                    limit_usd6,
                    source=source,
                )

    if blocked_reason is not None:
        return False, blocked_reason
    return True, None


def prune_cost_ledger(conn: sqlite3.Connection, retention_days: int = 365) -> None:
    conn.execute(
        """
        DELETE FROM cost_ledger
        WHERE created_at < datetime('now', ?)
        """,
        (f"-{int(retention_days)} days",),
    )


__all__ = [
    "PLAID_ITEM_MONTHLY_USD6",
    "PLAID_OPERATION_COSTS_USD6",
    "SettlementResult",
    "check_cost_limit",
    "dollars_to_usd6",
    "estimate_ai_cost_usd6",
    "prune_cost_ledger",
    "record_and_settle_cost",
    "record_cost",
]
