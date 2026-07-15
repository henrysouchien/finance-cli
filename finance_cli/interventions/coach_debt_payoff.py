"""Debt-payoff coaching intervention patterns.

The constant-payment evaluator deliberately uses a direct Python import of the
debt-payoff artifact helpers from ``finance_cli.mcp_server``. The import is
kept inside the artifact loader to avoid creating a registry-initialization
cycle while still using the PR-B artifact parser and directory resolver as the
source of truth.

Minimum-only detection is partial: the schema does not have a dedicated
payment-history table that cleanly links checking-account payments back to
individual liabilities. The primary signal uses transactions marked
``is_payment`` on the debt account itself; when that is unavailable, it falls
back to a coarse balance-snapshot proxy for non-decreasing balances.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import sqlite3
from typing import Any

from ..commands.common import fmt_dollars
from ..models import cents_to_dollars
from .context import InterventionContext
from .registry import (
    CFPDomain,
    CFPProcessStep,
    Intervention,
    Move,
    Priority,
    register_pattern,
)


_DOLLAR = Decimal("1")
_DTI_36 = Decimal("36")
_DTI_43 = Decimal("43")


@dataclass(frozen=True)
class _DtiCheck:
    dti_pct: Decimal
    debt_payments_cents: int
    income_cents: int
    sustained: bool
    approximation_used: bool
    prior_dti_pct: Decimal | None
    history_months: int


@dataclass(frozen=True)
class _MinimumOnlySignal:
    debt_id: str
    label: str
    months: int
    payment_cents: int
    source: str


@dataclass(frozen=True)
class _ScopeEntry:
    scope_id: str
    label: str | None
    payment_cents: int | None
    balance_cents: int | None


@dataclass(frozen=True)
class _DebtRecord:
    key: str
    liability_id: str | None
    account_id: str | None
    label: str
    balance_cents: int
    payment_cents: int
    source: str


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _money(cents: int) -> str:
    return fmt_dollars(cents_to_dollars(int(cents)))


def _pct_text(value: Decimal) -> str:
    return str(int(value.quantize(_DOLLAR, rounding=ROUND_HALF_UP)))


def _month_keys(as_of: date, months: int) -> list[str]:
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    cursor = end_of_last_complete_month.replace(day=1)
    values: list[str] = []
    for _ in range(int(months)):
        values.append(cursor.strftime("%Y-%m"))
        cursor = (cursor - timedelta(days=1)).replace(day=1)
    values.reverse()
    return values


def _month_bounds(month_key: str) -> tuple[date, date]:
    year, month = (int(part) for part in month_key.split("-", 1))
    start = date(year, month, 1)
    if month == 12:
        next_start = date(year + 1, 1, 1)
    else:
        next_start = date(year, month + 1, 1)
    return start, next_start - timedelta(days=1)


def _label(
    institution_name: Any,
    account_name: Any,
    card_ending: Any,
    fallback: str,
) -> str:
    pieces = [str(value).strip() for value in (institution_name, account_name) if str(value or "").strip()]
    label = " / ".join(pieces) if pieces else f"Debt {fallback[:8]}"
    ending = str(card_ending or "").strip()
    return f"{label} (...{ending})" if ending else label


def _income_cents_for_month(conn: sqlite3.Connection, month_key: str) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(t.amount_cents), 0) AS income_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE c.is_income = 1
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents > 0
           AND substr(t.date, 1, 7) = ?
        """,
        (month_key,),
    ).fetchone()
    return int(row["income_cents"] or 0)


def _recent_monthly_income_cents(conn: sqlite3.Connection, *, as_of: date) -> int:
    month_keys = _month_keys(as_of, 3)
    values = [_income_cents_for_month(conn, month_key) for month_key in month_keys]
    if values and values[-1] > 0:
        return values[-1]
    total = sum(values)
    if total <= 0:
        return 0
    return int((Decimal(total) / Decimal(len(values))).quantize(_DOLLAR, rounding=ROUND_HALF_UP))


def _active_debt_payment_obligations_cents(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(COALESCE(l.minimum_payment_cents, l.next_monthly_payment_cents, 0)), 0)
               AS payment_cents
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
        """
    ).fetchone()
    liability_payments = int(row["payment_cents"] or 0)
    loan_row = conn.execute(
        """
        SELECT COALESCE(SUM(monthly_payment_cents), 0) AS payment_cents
          FROM manual_loans
         WHERE is_active = 1
           AND current_balance_cents > 0
           AND monthly_payment_cents IS NOT NULL
        """
    ).fetchone()
    return liability_payments + int(loan_row["payment_cents"] or 0)


def _current_debt_balance_cents(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(ABS(COALESCE(a.balance_current_cents, 0))), 0) AS balance_cents
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
        """
    ).fetchone()
    liability_balance = int(row["balance_cents"] or 0)
    loan_row = conn.execute(
        """
        SELECT COALESCE(SUM(current_balance_cents), 0) AS balance_cents
          FROM manual_loans
         WHERE is_active = 1
        """
    ).fetchone()
    return liability_balance + int(loan_row["balance_cents"] or 0)


def _debt_snapshot_month_count(conn: sqlite3.Connection, *, month_keys: list[str]) -> int:
    if not month_keys:
        return 0
    placeholders = ",".join("?" for _ in month_keys)
    rows = conn.execute(
        f"""
        SELECT DISTINCT substr(bs.snapshot_date, 1, 7) AS month
          FROM balance_snapshots bs
          JOIN liabilities l ON l.account_id = bs.account_id
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
           AND substr(bs.snapshot_date, 1, 7) IN ({placeholders})
        """,
        tuple(month_keys),
    ).fetchall()
    snapshot_months = {str(row["month"]) for row in rows}
    payment_rows = conn.execute(
        f"""
        SELECT DISTINCT substr(t.date, 1, 7) AS month
          FROM transactions t
          JOIN liabilities l ON l.account_id = t.account_id
          JOIN accounts a ON a.id = l.account_id
         WHERE t.is_active = 1
           AND t.is_payment = 1
           AND l.is_active = 1
           AND a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
           AND substr(t.date, 1, 7) IN ({placeholders})
        """,
        tuple(month_keys),
    ).fetchall()
    payment_months = {str(row["month"]) for row in payment_rows}
    return len(snapshot_months | payment_months)


def _has_debt_snapshot_for_month(conn: sqlite3.Connection, month_key: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM balance_snapshots bs
          JOIN liabilities l ON l.account_id = bs.account_id
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
           AND substr(bs.snapshot_date, 1, 7) = ?
         LIMIT 1
        """,
        (month_key,),
    ).fetchone()
    return row is not None


def _dti_check(conn: sqlite3.Connection, ctx: InterventionContext, *, threshold_pct: Decimal) -> _DtiCheck | None:
    debt_payments_cents = _active_debt_payment_obligations_cents(conn)
    if debt_payments_cents <= 0:
        return None

    income_cents = _recent_monthly_income_cents(conn, as_of=ctx.now.date())
    if income_cents <= 0:
        return None

    dti_pct = Decimal(debt_payments_cents) / Decimal(income_cents) * Decimal("100")
    if dti_pct <= threshold_pct:
        return None

    recent_months = _month_keys(ctx.now.date(), 2)
    previous_month = recent_months[0] if recent_months else None
    prior_dti_pct: Decimal | None = None
    approximation_used = False
    sustained = False

    if previous_month and _has_debt_snapshot_for_month(conn, previous_month):
        previous_income_cents = _income_cents_for_month(conn, previous_month)
        if previous_income_cents > 0:
            prior_dti_pct = Decimal(debt_payments_cents) / Decimal(previous_income_cents) * Decimal("100")
            sustained = prior_dti_pct > threshold_pct
            approximation_used = True
    else:
        history_months = _debt_snapshot_month_count(conn, month_keys=recent_months)
        sustained = history_months >= 2
        approximation_used = sustained

    history_months = _debt_snapshot_month_count(conn, month_keys=recent_months)
    return _DtiCheck(
        dti_pct=dti_pct,
        debt_payments_cents=debt_payments_cents,
        income_cents=income_cents,
        sustained=sustained,
        approximation_used=approximation_used,
        prior_dti_pct=prior_dti_pct,
        history_months=history_months,
    )


def _previous_debt_balance_cents(conn: sqlite3.Connection, *, as_of: date) -> int | None:
    previous_month_end = as_of.replace(day=1) - timedelta(days=1)
    row = conn.execute(
        """
        SELECT COALESCE(SUM(ABS(COALESCE(bs.balance_current_cents, 0))), 0) AS balance_cents,
               COUNT(*) AS snapshot_count
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
          JOIN balance_snapshots bs
            ON bs.account_id = a.id
           AND bs.snapshot_date = (
                SELECT MAX(inner_bs.snapshot_date)
                  FROM balance_snapshots inner_bs
                 WHERE inner_bs.account_id = a.id
                   AND inner_bs.snapshot_date <= ?
           )
         WHERE l.is_active = 1
           AND a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
        """,
        (previous_month_end.isoformat(),),
    ).fetchone()
    if int(row["snapshot_count"] or 0) <= 0:
        return None
    return int(row["balance_cents"] or 0)


def _debt_balance_growing(conn: sqlite3.Connection, *, as_of: date) -> tuple[bool, int, int | None]:
    current_balance_cents = _current_debt_balance_cents(conn)
    previous_balance_cents = _previous_debt_balance_cents(conn, as_of=as_of)
    if previous_balance_cents is None:
        return False, current_balance_cents, None
    return current_balance_cents > previous_balance_cents, current_balance_cents, previous_balance_cents


def _dti_detail_bullets(check: _DtiCheck) -> tuple[str, ...]:
    bullets = [
        f"Monthly debt payments counted: {_money(check.debt_payments_cents)}",
        f"Monthly gross income used: {_money(check.income_cents)}",
    ]
    if check.prior_dti_pct is not None:
        bullets.append(
            f"Previous-month DTI approximation: {_pct_text(check.prior_dti_pct)}% using current debt obligations"
        )
    elif check.approximation_used:
        bullets.append(
            "Sustained check approximated from at least 2 months of debt-payment history; historical minimums are not versioned."
        )
    return tuple(bullets)


@register_pattern(
    id="dti_threshold_36",
    move=Move.DIAGNOSE,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE,),
)
def evaluate_dti_threshold_36(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    check = _dti_check(conn, ctx, threshold_pct=_DTI_36)
    if check is None or not check.sustained:
        return None

    threshold_payment_cents = int((Decimal(check.income_cents) * Decimal("0.36")).quantize(_DOLLAR))
    payment_gap_cents = max(check.debt_payments_cents - threshold_payment_cents, 0)

    return Intervention(
        pattern_id="dti_threshold_36",
        move=Move.DIAGNOSE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Your combined DTI is at {_pct_text(check.dti_pct)}%. Above 36% is the lender threshold "
            "for stress; this skill walks through reduction options."
        ),
        detail_bullets=_dti_detail_bullets(check),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=payment_gap_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("dti_threshold_36"),
    )


@register_pattern(
    id="dti_threshold_43",
    move=Move.WARN,
    tiers=(1, 4),
    priority=Priority.HIGH,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE,),
)
def evaluate_dti_threshold_43(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    check = _dti_check(conn, ctx, threshold_pct=_DTI_43)
    if check is None:
        return None

    debt_growing, current_balance_cents, previous_balance_cents = _debt_balance_growing(conn, as_of=ctx.now.date())
    if not check.sustained and not debt_growing:
        return None

    threshold_payment_cents = int((Decimal(check.income_cents) * Decimal("0.43")).quantize(_DOLLAR))
    payment_gap_cents = max(check.debt_payments_cents - threshold_payment_cents, 0)
    if debt_growing:
        headline = (
            f"DTI at {_pct_text(check.dti_pct)}% with debt growing - qualified-mortgage threshold crossed. "
            "DMP and bankruptcy belong on the table."
        )
    else:
        headline = (
            f"DTI at {_pct_text(check.dti_pct)}% for 2 months - qualified-mortgage threshold crossed. "
            "DMP and bankruptcy belong on the table."
        )

    bullets = list(_dti_detail_bullets(check))
    if previous_balance_cents is not None:
        bullets.append(
            f"Debt balance: {_money(current_balance_cents)} now vs {_money(previous_balance_cents)} last month"
        )

    return Intervention(
        pattern_id="dti_threshold_43",
        move=Move.WARN,
        tiers=(1, 4),
        priority=Priority.HIGH,
        headline=headline,
        detail_bullets=tuple(bullets),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=payment_gap_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("dti_threshold_43"),
    )


def _minimum_payment_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT l.id AS liability_id,
               a.id AS account_id,
               a.institution_name,
               a.account_name,
               a.card_ending,
               ABS(COALESCE(a.balance_current_cents, 0)) AS balance_cents,
               COALESCE(l.minimum_payment_cents, l.next_monthly_payment_cents, 0) AS payment_cents
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND a.is_active = 1
           AND COALESCE(l.minimum_payment_cents, l.next_monthly_payment_cents, 0) > 0
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
        """
    ).fetchall()


def _near_minimum_payment(payment_cents: int, minimum_cents: int) -> bool:
    if minimum_cents <= 0 or payment_cents <= 0:
        return False
    lower = int((Decimal(minimum_cents) * Decimal("0.95")).quantize(_DOLLAR, rounding=ROUND_HALF_UP))
    upper = int((Decimal(minimum_cents) * Decimal("1.10")).quantize(_DOLLAR, rounding=ROUND_HALF_UP))
    return lower <= payment_cents <= upper


def _minimum_only_from_transactions(conn: sqlite3.Connection, *, as_of: date) -> list[_MinimumOnlySignal]:
    rows = _minimum_payment_rows(conn)
    if not rows:
        return []

    month_keys = _month_keys(as_of, 3)
    start, _ = _month_bounds(month_keys[0])
    _, end = _month_bounds(month_keys[-1])
    by_debt: dict[str, dict[str, Any]] = {}
    for row in rows:
        by_debt[str(row["liability_id"])] = {
            "row": row,
            "months": set(),
        }

    payment_rows = conn.execute(
        """
        SELECT l.id AS liability_id,
               substr(t.date, 1, 7) AS month,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS payment_cents
          FROM transactions t
          JOIN liabilities l ON l.account_id = t.account_id
          JOIN accounts a ON a.id = l.account_id
         WHERE t.is_active = 1
           AND t.is_payment = 1
           AND l.is_active = 1
           AND a.is_active = 1
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY l.id, substr(t.date, 1, 7)
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchall()

    for payment_row in payment_rows:
        debt_id = str(payment_row["liability_id"])
        item = by_debt.get(debt_id)
        if item is None:
            continue
        minimum_cents = int(item["row"]["payment_cents"] or 0)
        if _near_minimum_payment(int(payment_row["payment_cents"] or 0), minimum_cents):
            item["months"].add(str(payment_row["month"]))

    signals: list[_MinimumOnlySignal] = []
    for debt_id, item in by_debt.items():
        months = item["months"]
        if len(months) < 3:
            continue
        row = item["row"]
        signals.append(
            _MinimumOnlySignal(
                debt_id=debt_id,
                label=_label(row["institution_name"], row["account_name"], row["card_ending"], debt_id),
                months=len(months),
                payment_cents=int(row["payment_cents"] or 0),
                source="payment_transactions",
            )
        )
    return signals


def _snapshot_balance_for_month(conn: sqlite3.Connection, *, account_id: str, month_key: str) -> int | None:
    start, end = _month_bounds(month_key)
    row = conn.execute(
        """
        SELECT ABS(COALESCE(balance_current_cents, 0)) AS balance_cents
          FROM balance_snapshots
         WHERE account_id = ?
           AND snapshot_date >= ?
           AND snapshot_date <= ?
         ORDER BY snapshot_date DESC, created_at DESC
         LIMIT 1
        """,
        (account_id, start.isoformat(), end.isoformat()),
    ).fetchone()
    if row is None:
        return None
    return int(row["balance_cents"] or 0)


def _minimum_only_from_balance_snapshots(conn: sqlite3.Connection, *, as_of: date) -> list[_MinimumOnlySignal]:
    month_keys = _month_keys(as_of, 3)
    signals: list[_MinimumOnlySignal] = []
    for row in _minimum_payment_rows(conn):
        balances: list[int] = []
        for month_key in month_keys:
            balance = _snapshot_balance_for_month(conn, account_id=str(row["account_id"]), month_key=month_key)
            if balance is None:
                break
            balances.append(balance)
        if len(balances) != 3:
            continue
        if not (balances[0] <= balances[1] <= balances[2]):
            continue
        debt_id = str(row["liability_id"])
        signals.append(
            _MinimumOnlySignal(
                debt_id=debt_id,
                label=_label(row["institution_name"], row["account_name"], row["card_ending"], debt_id),
                months=3,
                payment_cents=int(row["payment_cents"] or 0),
                source="balance_snapshot_proxy",
            )
        )
    return signals


@register_pattern(
    id="minimum_only_payments",
    move=Move.DIAGNOSE,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=90),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.IMPLEMENT,),
)
def evaluate_minimum_only_payments(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    signals = _minimum_only_from_transactions(conn, as_of=ctx.now.date())
    if len(signals) < 2:
        signals = _minimum_only_from_balance_snapshots(conn, as_of=ctx.now.date())
    if len(signals) < 2:
        return None

    signals = sorted(signals, key=lambda item: (-item.payment_cents, item.label))
    months = min(signal.months for signal in signals)
    total_payment_cents = sum(signal.payment_cents for signal in signals)
    source = signals[0].source
    source_text = (
        "payment transactions"
        if source == "payment_transactions"
        else "balance snapshots as a minimum-only proxy"
    )

    return Intervention(
        pattern_id="minimum_only_payments",
        move=Move.DIAGNOSE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"{len(signals)} cards on minimum-only for ~{months} months - this is the trap pattern. "
            "The skill can map a path out."
        ),
        detail_bullets=(
            f"Detected from {source_text}.",
            "Matched debts: " + ", ".join(signal.label for signal in signals[:3]),
            f"Minimum-payment cash flow in pattern: {_money(total_payment_cents)}/mo",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=total_payment_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("minimum_only_payments"),
    )


def _latest_debt_payoff_artifact() -> tuple[Path, dict[str, Any]] | None:
    from ..mcp_server import _debt_payoff_artifact_dir, _parse_debt_payoff_artifact

    artifact_dir = _debt_payoff_artifact_dir()
    if not artifact_dir.exists():
        return None

    artifact_files = list(artifact_dir.glob("*.md"))
    if not artifact_files:
        return None

    latest = max(artifact_files, key=lambda path: (path.stat().st_mtime, path.name))
    payload = _parse_debt_payoff_artifact(latest.read_text(encoding="utf-8"))
    if not payload:
        return None
    return latest, payload


def _parse_generated_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
        return parsed.date()
    except ValueError:
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None


def _entry_payment_cents(item: Any) -> int | None:
    if not isinstance(item, dict):
        return None
    for key in ("minimum_payment_cents", "monthly_payment_cents", "payment_cents", "min_payment_cents"):
        value = item.get(key)
        if value is not None:
            return max(_as_int(value), 0)
    return None


def _entry_balance_cents(item: Any) -> int | None:
    if not isinstance(item, dict):
        return None
    for key in ("balance_cents", "current_balance_cents", "starting_balance_cents", "statement_balance_cents"):
        value = item.get(key)
        if value is not None:
            return max(_as_int(value), 0)
    return None


def _scope_entries(value: Any) -> list[_ScopeEntry]:
    if not isinstance(value, list):
        return []
    entries: list[_ScopeEntry] = []
    for item in value:
        if isinstance(item, dict):
            scope_id = (
                item.get("id")
                or item.get("liability_id")
                or item.get("account_id")
                or item.get("card_id")
                or item.get("loan_id")
            )
            if not scope_id:
                continue
            label = item.get("label") or item.get("name") or item.get("account_name") or item.get("creditor_name")
            entries.append(
                _ScopeEntry(
                    scope_id=str(scope_id),
                    label=None if label is None else str(label),
                    payment_cents=_entry_payment_cents(item),
                    balance_cents=_entry_balance_cents(item),
                )
            )
        elif item is not None:
            entries.append(_ScopeEntry(scope_id=str(item), label=None, payment_cents=None, balance_cents=None))
    return entries


def _debt_records_for_scope(conn: sqlite3.Connection, entries: list[_ScopeEntry]) -> dict[str, _DebtRecord]:
    ids = {entry.scope_id for entry in entries}
    if not ids:
        return {}

    result: dict[str, _DebtRecord] = {}
    placeholders = ",".join("?" for _ in ids)
    liability_rows = conn.execute(
        f"""
        SELECT l.id AS liability_id,
               l.account_id,
               a.institution_name,
               a.account_name,
               a.card_ending,
               ABS(COALESCE(a.balance_current_cents, 0)) AS balance_cents,
               COALESCE(l.minimum_payment_cents, l.next_monthly_payment_cents, 0) AS payment_cents
          FROM liabilities l
          LEFT JOIN accounts a ON a.id = l.account_id
         WHERE l.id IN ({placeholders})
            OR l.account_id IN ({placeholders})
        """,
        tuple(ids) + tuple(ids),
    ).fetchall()
    for row in liability_rows:
        liability_id = str(row["liability_id"])
        account_id = None if row["account_id"] is None else str(row["account_id"])
        entry = next((item for item in entries if item.scope_id in {liability_id, account_id}), None)
        label = (
            entry.label
            if entry is not None and entry.label
            else _label(row["institution_name"], row["account_name"], row["card_ending"], liability_id)
        )
        payment_cents = int(row["payment_cents"] or 0)
        if payment_cents <= 0 and entry is not None and entry.payment_cents is not None:
            payment_cents = entry.payment_cents
        record = _DebtRecord(
            key=liability_id,
            liability_id=liability_id,
            account_id=account_id,
            label=label,
            balance_cents=int(row["balance_cents"] or 0),
            payment_cents=payment_cents,
            source="liability",
        )
        result[liability_id] = record
        if account_id is not None:
            result[account_id] = record

    loan_rows = conn.execute(
        f"""
        SELECT id, creditor_name, current_balance_cents, monthly_payment_cents
          FROM manual_loans
         WHERE id IN ({placeholders})
        """,
        tuple(ids),
    ).fetchall()
    for row in loan_rows:
        loan_id = str(row["id"])
        entry = next((item for item in entries if item.scope_id == loan_id), None)
        label = entry.label if entry is not None and entry.label else f"Loan: {row['creditor_name']}"
        payment_cents = int(row["monthly_payment_cents"] or 0)
        if payment_cents <= 0 and entry is not None and entry.payment_cents is not None:
            payment_cents = entry.payment_cents
        result[loan_id] = _DebtRecord(
            key=loan_id,
            liability_id=None,
            account_id=None,
            label=label,
            balance_cents=int(row["current_balance_cents"] or 0),
            payment_cents=payment_cents,
            source="manual_loan",
        )
    return result


def _had_positive_balance_since(
    conn: sqlite3.Connection,
    record: _DebtRecord,
    entry: _ScopeEntry,
    *,
    generated_on: date,
    as_of: date,
) -> bool:
    if entry.balance_cents is not None and entry.balance_cents > 0:
        return True
    if record.account_id is not None:
        row = conn.execute(
            """
            SELECT 1
              FROM balance_snapshots
             WHERE account_id = ?
               AND snapshot_date >= ?
               AND snapshot_date <= ?
               AND ABS(COALESCE(balance_current_cents, 0)) > 0
             LIMIT 1
            """,
            (record.account_id, generated_on.isoformat(), as_of.isoformat()),
        ).fetchone()
        if row is not None:
            return True
    return generated_on <= as_of


def _unique_records(records: list[_DebtRecord]) -> list[_DebtRecord]:
    seen: set[str] = set()
    unique: list[_DebtRecord] = []
    for record in records:
        if record.key in seen:
            continue
        seen.add(record.key)
        unique.append(record)
    return unique


def _payment_transactions_for_records(
    conn: sqlite3.Connection,
    records: list[_DebtRecord],
    *,
    start_date: date,
    end_date: date,
) -> int:
    account_ids = [record.account_id for record in records if record.account_id is not None]
    total_cents = 0
    if account_ids:
        placeholders = ",".join("?" for _ in account_ids)
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(ABS(amount_cents)), 0) AS payment_cents
              FROM transactions
             WHERE is_active = 1
               AND is_payment = 1
               AND account_id IN ({placeholders})
               AND date >= ?
               AND date <= ?
            """,
            tuple(account_ids) + (start_date.isoformat(), end_date.isoformat()),
        ).fetchone()
        total_cents += int(row["payment_cents"] or 0)

    loan_ids = [record.key for record in records if record.source == "manual_loan"]
    if loan_ids:
        placeholders = ",".join("?" for _ in loan_ids)
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(amount_cents), 0) AS payment_cents
              FROM loan_payments
             WHERE loan_id IN ({placeholders})
               AND payment_date >= ?
               AND payment_date <= ?
            """,
            tuple(loan_ids) + (start_date.isoformat(), end_date.isoformat()),
        ).fetchone()
        total_cents += int(row["payment_cents"] or 0)
    return total_cents


@register_pattern(
    id="constant_payment_violation",
    move=Move.COACH,
    tiers=(1, 4),
    priority=Priority.HIGH,
    cooldown=timedelta(days=14),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.MONITOR,),
)
def evaluate_constant_payment_violation(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    artifact = _latest_debt_payoff_artifact()
    if artifact is None:
        return None

    artifact_path, payload = artifact
    monthly_commitment_cents = _as_int(payload.get("monthly_commitment_cents"))
    if monthly_commitment_cents <= 0:
        return None

    generated_on = _parse_generated_date(payload.get("generated_at"))
    if generated_on is None:
        return None

    entries = _scope_entries(payload.get("debts_in_scope"))
    if not entries:
        return None

    records_by_scope = _debt_records_for_scope(conn, entries)
    cleared_records: list[_DebtRecord] = []
    all_records: list[_DebtRecord] = []
    for entry in entries:
        record = records_by_scope.get(entry.scope_id)
        if record is None:
            continue
        all_records.append(record)
        if record.balance_cents > 0:
            continue
        if _had_positive_balance_since(conn, record, entry, generated_on=generated_on, as_of=ctx.now.date()):
            cleared_records.append(record)

    cleared_records = _unique_records(cleared_records)
    if not cleared_records:
        return None

    all_records = _unique_records(all_records)
    open_records = [record for record in all_records if record.balance_cents > 0]
    remaining_minimums_cents = sum(max(record.payment_cents, 0) for record in open_records)
    current_month_start = ctx.now.date().replace(day=1)
    transaction_payment_cents = _payment_transactions_for_records(
        conn,
        all_records,
        start_date=current_month_start,
        end_date=ctx.now.date(),
    )
    actual_monthly_debt_payments_cents = max(transaction_payment_cents, remaining_minimums_cents)
    commitment_floor_cents = int(
        (Decimal(monthly_commitment_cents) * Decimal("0.95")).quantize(_DOLLAR, rounding=ROUND_HALF_UP)
    )
    if actual_monthly_debt_payments_cents >= commitment_floor_cents:
        return None

    cleared = max(cleared_records, key=lambda record: (record.payment_cents, record.label))
    gap_cents = max(monthly_commitment_cents - actual_monthly_debt_payments_cents, 0)
    freed_up_cents = max(cleared.payment_cents, gap_cents)
    amount = _money(freed_up_cents)

    return Intervention(
        pattern_id="constant_payment_violation",
        move=Move.COACH,
        tiers=(1, 4),
        priority=Priority.HIGH,
        headline=(
            f"You cleared {cleared.label} this month - but the freed-up {amount}/mo is back in spending "
            f"instead of attacking the next debt. The plan needs that {amount} redirected."
        ),
        detail_bullets=(
            f"Plan commitment: {_money(monthly_commitment_cents)}/mo",
            f"Current debt-payment run rate: {_money(actual_monthly_debt_payments_cents)}/mo",
            f"Artifact baseline: {artifact_path.name}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=gap_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("constant_payment_violation"),
    )
