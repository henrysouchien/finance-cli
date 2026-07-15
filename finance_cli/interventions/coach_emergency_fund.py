"""Emergency-fund coaching intervention patterns.

Mirrors the structure of ``coach_debt_payoff``: four registered patterns whose
evaluators query historical data via ``balance_snapshots`` (using the
``MAX(snapshot_date <= ?)`` pattern for as-of snapshots) and transaction
history rather than persisting evaluator-observation state. The drawdown
pattern is artifact-driven and re-fire-suppressed by classification entries
stored in the artifact's machine-readable footer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import sqlite3
import statistics
from typing import Any

from ..commands.common import fmt_dollars
from ..models import cents_to_dollars
from ..spending_analysis import category_spending_averages
from .context import InterventionContext
from .registry import (
    CFPDomain,
    CFPProcessStep,
    Intervention,
    InterventionAction,
    Move,
    Priority,
    register_pattern,
)


_DOLLAR = Decimal("1")
_LIQUIDITY_TARGET_MONTHS = Decimal("3")
_CASH_FLOW_SURPLUS_90D_CENTS = 150_000  # $1,500 over a 90-day window (~$500/mo equivalent)
_DRAWDOWN_DROP_PCT = Decimal("20")
_DRAWDOWN_REPLENISH_PCT = Decimal("5")
_DRAWDOWN_DROP_WINDOW_DAYS = 60
_DRAWDOWN_REPLENISH_WINDOW_DAYS = 90
_INCOME_SHOCK_SEVERE_FRACTION = Decimal("0.60")
_INCOME_SHOCK_SUSTAINED_FRACTION = Decimal("0.80")
_INCOME_TX_COMPLETENESS_FRACTION = Decimal("0.50")


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _money(cents: int) -> str:
    return fmt_dollars(cents_to_dollars(int(cents)))


def _pct_text(value: Decimal) -> str:
    return str(int(value.quantize(_DOLLAR, rounding=ROUND_HALF_UP)))


def _ratio_text(value: Decimal) -> str:
    quantized = value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    return format(quantized, "f")


def _month_keys(as_of: date, months: int) -> list[str]:
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    cursor = end_of_last_complete_month.replace(day=1)
    values: list[str] = []
    for _ in range(int(months)):
        values.append(cursor.strftime("%Y-%m"))
        cursor = (cursor - timedelta(days=1)).replace(day=1)
    values.reverse()
    return values


def _previous_month_end(as_of: date) -> date:
    return as_of.replace(day=1) - timedelta(days=1)


def _liquid_balance_account_ids(conn: sqlite3.Connection) -> list[str]:
    """Return active liquid account ids (checking + savings; excludes aliased hashes).

    Schema CHECK constraint on ``accounts.account_type`` restricts values to
    {checking, savings, credit_card, investment, loan}. MMA accounts in this
    DB surface as ``savings``; standalone MMA / money-market mutual funds
    aren't supported as a distinct type at the schema level. v0.1 treats
    checking + savings as the canonical liquid set.
    """
    rows = conn.execute(
        """
        SELECT a.id
          FROM accounts a
         WHERE a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
           AND COALESCE(LOWER(a.account_type), '') IN ('checking', 'savings')
        """
    ).fetchall()
    return [str(row["id"]) for row in rows]


def _liquid_balance_now_cents(conn: sqlite3.Connection) -> int:
    ids = _liquid_balance_account_ids(conn)
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    row = conn.execute(
        f"""
        SELECT COALESCE(SUM(CASE WHEN COALESCE(balance_current_cents, 0) > 0
                                 THEN balance_current_cents ELSE 0 END), 0) AS balance_cents
          FROM accounts
         WHERE id IN ({placeholders})
        """,
        tuple(ids),
    ).fetchone()
    return int(row["balance_cents"] or 0)


def _liquid_balance_as_of_cents(conn: sqlite3.Connection, *, as_of: date) -> tuple[int, int]:
    """Sum of liquid balances across snapshots <= as_of.

    Returns (balance_cents, accounts_with_snapshots). When no account has a
    snapshot at-or-before ``as_of``, returns (0, 0) so callers can detect the
    missing-history case rather than treating absence as a zero balance.
    """
    ids = _liquid_balance_account_ids(conn)
    if not ids:
        return 0, 0
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT a.id AS account_id,
               (
                 SELECT bs.balance_current_cents
                   FROM balance_snapshots bs
                  WHERE bs.account_id = a.id
                    AND bs.snapshot_date <= ?
                  ORDER BY bs.snapshot_date DESC, bs.created_at DESC
                  LIMIT 1
               ) AS balance_cents
          FROM accounts a
         WHERE a.id IN ({placeholders})
        """,
        (as_of.isoformat(),) + tuple(ids),
    ).fetchall()
    total = 0
    accounts_with_snapshots = 0
    for row in rows:
        raw = row["balance_cents"]
        if raw is None:
            continue
        accounts_with_snapshots += 1
        value = int(raw)
        if value > 0:
            total += value
    return total, accounts_with_snapshots


def _account_sum_as_of_cents(
    conn: sqlite3.Connection,
    account_ids: list[str],
    *,
    as_of: date,
) -> tuple[int, int]:
    """Sum balances across an explicit set of accounts at MAX(snapshot_date <= as_of).

    Returns (balance_cents, accounts_with_snapshots).
    """
    if not account_ids:
        return 0, 0
    placeholders = ",".join("?" for _ in account_ids)
    rows = conn.execute(
        f"""
        SELECT a.id AS account_id,
               (
                 SELECT ABS(COALESCE(bs.balance_current_cents, 0))
                   FROM balance_snapshots bs
                  WHERE bs.account_id = a.id
                    AND bs.snapshot_date <= ?
                  ORDER BY bs.snapshot_date DESC, bs.created_at DESC
                  LIMIT 1
               ) AS balance_cents
          FROM accounts a
         WHERE a.id IN ({placeholders})
        """,
        (as_of.isoformat(),) + tuple(account_ids),
    ).fetchall()
    total = 0
    accounts_with_snapshots = 0
    for row in rows:
        raw = row["balance_cents"]
        if raw is None:
            continue
        accounts_with_snapshots += 1
        total += int(raw)
    return total, accounts_with_snapshots


def _essential_monthly_cents(
    conn: sqlite3.Connection,
    *,
    as_of: date | None = None,
    months: int = 3,
    rules_path: Path | None = None,
) -> int:
    categories = category_spending_averages(
        conn,
        months=months,
        as_of=as_of,
        rules_path=rules_path,
    )
    return sum(
        int(entry.avg_monthly_cents)
        for entry in categories
        if entry.classification == "essential"
    )


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


def _income_tx_count_for_month(conn: sqlite3.Connection, month_key: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS tx_count
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
    return int(row["tx_count"] or 0)


def _net_flow_cents(
    conn: sqlite3.Connection,
    *,
    start_date: date,
    end_date: date,
) -> int:
    """Net cash flow = income - non-payment expense over an inclusive window."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(
                 CASE
                   WHEN c.is_income = 1 AND t.amount_cents > 0 THEN t.amount_cents
                   WHEN c.is_income = 0 AND t.is_payment = 0 AND t.amount_cents < 0 THEN t.amount_cents
                   ELSE 0
                 END
               ), 0) AS net_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.date >= ?
           AND t.date <= ?
        """,
        (start_date.isoformat(), end_date.isoformat()),
    ).fetchone()
    return int(row["net_cents"] or 0)


@dataclass(frozen=True)
class _LiquidityCheck:
    current_ratio: Decimal
    prior_ratio: Decimal | None
    liquid_now_cents: int
    liquid_prior_cents: int
    essential_now_cents: int
    essential_prior_cents: int


def _liquidity_check(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> _LiquidityCheck | None:
    essential_now = _essential_monthly_cents(
        conn,
        as_of=ctx.now.date(),
        rules_path=ctx.rules_path,
    )
    if essential_now <= 0:
        return None
    liquid_now = _liquid_balance_now_cents(conn)
    current_ratio = Decimal(liquid_now) / Decimal(essential_now)

    prior_month_end = _previous_month_end(ctx.now.date())
    liquid_prior, accounts_with_snapshots = _liquid_balance_as_of_cents(
        conn,
        as_of=prior_month_end,
    )
    if accounts_with_snapshots == 0:
        return _LiquidityCheck(
            current_ratio=current_ratio,
            prior_ratio=None,
            liquid_now_cents=liquid_now,
            liquid_prior_cents=0,
            essential_now_cents=essential_now,
            essential_prior_cents=0,
        )
    essential_prior = _essential_monthly_cents(
        conn,
        as_of=prior_month_end,
        rules_path=ctx.rules_path,
    )
    if essential_prior <= 0:
        # Insufficient prior-month essentials data — treat as unknown.
        return _LiquidityCheck(
            current_ratio=current_ratio,
            prior_ratio=None,
            liquid_now_cents=liquid_now,
            liquid_prior_cents=liquid_prior,
            essential_now_cents=essential_now,
            essential_prior_cents=0,
        )
    prior_ratio = Decimal(liquid_prior) / Decimal(essential_prior)
    return _LiquidityCheck(
        current_ratio=current_ratio,
        prior_ratio=prior_ratio,
        liquid_now_cents=liquid_now,
        liquid_prior_cents=liquid_prior,
        essential_now_cents=essential_now,
        essential_prior_cents=essential_prior,
    )


@register_pattern(
    id="liquidity_below_3_months",
    move=Move.DIAGNOSE,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=14),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.RISK_INSURANCE),
    cfp_steps=(CFPProcessStep.ANALYZE,),
)
def evaluate_liquidity_below_3_months(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    check = _liquidity_check(conn, ctx)
    if check is None:
        return None
    if check.current_ratio >= _LIQUIDITY_TARGET_MONTHS:
        return None
    if check.prior_ratio is None or check.prior_ratio >= _LIQUIDITY_TARGET_MONTHS:
        # The "sustained" requirement: we need the prior calendar-month-end
        # ratio also below 3. Missing prior history -> we cannot assert
        # sustained -> do not fire (avoids one-month-of-data false positives).
        return None
    target_balance_cents = int(
        (Decimal(check.essential_now_cents) * _LIQUIDITY_TARGET_MONTHS).quantize(
            _DOLLAR, rounding=ROUND_HALF_UP
        )
    )
    gap_cents = max(target_balance_cents - check.liquid_now_cents, 0)
    return Intervention(
        pattern_id="liquidity_below_3_months",
        move=Move.DIAGNOSE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Liquidity sits at {_ratio_text(check.current_ratio)} months of essentials - "
            "below the 3-month buffer that absorbs shocks without new debt."
        ),
        detail_bullets=(
            f"Liquid balance: {_money(check.liquid_now_cents)}",
            f"Essential monthly expenses: {_money(check.essential_now_cents)}/mo (3-month average)",
            f"Prior-month-end ratio: {_ratio_text(check.prior_ratio)} months (sustained below 3)",
            f"Gap to 3-month buffer: {_money(gap_cents)}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Walk through emergency-fund coaching",
            tool="activate_skill",
            params={"name": "coach_emergency_fund"},
            build_stub=False,
        ),
        dollar_impact_cents=gap_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("liquidity_below_3_months"),
    )


@dataclass(frozen=True)
class _SurplusCheck:
    surplus_cents: int
    balance_change_cents: int
    liquid_start_cents: int
    liquid_end_cents: int
    snapshots_start: int
    snapshots_end: int


def _cash_flow_surplus_check(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> _SurplusCheck | None:
    end_date = ctx.now.date()
    start_date = end_date - timedelta(days=90)
    surplus = _net_flow_cents(conn, start_date=start_date, end_date=end_date)
    liquid_start, snapshots_start = _liquid_balance_as_of_cents(conn, as_of=start_date)
    liquid_end, snapshots_end = _liquid_balance_as_of_cents(conn, as_of=end_date)
    if snapshots_start == 0 or snapshots_end == 0:
        return None
    return _SurplusCheck(
        surplus_cents=surplus,
        balance_change_cents=liquid_end - liquid_start,
        liquid_start_cents=liquid_start,
        liquid_end_cents=liquid_end,
        snapshots_start=snapshots_start,
        snapshots_end=snapshots_end,
    )


@register_pattern(
    id="cash_flow_surplus_no_savings",
    move=Move.COACH,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.DEVELOP,),
)
def evaluate_cash_flow_surplus_no_savings(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    check = _cash_flow_surplus_check(conn, ctx)
    if check is None:
        return None
    if check.surplus_cents < _CASH_FLOW_SURPLUS_90D_CENTS:
        return None
    # Balance growth needs to be less than half the surplus for this pattern to fire.
    growth_ceiling_cents = check.surplus_cents // 2
    if check.balance_change_cents >= growth_ceiling_cents:
        return None
    # Reciprocal disjointness gate with `coach_savings_goal`'s
    # `cash_flow_surplus_no_savings_goal` per the savings-goal plan
    # §"Compatible coexistence" disposition. Once the e-fund is built
    # (coverage >= 3 months at the period-end snapshot), the savings-goal
    # pattern owns the surplus window and this pattern must defer. Share
    # ctx.rules_path with the savings-goal evaluator so both gates derive
    # the same essentials denominator under user-customized rules.
    essential = _essential_monthly_cents(
        conn,
        as_of=ctx.now.date(),
        rules_path=ctx.rules_path,
    )
    if essential > 0:
        coverage = Decimal(check.liquid_end_cents) / Decimal(essential)
        if coverage >= _LIQUIDITY_TARGET_MONTHS:
            return None
    return Intervention(
        pattern_id="cash_flow_surplus_no_savings",
        move=Move.COACH,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"~{_money(check.surplus_cents)} of cash-flow surplus over the last 90 days "
            f"and only {_money(max(check.balance_change_cents, 0))} of it landed in liquid savings - "
            "this is the window to build the buffer."
        ),
        detail_bullets=(
            f"90-day net cash flow: {_money(check.surplus_cents)}",
            f"90-day liquid-balance change: {_money(check.balance_change_cents)}",
            f"Snapshots at window endpoints: {check.snapshots_start} -> {check.snapshots_end}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Walk through emergency-fund coaching",
            tool="activate_skill",
            params={"name": "coach_emergency_fund"},
            build_stub=False,
        ),
        dollar_impact_cents=check.surplus_cents - max(check.balance_change_cents, 0),
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("cash_flow_surplus_no_savings"),
    )


# ---------------------------------------------------------------------------
# Drawdown intervention — artifact-driven, classification-aware
# ---------------------------------------------------------------------------


def _latest_emergency_fund_artifact() -> tuple[Path, dict[str, Any]] | None:
    """Return (path, parsed_payload) for the most-recent emergency-fund artifact.

    Uses ``finance_cli.mcp_server`` directly (the same lazy-import pattern as
    the debt-payoff constant-payment-violation evaluator) so the artifact
    parser and directory resolver are the single source of truth.
    """
    from ..mcp_server import (
        _emergency_fund_artifact_dir,
        _latest_artifact_path,
        _parse_emergency_fund_artifact,
    )

    artifact_dir = _emergency_fund_artifact_dir()
    if not artifact_dir.exists():
        return None
    latest = _latest_artifact_path(artifact_dir)
    if latest is None:
        return None
    payload = _parse_emergency_fund_artifact(latest.read_text(encoding="utf-8"))
    if not payload:
        return None
    return latest, payload


def _parse_iso_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw).date()
    except ValueError:
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None


@dataclass(frozen=True)
class _DrawdownEvent:
    pre_drop_date: date
    low_date: date
    pre_drop_balance_cents: int
    low_balance_cents: int
    account_ids: tuple[str, ...]


def _scan_drawdown_event(
    conn: sqlite3.Connection,
    account_ids: list[str],
    *,
    as_of: date,
    artifact_generated_on: date | None,
) -> _DrawdownEvent | None:
    """Find the most-recent drawdown event that exceeds the drop threshold.

    Scans the 60-day rolling window ending at ``as_of`` plus the prior 90-day
    replenishment-check window. Returns the event only if (a) the drop from
    ``pre_drop`` to ``low`` exceeds ``_DRAWDOWN_DROP_PCT``, AND (b) the
    balance has NOT recovered to within ``_DRAWDOWN_REPLENISH_PCT`` of
    ``pre_drop`` within the next 90 days.
    """
    if not account_ids:
        return None
    scan_start = max(
        as_of - timedelta(days=_DRAWDOWN_DROP_WINDOW_DAYS + _DRAWDOWN_REPLENISH_WINDOW_DAYS),
        artifact_generated_on or date.min,
    )
    placeholders = ",".join("?" for _ in account_ids)
    rows = conn.execute(
        f"""
        SELECT DISTINCT snapshot_date
          FROM balance_snapshots
         WHERE account_id IN ({placeholders})
           AND snapshot_date >= ?
           AND snapshot_date <= ?
         ORDER BY snapshot_date ASC
        """,
        tuple(account_ids) + (scan_start.isoformat(), as_of.isoformat()),
    ).fetchall()
    snapshot_dates = [date.fromisoformat(str(row["snapshot_date"])) for row in rows]
    if len(snapshot_dates) < 2:
        return None

    sorted_ids = tuple(sorted(account_ids))
    pre_drop_date: date | None = None
    pre_drop_balance: int = 0
    low_date: date | None = None
    low_balance: int = 0
    for current in snapshot_dates:
        if (as_of - current).days > _DRAWDOWN_DROP_WINDOW_DAYS:
            continue
        # Rolling window: pre_drop is the highest balance in [current - 60d, current]
        window_start = current - timedelta(days=_DRAWDOWN_DROP_WINDOW_DAYS)
        window_balance_cents, snapshots = _account_sum_as_of_cents(
            conn,
            account_ids,
            as_of=window_start,
        )
        if snapshots == 0:
            continue
        current_balance_cents, _ = _account_sum_as_of_cents(
            conn,
            account_ids,
            as_of=current,
        )
        if window_balance_cents <= 0:
            continue
        drop_pct = (
            Decimal(window_balance_cents - current_balance_cents)
            / Decimal(window_balance_cents)
            * Decimal("100")
        )
        if drop_pct < _DRAWDOWN_DROP_PCT:
            continue
        if pre_drop_date is None or current > (low_date or date.min):
            pre_drop_date = window_start
            pre_drop_balance = window_balance_cents
            low_date = current
            low_balance = current_balance_cents

    if pre_drop_date is None or low_date is None:
        return None

    # Replenishment check: any snapshot in the 90 days after low_date that
    # brings balance to within 5% of pre_drop?
    replenish_end = min(low_date + timedelta(days=_DRAWDOWN_REPLENISH_WINDOW_DAYS), as_of)
    if replenish_end > low_date:
        threshold = int(
            (Decimal(pre_drop_balance) * (Decimal("100") - _DRAWDOWN_REPLENISH_PCT) / Decimal("100"))
            .quantize(_DOLLAR, rounding=ROUND_HALF_UP)
        )
        check_rows = conn.execute(
            f"""
            SELECT DISTINCT snapshot_date
              FROM balance_snapshots
             WHERE account_id IN ({placeholders})
               AND snapshot_date > ?
               AND snapshot_date <= ?
             ORDER BY snapshot_date ASC
            """,
            tuple(account_ids) + (low_date.isoformat(), replenish_end.isoformat()),
        ).fetchall()
        for check_row in check_rows:
            check_date = date.fromisoformat(str(check_row["snapshot_date"]))
            balance, _ = _account_sum_as_of_cents(conn, account_ids, as_of=check_date)
            if balance >= threshold:
                return None

    return _DrawdownEvent(
        pre_drop_date=pre_drop_date,
        low_date=low_date,
        pre_drop_balance_cents=pre_drop_balance,
        low_balance_cents=low_balance,
        account_ids=sorted_ids,
    )


def _intervals_overlap(a_start: date, a_end: date, b_start: date, b_end: date) -> bool:
    return a_start <= b_end and b_start <= a_end


def _drawdown_already_classified(
    event: _DrawdownEvent,
    classified_entries: list[dict[str, Any]],
) -> bool:
    """Match the candidate event against artifact-classified entries.

    Suppression rule (R6): an event is considered already-classified when ALL of:
      (a) ``[pre_drop_date, low_date]`` intervals overlap,
      (b) ``account_ids`` sets are equal, and
      (c) ``pre_drop_balance_cents`` and ``low_balance_cents`` are within 5%
          of the classified entry's values.
    Both ``user_classified_as_emergency=True`` and ``=False`` suppress re-fire;
    the user has decided and the intervention's job is done.
    """
    candidate_account_ids = set(event.account_ids)
    for entry in classified_entries:
        if not isinstance(entry, dict):
            continue
        entry_pre = _parse_iso_date(entry.get("pre_drop_date"))
        entry_low = _parse_iso_date(entry.get("low_date"))
        if entry_pre is None or entry_low is None:
            continue
        entry_ids = entry.get("account_ids") or []
        if not isinstance(entry_ids, (list, tuple)):
            continue
        entry_ids_set = {str(i) for i in entry_ids}
        if entry_ids_set != candidate_account_ids:
            continue
        if not _intervals_overlap(event.pre_drop_date, event.low_date, entry_pre, entry_low):
            continue
        entry_pre_balance = _as_int(entry.get("pre_drop_balance_cents"))
        entry_low_balance = _as_int(entry.get("low_balance_cents"))
        if entry_pre_balance <= 0:
            continue
        pre_tolerance = abs(event.pre_drop_balance_cents - entry_pre_balance) / max(entry_pre_balance, 1)
        low_denom = max(abs(entry_low_balance), 1)
        low_tolerance = abs(event.low_balance_cents - entry_low_balance) / low_denom
        if pre_tolerance > 0.05 or low_tolerance > 0.05:
            continue
        return True
    return False


@register_pattern(
    id="emergency_fund_drawdown_no_replenishment",
    move=Move.COACH,
    tiers=(1, 4),
    priority=Priority.HIGH,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.RISK_INSURANCE),
    cfp_steps=(CFPProcessStep.MONITOR,),
)
def evaluate_emergency_fund_drawdown(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    artifact = _latest_emergency_fund_artifact()
    if artifact is None:
        return None
    artifact_path, payload = artifact
    raw_account_ids = payload.get("account_ids_in_fund") or []
    if not isinstance(raw_account_ids, list):
        return None
    account_ids = [str(a) for a in raw_account_ids if a]
    if not account_ids:
        return None
    generated_on = _parse_iso_date(payload.get("generated_at"))
    event = _scan_drawdown_event(
        conn,
        account_ids,
        as_of=ctx.now.date(),
        artifact_generated_on=generated_on,
    )
    if event is None:
        return None
    classified_entries = payload.get("drawdown_events_classified") or []
    if not isinstance(classified_entries, list):
        classified_entries = []
    if _drawdown_already_classified(event, classified_entries):
        return None

    drop_pct = (
        Decimal(event.pre_drop_balance_cents - event.low_balance_cents)
        / Decimal(event.pre_drop_balance_cents)
        * Decimal("100")
    )
    drop_dollars = max(event.pre_drop_balance_cents - event.low_balance_cents, 0)
    return Intervention(
        pattern_id="emergency_fund_drawdown_no_replenishment",
        move=Move.COACH,
        tiers=(1, 4),
        priority=Priority.HIGH,
        headline=(
            f"Emergency fund dropped {_pct_text(drop_pct)}% "
            f"({_money(event.pre_drop_balance_cents)} -> {_money(event.low_balance_cents)}) "
            "and has not rebuilt - time to reaffirm the drawdown rule you wrote down."
        ),
        detail_bullets=(
            f"Pre-drop date: {event.pre_drop_date.isoformat()}",
            f"Low date: {event.low_date.isoformat()}",
            f"Drop magnitude: {_money(drop_dollars)} ({_pct_text(drop_pct)}%)",
            f"Artifact baseline: {artifact_path.name}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Classify this drawdown",
            tool="coach_emergency_fund_artifact_read",
            params={
                "artifact_path": artifact_path.name,
                "pre_drop_date": event.pre_drop_date.isoformat(),
                "low_date": event.low_date.isoformat(),
                "pre_drop_balance_cents": event.pre_drop_balance_cents,
                "low_balance_cents": event.low_balance_cents,
                "account_ids": list(event.account_ids),
            },
            build_stub=False,
        ),
        dollar_impact_cents=drop_dollars,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("emergency_fund_drawdown_no_replenishment"),
    )


# ---------------------------------------------------------------------------
# Income-shock detection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _IncomeShockSignal:
    severity: str  # "severe_1mo" | "sustained_2mo"
    current_month: str
    prior_month: str | None
    current_income_cents: int
    prior_income_cents: int | None
    median_income_cents: int
    median_tx_count: float


def _income_shock_signal(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> _IncomeShockSignal | None:
    months = _month_keys(ctx.now.date(), 12)
    if len(months) < 6:
        return None
    current_month = ctx.now.date().strftime("%Y-%m")
    incomes = [_income_cents_for_month(conn, month) for month in months]
    nonzero = [value for value in incomes if value > 0]
    if len(nonzero) < 6:
        return None
    median_income = int(statistics.median(nonzero))
    if median_income <= 0:
        return None
    tx_counts = [_income_tx_count_for_month(conn, month) for month in months]
    median_tx_count = statistics.median(tx_counts) if tx_counts else 0.0

    current_income = _income_cents_for_month(conn, current_month)
    current_tx_count = _income_tx_count_for_month(conn, current_month)

    # Data-completeness suppression
    if median_tx_count > 0:
        completeness_floor = Decimal(median_tx_count) * _INCOME_TX_COMPLETENESS_FRACTION
        if Decimal(current_tx_count) < completeness_floor:
            return None

    severe_threshold = int(
        (Decimal(median_income) * _INCOME_SHOCK_SEVERE_FRACTION).quantize(_DOLLAR)
    )
    sustained_threshold = int(
        (Decimal(median_income) * _INCOME_SHOCK_SUSTAINED_FRACTION).quantize(_DOLLAR)
    )

    if current_income > 0 and current_income < severe_threshold:
        return _IncomeShockSignal(
            severity="severe_1mo",
            current_month=current_month,
            prior_month=None,
            current_income_cents=current_income,
            prior_income_cents=None,
            median_income_cents=median_income,
            median_tx_count=float(median_tx_count),
        )

    if len(months) >= 1:
        prior_month = months[-1]  # most-recent completed month (12 keys span months_back..back1)
        prior_income = _income_cents_for_month(conn, prior_month)
        if (
            current_income > 0
            and current_income < sustained_threshold
            and prior_income > 0
            and prior_income < sustained_threshold
        ):
            return _IncomeShockSignal(
                severity="sustained_2mo",
                current_month=current_month,
                prior_month=prior_month,
                current_income_cents=current_income,
                prior_income_cents=prior_income,
                median_income_cents=median_income,
                median_tx_count=float(median_tx_count),
            )
    return None


@register_pattern(
    id="income_shock_detected",
    move=Move.WARN,
    tiers=(1, 4),
    priority=Priority.HIGH,
    cooldown=timedelta(days=1),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.RISK_INSURANCE),
    cfp_steps=(CFPProcessStep.UNDERSTAND,),
)
def evaluate_income_shock_detected(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    signal = _income_shock_signal(conn, ctx)
    if signal is None:
        return None
    median_text = _money(signal.median_income_cents)
    current_text = _money(signal.current_income_cents)
    drop_pct = (
        Decimal("100")
        - Decimal(signal.current_income_cents) / Decimal(signal.median_income_cents) * Decimal("100")
    )
    if signal.severity == "severe_1mo":
        headline = (
            f"Income this month {current_text} is {_pct_text(drop_pct)}% below "
            f"your 12-month median {median_text} - this is the kind of shock the emergency fund exists for."
        )
        bullets: tuple[str, ...] = (
            f"Current month: {signal.current_month}",
            f"12-month median income: {median_text}/mo",
        )
    else:
        prior_text = _money(signal.prior_income_cents or 0)
        headline = (
            f"Two-month income dip: {prior_text} -> {current_text} versus a "
            f"12-month median of {median_text}. Pause and re-plan."
        )
        bullets = (
            f"Current month: {signal.current_month} ({current_text})",
            f"Prior month: {signal.prior_month} ({prior_text})",
            f"12-month median income: {median_text}/mo",
        )
    return Intervention(
        pattern_id="income_shock_detected",
        move=Move.WARN,
        tiers=(1, 4),
        priority=Priority.HIGH,
        headline=headline,
        detail_bullets=bullets,
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Walk through emergency-fund coaching",
            tool="activate_skill",
            params={"name": "coach_emergency_fund"},
            build_stub=False,
        ),
        dollar_impact_cents=max(signal.median_income_cents - signal.current_income_cents, 0),
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("income_shock_detected"),
    )
