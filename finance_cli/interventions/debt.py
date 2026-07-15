from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import sqlite3

from ..commands.common import fmt_dollars
from ..debt_calculator import (
    DebtCard,
    compare_strategies,
    load_debt_cards,
    monthly_interest_cents,
    simulate_paydown,
)
from ..models import cents_to_dollars
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


_ZERO_APR_MAX = 0.01
_HIGH_APR_MIN = 15.0
_D2_LOOKBACK_DAYS = 183
_D2_SMALL_BALANCE_SHARE_MAX = Decimal("0.20")
_D2_AVALANCHE_SAVINGS_SUPPRESSION_CENTS = 100_000
_D2_STRONG_ABANDONED_SIGNAL_COUNT = 2
_D2_ABANDONED_EVENTS = (
    "feature.goal_abandoned",
    "feature.plan_abandoned",
)
_D3_SPEND_LOOKBACK_DAYS = 90
_D3_MIN_AVG_MONTHLY_SPEND_CENTS = 10_000
_D3_MIN_INTEREST_SAVED_CENTS = 2_500
_D4_HIGH_APR_MIN = 18.0
_D4_LOOKBACK_MONTHS = 3
_D4_MIN_NEAR_MINIMUM_MONTHS = 2
_D4_ACTION_EXTRA_CENTS = 15_000
_D4_OPTION_EXTRA_CENTS = (10_000, 20_000)
_D4_MIN_INTEREST_SAVED_CENTS = 10_000
_D5_HIGH_APR_MIN = 18.0
_D5_MIN_CHECKING_SURPLUS_CENTS = 100_000
_D6_MIN_BALANCE_CENTS = 200_000
_D6_HIGH_APR_MIN = 18.0
_D6_TRANSFER_FEE_PERCENT = Decimal("3.0")
_D6_DEFAULT_REMINDER_OFFSET_DAYS = 14
_D7_STREAK_MONTHS = 3
_D7_BASELINE_MAX_AGE_DAYS = 45
_D7_MIN_PAYMENT_LIFT_PERCENT = Decimal("1.10")
_D7_MIN_AHEAD_CENTS = 10_000
_AVG_DAYS_PER_MONTH = Decimal("30.4375")
_CENT = Decimal("1")


def _allows_avalanche(ctx: InterventionContext) -> bool:
    return ctx.strategy_prefs.debt_strategy != "snowball"


def _allows_snowball_recommendation(ctx: InterventionContext) -> bool:
    return ctx.strategy_prefs.debt_strategy in {None, "snowball"}


def _allows_min_payment_trap_warning(ctx: InterventionContext) -> bool:
    return ctx.strategy_prefs.debt_strategy != "minimum_commitment"


def _d4_simulation_strategy(ctx: InterventionContext) -> str:
    return "snowball" if ctx.strategy_prefs.debt_strategy == "snowball" else "avalanche"


@dataclass(frozen=True)
class _D3Candidate:
    zero_card: DebtCard
    high_card: DebtCard
    intro_end_date: date
    avg_monthly_spend_cents: int
    redirectable_cents: int
    interest_saved_cents: int
    months_remaining: Decimal


@dataclass(frozen=True)
class _D4Option:
    extra_cents: int
    months_to_payoff: int
    total_interest_cents: int
    interest_saved_cents: int
    months_saved: int
    fully_paid_off: bool


@dataclass(frozen=True)
class _D2Candidate:
    smallest_card: DebtCard
    total_balance_cents: int
    abandoned_signal_count: int
    payoff_lump_sum_cents: int
    freed_min_payment_cents: int
    snowball_months_to_payoff: int
    avalanche_savings_cents: int
    interest_saved_vs_current_snowball_cents: int


@dataclass(frozen=True)
class _D4Candidate:
    card: DebtCard
    near_minimum_months: int
    current: _D4Option
    action: _D4Option
    comparison_options: tuple[_D4Option, ...]


@dataclass(frozen=True)
class _D5Candidate:
    card: DebtCard
    cash_source_account_id: str
    cash_source_label: str
    checking_balance_cents: int
    retained_buffer_cents: int
    suggested_payment_cents: int
    interest_saved_annual_cents: int


@dataclass(frozen=True)
class _D6Candidate:
    card: DebtCard
    balance_transfer_fee_cents: int
    interest_avoided_12mo_cents: int
    net_savings_12mo_cents: int
    suggested_remind_on: date


@dataclass(frozen=True)
class _D7Candidate:
    card: DebtCard
    months: tuple[str, ...]
    start_balance_cents: int
    current_balance_cents: int
    projected_minimum_balance_cents: int
    average_payment_cents: int
    ahead_cents: int


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


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


def _account_id_and_aliases(conn: sqlite3.Connection, *, account_id: str) -> tuple[str, ...]:
    alias_rows = conn.execute(
        """
        SELECT hash_account_id
          FROM account_aliases
         WHERE canonical_id = ?
         ORDER BY hash_account_id
        """,
        (account_id,),
    ).fetchall()
    return (account_id, *tuple(str(row["hash_account_id"]) for row in alias_rows))


def _active_credit_account_ids(conn: sqlite3.Connection) -> frozenset[str]:
    rows = conn.execute(
        """
        SELECT a.id
          FROM accounts a
         WHERE a.account_type = 'credit_card'
           AND a.is_active = 1
           AND a.balance_current_cents IS NOT NULL
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
        """
    ).fetchall()
    return frozenset(str(row["id"]) for row in rows)


def _active_personal_credit_account_ids(conn: sqlite3.Connection) -> frozenset[str]:
    rows = conn.execute(
        """
        SELECT a.id
          FROM accounts a
         WHERE a.account_type = 'credit_card'
           AND a.is_active = 1
           AND COALESCE(a.is_business, 0) = 0
           AND a.balance_current_cents IS NOT NULL
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
        """
    ).fetchall()
    return frozenset(str(row["id"]) for row in rows)


def _active_manual_loan_ids(conn: sqlite3.Connection) -> frozenset[str]:
    rows = conn.execute(
        """
        SELECT id
         FROM manual_loans
         WHERE is_active = 1
           AND current_balance_cents > 0
           AND use_type = 'Personal'
        """
    ).fetchall()
    return frozenset(str(row["id"]) for row in rows)


def _has_d3_behavior_fit_red_flag(conn: sqlite3.Connection, *, as_of: date) -> bool:
    late_row = conn.execute(
        """
        SELECT 1
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
           AND (
                COALESCE(l.is_overdue, 0) = 1
                OR COALESCE(l.past_due_amount_cents, 0) > 0
                OR COALESCE(l.current_late_fee_cents, 0) > 0
           )
         LIMIT 1
        """
    ).fetchone()
    if late_row is not None:
        return True

    expired_intro_row = conn.execute(
        """
        SELECT 1
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND l.liability_type = 'credit'
           AND a.is_active = 1
           AND a.account_type = 'credit_card'
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
           AND l.intro_apr_end_date IS NOT NULL
           AND l.intro_apr_end_date < ?
           AND ABS(COALESCE(a.balance_current_cents, 0)) > 0
         LIMIT 1
        """,
        (as_of.isoformat(),),
    ).fetchone()
    return expired_intro_row is not None


def _near_minimum_payment(payment_cents: int, minimum_cents: int) -> bool:
    if minimum_cents <= 0 or payment_cents <= 0:
        return False
    lower = int((Decimal(minimum_cents) * Decimal("0.95")).quantize(_CENT, rounding=ROUND_HALF_UP))
    upper = int((Decimal(minimum_cents) * Decimal("1.10")).quantize(_CENT, rounding=ROUND_HALF_UP))
    return lower <= payment_cents <= upper


def _recent_abandoned_goal_or_plan_keys(
    conn: sqlite3.Connection,
    *,
    as_of: datetime,
) -> frozenset[str]:
    since = as_of - timedelta(days=_D2_LOOKBACK_DAYS)
    placeholders = ",".join("?" for _ in _D2_ABANDONED_EVENTS)
    rows = conn.execute(
        f"""
        SELECT event,
               COALESCE(
                   NULLIF(json_extract(properties, '$.goal_id'), ''),
                   NULLIF(json_extract(properties, '$.goal_name'), ''),
                   NULLIF(json_extract(properties, '$.month'), ''),
                   ''
               ) AS entity_key
          FROM analytics_events
         WHERE outcome = 'abandoned'
           AND event IN ({placeholders})
           AND datetime(created_at) >= datetime(?)
        """,
        (
            *_D2_ABANDONED_EVENTS,
            since.isoformat(sep=" "),
        ),
    ).fetchall()
    keys: set[str] = set()
    for row in rows:
        event = str(row["event"])
        entity_key = str(row["entity_key"] or "").strip()
        if entity_key:
            keys.add(f"{event}:{entity_key}")
        else:
            keys.add(f"{event}:unknown")
    return frozenset(keys)


def _d2_payoff_lump_sum_cents(card: DebtCard) -> int:
    if card.apr is None:
        return 0
    payoff_balance = card.balance_cents + monthly_interest_cents(
        card.balance_cents,
        float(card.apr),
    )
    return max(0, payoff_balance - card.min_payment_cents)


def _best_d2_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _D2Candidate | None:
    if not _allows_snowball_recommendation(ctx):
        return None

    abandoned_count = len(_recent_abandoned_goal_or_plan_keys(conn, as_of=ctx.now))
    if abandoned_count < 1:
        return None

    personal_credit_account_ids = _active_personal_credit_account_ids(conn)
    manual_loan_ids = _active_manual_loan_ids(conn)
    cards = [
        card
        for card in load_debt_cards(conn)
        if card.balance_cents > 0
        and (
            card.card_id in personal_credit_account_ids
            or card.card_id in manual_loan_ids
        )
    ]
    if len(cards) < 2:
        return None
    if any(card.apr is None for card in cards):
        return None

    total_balance_cents = sum(card.balance_cents for card in cards)
    if total_balance_cents <= 0:
        return None

    smallest = min(
        cards,
        key=lambda card: (card.balance_cents, -float(card.apr or 0), card.card_id),
    )
    balance_share = Decimal(smallest.balance_cents) / Decimal(total_balance_cents)
    if balance_share >= _D2_SMALL_BALANCE_SHARE_MAX:
        return None

    payoff_lump_sum_cents = _d2_payoff_lump_sum_cents(smallest)
    if payoff_lump_sum_cents <= 0 or smallest.min_payment_cents <= 0:
        return None

    current_snowball = simulate_paydown(
        cards,
        extra_cents=0,
        strategy="snowball",
        summary_only=True,
    )
    proposed_snowball = simulate_paydown(
        cards,
        extra_cents=0,
        strategy="snowball",
        summary_only=True,
        lump_sum_cents=payoff_lump_sum_cents,
        lump_sum_month=1,
    )
    comparison = compare_strategies(
        cards,
        extra_cents=0,
        summary_only=True,
        lump_sum_cents=payoff_lump_sum_cents,
        lump_sum_month=1,
    )
    avalanche_interest = int(comparison["avalanche"]["total_interest_cents"])
    snowball_interest = int(comparison["snowball"]["total_interest_cents"])
    avalanche_savings = max(snowball_interest - avalanche_interest, 0)
    explicit_snowball = ctx.strategy_prefs.debt_strategy == "snowball"
    if (
        avalanche_savings > _D2_AVALANCHE_SAVINGS_SUPPRESSION_CENTS
        and abandoned_count < _D2_STRONG_ABANDONED_SIGNAL_COUNT
        and not explicit_snowball
    ):
        return None

    return _D2Candidate(
        smallest_card=smallest,
        total_balance_cents=total_balance_cents,
        abandoned_signal_count=abandoned_count,
        payoff_lump_sum_cents=payoff_lump_sum_cents,
        freed_min_payment_cents=smallest.min_payment_cents,
        snowball_months_to_payoff=int(proposed_snowball["months_to_payoff"]),
        avalanche_savings_cents=avalanche_savings,
        interest_saved_vs_current_snowball_cents=max(
            int(current_snowball["total_interest_cents"])
            - int(proposed_snowball["total_interest_cents"]),
            0,
        ),
    )


def _recent_near_minimum_payment_months(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    minimum_payment_cents: int,
    as_of: date,
) -> int:
    if minimum_payment_cents <= 0:
        return 0
    account_ids = _account_id_and_aliases(conn, account_id=account_id)
    month_keys = _month_keys(as_of, _D4_LOOKBACK_MONTHS)
    if not month_keys:
        return 0
    start, _ = _month_bounds(month_keys[0])
    _, end = _month_bounds(month_keys[-1])
    placeholders = ",".join("?" for _ in account_ids)
    rows = conn.execute(
        f"""
        SELECT substr(t.date, 1, 7) AS month,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS payment_cents
          FROM transactions t
         WHERE t.account_id IN ({placeholders})
           AND t.is_active = 1
           AND t.is_payment = 1
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY substr(t.date, 1, 7)
        """,
        (*account_ids, start.isoformat(), end.isoformat()),
    ).fetchall()
    near_months = 0
    expected_months = set(month_keys)
    for row in rows:
        month = str(row["month"])
        if month not in expected_months:
            continue
        if _near_minimum_payment(int(row["payment_cents"] or 0), minimum_payment_cents):
            near_months += 1
    return near_months


def _recent_monthly_payment_totals(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    month_keys: list[str],
) -> dict[str, int]:
    if not month_keys:
        return {}
    account_ids = _account_id_and_aliases(conn, account_id=account_id)
    start, _ = _month_bounds(month_keys[0])
    _, end = _month_bounds(month_keys[-1])
    placeholders = ",".join("?" for _ in account_ids)
    rows = conn.execute(
        f"""
        SELECT substr(t.date, 1, 7) AS month,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS payment_cents
          FROM transactions t
         WHERE t.account_id IN ({placeholders})
           AND t.is_active = 1
           AND t.is_payment = 1
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY substr(t.date, 1, 7)
        """,
        (*account_ids, start.isoformat(), end.isoformat()),
    ).fetchall()
    expected_months = set(month_keys)
    return {
        str(row["month"]): int(row["payment_cents"] or 0)
        for row in rows
        if str(row["month"]) in expected_months
    }


def _balance_snapshot_on_or_before(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    snapshot_date: date,
    earliest_date: date | None = None,
) -> int | None:
    account_ids = _account_id_and_aliases(conn, account_id=account_id)
    placeholders = ",".join("?" for _ in account_ids)
    earliest_clause = ""
    params: list[str] = [*account_ids, snapshot_date.isoformat()]
    if earliest_date is not None:
        earliest_clause = "AND snapshot_date >= ?"
        params.append(earliest_date.isoformat())
    row = conn.execute(
        f"""
        SELECT ABS(COALESCE(balance_current_cents, 0)) AS balance_cents
          FROM balance_snapshots
         WHERE account_id IN ({placeholders})
           AND snapshot_date <= ?
           {earliest_clause}
         ORDER BY snapshot_date DESC, created_at DESC
         LIMIT 1
        """,
        params,
    ).fetchone()
    if row is None:
        return None
    return int(row["balance_cents"] or 0)


def _project_minimum_balance_cents(
    *,
    start_balance_cents: int,
    minimum_payment_cents: int,
    apr: float,
    months: int,
) -> int:
    balance_cents = max(0, int(start_balance_cents))
    for _ in range(int(months)):
        if balance_cents <= 0:
            return 0
        interest_cents = monthly_interest_cents(balance_cents, apr)
        balance_cents = max(0, balance_cents + interest_cents - int(minimum_payment_cents))
    return balance_cents


def _d7_candidate_for_card(
    conn: sqlite3.Connection,
    *,
    card: DebtCard,
    as_of: date,
) -> _D7Candidate | None:
    if card.balance_cents <= 0 or card.apr is None or card.min_payment_cents <= 0:
        return None
    month_keys = _month_keys(as_of, _D7_STREAK_MONTHS)
    if len(month_keys) != _D7_STREAK_MONTHS:
        return None
    first_month_start, _ = _month_bounds(month_keys[0])
    start_balance_cents = _balance_snapshot_on_or_before(
        conn,
        account_id=card.card_id,
        snapshot_date=first_month_start - timedelta(days=1),
        earliest_date=first_month_start - timedelta(days=_D7_BASELINE_MAX_AGE_DAYS),
    )
    if start_balance_cents is None or start_balance_cents <= card.balance_cents:
        return None

    payment_totals = _recent_monthly_payment_totals(
        conn,
        account_id=card.card_id,
        month_keys=month_keys,
    )
    if set(payment_totals) != set(month_keys):
        return None

    lifted_floor_cents = int(
        (Decimal(card.min_payment_cents) * _D7_MIN_PAYMENT_LIFT_PERCENT).quantize(
            _CENT,
            rounding=ROUND_HALF_UP,
        )
    )
    if any(payment_totals[month_key] < lifted_floor_cents for month_key in month_keys):
        return None

    projected_minimum_balance_cents = _project_minimum_balance_cents(
        start_balance_cents=start_balance_cents,
        minimum_payment_cents=card.min_payment_cents,
        apr=float(card.apr),
        months=_D7_STREAK_MONTHS,
    )
    ahead_cents = projected_minimum_balance_cents - card.balance_cents
    if ahead_cents < _D7_MIN_AHEAD_CENTS:
        return None

    average_payment_cents = int(
        (Decimal(sum(payment_totals.values())) / Decimal(len(payment_totals))).quantize(
            _CENT,
            rounding=ROUND_HALF_UP,
        )
    )
    return _D7Candidate(
        card=card,
        months=tuple(month_keys),
        start_balance_cents=start_balance_cents,
        current_balance_cents=card.balance_cents,
        projected_minimum_balance_cents=projected_minimum_balance_cents,
        average_payment_cents=average_payment_cents,
        ahead_cents=ahead_cents,
    )


def _best_d7_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _D7Candidate | None:
    as_of = ctx.now.date()
    personal_credit_account_ids = _active_personal_credit_account_ids(conn)
    candidates = [
        candidate
        for card in load_debt_cards(conn)
        if card.card_id in personal_credit_account_ids
        for candidate in (_d7_candidate_for_card(conn, card=card, as_of=as_of),)
        if candidate is not None
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            item.ahead_cents,
            item.start_balance_cents - item.current_balance_cents,
            item.average_payment_cents,
            item.card.card_id,
        ),
    )


def _d4_simulation_option(
    cards: list[DebtCard],
    *,
    extra_cents: int,
    strategy: str,
    current: _D4Option | None = None,
) -> _D4Option:
    result = simulate_paydown(
        cards,
        extra_cents=extra_cents,
        strategy=strategy,
        summary_only=True,
    )
    total_interest_cents = int(result["total_interest_cents"])
    months_to_payoff = int(result["months_to_payoff"])
    if current is None:
        interest_saved_cents = 0
        months_saved = 0
    else:
        interest_saved_cents = max(current.total_interest_cents - total_interest_cents, 0)
        months_saved = max(current.months_to_payoff - months_to_payoff, 0)
    return _D4Option(
        extra_cents=int(extra_cents),
        months_to_payoff=months_to_payoff,
        total_interest_cents=total_interest_cents,
        interest_saved_cents=interest_saved_cents,
        months_saved=months_saved,
        fully_paid_off=bool(result["fully_paid_off"]),
    )


def _best_d4_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _D4Candidate | None:
    as_of = ctx.now.date()
    personal_credit_account_ids = _active_personal_credit_account_ids(conn)
    all_cards = [
        card
        for card in load_debt_cards(conn)
        if card.card_id in personal_credit_account_ids
    ]
    strategy = _d4_simulation_strategy(ctx)
    candidates: list[_D4Candidate] = []
    for card in all_cards:
        if card.card_id not in personal_credit_account_ids:
            continue
        if card.balance_cents <= 0 or card.apr is None or float(card.apr) < _D4_HIGH_APR_MIN:
            continue
        near_months = _recent_near_minimum_payment_months(
            conn,
            account_id=card.card_id,
            minimum_payment_cents=card.min_payment_cents,
            as_of=as_of,
        )
        if near_months < _D4_MIN_NEAR_MINIMUM_MONTHS:
            continue

        card_scope = [card]
        current = _d4_simulation_option(card_scope, extra_cents=0, strategy=strategy)
        action = _d4_simulation_option(
            card_scope,
            extra_cents=_D4_ACTION_EXTRA_CENTS,
            strategy=strategy,
            current=current,
        )
        if not action.fully_paid_off:
            continue
        if action.interest_saved_cents < _D4_MIN_INTEREST_SAVED_CENTS and action.months_saved <= 0:
            continue

        comparison_options = tuple(
            _d4_simulation_option(
                card_scope,
                extra_cents=extra_cents,
                strategy=strategy,
                current=current,
            )
            for extra_cents in _D4_OPTION_EXTRA_CENTS
        )
        candidates.append(
            _D4Candidate(
                card=card,
                near_minimum_months=near_months,
                current=current,
                action=action,
                comparison_options=comparison_options,
            )
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            item.action.interest_saved_cents,
            item.action.months_saved,
            float(item.card.apr or 0),
            item.card.balance_cents,
            item.near_minimum_months,
            item.card.card_id,
        ),
    )


def _has_existing_d5_paydown_flag(
    conn: sqlite3.Connection,
    *,
    account_id: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM card_paydown_flags
         WHERE account_id = ?
           AND status = 'active'
         LIMIT 1
        """,
        (account_id,),
    ).fetchone()
    return row is not None


def _checking_cash_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT a.id, a.institution_name, a.account_name, a.balance_current_cents
          FROM accounts a
         WHERE a.is_active = 1
           AND a.account_type = 'checking'
           AND COALESCE(a.is_business, 0) = 0
           AND COALESCE(a.balance_current_cents, 0) > 0
           AND NOT EXISTS (
                SELECT 1
                  FROM account_aliases aa
                 WHERE aa.hash_account_id = a.id
           )
         ORDER BY a.balance_current_cents DESC, a.id
        """
    ).fetchall()


def _cash_account_label(row: sqlite3.Row) -> str:
    institution = str(row["institution_name"] or "").strip()
    account_name = str(row["account_name"] or "").strip()
    label = " ".join(part for part in (institution, account_name) if part)
    return label or str(row["id"])


def _d5_interest_saved_annual_cents(*, suggested_payment_cents: int, apr: float) -> int:
    return max(
        0,
        int(
            (
                Decimal(suggested_payment_cents)
                * Decimal(str(apr))
                / Decimal("100")
            ).quantize(_CENT, rounding=ROUND_HALF_UP)
        ),
    )


def _percent_of_cents(amount_cents: int, percent: Decimal | float) -> int:
    return max(
        0,
        int(
            (
                Decimal(int(amount_cents))
                * Decimal(str(percent))
                / Decimal("100")
            ).quantize(_CENT, rounding=ROUND_HALF_UP)
        ),
    )


def _best_d5_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _D5Candidate | None:
    retained_buffer_cents = int(ctx.trailing_3mo_avg_expense_cents)
    if retained_buffer_cents <= 0:
        return None

    checking_rows = _checking_cash_rows(conn)
    if not checking_rows:
        return None
    checking_balance_cents = sum(int(row["balance_current_cents"] or 0) for row in checking_rows)
    checking_surplus_cents = checking_balance_cents - retained_buffer_cents
    if checking_surplus_cents <= _D5_MIN_CHECKING_SURPLUS_CENTS:
        return None

    source = checking_rows[0]
    source_balance_cents = int(source["balance_current_cents"] or 0)
    personal_credit_account_ids = _active_personal_credit_account_ids(conn)
    cards = [
        card
        for card in load_debt_cards(conn)
        if card.card_id in personal_credit_account_ids
        and card.balance_cents > 0
        and card.apr is not None
        and float(card.apr) >= _D5_HIGH_APR_MIN
        and not _has_existing_d5_paydown_flag(conn, account_id=card.card_id)
    ]
    if not cards:
        return None

    candidates: list[_D5Candidate] = []
    for card in cards:
        suggested_payment_cents = min(
            int(card.balance_cents),
            int(checking_surplus_cents),
            source_balance_cents,
        )
        if suggested_payment_cents <= 0:
            continue
        interest_saved = _d5_interest_saved_annual_cents(
            suggested_payment_cents=suggested_payment_cents,
            apr=float(card.apr or 0),
        )
        if interest_saved <= 0:
            continue
        candidates.append(
            _D5Candidate(
                card=card,
                cash_source_account_id=str(source["id"]),
                cash_source_label=_cash_account_label(source),
                checking_balance_cents=checking_balance_cents,
                retained_buffer_cents=retained_buffer_cents,
                suggested_payment_cents=suggested_payment_cents,
                interest_saved_annual_cents=interest_saved,
            )
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            item.interest_saved_annual_cents,
            float(item.card.apr or 0),
            item.card.balance_cents,
            item.card.card_id,
        ),
    )


def _has_d6_behavior_fit_red_flag(conn: sqlite3.Connection, *, as_of: date) -> bool:
    late_row = conn.execute(
        """
        SELECT 1
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND a.is_active = 1
           AND a.account_type = 'credit_card'
           AND COALESCE(a.is_business, 0) = 0
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
           AND (
                COALESCE(l.is_overdue, 0) = 1
                OR COALESCE(l.past_due_amount_cents, 0) > 0
                OR COALESCE(l.current_late_fee_cents, 0) > 0
           )
         LIMIT 1
        """
    ).fetchone()
    if late_row is not None:
        return True

    expired_intro_row = conn.execute(
        """
        SELECT 1
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND l.liability_type = 'credit'
           AND a.is_active = 1
           AND a.account_type = 'credit_card'
           AND COALESCE(a.is_business, 0) = 0
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
           AND l.intro_apr_end_date IS NOT NULL
           AND l.intro_apr_end_date < ?
           AND ABS(COALESCE(a.balance_current_cents, 0)) > 0
         LIMIT 1
        """,
        (as_of.isoformat(),),
    ).fetchone()
    return expired_intro_row is not None


def _has_pending_d6_reminder(conn: sqlite3.Connection, *, account_id: str) -> bool:
    account_ids = _account_id_and_aliases(conn, account_id=account_id)
    placeholders = ",".join("?" for _ in account_ids)
    row = conn.execute(
        f"""
        SELECT 1
          FROM reminders
         WHERE kind = 'balance_transfer'
           AND status = 'pending'
           AND json_extract(payload_json, '$.account_id') IN ({placeholders})
         LIMIT 1
        """,
        account_ids,
    ).fetchone()
    return row is not None


def _d6_liability_dates(conn: sqlite3.Connection, *, account_id: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT last_statement_issue_date, next_payment_due_date
          FROM liabilities
         WHERE account_id = ?
           AND is_active = 1
           AND liability_type = 'credit'
         ORDER BY id
         LIMIT 1
        """,
        (account_id,),
    ).fetchone()


def _d6_suggested_remind_on(
    row: sqlite3.Row | None,
    *,
    as_of: date,
) -> date:
    if row is not None:
        last_statement = _parse_iso_date(row["last_statement_issue_date"])
        if last_statement is not None:
            post_next_statement = last_statement + timedelta(days=31)
            if post_next_statement > as_of:
                return post_next_statement

        next_payment_due = _parse_iso_date(row["next_payment_due_date"])
        if next_payment_due is not None:
            post_payment_window = next_payment_due + timedelta(days=1)
            if post_payment_window > as_of:
                return post_payment_window

    return as_of + timedelta(days=_D6_DEFAULT_REMINDER_OFFSET_DAYS)


def _best_d6_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _D6Candidate | None:
    as_of = ctx.now.date()
    if _has_d6_behavior_fit_red_flag(conn, as_of=as_of):
        return None

    personal_credit_account_ids = _active_personal_credit_account_ids(conn)
    cards = [
        card
        for card in load_debt_cards(conn)
        if card.card_id in personal_credit_account_ids
        and card.balance_cents >= _D6_MIN_BALANCE_CENTS
        and card.apr is not None
        and float(card.apr) >= _D6_HIGH_APR_MIN
        and not _has_pending_d6_reminder(conn, account_id=card.card_id)
    ]
    candidates: list[_D6Candidate] = []
    for card in cards:
        fee_cents = _percent_of_cents(card.balance_cents, _D6_TRANSFER_FEE_PERCENT)
        interest_avoided = _percent_of_cents(card.balance_cents, float(card.apr or 0))
        net_savings = interest_avoided - fee_cents
        if net_savings <= 0:
            continue
        candidates.append(
            _D6Candidate(
                card=card,
                balance_transfer_fee_cents=fee_cents,
                interest_avoided_12mo_cents=interest_avoided,
                net_savings_12mo_cents=net_savings,
                suggested_remind_on=_d6_suggested_remind_on(
                    _d6_liability_dates(conn, account_id=card.card_id),
                    as_of=as_of,
                ),
            )
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            item.net_savings_12mo_cents,
            float(item.card.apr or 0),
            item.card.balance_cents,
            item.card.card_id,
        ),
    )


def _recent_card_purchase_spend_cents(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    as_of: date,
) -> int:
    start = as_of - timedelta(days=_D3_SPEND_LOOKBACK_DAYS)
    row = conn.execute(
        """
        SELECT COALESCE(SUM(ABS(t.amount_cents)), 0) AS total_cents
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.account_id = ?
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND (c.id IS NULL OR c.is_income = 0)
           AND t.date >= ?
           AND t.date <= ?
        """,
        (account_id, start.isoformat(), as_of.isoformat()),
    ).fetchone()
    total_cents = Decimal(int(row["total_cents"] or 0))
    return int((total_cents / Decimal("3")).quantize(_CENT, rounding=ROUND_HALF_UP))


def _estimate_d3_interest_saved_cents(
    *,
    balance_cents: int,
    avg_monthly_spend_cents: int,
    apr: float,
    months_remaining: Decimal,
) -> int:
    """Estimate interest saved from redirecting monthly spend to high-APR principal."""
    if balance_cents <= 0 or avg_monthly_spend_cents <= 0 or apr <= 0 or months_remaining <= 0:
        return 0

    balance = Decimal(balance_cents)
    monthly_spend = Decimal(avg_monthly_spend_cents)
    monthly_rate = Decimal(str(apr)) / Decimal("100") / Decimal("12")
    months_to_payoff_extra = balance / monthly_spend

    if months_remaining <= months_to_payoff_extra:
        principal_months = monthly_spend * months_remaining * months_remaining / Decimal("2")
    else:
        ramp_months = balance * months_to_payoff_extra / Decimal("2")
        full_balance_months = balance * (months_remaining - months_to_payoff_extra)
        principal_months = ramp_months + full_balance_months

    return max(0, int((principal_months * monthly_rate).quantize(_CENT, rounding=ROUND_HALF_UP)))


def _d3_candidate_for_pair(
    conn: sqlite3.Connection,
    *,
    zero_card: DebtCard,
    high_card: DebtCard,
    intro_end_date: date,
    as_of: date,
) -> _D3Candidate | None:
    if high_card.apr is None or float(high_card.apr) < _HIGH_APR_MIN:
        return None
    if high_card.balance_cents <= 0:
        return None

    days_remaining = (intro_end_date - as_of).days
    if days_remaining <= 0:
        return None

    avg_monthly_spend_cents = _recent_card_purchase_spend_cents(
        conn,
        account_id=high_card.card_id,
        as_of=as_of,
    )
    if avg_monthly_spend_cents < _D3_MIN_AVG_MONTHLY_SPEND_CENTS:
        return None

    months_remaining = Decimal(days_remaining) / _AVG_DAYS_PER_MONTH
    redirectable_cents = int(
        min(
            Decimal(high_card.balance_cents),
            (Decimal(avg_monthly_spend_cents) * months_remaining).quantize(_CENT, rounding=ROUND_HALF_UP),
        )
    )
    interest_saved_cents = _estimate_d3_interest_saved_cents(
        balance_cents=high_card.balance_cents,
        avg_monthly_spend_cents=avg_monthly_spend_cents,
        apr=float(high_card.apr),
        months_remaining=months_remaining,
    )
    if interest_saved_cents < _D3_MIN_INTEREST_SAVED_CENTS:
        return None

    return _D3Candidate(
        zero_card=zero_card,
        high_card=high_card,
        intro_end_date=intro_end_date,
        avg_monthly_spend_cents=avg_monthly_spend_cents,
        redirectable_cents=redirectable_cents,
        interest_saved_cents=interest_saved_cents,
        months_remaining=months_remaining,
    )


def _best_d3_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _D3Candidate | None:
    as_of = ctx.now.date()
    if _has_d3_behavior_fit_red_flag(conn, as_of=as_of):
        return None

    credit_account_ids = _active_credit_account_ids(conn)
    if len(credit_account_ids) < 2:
        return None

    cards = [card for card in load_debt_cards(conn, include_zero_balance=True) if card.card_id in credit_account_ids]
    zero_cards: list[tuple[DebtCard, date]] = []
    high_cards: list[DebtCard] = []
    for card in cards:
        if card.apr is not None and 0 <= float(card.apr) <= _ZERO_APR_MAX:
            intro_end_date = _parse_iso_date(card.intro_apr_end_date)
            if intro_end_date is not None and intro_end_date > as_of:
                zero_cards.append((card, intro_end_date))
        if card.apr is not None and float(card.apr) >= _HIGH_APR_MIN and card.balance_cents > 0:
            high_cards.append(card)

    candidates: list[_D3Candidate] = []
    for zero_card, intro_end_date in zero_cards:
        for high_card in high_cards:
            if zero_card.card_id == high_card.card_id:
                continue
            candidate = _d3_candidate_for_pair(
                conn,
                zero_card=zero_card,
                high_card=high_card,
                intro_end_date=intro_end_date,
                as_of=as_of,
            )
            if candidate is not None:
                candidates.append(candidate)

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            item.interest_saved_cents,
            item.redirectable_cents,
            item.intro_end_date,
            item.high_card.card_id,
        ),
    )


def _payoff_time_text(option: _D4Option) -> str:
    if not option.fully_paid_off:
        years = max(1, option.months_to_payoff // 12)
        return f"{years}+ years"
    months = option.months_to_payoff
    if months == 1:
        return "1 month"
    if months < 12:
        return f"{months} months"
    years, remaining_months = divmod(months, 12)
    if remaining_months == 0:
        unit = "year" if years == 1 else "years"
        return f"{years} {unit}"
    year_unit = "year" if years == 1 else "years"
    month_unit = "month" if remaining_months == 1 else "months"
    return f"{years} {year_unit} {remaining_months} {month_unit}"


def _d4_current_pace_clause(option: _D4Option) -> str:
    interest = fmt_dollars(cents_to_dollars(option.total_interest_cents))
    if option.fully_paid_off:
        return f"at this pace you're done in {_payoff_time_text(option)} and pay {interest} in interest"
    return f"at this pace you're not projected to finish within {_payoff_time_text(option)}"


def _d4_option_bullet(option: _D4Option) -> str:
    extra = fmt_dollars(cents_to_dollars(option.extra_cents))
    if not option.fully_paid_off:
        return f"{extra}/mo still does not pay everything off inside {_payoff_time_text(option)}."
    return (
        f"{extra}/mo finishes in {_payoff_time_text(option)} with "
        f"{fmt_dollars(cents_to_dollars(option.total_interest_cents))} interest, saving "
        f"{fmt_dollars(cents_to_dollars(option.interest_saved_cents))}."
    )


@register_pattern(
    id="D-4",
    move=Move.WARN,
    tiers=(1, 4),
    priority=Priority.MEDIUM,
    tool="debt_simulate",
    strategy_check=_allows_min_payment_trap_warning,
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP),
)
def evaluate_d4_min_payment_trap_warning(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    candidate = _best_d4_candidate(conn, ctx)
    if candidate is None:
        return None

    action_extra = fmt_dollars(cents_to_dollars(_D4_ACTION_EXTRA_CENTS))
    action_interest = fmt_dollars(cents_to_dollars(candidate.action.total_interest_cents))
    interest_saved = fmt_dollars(cents_to_dollars(candidate.action.interest_saved_cents))
    months_saved = candidate.action.months_saved
    current_clause = _d4_current_pace_clause(candidate.current)
    return Intervention(
        pattern_id="D-4",
        move=Move.WARN,
        tiers=(1, 4),
        priority=Priority.MEDIUM,
        headline=(
            f"Min-payment trap: {current_clause}. Adding {action_extra}/mo finishes in "
            f"{_payoff_time_text(candidate.action)} with {action_interest} in interest. "
            f"That's {interest_saved} saved."
        ),
        detail_bullets=(
            (
                f"{candidate.card.label} has {fmt_dollars(cents_to_dollars(candidate.card.balance_cents))} "
                f"at {float(candidate.card.apr or 0):.2f}% APR; recent payments were near "
                f"the {fmt_dollars(cents_to_dollars(candidate.card.min_payment_cents))} minimum "
                f"for {candidate.near_minimum_months}/{_D4_LOOKBACK_MONTHS} complete months."
            ),
            _d4_option_bullet(candidate.comparison_options[0]),
            _d4_option_bullet(candidate.comparison_options[1]),
            f"Tier 4: adding {action_extra}/mo shaves {months_saved} months off the payoff path.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Run +$150/mo debt simulation",
            tool="debt_simulate",
            params={"strategy": "compare", "extra_dollars": 150},
            build_stub=False,
        ),
        dollar_impact_cents=candidate.action.interest_saved_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("D-4"),
    )


@register_pattern(
    id="D-1",
    move=Move.PRESCRIBE,
    tiers=(1, 4),
    priority=Priority.HIGH,
    cooldown=timedelta(days=30),
    tool="debt_simulate",
    strategy_check=_allows_avalanche,
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP),
)
def evaluate_d1_apr_avalanche(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    cards = [card for card in load_debt_cards(conn) if card.balance_cents > 0 and card.apr is not None]
    if len(cards) < 2:
        return None

    sorted_cards = sorted(cards, key=lambda card: (-float(card.apr or 0), -card.balance_cents, card.card_id))
    highest_apr = sorted_cards[0]
    lowest_apr = min(sorted_cards, key=lambda card: (float(card.apr or 0), -card.balance_cents, card.card_id))
    if highest_apr.apr is None or lowest_apr.apr is None:
        return None
    if float(highest_apr.apr) - float(lowest_apr.apr) < 3.0:
        return None

    comparison = compare_strategies(cards, extra_cents=0, summary_only=True)
    avalanche_interest = int(comparison["avalanche"]["total_interest_cents"])
    snowball_interest = int(comparison["snowball"]["total_interest_cents"])
    interest_saved_cents = max(snowball_interest - avalanche_interest, 0)
    if interest_saved_cents <= 0:
        return None

    return Intervention(
        pattern_id="D-1",
        move=Move.PRESCRIBE,
        tiers=(1, 4),
        priority=Priority.HIGH,
        headline=(
            f"{highest_apr.label} is at {float(highest_apr.apr):.2f}%. "
            f"{lowest_apr.label} is at {float(lowest_apr.apr):.2f}%. "
            f"Hit {highest_apr.label} first - saves you "
            f"{fmt_dollars(cents_to_dollars(interest_saved_cents))} in interest."
        ),
        detail_bullets=(
            f"Avalanche total interest: {fmt_dollars(cents_to_dollars(avalanche_interest))}",
            f"Snowball total interest: {fmt_dollars(cents_to_dollars(snowball_interest))}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Run avalanche simulation",
            tool="debt_simulate",
            params={"strategy": "avalanche", "extra_cents": 0},
            build_stub=False,
        ),
        dollar_impact_cents=interest_saved_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("D-1"),
    )


@register_pattern(
    id="D-2",
    move=Move.PRESCRIBE,
    tiers=(1, 4),
    priority=Priority.HIGH,
    cooldown=timedelta(days=30),
    tool="debt_simulate",
    strategy_check=_allows_snowball_recommendation,
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP),
)
def evaluate_d2_snowball_psychology(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    candidate = _best_d2_candidate(conn, ctx)
    if candidate is None:
        return None

    smallest_balance = fmt_dollars(cents_to_dollars(candidate.smallest_card.balance_cents))
    lump_sum = fmt_dollars(cents_to_dollars(candidate.payoff_lump_sum_cents))
    freed_payment = fmt_dollars(cents_to_dollars(candidate.freed_min_payment_cents))
    total_debt = fmt_dollars(cents_to_dollars(candidate.total_balance_cents))
    interest_saved = fmt_dollars(
        cents_to_dollars(candidate.interest_saved_vs_current_snowball_cents)
    )
    avalanche_gap = fmt_dollars(cents_to_dollars(candidate.avalanche_savings_cents))
    return Intervention(
        pattern_id="D-2",
        move=Move.PRESCRIBE,
        tiers=(1, 4),
        priority=Priority.HIGH,
        headline=(
            f"Your {candidate.smallest_card.label} is at {smallest_balance}. "
            f"Clearing it with a {lump_sum} snowball hit is the quick win: it frees "
            f"{freed_payment}/mo for the next debt."
        ),
        detail_bullets=(
            f"Smallest balance is under 20% of total active personal debt ({total_debt}).",
            (
                f"Recent abandoned goal/plan signals: {candidate.abandoned_signal_count} "
                f"in the last {_D2_LOOKBACK_DAYS} days."
            ),
            (
                f"Modeled payoff path: snowball with a month-1 lump sum finishes in "
                f"{candidate.snowball_months_to_payoff} months and saves about "
                f"{interest_saved} versus waiting for the normal snowball path."
            ),
            f"Avalanche interest advantage under the same lump sum: {avalanche_gap}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Run snowball simulation",
            tool="debt_simulate",
            params={
                "strategy": "snowball",
                "extra_dollars": 0,
                "lump_sum": cents_to_dollars(candidate.payoff_lump_sum_cents),
                "lump_sum_month": 1,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.interest_saved_vs_current_snowball_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("D-2"),
    )


@register_pattern(
    id="D-3",
    move=Move.COMPARE,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    tool="card_rotation_reminder_set",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP),
)
def evaluate_d3_zero_apr_card_swap(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    candidate = _best_d3_candidate(conn, ctx)
    if candidate is None:
        return None

    high_apr = float(candidate.high_card.apr or 0)
    months_text = str(
        max(
            1,
            int(candidate.months_remaining.quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
        )
    )
    return Intervention(
        pattern_id="D-3",
        move=Move.COMPARE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"{candidate.zero_card.label} is at 0% until {candidate.intro_end_date.isoformat()}. "
            f"Option: park daily spend there and send about "
            f"{fmt_dollars(cents_to_dollars(candidate.avg_monthly_spend_cents))}/mo to "
            f"{candidate.high_card.label}; estimated interest saved by then: "
            f"{fmt_dollars(cents_to_dollars(candidate.interest_saved_cents))}."
        ),
        detail_bullets=(
            f"{candidate.high_card.label} carries {fmt_dollars(cents_to_dollars(candidate.high_card.balance_cents))} "
            f"at {high_apr:.2f}% APR.",
            f"Recent purchase spend on that card: "
            f"{fmt_dollars(cents_to_dollars(candidate.avg_monthly_spend_cents))}/mo.",
            f"Intro window: about {months_text} months; redirectable cash flow before expiry: "
            f"{fmt_dollars(cents_to_dollars(candidate.redirectable_cents))}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Set card-rotation reminder",
            tool="card_rotation_reminder_set",
            params={
                "zero_apr_account_id": candidate.zero_card.card_id,
                "paydown_account_id": candidate.high_card.card_id,
                "intro_apr_end_date": candidate.intro_end_date.isoformat(),
                "avg_monthly_spend_cents": candidate.avg_monthly_spend_cents,
                "estimated_interest_saved_cents": candidate.interest_saved_cents,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.interest_saved_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("D-3"),
    )


@register_pattern(
    id="D-5",
    move=Move.DIAGNOSE,
    tiers=(1, 4),
    priority=Priority.HIGH,
    cooldown=timedelta(days=30),
    tool="flag_card_for_paydown",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
)
def evaluate_d5_cash_vs_debt_arbitrage(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    candidate = _best_d5_candidate(conn, ctx)
    if candidate is None:
        return None

    card_apr = float(candidate.card.apr or 0)
    suggested = fmt_dollars(cents_to_dollars(candidate.suggested_payment_cents))
    interest_saved = fmt_dollars(cents_to_dollars(candidate.interest_saved_annual_cents))
    return Intervention(
        pattern_id="D-5",
        move=Move.DIAGNOSE,
        tiers=(1, 4),
        priority=Priority.HIGH,
        headline=(
            f"You have {fmt_dollars(cents_to_dollars(candidate.checking_balance_cents))} in checking. "
            f"{candidate.card.label} is at {card_apr:.2f}%. "
            f"Throwing {suggested} at the card saves about {interest_saved}/yr, "
            "while still leaving one month of expenses in cash."
        ),
        detail_bullets=(
            f"Cash source: {candidate.cash_source_label}",
            f"Retained cash buffer: {fmt_dollars(cents_to_dollars(candidate.retained_buffer_cents))}",
            f"Card balance: {fmt_dollars(cents_to_dollars(candidate.card.balance_cents))}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Flag card for next paydown",
            tool="flag_card_for_paydown",
            params={
                "account_id": candidate.card.card_id,
                "suggested_payment_cents": candidate.suggested_payment_cents,
                "cash_source_account_id": candidate.cash_source_account_id,
                "interest_saved_annual_cents": candidate.interest_saved_annual_cents,
                "reason": (
                    f"Use checking surplus above one month of expenses to pay down "
                    f"{candidate.card.label} at {card_apr:.2f}% APR."
                ),
                "source": "agent",
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.interest_saved_annual_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("D-5"),
    )


@register_pattern(
    id="D-6",
    move=Move.COMPARE,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=60),
    tool="set_balance_transfer_reminder",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
)
def evaluate_d6_balance_transfer_opportunity(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _best_d6_candidate(conn, ctx)
    if candidate is None:
        return None

    card_apr = float(candidate.card.apr or 0)
    balance = fmt_dollars(cents_to_dollars(candidate.card.balance_cents))
    fee = fmt_dollars(cents_to_dollars(candidate.balance_transfer_fee_cents))
    interest_avoided = fmt_dollars(cents_to_dollars(candidate.interest_avoided_12mo_cents))
    net_savings = fmt_dollars(cents_to_dollars(candidate.net_savings_12mo_cents))
    return Intervention(
        pattern_id="D-6",
        move=Move.COMPARE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"You have {balance} on {candidate.card.label} at {card_apr:.2f}%. "
            f"A 0% balance-transfer offer with a 3% fee ({fee}) could avoid about "
            f"{interest_avoided} of interest over 12 months; net estimate {net_savings}."
        ),
        detail_bullets=(
            "Confirm eligibility, promo length, transfer fee, credit impact, and payoff plan before applying.",
            f"Suggested reminder date: {candidate.suggested_remind_on.isoformat()}",
            "Suppressed when current late-payment or expired-intro balance signals suggest poor fit.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Set balance-transfer reminder",
            tool="set_balance_transfer_reminder",
            params={
                "account_id": candidate.card.card_id,
                "remind_on": candidate.suggested_remind_on.isoformat(),
                "balance_transfer_fee_percent": float(_D6_TRANSFER_FEE_PERCENT),
                "channel": "telegram",
                "note": (
                    "Compare 0% balance-transfer offers; confirm fee, promo APR length, "
                    "credit impact, and payoff plan before applying."
                ),
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.net_savings_12mo_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("D-6"),
    )


@register_pattern(
    id="D-7",
    move=Move.COACH,
    tiers=(4,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT, CFPProcessStep.MONITOR),
)
def evaluate_d7_debt_streak_reinforcement(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _best_d7_candidate(conn, ctx)
    if candidate is None:
        return None

    first_month = date.fromisoformat(f"{candidate.months[0]}-01").strftime("%B")
    return Intervention(
        pattern_id="D-7",
        move=Move.COACH,
        tiers=(4,),
        priority=Priority.MEDIUM,
        headline=(
            f"Three months of above-minimum payments on {candidate.card.label}. "
            f"You're {fmt_dollars(cents_to_dollars(candidate.ahead_cents))} ahead of the "
            "minimum-payment path. Keep this pace."
        ),
        detail_bullets=(
            (
                f"Average payment: {fmt_dollars(cents_to_dollars(candidate.average_payment_cents))}/mo "
                f"vs {fmt_dollars(cents_to_dollars(candidate.card.min_payment_cents))} minimum."
            ),
            (
                f"Balance moved from {fmt_dollars(cents_to_dollars(candidate.start_balance_cents))} "
                f"at the start of {first_month} to "
                f"{fmt_dollars(cents_to_dollars(candidate.current_balance_cents))} now."
            ),
            (
                "Minimum-payment projection would be about "
                f"{fmt_dollars(cents_to_dollars(candidate.projected_minimum_balance_cents))} today."
            ),
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=candidate.ahead_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("D-7"),
    )
