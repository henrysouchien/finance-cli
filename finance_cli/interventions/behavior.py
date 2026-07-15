from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import sqlite3

from ..categorizer import normalize_description
from ..commands.common import fmt_dollars
from ..models import cents_to_dollars
from ..spending_analysis import is_essential, is_excluded, load_essential_categories
from ..subscriptions import _monthly_equivalent, subscription_spend_history
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


_B1_LOOKBACK_MONTHS = 7
_B1_MIN_GROWTH_PERCENT = 15
_B2_LOOKBACK_MONTHS = 6
_B2_HALF_MONTHS = 3
_B2_MIN_SPEND_GROWTH_PERCENT = 10
_B2_INCOME_FLAT_TOLERANCE_PERCENT = 3
_B3_OVER_BUDGET_PERCENT = 30
_B3_ONE_OFF_OVERAGE_SHARE_PERCENT = 60
_B3_TREND_LOOKBACK_MONTHS = 2
_B4_LOOKBACK_MONTHS = 3
_B4_MIN_LATE_SPEND_LIFT_PERCENT = 40
_B4_PROMPT_WINDOW_DAYS = 10
_B5_STREAK_MONTHS = 3
_B6_LOOKBACK_CATALOG_DATE = "2026-06-21"
_B7_PLANNING_MONTHS = frozenset({9, 10, 11})
_B7_Q4_MONTHS = (10, 11, 12)
_B7_LOOKBACK_YEARS = 2
_B7_MIN_OVERAGE_PERCENT = 15
_B7_MIN_AVG_OVERAGE_CENTS = 10_000


@dataclass(frozen=True)
class _B2Driver:
    category: str
    baseline_avg_cents: int
    current_avg_cents: int
    delta_cents: int


@dataclass(frozen=True)
class _B2Candidate:
    baseline_spend_avg_cents: int
    current_spend_avg_cents: int
    spend_delta_cents: int
    baseline_income_avg_cents: int
    current_income_avg_cents: int
    drivers: tuple[_B2Driver, ...]


@dataclass(frozen=True)
class _B3Candidate:
    month_key: str
    category_id: str
    category_name: str
    budget_cents: int
    actual_cents: int
    overage_cents: int
    one_off_description: str
    one_off_amount_cents: int
    one_off_date: str


@dataclass(frozen=True)
class _B4Candidate:
    months: tuple[str, ...]
    first_window_avg_cents: int
    late_window_avg_cents: int
    buffer_cents: int
    lift_percent: int


@dataclass(frozen=True)
class _B5Candidate:
    category_name: str
    current_budget_cents: int
    actual_avg_cents: int
    freed_cents: int
    months: tuple[str, ...]


@dataclass(frozen=True)
class _B6ServiceDef:
    id: str
    name: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class _B6BundleDef:
    id: str
    family_id: str
    name: str
    monthly_cents: int
    service_ids: tuple[str, ...]
    source_url: str


@dataclass(frozen=True)
class _B6MatchedSubscription:
    service_id: str
    service_name: str
    vendor_name: str
    monthly_cents: int
    account_id: str | None


@dataclass(frozen=True)
class _B6Candidate:
    service_names: tuple[str, ...]
    vendor_names: tuple[str, ...]
    bundle_name: str
    bundle_monthly_cents: int
    separate_monthly_cents: int
    annual_savings_cents: int
    source_url: str


@dataclass(frozen=True)
class _B7YearSignal:
    year: int
    budget_cents: int
    actual_cents: int
    overage_cents: int
    comparison_label: str = "budget"


@dataclass(frozen=True)
class _B7Candidate:
    goal_name: str
    years: tuple[_B7YearSignal, ...]
    avg_overage_cents: int
    top_category: str
    top_category_overage_cents: int


_B6_SERVICES: tuple[_B6ServiceDef, ...] = (
    _B6ServiceDef("disney_plus", "Disney+", ("disney+", "disney plus", "disney")),
    _B6ServiceDef("hulu", "Hulu", ("hulu",)),
    _B6ServiceDef("espn", "ESPN", ("espn+", "espn plus", "espn")),
    _B6ServiceDef("apple_music", "Apple Music", ("apple music",)),
    _B6ServiceDef("apple_tv", "Apple TV", ("apple tv+", "apple tv plus", "apple tv")),
    _B6ServiceDef("apple_arcade", "Apple Arcade", ("apple arcade",)),
    _B6ServiceDef("icloud", "iCloud+", ("icloud+", "icloud plus", "icloud")),
    _B6ServiceDef("apple_fitness", "Apple Fitness+", ("apple fitness+", "apple fitness plus", "apple fitness")),
    _B6ServiceDef("apple_news", "Apple News+", ("apple news+", "apple news plus", "apple news")),
)
_B6_SERVICE_BY_ID = {service.id: service for service in _B6_SERVICES}

# Versioned from official public bundle pages on 2026-06-21.
_B6_BUNDLES: tuple[_B6BundleDef, ...] = (
    _B6BundleDef(
        id="disney_hulu_duo",
        family_id="disney",
        name="Disney+, Hulu Bundle",
        monthly_cents=1_299,
        service_ids=("disney_plus", "hulu"),
        source_url="https://www.disneyplus.com/welcome/disney-hulu-espn-bundle",
    ),
    _B6BundleDef(
        id="disney_hulu_espn_unlimited",
        family_id="disney",
        name="Disney+, Hulu, ESPN Unlimited Bundle",
        monthly_cents=3_599,
        service_ids=("disney_plus", "hulu", "espn"),
        source_url="https://www.disneyplus.com/welcome/disney-hulu-espn-bundle",
    ),
    _B6BundleDef(
        id="apple_one_individual",
        family_id="apple",
        name="Apple One Individual",
        monthly_cents=1_995,
        service_ids=("apple_music", "apple_tv", "apple_arcade", "icloud"),
        source_url="https://www.apple.com/apple-one/",
    ),
    _B6BundleDef(
        id="apple_one_family",
        family_id="apple",
        name="Apple One Family",
        monthly_cents=2_595,
        service_ids=("apple_music", "apple_tv", "apple_arcade", "icloud"),
        source_url="https://www.apple.com/apple-one/",
    ),
    _B6BundleDef(
        id="apple_one_premier",
        family_id="apple",
        name="Apple One Premier",
        monthly_cents=3_795,
        service_ids=("apple_music", "apple_tv", "apple_arcade", "icloud", "apple_fitness", "apple_news"),
        source_url="https://www.apple.com/apple-one/",
    ),
)


def _month_keys_ending(end_month: date, months: int) -> list[str]:
    anchor = end_month.replace(day=1)
    keys: list[str] = []
    for offset in range(months - 1, -1, -1):
        current = anchor
        for _ in range(offset):
            current = (current - timedelta(days=1)).replace(day=1)
        keys.append(current.strftime("%Y-%m"))
    return keys


def _month_end(month_key: str) -> date:
    year_text, month_text = month_key.split("-", 1)
    first = date(int(year_text), int(month_text), 1)
    if first.month == 12:
        next_month = first.replace(year=first.year + 1, month=1)
    else:
        next_month = first.replace(month=first.month + 1)
    return next_month - timedelta(days=1)


def _avg_cents(values: list[int]) -> int:
    if not values:
        return 0
    return int((Decimal(sum(values)) / Decimal(len(values))).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _previous_month_key(month_key: str) -> str:
    year_text, month_text = month_key.split("-", 1)
    first = date(int(year_text), int(month_text), 1)
    return (first - timedelta(days=1)).strftime("%Y-%m")


def _month_label(month_key: str) -> str:
    year_text, month_text = month_key.split("-", 1)
    return date(int(year_text), int(month_text), 1).strftime("%B")


def _load_b2_spend_history(
    conn: sqlite3.Connection,
    *,
    month_keys: list[str],
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    if not month_keys:
        return {}, {}
    month_set = set(month_keys)
    month_totals = {month: 0 for month in month_keys}
    by_category: dict[str, dict[str, int]] = {}
    rows = conn.execute(
        """
        SELECT substr(t.date, 1, 7) AS month,
               COALESCE(c.name, 'Uncategorized') AS category,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS total_cents
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.amount_cents < 0
           AND t.is_payment = 0
           AND t.is_active = 1
           AND COALESCE(c.is_income, 0) = 0
           AND (t.use_type = 'Personal' OR t.use_type IS NULL)
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY substr(t.date, 1, 7), COALESCE(c.name, 'Uncategorized')
        """,
        (f"{month_keys[0]}-01", _month_end(month_keys[-1]).isoformat()),
    ).fetchall()
    for row in rows:
        month = str(row["month"])
        if month not in month_set:
            continue
        category = str(row["category"] or "Uncategorized")
        amount_cents = int(row["total_cents"] or 0)
        month_totals[month] += amount_cents
        by_category.setdefault(category, {key: 0 for key in month_keys})[month] += amount_cents
    return month_totals, by_category


def _load_b2_income_history(
    conn: sqlite3.Connection,
    *,
    month_keys: list[str],
) -> dict[str, int]:
    if not month_keys:
        return {}
    month_set = set(month_keys)
    month_totals = {month: 0 for month in month_keys}
    rows = conn.execute(
        """
        SELECT substr(t.date, 1, 7) AS month,
               COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE c.is_income = 1
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents > 0
           AND (t.use_type = 'Personal' OR t.use_type IS NULL)
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY substr(t.date, 1, 7)
        """,
        (f"{month_keys[0]}-01", _month_end(month_keys[-1]).isoformat()),
    ).fetchall()
    for row in rows:
        month = str(row["month"])
        if month in month_set:
            month_totals[month] = int(row["total_cents"] or 0)
    return month_totals


def _find_b2_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _B2Candidate | None:
    last_complete_month = ctx.now.date().replace(day=1) - timedelta(days=1)
    month_keys = _month_keys_ending(last_complete_month, _B2_LOOKBACK_MONTHS)
    baseline_months = month_keys[:_B2_HALF_MONTHS]
    current_months = month_keys[_B2_HALF_MONTHS:]

    spend_by_month, spend_by_category = _load_b2_spend_history(conn, month_keys=month_keys)
    income_by_month = _load_b2_income_history(conn, month_keys=month_keys)
    if any(spend_by_month.get(month, 0) <= 0 for month in month_keys):
        return None

    baseline_spend_avg = _avg_cents([spend_by_month[month] for month in baseline_months])
    current_spend_avg = _avg_cents([spend_by_month[month] for month in current_months])
    if baseline_spend_avg <= 0:
        return None
    if current_spend_avg * 100 < baseline_spend_avg * (100 + _B2_MIN_SPEND_GROWTH_PERCENT):
        return None

    baseline_income_avg = _avg_cents([income_by_month[month] for month in baseline_months])
    current_income_avg = _avg_cents([income_by_month[month] for month in current_months])
    if baseline_income_avg <= 0:
        return None
    if current_income_avg * 100 > baseline_income_avg * (100 + _B2_INCOME_FLAT_TOLERANCE_PERCENT):
        return None

    drivers: list[_B2Driver] = []
    for category, category_months in spend_by_category.items():
        baseline_avg = _avg_cents([category_months.get(month, 0) for month in baseline_months])
        current_avg = _avg_cents([category_months.get(month, 0) for month in current_months])
        delta = current_avg - baseline_avg
        if delta <= 0:
            continue
        drivers.append(
            _B2Driver(
                category=category,
                baseline_avg_cents=baseline_avg,
                current_avg_cents=current_avg,
                delta_cents=delta,
            )
        )
    if not drivers:
        return None

    drivers.sort(key=lambda item: (-item.delta_cents, item.category.casefold()))
    return _B2Candidate(
        baseline_spend_avg_cents=baseline_spend_avg,
        current_spend_avg_cents=current_spend_avg,
        spend_delta_cents=current_spend_avg - baseline_spend_avg,
        baseline_income_avg_cents=baseline_income_avg,
        current_income_avg_cents=current_income_avg,
        drivers=tuple(drivers[:2]),
    )


def _active_personal_monthly_budget_cents(
    conn: sqlite3.Connection,
    *,
    category_id: str,
    month_key: str,
) -> int | None:
    row = conn.execute(
        """
        SELECT amount_cents
          FROM budgets
         WHERE category_id = ?
           AND period = 'monthly'
           AND use_type = 'Personal'
           AND date(effective_from) <= date(?)
           AND date(COALESCE(effective_to, '9999-12-31')) >= date(?)
         ORDER BY date(effective_from) DESC, id DESC
         LIMIT 1
        """,
        (category_id, _month_end(month_key).isoformat(), f"{month_key}-01"),
    ).fetchone()
    if row is None:
        return None
    return int(row["amount_cents"] or 0)


def _personal_category_spend_cents(
    conn: sqlite3.Connection,
    *,
    category_id: str,
    month_key: str,
) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(ABS(amount_cents)), 0) AS total_cents
          FROM transactions
         WHERE category_id = ?
           AND is_active = 1
           AND is_payment = 0
           AND amount_cents < 0
           AND (use_type = 'Personal' OR use_type IS NULL)
           AND date >= ?
           AND date <= ?
        """,
        (category_id, f"{month_key}-01", _month_end(month_key).isoformat()),
    ).fetchone()
    return int(row["total_cents"] or 0)


def _budgeted_category_ids_for_month(conn: sqlite3.Connection, *, month_key: str) -> tuple[str, ...]:
    rows = conn.execute(
        """
        SELECT DISTINCT category_id
          FROM budgets
         WHERE period = 'monthly'
           AND use_type = 'Personal'
           AND amount_cents > 0
           AND date(effective_from) <= date(?)
           AND date(COALESCE(effective_to, '9999-12-31')) >= date(?)
         ORDER BY category_id
        """,
        (_month_end(month_key).isoformat(), f"{month_key}-01"),
    ).fetchall()
    return tuple(str(row["category_id"]) for row in rows)


def _category_name(conn: sqlite3.Connection, *, category_id: str) -> str:
    row = conn.execute("SELECT name FROM categories WHERE id = ?", (category_id,)).fetchone()
    if row is None:
        return "budgeted spending"
    return str(row["name"] or "budgeted spending")


def _b7_explicit_goal_name(ctx: InterventionContext) -> str | None:
    for goal in ctx.goals:
        if goal.target_cents is not None and goal.target_cents > 0:
            return goal.name
    return None


def _b7_q4_year_signal(
    conn: sqlite3.Connection,
    *,
    year: int,
) -> tuple[_B7YearSignal | None, dict[str, int]]:
    signal, category_overages, has_complete_budget_history = _b7_budget_q4_year_signal(
        conn,
        year=year,
    )
    if signal is not None or has_complete_budget_history:
        return signal, category_overages
    return _b7_budgetless_q4_year_signal(conn, year=year)


def _b7_budget_q4_year_signal(
    conn: sqlite3.Connection,
    *,
    year: int,
) -> tuple[_B7YearSignal | None, dict[str, int], bool]:
    budget_total_cents = 0
    actual_total_cents = 0
    category_overages: dict[str, int] = {}
    for month in _B7_Q4_MONTHS:
        month_key = f"{year}-{month:02d}"
        category_ids = _budgeted_category_ids_for_month(conn, month_key=month_key)
        if not category_ids:
            return None, {}, False
        for category_id in category_ids:
            budget_cents = _active_personal_monthly_budget_cents(
                conn,
                category_id=category_id,
                month_key=month_key,
            )
            if budget_cents is None or budget_cents <= 0:
                continue
            actual_cents = _personal_category_spend_cents(
                conn,
                category_id=category_id,
                month_key=month_key,
            )
            budget_total_cents += budget_cents
            actual_total_cents += actual_cents
            overage_cents = actual_cents - budget_cents
            if overage_cents > 0:
                category_name = _category_name(conn, category_id=category_id)
                category_overages[category_name] = category_overages.get(category_name, 0) + overage_cents

    if budget_total_cents <= 0:
        return None, {}, False
    overage_cents = actual_total_cents - budget_total_cents
    if overage_cents < _B7_MIN_AVG_OVERAGE_CENTS:
        return None, {}, True
    if actual_total_cents * 100 < budget_total_cents * (100 + _B7_MIN_OVERAGE_PERCENT):
        return None, {}, True
    return (
        _B7YearSignal(
            year=year,
            budget_cents=budget_total_cents,
            actual_cents=actual_total_cents,
            overage_cents=overage_cents,
        ),
        category_overages,
        True,
    )


def _b7_budgetless_q4_year_signal(
    conn: sqlite3.Connection,
    *,
    year: int,
) -> tuple[_B7YearSignal | None, dict[str, int]]:
    baseline_months = [f"{year}-{month:02d}" for month in (7, 8, 9)]
    q4_months = [f"{year}-{month:02d}" for month in _B7_Q4_MONTHS]
    month_keys = [*baseline_months, *q4_months]
    spend_by_month, spend_by_category = _load_b2_spend_history(conn, month_keys=month_keys)
    if any(spend_by_month.get(month, 0) <= 0 for month in month_keys):
        return None, {}

    baseline_total_cents = sum(spend_by_month[month] for month in baseline_months)
    actual_total_cents = sum(spend_by_month[month] for month in q4_months)
    if baseline_total_cents <= 0:
        return None, {}
    overage_cents = actual_total_cents - baseline_total_cents
    if overage_cents < _B7_MIN_AVG_OVERAGE_CENTS:
        return None, {}
    if actual_total_cents * 100 < baseline_total_cents * (100 + _B7_MIN_OVERAGE_PERCENT):
        return None, {}

    category_overages: dict[str, int] = {}
    for category_name, category_months in spend_by_category.items():
        baseline_cents = sum(category_months.get(month, 0) for month in baseline_months)
        q4_cents = sum(category_months.get(month, 0) for month in q4_months)
        category_overage_cents = q4_cents - baseline_cents
        if category_overage_cents > 0:
            category_overages[category_name] = category_overage_cents

    if not category_overages:
        return None, {}
    return (
        _B7YearSignal(
            year=year,
            budget_cents=baseline_total_cents,
            actual_cents=actual_total_cents,
            overage_cents=overage_cents,
            comparison_label="July-September baseline",
        ),
        category_overages,
    )


def _find_b7_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _B7Candidate | None:
    as_of = ctx.now.date()
    if as_of.month not in _B7_PLANNING_MONTHS:
        return None
    goal_name = _b7_explicit_goal_name(ctx)
    if goal_name is None:
        return None

    year_signals: list[_B7YearSignal] = []
    category_overages: dict[str, int] = {}
    for year in range(as_of.year - _B7_LOOKBACK_YEARS, as_of.year):
        signal, signal_category_overages = _b7_q4_year_signal(conn, year=year)
        if signal is None:
            return None
        year_signals.append(signal)
        for category_id, overage_cents in signal_category_overages.items():
            category_overages[category_id] = category_overages.get(category_id, 0) + overage_cents

    if len(year_signals) != _B7_LOOKBACK_YEARS:
        return None
    avg_overage_cents = _avg_cents([signal.overage_cents for signal in year_signals])
    if avg_overage_cents < _B7_MIN_AVG_OVERAGE_CENTS:
        return None
    if not category_overages:
        return None
    top_category, top_category_overage_cents = max(
        category_overages.items(),
        key=lambda item: (item[1], item[0].casefold()),
    )
    return _B7Candidate(
        goal_name=goal_name,
        years=tuple(year_signals),
        avg_overage_cents=avg_overage_cents,
        top_category=top_category,
        top_category_overage_cents=top_category_overage_cents,
    )


def _has_b3_category_trend(
    conn: sqlite3.Connection,
    *,
    category_id: str,
    month_key: str,
) -> bool:
    cursor_month = month_key
    for _ in range(_B3_TREND_LOOKBACK_MONTHS):
        cursor_month = _previous_month_key(cursor_month)
        budget_cents = _active_personal_monthly_budget_cents(conn, category_id=category_id, month_key=cursor_month)
        if budget_cents is None or budget_cents <= 0:
            continue
        actual_cents = _personal_category_spend_cents(conn, category_id=category_id, month_key=cursor_month)
        if actual_cents * 100 >= budget_cents * (100 + _B3_OVER_BUDGET_PERCENT):
            return True
    return False


def _find_b3_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _B3Candidate | None:
    last_complete_month = ctx.now.date().replace(day=1) - timedelta(days=1)
    month_key = last_complete_month.strftime("%Y-%m")
    budget_rows = conn.execute(
        """
        SELECT b.category_id,
               c.name AS category_name,
               b.amount_cents
          FROM budgets b
          JOIN categories c ON c.id = b.category_id
         WHERE b.period = 'monthly'
           AND b.use_type = 'Personal'
           AND b.amount_cents > 0
           AND date(b.effective_from) <= date(?)
           AND date(COALESCE(b.effective_to, '9999-12-31')) >= date(?)
         ORDER BY c.name, date(b.effective_from) DESC, b.id DESC
        """,
        (last_complete_month.isoformat(), f"{month_key}-01"),
    ).fetchall()

    candidates: list[_B3Candidate] = []
    seen_categories: set[str] = set()
    for budget in budget_rows:
        category_id = str(budget["category_id"])
        if category_id in seen_categories:
            continue
        seen_categories.add(category_id)
        budget_cents = int(budget["amount_cents"] or 0)
        if budget_cents <= 0:
            continue

        txns = conn.execute(
            """
            SELECT id, date, description, ABS(amount_cents) AS amount_cents
              FROM transactions
             WHERE category_id = ?
               AND is_active = 1
               AND is_payment = 0
               AND amount_cents < 0
               AND (use_type = 'Personal' OR use_type IS NULL)
               AND date >= ?
               AND date <= ?
             ORDER BY ABS(amount_cents) DESC, date DESC, id
            """,
            (category_id, f"{month_key}-01", last_complete_month.isoformat()),
        ).fetchall()
        if not txns:
            continue

        actual_cents = sum(int(row["amount_cents"] or 0) for row in txns)
        if actual_cents * 100 < budget_cents * (100 + _B3_OVER_BUDGET_PERCENT):
            continue
        overage_cents = actual_cents - budget_cents
        if overage_cents <= 0:
            continue

        largest = txns[0]
        largest_cents = int(largest["amount_cents"] or 0)
        if largest_cents * 100 < overage_cents * _B3_ONE_OFF_OVERAGE_SHARE_PERCENT:
            continue
        if _has_b3_category_trend(conn, category_id=category_id, month_key=month_key):
            continue

        description = str(largest["description"] or "").strip() or "one-time transaction"
        candidates.append(
            _B3Candidate(
                month_key=month_key,
                category_id=category_id,
                category_name=str(budget["category_name"]),
                budget_cents=budget_cents,
                actual_cents=actual_cents,
                overage_cents=overage_cents,
                one_off_description=description,
                one_off_amount_cents=largest_cents,
                one_off_date=str(largest["date"]),
            )
        )

    if not candidates:
        return None
    candidates.sort(key=lambda item: (-item.overage_cents, item.category_name.casefold()))
    return candidates[0]


def _b4_personal_discretionary_window_spend(
    conn: sqlite3.Connection,
    *,
    month_key: str,
    essential_categories: frozenset[str],
) -> tuple[int, int]:
    month_start = date.fromisoformat(f"{month_key}-01")
    month_end = _month_end(month_key)
    late_start = month_end - timedelta(days=9)
    rows = conn.execute(
        """
        SELECT t.date,
               ABS(t.amount_cents) AS amount_cents,
               c.name AS category_name,
               COALESCE(p.name, '') AS parent_name
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          LEFT JOIN categories p ON p.id = c.parent_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND c.is_income = 0
           AND (t.use_type = 'Personal' OR t.use_type IS NULL)
           AND t.date >= ?
           AND t.date <= ?
        """,
        (month_start.isoformat(), month_end.isoformat()),
    ).fetchall()

    first_window_cents = 0
    late_window_cents = 0
    for row in rows:
        category_name = str(row["category_name"] or "")
        parent_name = str(row["parent_name"] or "")
        if (
            is_excluded(category_name)
            or is_excluded(parent_name)
            or is_essential(category_name, essential_categories)
            or (parent_name and is_essential(parent_name, essential_categories))
        ):
            continue
        try:
            txn_date = date.fromisoformat(str(row["date"])[:10])
        except ValueError:
            continue
        amount_cents = int(row["amount_cents"] or 0)
        if month_start <= txn_date <= month_start + timedelta(days=9):
            first_window_cents += amount_cents
        if late_start <= txn_date <= month_end:
            late_window_cents += amount_cents
    return first_window_cents, late_window_cents


def _first_day_next_month(as_of: date) -> date:
    current_month = as_of.replace(day=1)
    if current_month.month == 12:
        return current_month.replace(year=current_month.year + 1, month=1)
    return current_month.replace(month=current_month.month + 1)


def _is_b4_prompt_window(as_of: date) -> bool:
    current_month_end = _month_end(as_of.strftime("%Y-%m"))
    return as_of >= current_month_end - timedelta(days=_B4_PROMPT_WINDOW_DAYS - 1)


def _find_b4_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _B4Candidate | None:
    if not _is_b4_prompt_window(ctx.now.date()):
        return None

    last_complete_month = ctx.now.date().replace(day=1) - timedelta(days=1)
    month_keys = _month_keys_ending(last_complete_month, _B4_LOOKBACK_MONTHS)
    essential_categories = load_essential_categories(ctx.rules_path)

    first_values: list[int] = []
    late_values: list[int] = []
    for month_key in month_keys:
        first_cents, late_cents = _b4_personal_discretionary_window_spend(
            conn,
            month_key=month_key,
            essential_categories=essential_categories,
        )
        if first_cents <= 0:
            return None
        if late_cents * 100 < first_cents * (100 + _B4_MIN_LATE_SPEND_LIFT_PERCENT):
            return None
        first_values.append(first_cents)
        late_values.append(late_cents)

    first_avg = _avg_cents(first_values)
    late_avg = _avg_cents(late_values)
    buffer_cents = late_avg - first_avg
    if first_avg <= 0 or buffer_cents <= 0:
        return None
    lift_percent = round(buffer_cents * 100 / first_avg)
    return _B4Candidate(
        months=tuple(month_keys),
        first_window_avg_cents=first_avg,
        late_window_avg_cents=late_avg,
        buffer_cents=buffer_cents,
        lift_percent=lift_percent,
    )


def _find_b5_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _B5Candidate | None:
    last_complete_month = ctx.now.date().replace(day=1) - timedelta(days=1)
    month_keys = _month_keys_ending(last_complete_month, _B5_STREAK_MONTHS)
    budget_rows = conn.execute(
        """
        SELECT b.category_id,
               c.name AS category_name,
               b.amount_cents
          FROM budgets b
          JOIN categories c ON c.id = b.category_id
        WHERE b.period = 'monthly'
           AND b.use_type = 'Personal'
           AND b.amount_cents > 0
           AND b.effective_to IS NULL
         ORDER BY c.name, date(b.effective_from) DESC, b.id DESC
        """,
    ).fetchall()

    candidates: list[_B5Candidate] = []
    seen_categories: set[str] = set()
    for budget in budget_rows:
        category_id = str(budget["category_id"])
        if category_id in seen_categories:
            continue
        seen_categories.add(category_id)
        current_budget_cents = int(budget["amount_cents"] or 0)
        if current_budget_cents <= 0:
            continue

        actuals: list[int] = []
        streak_ok = True
        for month_key in month_keys:
            budget_cents = _active_personal_monthly_budget_cents(conn, category_id=category_id, month_key=month_key)
            if budget_cents is None or budget_cents <= 0:
                streak_ok = False
                break
            actual_cents = _personal_category_spend_cents(conn, category_id=category_id, month_key=month_key)
            if actual_cents <= 0 or actual_cents >= budget_cents:
                streak_ok = False
                break
            actuals.append(actual_cents)
        if not streak_ok:
            continue

        actual_avg_cents = _avg_cents(actuals)
        freed_cents = current_budget_cents - actual_avg_cents
        if actual_avg_cents <= 0 or freed_cents <= 0:
            continue
        candidates.append(
            _B5Candidate(
                category_name=str(budget["category_name"]),
                current_budget_cents=current_budget_cents,
                actual_avg_cents=actual_avg_cents,
                freed_cents=freed_cents,
                months=tuple(month_keys),
            )
        )

    if not candidates:
        return None
    candidates.sort(key=lambda item: (-item.freed_cents, item.category_name.casefold()))
    return candidates[0]


def _b6_match_text(value: str) -> str:
    expanded = value.casefold().replace("+", " plus ").replace("&", " and ").replace(".", " ")
    return f" {normalize_description(expanded)} "


def _b6_is_bundle_like(vendor_name: str) -> bool:
    text = _b6_match_text(vendor_name)
    return any(marker in text for marker in (" bundle ", " apple one ", " duo ", " trio "))


def _b6_service_for_vendor(vendor_name: str) -> _B6ServiceDef | None:
    if _b6_is_bundle_like(vendor_name):
        return None
    text = _b6_match_text(vendor_name)
    matches: list[_B6ServiceDef] = []
    for service in _B6_SERVICES:
        for alias in service.aliases:
            alias_text = _b6_match_text(alias)
            if alias_text in text:
                matches.append(service)
                break
    if len(matches) != 1:
        return None
    return matches[0]


def _b6_service_names(service_ids: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(_B6_SERVICE_BY_ID[service_id].name for service_id in service_ids)


def _b6_human_list(values: tuple[str, ...]) -> str:
    if len(values) == 0:
        return ""
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return f"{values[0]} and {values[1]}"
    return f"{', '.join(values[:-1])}, and {values[-1]}"


def _b6_has_apple_premier_signal(matched: dict[str, _B6MatchedSubscription]) -> bool:
    if any(service_id in matched for service_id in ("apple_fitness", "apple_news")):
        return True
    for subscription in matched.values():
        text = _b6_match_text(subscription.vendor_name)
        if any(marker in text for marker in (" 2tb ", " 2 tb ", " premier ")):
            return True
        if subscription.service_id == "icloud" and subscription.monthly_cents >= 999:
            return True
    return False


def _b6_has_apple_family_signal(matched: dict[str, _B6MatchedSubscription]) -> bool:
    for subscription in matched.values():
        text = _b6_match_text(subscription.vendor_name)
        if any(marker in text for marker in (" family ", " 200gb ", " 200 gb ")):
            return True
        if subscription.service_id == "apple_music" and subscription.monthly_cents >= 1_600:
            return True
        if subscription.service_id == "icloud" and 299 <= subscription.monthly_cents < 999:
            return True
    return False


def _b6_bundle_price_applies(bundle: _B6BundleDef, matched: dict[str, _B6MatchedSubscription]) -> bool:
    if bundle.family_id != "apple":
        return True
    has_premier_signal = _b6_has_apple_premier_signal(matched)
    has_family_signal = _b6_has_apple_family_signal(matched)
    if bundle.id == "apple_one_premier":
        return has_premier_signal
    if bundle.id == "apple_one_family":
        return has_family_signal and not has_premier_signal
    if bundle.id == "apple_one_individual":
        return not has_family_signal and not has_premier_signal
    return True


def _find_b6_candidate(conn: sqlite3.Connection) -> _B6Candidate | None:
    rows = conn.execute(
        """
        SELECT id, vendor_name, amount_cents, frequency, account_id, use_type
         FROM subscriptions
         WHERE is_active = 1
           AND use_type = 'Personal'
        """
    ).fetchall()

    grouped: dict[tuple[str, str | None], dict[str, _B6MatchedSubscription]] = {}
    for row in rows:
        vendor_name = str(row["vendor_name"] or "").strip()
        service = _b6_service_for_vendor(vendor_name)
        if service is None:
            continue
        monthly_cents = _monthly_equivalent(abs(int(row["amount_cents"] or 0)), str(row["frequency"] or "monthly"))
        if monthly_cents <= 0:
            continue
        family_id = "disney" if service.id in {"disney_plus", "hulu", "espn"} else "apple"
        if row["account_id"] is None:
            continue
        account_id = str(row["account_id"])
        service_map = grouped.setdefault((family_id, account_id), {})
        existing = service_map.get(service.id)
        if existing is None or monthly_cents > existing.monthly_cents:
            service_map[service.id] = _B6MatchedSubscription(
                service_id=service.id,
                service_name=service.name,
                vendor_name=vendor_name,
                monthly_cents=monthly_cents,
                account_id=account_id,
            )

    candidates: list[_B6Candidate] = []
    for (family_id, _account_id), service_map in grouped.items():
        for bundle in _B6_BUNDLES:
            if bundle.family_id != family_id:
                continue
            matched_service_ids = tuple(service_id for service_id in bundle.service_ids if service_id in service_map)
            if len(matched_service_ids) < 2:
                continue
            matched = {service_id: service_map[service_id] for service_id in matched_service_ids}
            if not _b6_bundle_price_applies(bundle, matched):
                continue
            separate_monthly_cents = sum(subscription.monthly_cents for subscription in matched.values())
            monthly_savings_cents = separate_monthly_cents - bundle.monthly_cents
            if monthly_savings_cents <= 0:
                continue
            candidates.append(
                _B6Candidate(
                    service_names=_b6_service_names(matched_service_ids),
                    vendor_names=tuple(subscription.vendor_name for subscription in matched.values()),
                    bundle_name=bundle.name,
                    bundle_monthly_cents=bundle.monthly_cents,
                    separate_monthly_cents=separate_monthly_cents,
                    annual_savings_cents=monthly_savings_cents * 12,
                    source_url=bundle.source_url,
                )
            )

    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            -item.annual_savings_cents,
            item.bundle_name.casefold(),
            tuple(name.casefold() for name in item.service_names),
        )
    )
    return candidates[0]


@register_pattern(
    id="B-1",
    move=Move.PATTERN_CATCH,
    tiers=(1, 4),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    tool="subs_audit",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
)
def evaluate_b1_subscription_drift(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    essential_categories = load_essential_categories(ctx.rules_path)
    last_complete_month = ctx.now.date().replace(day=1) - timedelta(days=1)
    history = subscription_spend_history(
        conn,
        months=_B1_LOOKBACK_MONTHS,
        as_of=last_complete_month,
        essential_categories=essential_categories,
    )
    months = list(history["months"])
    totals = dict(history["totals_cents"])
    if len(months) < _B1_LOOKBACK_MONTHS or int(history.get("transaction_count", 0)) <= 0:
        return None

    baseline_month = months[0]
    current_month = months[-1]
    baseline_cents = int(totals.get(baseline_month, 0))
    current_cents = int(totals.get(current_month, 0))
    if baseline_cents <= 0:
        return None
    if current_cents * 100 < baseline_cents * (100 + _B1_MIN_GROWTH_PERCENT):
        return None

    monthly_delta_cents = current_cents - baseline_cents
    annualized_delta_cents = monthly_delta_cents * 12
    if monthly_delta_cents <= 0:
        return None

    return Intervention(
        pattern_id="B-1",
        move=Move.PATTERN_CATCH,
        tiers=(1, 4),
        priority=Priority.MEDIUM,
        headline=(
            f"Subscription drift: {fmt_dollars(cents_to_dollars(current_cents))}/mo in matched subs, "
            f"up from {fmt_dollars(cents_to_dollars(baseline_cents))}/mo six months ago. "
            f"That's {fmt_dollars(cents_to_dollars(annualized_delta_cents))}/yr of extra recurring burn."
        ),
        detail_bullets=(
            f"{baseline_month}: {fmt_dollars(cents_to_dollars(baseline_cents))} matched subscription spend.",
            f"{current_month}: {fmt_dollars(cents_to_dollars(current_cents))} matched subscription spend.",
            f"Increase: {fmt_dollars(cents_to_dollars(monthly_delta_cents))}/mo.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Audit subscriptions",
            tool="subs_audit",
            params={},
            build_stub=False,
        ),
        dollar_impact_cents=annualized_delta_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("B-1"),
    )


def _b2_driver_phrase(drivers: tuple[_B2Driver, ...]) -> str:
    if len(drivers) >= 2:
        first, second = drivers[0], drivers[1]
        return (
            f"{first.category} (+{fmt_dollars(cents_to_dollars(first.delta_cents))}/mo) and "
            f"{second.category} (+{fmt_dollars(cents_to_dollars(second.delta_cents))}/mo)"
        )
    first = drivers[0]
    return f"{first.category} (+{fmt_dollars(cents_to_dollars(first.delta_cents))}/mo)"


@register_pattern(
    id="B-2",
    move=Move.WARN,
    tiers=(1, 4),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    tool="spending_trends",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.MONITOR),
)
def evaluate_b2_lifestyle_creep(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_b2_candidate(conn, ctx)
    if candidate is None:
        return None

    top_driver = candidate.drivers[0]
    driver_phrase = _b2_driver_phrase(candidate.drivers)
    spend_delta = fmt_dollars(cents_to_dollars(candidate.spend_delta_cents))
    reclaim = fmt_dollars(cents_to_dollars(top_driver.delta_cents))
    annualized_reclaim_cents = top_driver.delta_cents * 12
    income_clause = (
        "Income is down."
        if candidate.current_income_avg_cents < candidate.baseline_income_avg_cents
        else "Income hasn't moved."
    )
    return Intervention(
        pattern_id="B-2",
        move=Move.WARN,
        tiers=(1, 4),
        priority=Priority.MEDIUM,
        headline=(
            f"Spending's up {spend_delta}/mo over six months - most of it is {driver_phrase}. "
            f"{income_clause} Pulling {top_driver.category} back to last quarter's level "
            f"frees {reclaim}/mo."
        ),
        detail_bullets=(
            (
                "Personal spending average moved from "
                f"{fmt_dollars(cents_to_dollars(candidate.baseline_spend_avg_cents))}/mo "
                f"to {fmt_dollars(cents_to_dollars(candidate.current_spend_avg_cents))}/mo."
            ),
            (
                "Income average moved from "
                f"{fmt_dollars(cents_to_dollars(candidate.baseline_income_avg_cents))}/mo "
                f"to {fmt_dollars(cents_to_dollars(candidate.current_income_avg_cents))}/mo."
            ),
            (
                f"Top driver: {top_driver.category} moved from "
                f"{fmt_dollars(cents_to_dollars(top_driver.baseline_avg_cents))}/mo "
                f"to {fmt_dollars(cents_to_dollars(top_driver.current_avg_cents))}/mo."
            ),
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Show six-month spending trends",
            tool="spending_trends",
            params={
                "months": 6,
                "view": "personal",
                "categories": [driver.category for driver in candidate.drivers],
            },
            build_stub=False,
        ),
        dollar_impact_cents=annualized_reclaim_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("B-2"),
    )


@register_pattern(
    id="B-3",
    move=Move.DIAGNOSE,
    tiers=(1,),
    priority=Priority.LOW,
    tool=None,
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT),
)
def evaluate_b3_one_off_vs_trend(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_b3_candidate(conn, ctx)
    if candidate is None:
        return None

    month_label = _month_label(candidate.month_key)
    overage = fmt_dollars(cents_to_dollars(candidate.overage_cents))
    one_off_amount = fmt_dollars(cents_to_dollars(candidate.one_off_amount_cents))
    return Intervention(
        pattern_id="B-3",
        move=Move.DIAGNOSE,
        tiers=(1,),
        priority=Priority.LOW,
        headline=(
            f"Your {month_label} was {overage} over on {candidate.category_name}, "
            f"but {one_off_amount} was a one-time {candidate.one_off_description}. "
            "Not a trend - you absorbed a hit."
        ),
        detail_bullets=(
            (
                f"{candidate.category_name} budget: "
                f"{fmt_dollars(cents_to_dollars(candidate.budget_cents))}."
            ),
            (
                f"{candidate.category_name} actual: "
                f"{fmt_dollars(cents_to_dollars(candidate.actual_cents))}."
            ),
            (
                f"Largest transaction: {one_off_amount} on {candidate.one_off_date} "
                f"({candidate.one_off_description})."
            ),
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("B-3"),
    )


@register_pattern(
    id="B-4",
    move=Move.PATTERN_CATCH,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=60),
    tool="add_late_month_buffer_budget",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(
        CFPProcessStep.ANALYZE,
        CFPProcessStep.DEVELOP,
        CFPProcessStep.IMPLEMENT,
    ),
)
def evaluate_b4_end_of_month_spending_pattern(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_b4_candidate(conn, ctx)
    if candidate is None:
        return None

    first_avg = fmt_dollars(cents_to_dollars(candidate.first_window_avg_cents))
    late_avg = fmt_dollars(cents_to_dollars(candidate.late_window_avg_cents))
    buffer = fmt_dollars(cents_to_dollars(candidate.buffer_cents))
    effective_from = _first_day_next_month(ctx.now.date()).isoformat()
    return Intervention(
        pattern_id="B-4",
        move=Move.PATTERN_CATCH,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Your discretionary spend runs {candidate.lift_percent}% higher in "
            "the last 10 days of the month. The late-month pattern is consistent; "
            "want to budget around it instead of fighting it?"
        ),
        detail_bullets=(
            f"First 10 days average: {first_avg}.",
            f"Last 10 days average: {late_avg}.",
            f"Months checked: {', '.join(candidate.months)}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label=f"Add {buffer}/mo late-month buffer",
            tool="add_late_month_buffer_budget",
            params={
                "amount_cents": candidate.buffer_cents,
                "category_name": "Late-Month Buffer",
                "effective_from": effective_from,
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("B-4"),
    )


@register_pattern(
    id="B-5",
    move=Move.COACH,
    tiers=(1, 4),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    tool="budget_update",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(
        CFPProcessStep.ANALYZE,
        CFPProcessStep.DEVELOP,
        CFPProcessStep.IMPLEMENT,
        CFPProcessStep.MONITOR,
    ),
)
def evaluate_b5_discipline_streak(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_b5_candidate(conn, ctx)
    if candidate is None:
        return None

    actual_avg = fmt_dollars(cents_to_dollars(candidate.actual_avg_cents))
    freed = fmt_dollars(cents_to_dollars(candidate.freed_cents))
    annualized_freed_cents = candidate.freed_cents * 12
    return Intervention(
        pattern_id="B-5",
        move=Move.COACH,
        tiers=(1, 4),
        priority=Priority.MEDIUM,
        headline=(
            f"Three months in a row under budget on {candidate.category_name}. "
            f"That's discipline. Want to lock {actual_avg} as your new target? "
            f"Frees up {freed}/mo for your goal."
        ),
        detail_bullets=(
            (
                f"Current monthly target: "
                f"{fmt_dollars(cents_to_dollars(candidate.current_budget_cents))}."
            ),
            f"Three-month actual average: {actual_avg}.",
            f"Months checked: {', '.join(candidate.months)}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label=f"Lower {candidate.category_name} budget target",
            tool="budget_update",
            params={
                "category": candidate.category_name,
                "amount": cents_to_dollars(candidate.actual_avg_cents),
                "period": "monthly",
                "view": "personal",
            },
            build_stub=False,
        ),
        dollar_impact_cents=annualized_freed_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("B-5"),
    )


@register_pattern(
    id="B-6",
    move=Move.COMPARE,
    tiers=(1,),
    priority=Priority.LOW,
    cooldown=timedelta(days=60),
    tool=None,
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT),
)
def evaluate_b6_subscription_bundle_opportunity(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_b6_candidate(conn)
    if candidate is None:
        return None

    service_list = _b6_human_list(candidate.service_names)
    separate_total = fmt_dollars(cents_to_dollars(candidate.separate_monthly_cents))
    bundle_total = fmt_dollars(cents_to_dollars(candidate.bundle_monthly_cents))
    annual_savings = fmt_dollars(cents_to_dollars(candidate.annual_savings_cents))
    return Intervention(
        pattern_id="B-6",
        move=Move.COMPARE,
        tiers=(1,),
        priority=Priority.LOW,
        headline=(
            f"{service_list} billing separate ({separate_total}/mo). "
            f"{candidate.bundle_name} is {bundle_total}/mo. "
            f"{annual_savings}/yr to switch."
        ),
        detail_bullets=(
            f"Matched subscriptions: {_b6_human_list(candidate.vendor_names)}.",
            f"Bundle catalog snapshot: {_B6_LOOKBACK_CATALOG_DATE} ({candidate.source_url}).",
            "No action taken; confirm plan eligibility and household needs before switching.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=candidate.annual_savings_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("B-6"),
    )


@register_pattern(
    id="B-7",
    move=Move.PATTERN_CATCH,
    tiers=(4,),
    priority=Priority.LOW,
    cooldown=timedelta(days=90),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.PSYCHOLOGY),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.MONITOR),
)
def evaluate_b7_q4_budget_drag(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_b7_candidate(conn, ctx)
    if candidate is None:
        return None

    years_text = " and ".join(str(signal.year) for signal in candidate.years)
    budget_backed = all(signal.comparison_label == "budget" for signal in candidate.years)
    if budget_backed:
        headline = (
            f"Q4 has run over budget in {years_text} by about "
            f"{fmt_dollars(cents_to_dollars(candidate.avg_overage_cents))}. "
            f"That is the seasonal drag on {candidate.goal_name}; plan the buffer before October."
        )
    else:
        comparison_label = (
            candidate.years[0].comparison_label
            if all(signal.comparison_label == candidate.years[0].comparison_label for signal in candidate.years)
            else "comparison baseline"
        )
        headline = (
            f"Q4 spending has run above its {comparison_label} in {years_text} by about "
            f"{fmt_dollars(cents_to_dollars(candidate.avg_overage_cents))}. "
            f"That is the seasonal drag on {candidate.goal_name}; plan the buffer before October."
        )
    return Intervention(
        pattern_id="B-7",
        move=Move.PATTERN_CATCH,
        tiers=(4,),
        priority=Priority.LOW,
        headline=headline,
        detail_bullets=(
            *(
                (
                    f"{signal.year} Q4: {fmt_dollars(cents_to_dollars(signal.actual_cents))} "
                    f"actual vs {fmt_dollars(cents_to_dollars(signal.budget_cents))} "
                    f"{signal.comparison_label}."
                )
                for signal in candidate.years
            ),
            (
                f"Biggest recurring Q4 pressure: {candidate.top_category} "
                f"({fmt_dollars(cents_to_dollars(candidate.top_category_overage_cents))} total overage)."
            ),
            "Fires only September-November so the pattern is surfaced while there is still time to pre-plan.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=candidate.avg_overage_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("B-7"),
    )
