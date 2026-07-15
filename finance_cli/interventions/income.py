from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import sqlite3

from ..commands.common import fmt_dollars
from ..models import cents_to_dollars
from .context import InterventionContext
from .helpers import bounded_whole_percent, income_by_stream
from .registry import (
    CFPDomain,
    CFPProcessStep,
    Intervention,
    InterventionAction,
    Move,
    Priority,
    register_pattern,
)


_I2_LOOKBACK_MONTHS = 3
_I2_MIN_TOP_SHARE = Decimal("0.50")
_I3_BASELINE_MONTHS = 3
_I3_MIN_HISTORY_YEARS = 2
_I3_MIN_DIP = Decimal("0.20")
_I3_MIN_LEAD_DAYS = 42
_I3_MAX_LEAD_DAYS = 56
_I4_LOOKBACK_DAYS = 90
_I4_MIN_CLUSTER_COUNT = 3
_I4_MIN_CLUSTER_CENTS = 10_000
_I4_REVIEW_PROJECT_NAME = "Billable Review"
_I4_CATEGORY_CLUSTERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "travel",
        (
            "travel",
            "airfare",
            "flight",
            "hotel",
            "lodging",
            "mileage",
            "parking",
            "rideshare",
            "taxi",
            "train",
            "uber",
            "lyft",
        ),
    ),
    (
        "meals",
        (
            "meal",
            "meals",
            "dining",
            "restaurant",
            "coffee",
            "food",
        ),
    ),
    (
        "software",
        (
            "software",
            "subscription",
            "saas",
            "hosting",
            "cloud",
            "tools",
        ),
    ),
)
_I4_CLUSTER_COPY = {
    "travel": "travel",
    "meals": "meal",
    "software": "software",
}
_I5_LOOKBACK_MONTHS = 6
_I5_RATE_STABILITY_TOLERANCE = Decimal("0.05")
_I5_TEST_BUMP_PCT = 10
_I5_GROWTH_DOMAINS = {"income", "business_income", "pricing"}
_I5_GROWTH_STRATEGIES = {
    "grow_income",
    "income_growth",
    "raise_rates",
    "rate_increase",
    "pricing_test",
}
_I5_TRACKED_HOURS_KEYS = (
    "tracked_hours",
    "billable_hours",
    "hours_worked",
    "hours",
)


@dataclass(frozen=True)
class _I2Candidate:
    stream: str
    top_income_cents: int
    total_income_cents: int
    top_share_pct: int
    monthly_top_income_cents: int
    liquid_balance_cents: int
    runway_weeks: int


@dataclass(frozen=True)
class _I3Candidate:
    season_month_start: date
    season_month_label: str
    weeks_until_start: int
    evidence_years: int
    historical_baseline_cents: int
    historical_slow_month_cents: int
    recent_avg_income_cents: int
    dip_pct: int
    suggested_extra_cents: int


@dataclass(frozen=True)
class _I4Candidate:
    cluster_label: str
    transaction_ids: tuple[str, ...]
    total_cents: int
    category_names: tuple[str, ...]
    start_date: date
    end_date: date


@dataclass(frozen=True)
class _I5Candidate:
    stream: str
    months: int
    rate_cents_per_hour: int
    bumped_rate_cents_per_hour: int
    total_income_cents: int
    total_hours: Decimal
    monthly_min_rate_cents_per_hour: int
    monthly_max_rate_cents_per_hour: int
    annualized_bump_cents: int


def _month_keys(as_of: date, months: int) -> list[str]:
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    cursor = end_of_last_complete_month.replace(day=1)
    values: list[str] = []
    for _ in range(months):
        values.append(cursor.strftime("%Y-%m"))
        cursor = (cursor - timedelta(days=1)).replace(day=1)
    values.reverse()
    return values


def _avg(values: list[int]) -> Decimal:
    if not values:
        return Decimal("0")
    return Decimal(sum(values)) / Decimal(len(values))


def _add_months(month_start: date, delta_months: int) -> date:
    month_index = (month_start.year * 12) + (month_start.month - 1) + delta_months
    return date(month_index // 12, (month_index % 12) + 1, 1)


def _month_key(month_start: date) -> str:
    return month_start.strftime("%Y-%m")


def _round_decimal_to_int(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _money(cents: int) -> str:
    return fmt_dollars(cents_to_dollars(int(cents)))


def _liquid_balance_cents(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(balance_current_cents), 0) AS total_cents
          FROM accounts
         WHERE is_active = 1
           AND account_type IN ('checking', 'savings')
           AND COALESCE(balance_current_cents, 0) > 0
        """
    ).fetchone()
    return int(row["total_cents"] or 0)


def _income_totals_by_month(
    conn: sqlite3.Connection,
    *,
    before: date,
) -> dict[str, int]:
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
           AND t.date < ?
         GROUP BY substr(t.date, 1, 7)
        """,
        (before.isoformat(),),
    ).fetchall()
    return {str(row["month"]): int(row["total_cents"] or 0) for row in rows}


def _find_i2_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _I2Candidate | None:
    months = _month_keys(ctx.now.date(), _I2_LOOKBACK_MONTHS)
    rows = income_by_stream(conn, months=_I2_LOOKBACK_MONTHS, as_of=ctx.now.date())
    if not rows:
        return None

    totals_by_month: dict[str, int] = {month: 0 for month in months}
    totals_by_stream: dict[str, int] = {}
    for row in rows:
        month = str(row["month"])
        stream = str(row["stream"])
        cents = int(row["total_cents"])
        if month not in totals_by_month:
            continue
        totals_by_month[month] += cents
        totals_by_stream[stream] = totals_by_stream.get(stream, 0) + cents

    if any(total <= 0 for total in totals_by_month.values()) or not totals_by_stream:
        return None

    total_income_cents = sum(totals_by_stream.values())
    if total_income_cents <= 0:
        return None

    stream, top_income_cents = max(
        totals_by_stream.items(),
        key=lambda item: (item[1], item[0].casefold()),
    )
    top_share = Decimal(top_income_cents) / Decimal(total_income_cents)
    if top_share < _I2_MIN_TOP_SHARE:
        return None

    monthly_top_income_cents = _round_decimal_to_int(
        Decimal(top_income_cents) / Decimal(_I2_LOOKBACK_MONTHS)
    )
    if monthly_top_income_cents <= 0:
        return None

    liquid_balance_cents = _liquid_balance_cents(conn)
    weekly_income_at_risk = Decimal(monthly_top_income_cents) * Decimal(12) / Decimal(52)
    runway_weeks = (
        _round_decimal_to_int(Decimal(liquid_balance_cents) / weekly_income_at_risk)
        if weekly_income_at_risk > 0
        else 0
    )
    return _I2Candidate(
        stream=stream,
        top_income_cents=top_income_cents,
        total_income_cents=total_income_cents,
        top_share_pct=bounded_whole_percent(top_income_cents, total_income_cents),
        monthly_top_income_cents=monthly_top_income_cents,
        liquid_balance_cents=liquid_balance_cents,
        runway_weeks=max(runway_weeks, 0),
    )


def _i3_target_month(as_of: date) -> date | None:
    current_month = as_of.replace(day=1)
    for offset in range(1, 4):
        candidate = _add_months(current_month, offset)
        days_until_start = (candidate - as_of).days
        if _I3_MIN_LEAD_DAYS <= days_until_start <= _I3_MAX_LEAD_DAYS:
            return candidate
    return None


def _find_i3_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _I3Candidate | None:
    as_of = ctx.now.date()
    season_month_start = _i3_target_month(as_of)
    if season_month_start is None:
        return None

    totals_by_month = _income_totals_by_month(conn, before=as_of.replace(day=1))
    recent_months = _month_keys(as_of, _I2_LOOKBACK_MONTHS)
    recent_values = [int(totals_by_month.get(month, 0)) for month in recent_months]
    if sum(recent_values) <= 0:
        return None

    evidence_baselines: list[int] = []
    evidence_slow_months: list[int] = []
    evidence_years = sorted(
        {
            int(month_key[:4])
            for month_key in totals_by_month
            if int(month_key[:4]) < season_month_start.year
        }
    )
    for year in evidence_years:
        historical_month = date(year, season_month_start.month, 1)
        slow_month_cents = int(totals_by_month.get(_month_key(historical_month), 0))
        baseline_values = [
            int(totals_by_month.get(_month_key(_add_months(historical_month, -offset)), 0))
            for offset in range(_I3_BASELINE_MONTHS, 0, -1)
        ]
        if slow_month_cents <= 0 or any(value <= 0 for value in baseline_values):
            continue
        baseline_cents = _round_decimal_to_int(_avg(baseline_values))
        if baseline_cents <= 0:
            continue
        evidence_baselines.append(baseline_cents)
        evidence_slow_months.append(slow_month_cents)

    if len(evidence_slow_months) < _I3_MIN_HISTORY_YEARS:
        return None

    historical_baseline_cents = _round_decimal_to_int(_avg(evidence_baselines))
    historical_slow_month_cents = _round_decimal_to_int(_avg(evidence_slow_months))
    if historical_baseline_cents <= 0:
        return None

    dip = (
        Decimal(historical_baseline_cents - historical_slow_month_cents)
        / Decimal(historical_baseline_cents)
    )
    if dip < _I3_MIN_DIP:
        return None

    recent_avg_income_cents = _round_decimal_to_int(_avg(recent_values))
    suggested_extra_cents = max(
        _round_decimal_to_int(
            max(Decimal(historical_baseline_cents), Decimal(recent_avg_income_cents))
            - Decimal(historical_slow_month_cents)
        ),
        0,
    )
    if suggested_extra_cents <= 0:
        return None

    return _I3Candidate(
        season_month_start=season_month_start,
        season_month_label=season_month_start.strftime("%B"),
        weeks_until_start=_round_decimal_to_int(Decimal((season_month_start - as_of).days) / Decimal(7)),
        evidence_years=len(evidence_slow_months),
        historical_baseline_cents=historical_baseline_cents,
        historical_slow_month_cents=historical_slow_month_cents,
        recent_avg_income_cents=recent_avg_income_cents,
        dip_pct=_round_decimal_to_int(dip * Decimal(100)),
        suggested_extra_cents=suggested_extra_cents,
    )


def _billable_cluster_for_category(category_name: str | None) -> str | None:
    name = (category_name or "").casefold()
    if not name:
        return None
    for cluster_label, tokens in _I4_CATEGORY_CLUSTERS:
        if any(token in name for token in tokens):
            return cluster_label
    return None


def _parse_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def _find_i4_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _I4Candidate | None:
    as_of = ctx.now.date()
    start_date = as_of - timedelta(days=_I4_LOOKBACK_DAYS)
    rows = conn.execute(
        """
        SELECT t.id,
               t.date,
               ABS(t.amount_cents) AS amount_cents,
               c.name AS category_name
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND t.use_type = 'Business'
           AND t.project_id IS NULL
           AND t.date >= ?
           AND t.date <= ?
           AND NOT EXISTS (
                SELECT 1
                  FROM contractor_payments cp
                 WHERE cp.transaction_id = t.id
           )
         ORDER BY t.date ASC, t.id ASC
        """,
        (start_date.isoformat(), as_of.isoformat()),
    ).fetchall()
    if not rows:
        return None

    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        cluster_label = _billable_cluster_for_category(row["category_name"])
        if cluster_label is None:
            continue
        grouped.setdefault(cluster_label, []).append(row)

    candidates: list[_I4Candidate] = []
    for cluster_label, cluster_rows in grouped.items():
        total_cents = sum(abs(int(row["amount_cents"] or 0)) for row in cluster_rows)
        if len(cluster_rows) < _I4_MIN_CLUSTER_COUNT or total_cents < _I4_MIN_CLUSTER_CENTS:
            continue
        row_dates = [_parse_date(str(row["date"])) for row in cluster_rows]
        category_names = tuple(
            sorted(
                {str(row["category_name"]) for row in cluster_rows if row["category_name"]},
                key=str.casefold,
            )
        )
        candidates.append(
            _I4Candidate(
                cluster_label=cluster_label,
                transaction_ids=tuple(str(row["id"]) for row in cluster_rows),
                total_cents=total_cents,
                category_names=category_names,
                start_date=min(row_dates),
                end_date=max(row_dates),
            )
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda candidate: (
            candidate.total_cents,
            len(candidate.transaction_ids),
            candidate.end_date,
            candidate.cluster_label,
        ),
    )


def _positive_decimal(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _tracked_hours_from_notes(notes: str | None) -> Decimal | None:
    if not notes:
        return None
    try:
        payload = json.loads(notes)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    for key in _I5_TRACKED_HOURS_KEYS:
        if key not in payload:
            continue
        hours = _positive_decimal(payload[key])
        if hours is not None:
            return hours
    return None


def _has_i5_income_growth_interest(conn: sqlite3.Connection) -> bool:
    rows = conn.execute(
        """
        SELECT domain, strategy
          FROM user_strategy_preferences
        """
    ).fetchall()
    for row in rows:
        domain = str(row["domain"] or "").strip().lower().replace("-", "_")
        strategy = str(row["strategy"] or "").strip().lower().replace("-", "_")
        if domain in _I5_GROWTH_DOMAINS and strategy in _I5_GROWTH_STRATEGIES:
            return True
    return False


def _hourly_rate_label(rate_cents_per_hour: int) -> str:
    if rate_cents_per_hour % 100 == 0:
        return f"${rate_cents_per_hour // 100:,}/hr"
    return f"{_money(rate_cents_per_hour)}/hr"


def _hours_label(hours: Decimal) -> str:
    if hours == hours.to_integral_value():
        return f"{int(hours):,}"
    return f"{hours.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP):,}"


def _find_i5_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _I5Candidate | None:
    if not _has_i5_income_growth_interest(conn):
        return None

    months = _month_keys(ctx.now.date(), _I5_LOOKBACK_MONTHS)
    if not months:
        return None
    start_date = date.fromisoformat(f"{months[0]}-01")
    end_date = ctx.now.date().replace(day=1)
    rows = conn.execute(
        """
        SELECT substr(t.date, 1, 7) AS month,
               t.amount_cents,
               t.notes,
               COALESCE(p.name, c.name, 'Income') AS stream
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          LEFT JOIN projects p ON p.id = t.project_id
         WHERE c.is_income = 1
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents > 0
           AND t.date >= ?
           AND t.date < ?
         ORDER BY stream ASC, t.date ASC, t.id ASC
        """,
        (start_date.isoformat(), end_date.isoformat()),
    ).fetchall()
    if not rows:
        return None

    by_stream: dict[str, dict[str, tuple[int, Decimal]]] = {}
    for row in rows:
        hours = _tracked_hours_from_notes(row["notes"])
        if hours is None:
            continue
        stream = str(row["stream"] or "Income")
        month = str(row["month"])
        income_cents = int(row["amount_cents"] or 0)
        if income_cents <= 0 or month not in months:
            continue
        stream_months = by_stream.setdefault(stream, {})
        current_income, current_hours = stream_months.get(month, (0, Decimal("0")))
        stream_months[month] = (current_income + income_cents, current_hours + hours)

    candidates: list[_I5Candidate] = []
    for stream, monthly_values in by_stream.items():
        if any(month not in monthly_values for month in months):
            continue
        monthly_rates: list[int] = []
        total_income_cents = 0
        total_hours = Decimal("0")
        for month in months:
            income_cents, hours = monthly_values[month]
            if income_cents <= 0 or hours <= 0:
                break
            total_income_cents += income_cents
            total_hours += hours
            monthly_rates.append(_round_decimal_to_int(Decimal(income_cents) / hours))
        else:
            if total_income_cents <= 0 or total_hours <= 0 or not monthly_rates:
                continue
            rate_cents_per_hour = _round_decimal_to_int(Decimal(total_income_cents) / total_hours)
            max_rate = max(monthly_rates)
            min_rate = min(monthly_rates)
            if rate_cents_per_hour <= 0:
                continue
            rate_spread = Decimal(max_rate - min_rate) / Decimal(rate_cents_per_hour)
            if rate_spread > _I5_RATE_STABILITY_TOLERANCE:
                continue
            annualized_bump_cents = _round_decimal_to_int(
                Decimal(total_income_cents) * Decimal(12) / Decimal(_I5_LOOKBACK_MONTHS)
                * Decimal(_I5_TEST_BUMP_PCT)
                / Decimal(100)
            )
            candidates.append(
                _I5Candidate(
                    stream=stream,
                    months=_I5_LOOKBACK_MONTHS,
                    rate_cents_per_hour=rate_cents_per_hour,
                    bumped_rate_cents_per_hour=_round_decimal_to_int(
                        Decimal(rate_cents_per_hour)
                        * (Decimal(100 + _I5_TEST_BUMP_PCT) / Decimal(100))
                    ),
                    total_income_cents=total_income_cents,
                    total_hours=total_hours,
                    monthly_min_rate_cents_per_hour=min_rate,
                    monthly_max_rate_cents_per_hour=max_rate,
                    annualized_bump_cents=max(annualized_bump_cents, 0),
                )
            )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda candidate: (
            candidate.annualized_bump_cents,
            candidate.total_income_cents,
            candidate.stream.casefold(),
        ),
    )


@register_pattern(
    id="I-1",
    move=Move.WARN,
    tiers=(1, 4),
    tool="biz_forecast",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
)
def evaluate_i1_income_slowdown(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    rows = income_by_stream(conn, months=6, as_of=ctx.now.date())
    if not rows:
        return None

    months = _month_keys(ctx.now.date(), 6)
    by_stream: dict[str, dict[str, int]] = {}
    for row in rows:
        stream = str(row["stream"])
        by_stream.setdefault(stream, {})[str(row["month"])] = int(row["total_cents"])

    best: tuple[int, Intervention] | None = None
    for stream, totals_by_month in by_stream.items():
        series = [int(totals_by_month.get(month, 0)) for month in months]
        last_month = series[-1]
        prior_three_avg = _avg(series[-4:-1])
        recent_three_avg = _avg(series[-3:])
        prior_period_avg = _avg(series[-6:-3])

        trigger_type = None
        comparison_prior = Decimal("0")
        comparison_current = Decimal("0")
        pct_drop = Decimal("0")

        if prior_three_avg > 0 and Decimal(last_month) <= prior_three_avg * Decimal("0.85"):
            trigger_type = "mom"
            comparison_prior = prior_three_avg
            comparison_current = Decimal(last_month)
            pct_drop = (prior_three_avg - Decimal(last_month)) / prior_three_avg * Decimal("100")

        if prior_period_avg > 0 and recent_three_avg <= prior_period_avg * Decimal("0.80"):
            period_drop = (prior_period_avg - recent_three_avg) / prior_period_avg * Decimal("100")
            if trigger_type is None or period_drop > pct_drop:
                trigger_type = "quarter"
                comparison_prior = prior_period_avg
                comparison_current = recent_three_avg
                pct_drop = period_drop

        if trigger_type is None:
            continue

        drop_cents = max(
            int((comparison_prior - comparison_current).quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
            0,
        )
        intervention = Intervention(
            pattern_id="I-1",
            move=Move.WARN,
            tiers=(1, 4),
            priority=Priority.MEDIUM,
            headline=(
                f"Your {stream} invoiced {fmt_dollars(cents_to_dollars(int(comparison_prior)))}"
                f"/mo last quarter, {fmt_dollars(cents_to_dollars(int(comparison_current)))}"
                f"/mo this quarter. {int(pct_drop.quantize(Decimal('1'), rounding=ROUND_HALF_UP))}% drop."
            ),
            detail_bullets=(
                f"Triggered on {'3-month trend' if trigger_type == 'quarter' else 'recent monthly slowdown'}",
            ),
            tier4_ladder=None,
            tier4_is_fallback=False,
            action=InterventionAction(
                label="Run biz forecast",
                tool="biz_forecast",
                params={"streams": [stream]},
                build_stub=False,
            ),
            dollar_impact_cents=drop_cents,
            goal_link=None,
            log_id=None,
            fired_at=ctx.now,
            last_fired_at=ctx.recent_fires.get("I-1"),
        )
        candidate_key = (drop_cents, intervention)
        if best is None or candidate_key[0] > best[0]:
            best = candidate_key

    return best[1] if best is not None else None


@register_pattern(
    id="I-2",
    move=Move.DIAGNOSE,
    tiers=(1,),
    cooldown=timedelta(days=90),
    tool="income_mix",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
)
def evaluate_i2_income_concentration_risk(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_i2_candidate(conn, ctx)
    if candidate is None:
        return None

    return Intervention(
        pattern_id="I-2",
        move=Move.DIAGNOSE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"{candidate.top_share_pct}% of your income comes from {candidate.stream}. "
            f"If they paused, your current liquid cash covers roughly "
            f"{candidate.runway_weeks} weeks of that income. Worth diversifying when you can."
        ),
        detail_bullets=(
            f"Last 3 complete months income: {fmt_dollars(cents_to_dollars(candidate.total_income_cents))}.",
            f"{candidate.stream} income: {fmt_dollars(cents_to_dollars(candidate.top_income_cents))} "
            f"({fmt_dollars(cents_to_dollars(candidate.monthly_top_income_cents))}/mo).",
            f"Current checking/savings buffer: {fmt_dollars(cents_to_dollars(candidate.liquid_balance_cents))}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Review income mix",
            tool="income_mix",
            params={"months": _I2_LOOKBACK_MONTHS},
            build_stub=False,
        ),
        dollar_impact_cents=candidate.top_income_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("I-2"),
    )


@register_pattern(
    id="I-3",
    move=Move.WARN,
    tiers=(1, 4),
    cooldown=timedelta(days=90),
    tool="goal_set",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
)
def evaluate_i3_seasonal_income_alert(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_i3_candidate(conn, ctx)
    if candidate is None:
        return None

    deadline = candidate.season_month_start - timedelta(days=1)
    return Intervention(
        pattern_id="I-3",
        move=Move.WARN,
        tiers=(1, 4),
        priority=Priority.MEDIUM,
        headline=(
            f"Your {candidate.season_month_label} income is historically "
            f"{candidate.dip_pct}% lower. Slow season starts in "
            f"{candidate.weeks_until_start} weeks - recommend banking "
            f"{_money(candidate.suggested_extra_cents)} extra now."
        ),
        detail_bullets=(
            f"Historical pre-season baseline: {_money(candidate.historical_baseline_cents)}/mo "
            f"across {candidate.evidence_years} years.",
            f"Historical {candidate.season_month_label} income: "
            f"{_money(candidate.historical_slow_month_cents)}/mo.",
            f"Recent 3-month average income: {_money(candidate.recent_avg_income_cents)}/mo.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Create slow-season buffer goal",
            tool="goal_set",
            params={
                "name": f"{candidate.season_month_label} slow-season buffer",
                "metric": "liquid_cash",
                "target": cents_to_dollars(candidate.suggested_extra_cents),
                "direction": "up",
                "deadline": deadline.isoformat(),
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.suggested_extra_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("I-3"),
    )


@register_pattern(
    id="I-4",
    move=Move.PATTERN_CATCH,
    tiers=(1, 2),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    tool="bulk_tag_billable_expenses",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.TAX),
    cfp_steps=(
        CFPProcessStep.IDENTIFY,
        CFPProcessStep.ANALYZE,
        CFPProcessStep.IMPLEMENT,
    ),
)
def evaluate_i4_missed_billable_detection(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_i4_candidate(conn, ctx)
    if candidate is None:
        return None

    category_detail = (
        f"Categories: {', '.join(candidate.category_names)}."
        if candidate.category_names
        else "Categories matched plausible billable business expenses."
    )
    expense_label = _I4_CLUSTER_COPY.get(candidate.cluster_label, candidate.cluster_label)
    return Intervention(
        pattern_id="I-4",
        move=Move.PATTERN_CATCH,
        tiers=(1, 2),
        priority=Priority.MEDIUM,
        headline=(
            f"{len(candidate.transaction_ids)} business {expense_label} expenses have no "
            f"invoice/project link. {_money(candidate.total_cents)} worth. "
            "Worth checking if these were billable?"
        ),
        detail_bullets=(
            category_detail,
            f"Window: {candidate.start_date.isoformat()} through {candidate.end_date.isoformat()}.",
            "Contractor payment rows and already project-linked expenses are excluded.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Preview billable project tags",
            tool="bulk_tag_billable_expenses",
            params={
                "ids": list(candidate.transaction_ids),
                "project": _I4_REVIEW_PROJECT_NAME,
                "overwrite_existing_project": False,
                "dry_run": True,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.total_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("I-4"),
    )


@register_pattern(
    id="I-5",
    move=Move.COMPARE,
    tiers=(1,),
    priority=Priority.LOW,
    cooldown=timedelta(days=90),
    tool=None,
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT),
)
def evaluate_i5_pricing_signal(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_i5_candidate(conn, ctx)
    if candidate is None:
        return None

    return Intervention(
        pattern_id="I-5",
        move=Move.COMPARE,
        tiers=(1,),
        priority=Priority.LOW,
        headline=(
            f"Your effective rate works out to "
            f"{_hourly_rate_label(candidate.rate_cents_per_hour)} based on tracked time. "
            f"You haven't raised it in {candidate.months} months. "
            f"Worth testing a {_I5_TEST_BUMP_PCT}% bump on the next project?"
        ),
        detail_bullets=(
            f"Income stream: {candidate.stream}.",
            (
                f"Tracked sample: {_hours_label(candidate.total_hours)} hours and "
                f"{_money(candidate.total_income_cents)} over {candidate.months} complete months."
            ),
            (
                "Monthly effective rate range: "
                f"{_hourly_rate_label(candidate.monthly_min_rate_cents_per_hour)} to "
                f"{_hourly_rate_label(candidate.monthly_max_rate_cents_per_hour)}."
            ),
            f"A {_I5_TEST_BUMP_PCT}% test rate would be about "
            f"{_hourly_rate_label(candidate.bumped_rate_cents_per_hour)}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=None,
        dollar_impact_cents=candidate.annualized_bump_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("I-5"),
    )
