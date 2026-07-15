"""Spending-plan coaching intervention patterns.

Five registered patterns:

- ``chronic_monthly_deficit`` (DIAGNOSE / 14d) — entry signal. The 2 most-recent
  complete calendar months are net-negative AND no spending-plan artifact
  exists (or the artifact is > 90d old) AND the trailing-60d data-quality
  gate passes (gap_ratio < 0.20). Direct SQL aggregation against
  ``transactions`` with NULL-as-personal view semantics.
- ``creeping_overspend_no_plan`` (WARN / 14d override) — entry signal. The
  trailing-3-month expense average is >= 10% above the trailing-6-month
  average AND income is flat or down vs the 6-month average AND no plan AND
  the trailing-180d data-quality gate passes. WARN's default cooldown is 1d;
  this pattern overrides to 14d because the underlying signal is slow.
- ``monthly_variance_review`` (COACH / 30d) — recurring re-entry surface.
  Artifact present + today is within [6, 14]d after the prior calendar-month
  boundary + the prior calendar month has NOT yet been reviewed (per the
  artifact's ``last_review_recorded_at`` field) + the prior month's
  data-completeness ratio is < 0.05 (tighter than entry-signal patterns since
  variance compute is more sensitive). Otherwise wait.
- ``directional_variance_pattern`` (COACH / 30d) — maintenance escalation.
  Artifact present + ``variance_history`` >= 2 entries + at least one
  category shows same-direction variance >= 25% across both months +
  per-category 60d re-fire suppression via ``last_directional_flag_at``.
- ``cross_skill_commitment_drift`` (COACH / 30d) — cross-skill drift surface.
  Sibling artifact (debt-payoff and/or emergency-fund) plus this skill's
  artifact both present + drift > 10% on at least one side + the user has
  not already classified that exact drift tuple (within 5% on both sides)
  + > 60d since the prior classification (re-flap floor).

Artifact reads use lazy imports from ``finance_cli.mcp_server`` per
SKILL_CREATION_PLAYBOOK Gotcha #6 (avoid registry import cycle). The
data-quality helper lives at ``finance_cli.interventions.helpers``; the MCP
wrapper ``data_quality_gap_ratio`` in ``mcp_server`` is a thin wrapper around
the same function so the in-skill check and the engine never disagree on the
same DB.
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
from .helpers import data_quality_gap_ratio
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
_DATA_QUALITY_GATE = Decimal("0.20")  # entry-signal patterns pass when < 0.20
_VARIANCE_REVIEW_GATE = Decimal("0.05")  # tighter — variance compute is more sensitive
_REVIEW_WINDOW_MIN_DAYS = 6
_REVIEW_WINDOW_MAX_DAYS = 14
_DIRECTIONAL_THRESHOLD_PCT = Decimal("25")
_DIRECTIONAL_REFIRE_FLOOR_DAYS = 60
_DRIFT_THRESHOLD_PCT = Decimal("10")
_DRIFT_NOISE_GATE_CENTS = 5_000  # $50 — both values must be at least this for drift to fire
_DRIFT_TUPLE_TOLERANCE = Decimal("0.05")  # 5% match window for "same drift" suppression
_DRIFT_REFIRE_FLOOR_DAYS = 60
_PLAN_STALENESS_DAYS = 90
_CREEPING_OVERSPEND_PCT = Decimal("10")  # 3mo avg >= 10% above 6mo avg
_CREEPING_INCOME_FLAT_PCT = Decimal("5")  # 3mo income within +/- 5% of 6mo


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _money(cents: int) -> str:
    return fmt_dollars(cents_to_dollars(int(cents)))


def _pct_text(value: Decimal | float) -> str:
    quantized = Decimal(str(value)).quantize(_DOLLAR, rounding=ROUND_HALF_UP)
    return str(int(quantized))


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def _month_bounds(month_key: str) -> tuple[date, date]:
    """Return (first_day, last_day) of a YYYY-MM month."""
    year, month = (int(part) for part in month_key.split("-", 1))
    start = date(year, month, 1)
    if month == 12:
        next_start = date(year + 1, 1, 1)
    else:
        next_start = date(year, month + 1, 1)
    return start, next_start - timedelta(days=1)


def _previous_month_keys(as_of: date, n: int) -> list[str]:
    """Return the N most-recent complete calendar months in YYYY-MM (oldest first)."""
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    cursor = end_of_last_complete_month.replace(day=1)
    values: list[str] = []
    for _ in range(int(n)):
        values.append(cursor.strftime("%Y-%m"))
        cursor = (cursor - timedelta(days=1)).replace(day=1)
    values.reverse()
    return values


def _net_for_month(conn: sqlite3.Connection, month_key: str) -> int | None:
    """Return personal-view net cents for a month, or None if no transactions exist."""
    start, end = _month_bounds(month_key)
    row = conn.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN c.is_income = 1 AND t.amount_cents > 0
                             THEN t.amount_cents ELSE 0 END), 0) AS income_cents,
          COALESCE(SUM(CASE WHEN c.is_income = 0 AND t.is_payment = 0 AND t.amount_cents < 0
                             THEN ABS(t.amount_cents) ELSE 0 END), 0) AS expense_cents,
          COUNT(*) AS tx_count
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND (t.use_type = 'Personal' OR t.use_type IS NULL)
           AND t.date >= ?
           AND t.date <= ?
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchone()
    if row is None or int(row["tx_count"] or 0) == 0:
        return None
    return int(row["income_cents"] or 0) - int(row["expense_cents"] or 0)


def _avg_expense_for_window(
    conn: sqlite3.Connection,
    *,
    start_date: date,
    end_date: date,
    months: int,
) -> int:
    """Average monthly personal expense over an inclusive [start, end] window."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(CASE WHEN c.is_income = 0 AND t.is_payment = 0 AND t.amount_cents < 0
                                  THEN ABS(t.amount_cents) ELSE 0 END), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND (t.use_type = 'Personal' OR t.use_type IS NULL)
           AND t.date >= ?
           AND t.date <= ?
        """,
        (start_date.isoformat(), end_date.isoformat()),
    ).fetchone()
    total = int(row["total_cents"] or 0) if row is not None else 0
    if months <= 0:
        return 0
    return int((Decimal(total) / Decimal(months)).quantize(_DOLLAR, rounding=ROUND_HALF_UP))


def _avg_income_for_window(
    conn: sqlite3.Connection,
    *,
    start_date: date,
    end_date: date,
    months: int,
) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(CASE WHEN c.is_income = 1 AND t.amount_cents > 0
                                  THEN t.amount_cents ELSE 0 END), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND (t.use_type = 'Personal' OR t.use_type IS NULL)
           AND t.date >= ?
           AND t.date <= ?
        """,
        (start_date.isoformat(), end_date.isoformat()),
    ).fetchone()
    total = int(row["total_cents"] or 0) if row is not None else 0
    if months <= 0:
        return 0
    return int((Decimal(total) / Decimal(months)).quantize(_DOLLAR, rounding=ROUND_HALF_UP))


def _latest_spending_plan_artifact() -> tuple[Path, dict[str, Any]] | None:
    """Return (path, parsed_payload) for the most-recent spending-plan artifact.

    Lazy-imports from ``mcp_server`` to avoid registry init cycle.
    """
    from ..mcp_server import (
        _latest_artifact_path,
        _parse_spending_plan_artifact,
        _spending_plan_artifact_dir,
    )

    artifact_dir = _spending_plan_artifact_dir()
    if not artifact_dir.exists():
        return None
    latest = _latest_artifact_path(artifact_dir)
    if latest is None:
        return None
    payload = _parse_spending_plan_artifact(latest.read_text(encoding="utf-8"))
    if not payload:
        return None
    return latest, payload


def _latest_debt_payoff_artifact() -> tuple[Path, dict[str, Any]] | None:
    from ..mcp_server import (
        _latest_artifact_path,
        _parse_debt_payoff_artifact,
        _debt_payoff_artifact_dir,
    )

    artifact_dir = _debt_payoff_artifact_dir()
    if not artifact_dir.exists():
        return None
    latest = _latest_artifact_path(artifact_dir)
    if latest is None:
        return None
    payload = _parse_debt_payoff_artifact(latest.read_text(encoding="utf-8"))
    if not payload:
        return None
    return latest, payload


def _latest_emergency_fund_artifact() -> tuple[Path, dict[str, Any]] | None:
    from ..mcp_server import (
        _latest_artifact_path,
        _parse_emergency_fund_artifact,
        _emergency_fund_artifact_dir,
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


def _plan_present_and_fresh(payload: dict[str, Any] | None, *, as_of: date) -> bool:
    """A plan is considered 'present' when the artifact exists and is < 90d old."""
    if not payload:
        return False
    generated_on = _parse_iso_date(payload.get("generated_at"))
    if generated_on is None:
        return False
    return (as_of - generated_on).days <= _PLAN_STALENESS_DAYS


# ---------------------------------------------------------------------------
# chronic_monthly_deficit
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _DeficitSignal:
    months: tuple[str, str]
    nets: tuple[int, int]


def _chronic_deficit_signal(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> _DeficitSignal | None:
    months = _previous_month_keys(ctx.now.date(), 2)
    if len(months) != 2:
        return None
    nets: list[int] = []
    for month_key in months:
        net = _net_for_month(conn, month_key)
        if net is None or net >= 0:
            return None
        nets.append(net)
    return _DeficitSignal(months=(months[0], months[1]), nets=(nets[0], nets[1]))


@register_pattern(
    id="chronic_monthly_deficit",
    move=Move.DIAGNOSE,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=14),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE,),
)
def evaluate_chronic_monthly_deficit(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    artifact = _latest_spending_plan_artifact()
    payload = artifact[1] if artifact else None
    if _plan_present_and_fresh(payload, as_of=ctx.now.date()):
        return None
    signal = _chronic_deficit_signal(conn, ctx)
    if signal is None:
        return None
    # Data-quality suppression — 60d window covering the two months in scope.
    window_end = ctx.now.date()
    window_start = window_end - timedelta(days=60)
    quality = data_quality_gap_ratio(
        conn,
        view="personal",
        date_from=window_start.isoformat(),
        date_to=window_end.isoformat(),
    )
    if Decimal(str(quality["gap_ratio"])) >= _DATA_QUALITY_GATE:
        return None
    total_deficit = abs(signal.nets[0]) + abs(signal.nets[1])
    return Intervention(
        pattern_id="chronic_monthly_deficit",
        move=Move.DIAGNOSE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Net cash flow has been negative two months running ({signal.months[0]}: "
            f"{_money(signal.nets[0])}, {signal.months[1]}: {_money(signal.nets[1])}) "
            "and there is no spending plan to land against."
        ),
        detail_bullets=(
            f"{signal.months[0]} net: {_money(signal.nets[0])}",
            f"{signal.months[1]} net: {_money(signal.nets[1])}",
            f"Categorization gap (60d window): {quality['gap_ratio']:.0%}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Walk through spending-plan coaching",
            tool="activate_skill",
            params={"name": "coach_spending_plan"},
            build_stub=False,
        ),
        dollar_impact_cents=total_deficit,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("chronic_monthly_deficit"),
    )


# ---------------------------------------------------------------------------
# creeping_overspend_no_plan
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _CreepingOverspendSignal:
    avg_3mo_expense_cents: int
    avg_6mo_expense_cents: int
    avg_3mo_income_cents: int
    avg_6mo_income_cents: int
    expense_creep_pct: Decimal


def _creeping_overspend_signal(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> _CreepingOverspendSignal | None:
    months_3 = _previous_month_keys(ctx.now.date(), 3)
    months_6 = _previous_month_keys(ctx.now.date(), 6)
    if len(months_3) != 3 or len(months_6) != 6:
        return None
    start_3, _ = _month_bounds(months_3[0])
    _, end_3 = _month_bounds(months_3[-1])
    start_6, _ = _month_bounds(months_6[0])
    _, end_6 = _month_bounds(months_6[-1])

    avg_3mo_expense = _avg_expense_for_window(
        conn, start_date=start_3, end_date=end_3, months=3
    )
    avg_6mo_expense = _avg_expense_for_window(
        conn, start_date=start_6, end_date=end_6, months=6
    )
    if avg_6mo_expense <= 0:
        return None
    creep_pct = (
        Decimal(avg_3mo_expense - avg_6mo_expense)
        / Decimal(avg_6mo_expense)
        * Decimal("100")
    )
    if creep_pct < _CREEPING_OVERSPEND_PCT:
        return None

    avg_3mo_income = _avg_income_for_window(
        conn, start_date=start_3, end_date=end_3, months=3
    )
    avg_6mo_income = _avg_income_for_window(
        conn, start_date=start_6, end_date=end_6, months=6
    )
    if avg_6mo_income > 0:
        income_change_pct = (
            Decimal(avg_3mo_income - avg_6mo_income)
            / Decimal(avg_6mo_income)
            * Decimal("100")
        )
        if income_change_pct > _CREEPING_INCOME_FLAT_PCT:
            # Income grew faster than the floor; the creep is funded by income, not signal.
            return None
    return _CreepingOverspendSignal(
        avg_3mo_expense_cents=avg_3mo_expense,
        avg_6mo_expense_cents=avg_6mo_expense,
        avg_3mo_income_cents=avg_3mo_income,
        avg_6mo_income_cents=avg_6mo_income,
        expense_creep_pct=creep_pct,
    )


@register_pattern(
    id="creeping_overspend_no_plan",
    move=Move.WARN,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=14),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.UNDERSTAND,),
)
def evaluate_creeping_overspend_no_plan(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    artifact = _latest_spending_plan_artifact()
    payload = artifact[1] if artifact else None
    if _plan_present_and_fresh(payload, as_of=ctx.now.date()):
        return None
    signal = _creeping_overspend_signal(conn, ctx)
    if signal is None:
        return None
    # Data-quality gate — 180d window covering the 6-month baseline.
    window_end = ctx.now.date()
    window_start = window_end - timedelta(days=180)
    quality = data_quality_gap_ratio(
        conn,
        view="personal",
        date_from=window_start.isoformat(),
        date_to=window_end.isoformat(),
    )
    if Decimal(str(quality["gap_ratio"])) >= _DATA_QUALITY_GATE:
        return None
    gap_cents = max(signal.avg_3mo_expense_cents - signal.avg_6mo_expense_cents, 0)
    return Intervention(
        pattern_id="creeping_overspend_no_plan",
        move=Move.WARN,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Trailing-3-month expenses are running {_pct_text(signal.expense_creep_pct)}% "
            f"above the 6-month baseline ({_money(signal.avg_6mo_expense_cents)} → "
            f"{_money(signal.avg_3mo_expense_cents)}/mo) and income is flat. No spending plan on file."
        ),
        detail_bullets=(
            f"3-month avg expense: {_money(signal.avg_3mo_expense_cents)}/mo",
            f"6-month avg expense: {_money(signal.avg_6mo_expense_cents)}/mo",
            f"Monthly drift: +{_money(gap_cents)}",
            f"Categorization gap (180d window): {quality['gap_ratio']:.0%}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Walk through spending-plan coaching",
            tool="activate_skill",
            params={"name": "coach_spending_plan"},
            build_stub=False,
        ),
        dollar_impact_cents=gap_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("creeping_overspend_no_plan"),
    )


# ---------------------------------------------------------------------------
# monthly_variance_review
# ---------------------------------------------------------------------------


@register_pattern(
    id="monthly_variance_review",
    move=Move.COACH,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.MONITOR,),
)
def evaluate_monthly_variance_review(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    artifact = _latest_spending_plan_artifact()
    if artifact is None:
        return None
    artifact_path, payload = artifact

    # Window: today must be [6, 14] days after the prior month's last day.
    today = ctx.now.date()
    prior_month_end = today.replace(day=1) - timedelta(days=1)
    days_after = (today - prior_month_end).days
    if not (_REVIEW_WINDOW_MIN_DAYS <= days_after <= _REVIEW_WINDOW_MAX_DAYS):
        return None

    # Has the prior month already been recorded in variance_history?
    last_review = _parse_iso_date(payload.get("last_review_recorded_at"))
    prior_month_start = prior_month_end.replace(day=1)
    if last_review is not None and last_review >= prior_month_start:
        return None

    # Data-completeness gate on the prior month — tighter than entry-signal patterns.
    quality = data_quality_gap_ratio(
        conn,
        view="personal",
        date_from=prior_month_start.isoformat(),
        date_to=prior_month_end.isoformat(),
    )
    if Decimal(str(quality["gap_ratio"])) >= _VARIANCE_REVIEW_GATE:
        return None

    return Intervention(
        pattern_id="monthly_variance_review",
        move=Move.COACH,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Time to review last month's plan-vs-actual — "
            f"{prior_month_start.strftime('%B %Y')} data has settled."
        ),
        detail_bullets=(
            f"Prior month: {prior_month_start.strftime('%Y-%m')}",
            f"Data freshness: gap {quality['gap_ratio']:.0%} < 5% target",
            f"Artifact: {artifact_path.name}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Run monthly spending-plan review",
            tool="activate_skill",
            params={"name": "coach_spending_plan"},
            build_stub=False,
        ),
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("monthly_variance_review"),
    )


# ---------------------------------------------------------------------------
# directional_variance_pattern
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _DirectionalSignal:
    category_id: str
    category_name: str
    direction: str  # "over" | "under"
    months: tuple[str, str]
    pcts: tuple[Decimal, Decimal]


def _directional_signal_from_history(
    payload: dict[str, Any],
    *,
    as_of: date,
) -> _DirectionalSignal | None:
    history = payload.get("variance_history")
    if not isinstance(history, list) or len(history) < 2:
        return None
    # Take the two most recent entries.
    recent_two = history[-2:]
    by_category: dict[str, list[tuple[str, Decimal]]] = {}
    category_names: dict[str, str] = {}
    for entry in recent_two:
        if not isinstance(entry, dict):
            return None
        month = str(entry.get("month") or "")
        per_category = entry.get("per_category")
        if not isinstance(per_category, list):
            continue
        for line in per_category:
            if not isinstance(line, dict):
                continue
            category_id = str(line.get("category_id") or line.get("category_name") or "")
            if not category_id:
                continue
            category_names.setdefault(
                category_id, str(line.get("category_name") or category_id)
            )
            pct_raw = line.get("variance_pct")
            if pct_raw is None:
                continue
            try:
                pct = Decimal(str(pct_raw))
            except (TypeError, ValueError, ArithmeticError):
                continue
            by_category.setdefault(category_id, []).append((month, pct))

    last_flags = payload.get("last_directional_flag_at") or {}
    if not isinstance(last_flags, dict):
        last_flags = {}

    for category_id, entries in by_category.items():
        if len(entries) < 2:
            continue
        pct_a, pct_b = entries[-2][1], entries[-1][1]
        # Same-direction + magnitude threshold on both months.
        if not (
            (pct_a >= _DIRECTIONAL_THRESHOLD_PCT and pct_b >= _DIRECTIONAL_THRESHOLD_PCT)
            or (pct_a <= -_DIRECTIONAL_THRESHOLD_PCT and pct_b <= -_DIRECTIONAL_THRESHOLD_PCT)
        ):
            continue
        # Per-category 60d re-fire suppression.
        last_flag = _parse_iso_date(last_flags.get(category_id))
        if last_flag is not None and (as_of - last_flag).days < _DIRECTIONAL_REFIRE_FLOOR_DAYS:
            continue
        direction = "over" if pct_a > 0 else "under"
        return _DirectionalSignal(
            category_id=category_id,
            category_name=category_names.get(category_id, category_id),
            direction=direction,
            months=(entries[-2][0], entries[-1][0]),
            pcts=(pct_a, pct_b),
        )
    return None


@register_pattern(
    id="directional_variance_pattern",
    move=Move.COACH,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.MONITOR,),
)
def evaluate_directional_variance_pattern(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    del conn  # Pure artifact-driven; no DB read needed.
    artifact = _latest_spending_plan_artifact()
    if artifact is None:
        return None
    artifact_path, payload = artifact
    signal = _directional_signal_from_history(payload, as_of=ctx.now.date())
    if signal is None:
        return None
    direction_word = "over-budget" if signal.direction == "over" else "under-budget"
    return Intervention(
        pattern_id="directional_variance_pattern",
        move=Move.COACH,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"{signal.category_name} is running {direction_word} two months in a row — "
            "this is the signal to re-baseline that line, not patch it."
        ),
        detail_bullets=(
            f"{signal.months[0]}: {signal.pcts[0]:+.0f}%",
            f"{signal.months[1]}: {signal.pcts[1]:+.0f}%",
            f"Artifact: {artifact_path.name}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Re-baseline this category in coach_spending_plan",
            tool="activate_skill",
            params={"name": "coach_spending_plan"},
            build_stub=False,
        ),
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("directional_variance_pattern"),
    )


# ---------------------------------------------------------------------------
# cross_skill_commitment_drift
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _DriftSignal:
    side: str  # "debt_paydown" | "emergency_fund"
    sibling_value_cents: int
    this_plan_value_cents: int
    drift_pct: Decimal


_DEBT_PAYDOWN_BY_CATEGORY_TYPES = frozenset({"debt_paydown", "debt"})
_EMERGENCY_FUND_BY_CATEGORY_TYPES = frozenset({"emergency_fund", "savings_transfer", "savings"})


def _allocation_cents(payload: dict[str, Any], side_key: str) -> int | None:
    """Return monthly_cents allocated to a cross-skill side.

    Reads the canonical top-level shape ``allocations.<side>.monthly_cents``
    first (per the playbook). When the agent inlines the side in
    ``by_category`` instead — a common drafting variation — falls back to
    summing any ``by_category`` entries whose ``type`` matches the side's
    synonym set. Returns None when no allocation is recorded under either
    shape (callers treat that as "no signal", which suppresses the drift
    intervention rather than firing a false positive).
    """
    allocations = payload.get("allocations")
    if not isinstance(allocations, dict):
        return None

    # Preferred: top-level side dict with monthly_cents.
    side = allocations.get(side_key)
    if isinstance(side, dict):
        raw = side.get("monthly_cents")
        if raw is not None:
            try:
                return int(raw)
            except (TypeError, ValueError):
                pass  # Fall through to by_category scan.

    # Fallback: scan by_category for matching `type` entries and sum them.
    synonyms = (
        _DEBT_PAYDOWN_BY_CATEGORY_TYPES
        if side_key == "debt_paydown"
        else _EMERGENCY_FUND_BY_CATEGORY_TYPES
    )
    by_category = allocations.get("by_category")
    if not isinstance(by_category, list):
        return None
    total = 0
    matched = False
    for entry in by_category:
        if not isinstance(entry, dict):
            continue
        entry_type = str(entry.get("type") or "").strip().lower()
        if entry_type not in synonyms:
            continue
        raw = entry.get("monthly_cents")
        if raw is None:
            continue
        try:
            total += int(raw)
            matched = True
        except (TypeError, ValueError):
            continue
    return total if matched else None


def _drift_already_classified(
    payload: dict[str, Any],
    side_key: str,
    *,
    sibling_value_cents: int,
    this_plan_value_cents: int,
    as_of: date,
) -> bool:
    """Suppress when (a) classified tuple within 5% of current values OR
    (b) > 60d since the prior classification (re-flap floor).
    """
    last_drift = payload.get("last_drift_classified")
    if not isinstance(last_drift, dict):
        return False
    entry = last_drift.get(side_key)
    if not isinstance(entry, dict):
        return False
    classified_at = _parse_iso_date(entry.get("classified_at"))
    sibling_classified = _as_int(entry.get("sibling_value_cents"))
    this_classified = _as_int(entry.get("this_plan_value_cents"))

    # 60d re-flap floor — applies even to materially-different drifts.
    if classified_at is not None and (as_of - classified_at).days < _DRIFT_REFIRE_FLOOR_DAYS:
        return True

    # Same-drift suppression: both values within tolerance of the classified tuple.
    def _within_tolerance(current: int, classified: int) -> bool:
        if classified <= 0:
            return False
        delta = Decimal(abs(current - classified)) / Decimal(abs(classified))
        return delta <= _DRIFT_TUPLE_TOLERANCE

    if sibling_classified > 0 and this_classified > 0:
        if _within_tolerance(sibling_value_cents, sibling_classified) and _within_tolerance(
            this_plan_value_cents, this_classified
        ):
            return True
    return False


def _evaluate_side(
    payload: dict[str, Any],
    sibling_payload: dict[str, Any] | None,
    *,
    side_key: str,
    sibling_value_key: str,
    as_of: date,
) -> _DriftSignal | None:
    if sibling_payload is None:
        return None
    sibling_value_raw = sibling_payload.get(sibling_value_key)
    try:
        sibling_value_cents = int(sibling_value_raw) if sibling_value_raw is not None else 0
    except (TypeError, ValueError):
        return None
    this_value_cents = _allocation_cents(payload, side_key)
    if this_value_cents is None:
        return None

    # Both-side noise gate.
    if sibling_value_cents < _DRIFT_NOISE_GATE_CENTS or this_value_cents < _DRIFT_NOISE_GATE_CENTS:
        return None
    max_value = max(abs(sibling_value_cents), abs(this_value_cents))
    if max_value <= 0:
        return None
    drift_pct = Decimal(abs(sibling_value_cents - this_value_cents)) / Decimal(max_value) * Decimal("100")
    if drift_pct <= _DRIFT_THRESHOLD_PCT:
        return None
    if _drift_already_classified(
        payload,
        side_key,
        sibling_value_cents=sibling_value_cents,
        this_plan_value_cents=this_value_cents,
        as_of=as_of,
    ):
        return None
    return _DriftSignal(
        side=side_key,
        sibling_value_cents=sibling_value_cents,
        this_plan_value_cents=this_value_cents,
        drift_pct=drift_pct,
    )


@register_pattern(
    id="cross_skill_commitment_drift",
    move=Move.COACH,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.MONITOR,),
)
def evaluate_cross_skill_commitment_drift(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    del conn  # Artifact-driven; no DB read.
    artifact = _latest_spending_plan_artifact()
    if artifact is None:
        return None
    artifact_path, payload = artifact

    debt = _latest_debt_payoff_artifact()
    debt_payload = debt[1] if debt else None
    efund = _latest_emergency_fund_artifact()
    efund_payload = efund[1] if efund else None

    debt_signal = _evaluate_side(
        payload,
        debt_payload,
        side_key="debt_paydown",
        sibling_value_key="monthly_commitment_cents",
        as_of=ctx.now.date(),
    )
    efund_signal = _evaluate_side(
        payload,
        efund_payload,
        side_key="emergency_fund",
        sibling_value_key="monthly_commitment_cents",
        as_of=ctx.now.date(),
    )

    # Pick the larger drift if both surface; otherwise the one that did.
    candidate = None
    if debt_signal and efund_signal:
        candidate = debt_signal if debt_signal.drift_pct >= efund_signal.drift_pct else efund_signal
    else:
        candidate = debt_signal or efund_signal
    if candidate is None:
        return None

    side_label = "debt-payoff" if candidate.side == "debt_paydown" else "emergency-fund"
    return Intervention(
        pattern_id="cross_skill_commitment_drift",
        move=Move.COACH,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Your {side_label} commitment ({_money(candidate.sibling_value_cents)}/mo) and the "
            f"spending-plan allocation ({_money(candidate.this_plan_value_cents)}/mo) have drifted "
            f"~{_pct_text(candidate.drift_pct)}% apart."
        ),
        detail_bullets=(
            f"{side_label} sibling commitment: {_money(candidate.sibling_value_cents)}/mo",
            f"This plan's allocation: {_money(candidate.this_plan_value_cents)}/mo",
            f"Drift: {_pct_text(candidate.drift_pct)}%",
            f"Artifact: {artifact_path.name}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Reconcile in coach_spending_plan Phase 5",
            tool="activate_skill",
            params={"name": "coach_spending_plan"},
            build_stub=False,
        ),
        dollar_impact_cents=abs(
            candidate.sibling_value_cents - candidate.this_plan_value_cents
        ),
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("cross_skill_commitment_drift"),
    )
