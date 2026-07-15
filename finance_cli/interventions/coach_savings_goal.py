"""Savings-goal coaching intervention patterns.

Three registered patterns mirror the debt-payoff + emergency-fund
structure: one entry-signal evaluator that offers the skill when surplus
exists and the e-fund is already in place, and two artifact-driven
maintenance evaluators that fire only after the user has saved a Savings
Goal Plan artifact (milestone celebration + stall detection).

Helpers reused from ``coach_emergency_fund`` (sibling intervention
module) for liquid-balance + cash-flow + essential-monthly computations.
Artifact reads use lazy imports from ``finance_cli.mcp_server`` per the
SKILL_CREATION_PLAYBOOK Gotcha #6 (avoid registry import cycle).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import sqlite3
from typing import Any

from ..commands.common import fmt_dollars
from ..models import cents_to_dollars
from .coach_emergency_fund import (
    _account_sum_as_of_cents,
    _essential_monthly_cents,
    _liquid_balance_as_of_cents,
    _net_flow_cents,
    _parse_iso_date,
)
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
_CASH_FLOW_SURPLUS_90D_CENTS = 150_000  # $1,500 over 90 days — matches e-fund's threshold
_STALL_WINDOW_DAYS = 60
_STALL_THRESHOLD_FRACTION = Decimal("0.5")  # < 50% of expected progress = stall


def _money(cents: int) -> str:
    return fmt_dollars(cents_to_dollars(int(cents)))


def _ratio_text(value: Decimal) -> str:
    quantized = value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    return format(quantized, "f")


def _latest_savings_goal_artifact() -> tuple[Path, dict[str, Any]] | None:
    """Return (path, parsed_payload) for the most-recent savings-goal artifact.

    Uses ``finance_cli.mcp_server`` helpers directly so the artifact parser
    and directory resolver remain the single source of truth. Lazy-imported
    inside the function to avoid registry import cycle.
    """
    from ..mcp_server import (
        _latest_artifact_path,
        _parse_savings_goal_artifact,
        _savings_goal_artifact_dir,
    )

    artifact_dir = _savings_goal_artifact_dir()
    if not artifact_dir.exists():
        return None
    latest = _latest_artifact_path(artifact_dir)
    if latest is None:
        return None
    payload = _parse_savings_goal_artifact(latest.read_text(encoding="utf-8"))
    if not payload:
        return None
    return latest, payload


def _account_ids_from_artifact(payload: dict[str, Any]) -> list[str]:
    """Extract account_ids from the artifact's ``account_ids_in_goal`` field.

    Accepts both flat string ids and dict entries with ``account_id`` key.
    """
    raw = payload.get("account_ids_in_goal") or []
    if not isinstance(raw, list):
        return []
    ids: list[str] = []
    for item in raw:
        if isinstance(item, dict):
            acct_id = item.get("account_id")
            if acct_id is None:
                continue
            ids.append(str(acct_id))
        elif item is not None:
            ids.append(str(item))
    return ids


# ---------------------------------------------------------------------------
# Entry signal: surplus + e-fund met + no active savings-goal engagement
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _SurplusEntryCheck:
    surplus_cents: int
    liquid_balance_cents: int
    essential_monthly_cents: int
    # Raw ratio — keep unrounded so the disjointness gate against e-fund's
    # `cash_flow_surplus_no_savings` (which uses an unrounded
    # `Decimal(liquid_end_cents) / Decimal(essential)`) is exact and there's
    # no narrow [2.995, 3.0) band where both patterns can fire.
    efund_coverage_months: Decimal


def _surplus_entry_check(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> _SurplusEntryCheck | None:
    end_date = ctx.now.date()
    start_date = end_date - timedelta(days=90)
    surplus = _net_flow_cents(conn, start_date=start_date, end_date=end_date)
    if surplus < _CASH_FLOW_SURPLUS_90D_CENTS:
        return None
    # Thread ctx.rules_path so user-customized essential_categories are
    # honored — this matches `liquidity_below_3_months` and keeps the
    # denominator consistent across the two reciprocal surplus gates so
    # disjointness with e-fund's pattern holds even under custom rules.
    essential = _essential_monthly_cents(
        conn,
        as_of=end_date,
        rules_path=ctx.rules_path,
    )
    if essential <= 0:
        return None
    # Plan §"Intervention Registry Entries" mandates the MAX(snapshot_date <= ?)
    # pattern for coverage so disjointness with the e-fund surplus pattern is
    # snapshot-consistent. Suppress when no snapshots are available — without
    # an as-of measure we can't assert coverage.
    liquid, snapshots = _liquid_balance_as_of_cents(conn, as_of=end_date)
    if snapshots == 0:
        return None
    coverage = Decimal(liquid) / Decimal(essential)
    return _SurplusEntryCheck(
        surplus_cents=surplus,
        liquid_balance_cents=liquid,
        essential_monthly_cents=essential,
        efund_coverage_months=coverage,
    )


def _active_savings_goal_engagement(conn: sqlite3.Connection) -> bool:
    """True when an active liquid_cash goal exists whose name matches the
    latest savings-goal artifact's ``goal_name``.

    This is the anti-duplicate gate: if a savings-goal skill engagement is
    in-flight (artifact saved + goals row active under the same name), the
    surplus entry-signal must not fire and re-prompt the user to engage a
    skill they're already in.
    """
    artifact = _latest_savings_goal_artifact()
    if artifact is None:
        return False
    _, payload = artifact
    goal_name = payload.get("goal_name")
    if not goal_name:
        return False
    row = conn.execute(
        """
        SELECT 1
          FROM goals
         WHERE name = ?
           AND is_active = 1
           AND metric = 'liquid_cash'
         LIMIT 1
        """,
        (str(goal_name),),
    ).fetchone()
    return row is not None


@register_pattern(
    id="cash_flow_surplus_no_savings_goal",
    move=Move.DIAGNOSE,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.DEVELOP,),
)
def evaluate_cash_flow_surplus_no_savings_goal(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    check = _surplus_entry_check(conn, ctx)
    if check is None:
        return None
    if check.efund_coverage_months < _LIQUIDITY_TARGET_MONTHS:
        # E-fund not yet built; e-fund's `cash_flow_surplus_no_savings` owns
        # this surplus window. Don't double-fire.
        return None
    if _active_savings_goal_engagement(conn):
        return None
    return Intervention(
        pattern_id="cash_flow_surplus_no_savings_goal",
        move=Move.DIAGNOSE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"~{_money(check.surplus_cents)} of cash-flow surplus over the last 90 days "
            f"with {_ratio_text(check.efund_coverage_months)} months of essentials already covered — "
            "this is the window to point surplus at a specific named goal."
        ),
        detail_bullets=(
            f"90-day net cash flow: {_money(check.surplus_cents)}",
            f"Current liquid balance: {_money(check.liquid_balance_cents)}",
            f"Essential monthly expenses: {_money(check.essential_monthly_cents)}/mo (3-month average)",
            f"Emergency-fund coverage: {_ratio_text(check.efund_coverage_months)} months "
            "(>= 3-month baseline)",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Walk through savings-goal coaching",
            tool="activate_skill",
            params={"name": "coach_savings_goal"},
            build_stub=False,
        ),
        dollar_impact_cents=check.surplus_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("cash_flow_surplus_no_savings_goal"),
    )


# ---------------------------------------------------------------------------
# Maintenance: stall detection — artifact-driven, balance-window evaluator
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _StallCheck:
    start_balance_cents: int
    end_balance_cents: int
    monthly_commitment_cents: int
    expected_progress_cents: int
    actual_progress_cents: int


def _stall_check(
    conn: sqlite3.Connection,
    payload: dict[str, Any],
    *,
    as_of: date,
) -> _StallCheck | None:
    account_ids = _account_ids_from_artifact(payload)
    if not account_ids:
        return None
    try:
        commitment = int(payload.get("monthly_commitment_cents") or 0)
    except (TypeError, ValueError):
        commitment = 0
    if commitment <= 0:
        return None
    end_date = as_of
    start_date = end_date - timedelta(days=_STALL_WINDOW_DAYS)
    # Don't fire if the artifact was generated mid-window — we don't have a
    # full 60-day commitment runway to measure against.
    generated_on = _parse_iso_date(payload.get("generated_at"))
    if generated_on is not None and generated_on > start_date:
        return None
    end_balance, end_snapshots = _account_sum_as_of_cents(conn, account_ids, as_of=end_date)
    if end_snapshots == 0:
        return None
    start_balance, start_snapshots = _account_sum_as_of_cents(conn, account_ids, as_of=start_date)
    if start_snapshots == 0:
        return None
    # Expected progress over a 60-day window = 2 × monthly commitment
    expected = int(
        (Decimal(commitment) * Decimal(_STALL_WINDOW_DAYS) / Decimal(30)).quantize(
            _DOLLAR, rounding=ROUND_HALF_UP
        )
    )
    actual = end_balance - start_balance
    return _StallCheck(
        start_balance_cents=start_balance,
        end_balance_cents=end_balance,
        monthly_commitment_cents=commitment,
        expected_progress_cents=expected,
        actual_progress_cents=actual,
    )


@register_pattern(
    id="savings_goal_stall",
    move=Move.COACH,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.MONITOR,),
)
def evaluate_savings_goal_stall(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    artifact = _latest_savings_goal_artifact()
    if artifact is None:
        return None
    artifact_path, payload = artifact
    target_phase = payload.get("target_phase")
    if target_phase not in {"full", "starter_only"}:
        return None
    check = _stall_check(conn, payload, as_of=ctx.now.date())
    if check is None:
        return None
    threshold_cents = int(
        (Decimal(check.expected_progress_cents) * _STALL_THRESHOLD_FRACTION).quantize(
            _DOLLAR, rounding=ROUND_HALF_UP
        )
    )
    if check.actual_progress_cents >= threshold_cents:
        return None
    shortfall = max(check.expected_progress_cents - check.actual_progress_cents, 0)
    return Intervention(
        pattern_id="savings_goal_stall",
        move=Move.COACH,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Savings goal progress has stalled — "
            f"{_money(check.actual_progress_cents)} of progress in the last 60 days "
            f"versus a planned ~{_money(check.expected_progress_cents)} at "
            f"{_money(check.monthly_commitment_cents)}/mo."
        ),
        detail_bullets=(
            f"Start-of-window balance: {_money(check.start_balance_cents)}",
            f"End-of-window balance: {_money(check.end_balance_cents)}",
            f"Monthly commitment: {_money(check.monthly_commitment_cents)}",
            f"Shortfall vs. plan: {_money(shortfall)}",
            f"Artifact baseline: {artifact_path.name}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Revisit savings-goal strategy fit",
            tool="activate_skill",
            params={"name": "coach_savings_goal"},
            build_stub=False,
        ),
        dollar_impact_cents=shortfall,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("savings_goal_stall"),
    )


# ---------------------------------------------------------------------------
# Maintenance: milestone celebration — artifact-driven, threshold cross
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _MilestoneHit:
    index: int
    threshold_pct: Any
    threshold_cents: int
    target_date: str | None
    current_balance_cents: int


def _first_unhit_milestone(
    payload: dict[str, Any],
    *,
    current_balance_cents: int,
) -> _MilestoneHit | None:
    """Return the first milestone (in artifact order) whose ``hit_at`` is null
    AND whose ``threshold_cents`` has been reached by ``current_balance_cents``.

    Iterating in artifact order matches the user's ordering intent (25 / 50 /
    75 / 100% by default) so the celebration surfaces sequentially.
    """
    milestones = payload.get("milestones") or []
    if not isinstance(milestones, list):
        return None
    for index, entry in enumerate(milestones):
        if not isinstance(entry, dict):
            continue
        hit_at = entry.get("hit_at")
        if hit_at:
            # Already classified; intervention's job is done for this milestone.
            continue
        try:
            threshold = int(entry.get("threshold_cents") or 0)
        except (TypeError, ValueError):
            continue
        if threshold <= 0:
            continue
        if current_balance_cents < threshold:
            continue
        return _MilestoneHit(
            index=index,
            threshold_pct=entry.get("threshold_pct"),
            threshold_cents=threshold,
            target_date=str(entry.get("target_date") or "") or None,
            current_balance_cents=current_balance_cents,
        )
    return None


@register_pattern(
    id="savings_goal_milestone_hit",
    move=Move.COACH,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=14),
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.MONITOR,),
)
def evaluate_savings_goal_milestone_hit(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    artifact = _latest_savings_goal_artifact()
    if artifact is None:
        return None
    artifact_path, payload = artifact
    account_ids = _account_ids_from_artifact(payload)
    if not account_ids:
        return None
    balance, snapshots = _account_sum_as_of_cents(
        conn,
        account_ids,
        as_of=ctx.now.date(),
    )
    if snapshots == 0:
        return None
    hit = _first_unhit_milestone(payload, current_balance_cents=balance)
    if hit is None:
        return None
    threshold_pct_text = (
        f"{hit.threshold_pct}%" if hit.threshold_pct is not None else "milestone"
    )
    return Intervention(
        pattern_id="savings_goal_milestone_hit",
        move=Move.COACH,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Savings goal hit the {threshold_pct_text} milestone "
            f"({_money(hit.threshold_cents)}) — current balance {_money(balance)}."
        ),
        detail_bullets=(
            f"Threshold reached: {_money(hit.threshold_cents)} ({threshold_pct_text})",
            f"Current balance: {_money(balance)}",
            f"Planned target date: {hit.target_date or 'unspecified'}",
            f"Artifact baseline: {artifact_path.name}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Celebrate + update savings-goal artifact",
            tool="coach_savings_goal_artifact_read",
            params={
                "artifact_path": artifact_path.name,
                "threshold_pct": hit.threshold_pct,
                "threshold_cents": hit.threshold_cents,
                "milestone_index": hit.index,
            },
            build_stub=False,
        ),
        dollar_impact_cents=hit.threshold_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("savings_goal_milestone_hit"),
    )
