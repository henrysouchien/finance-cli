from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import sqlite3

from ..commands.common import fmt_dollars
from ..models import cents_to_dollars
from .context import InterventionContext
from .helpers import income_by_stream
from .registry import (
    CFPDomain,
    CFPProcessStep,
    Intervention,
    InterventionAction,
    Move,
    Priority,
    register_pattern,
)


_W1_LOOKBACK_DAYS = 90
_W1_MAX_SNAPSHOT_STALENESS_DAYS = 7
_W1_MIN_STABLE_SURPLUS_CENTS = 200_000
_W1_MIN_RETAINED_BUFFER_CENTS = 200_000
_W1_ASSUMED_HYSA_APY_BPS = 450
_W1_CURRENT_CHECKING_APY_BPS = 0
_W1_HIGH_APR_DEBT_MIN = 18.0
_W2_ROTH_IRA_LIMIT_CENTS_BY_YEAR = {
    2026: 750_000,
}
_W2_ROTH_FULL_CONTRIBUTION_PHASEOUT_FLOOR_CENTS_BY_YEAR = {
    2026: 15_300_000,
}
_W3_MIN_SURPLUS_CENTS = 10_000
_W3_ASSUMED_RETIREMENT_RETURN_BPS = 700
_W3_GOAL_PROGRESS_SCORE_BPS = 600
_W3_TRANSFER_CATEGORY_NAMES = {
    "payments & transfers",
    "investment transfer",
    "savings transfer",
    "transfer",
    "transfers",
}
_W4_LOOKBACK_MONTHS = 3
_W4_MIN_HORIZON_MONTHS = 36
_CENT = Decimal("1")


@dataclass(frozen=True)
class _StableCheckingEvidence:
    observed_since: date
    latest_snapshot_date: date
    min_observed_balance_cents: int
    evidence_points: int


@dataclass(frozen=True)
class _W1Candidate:
    account_id: str
    account_label: str
    current_balance_cents: int
    retained_buffer_cents: int
    minimum_balance_cents: int
    suggested_transfer_cents: int
    current_apy_bps: int
    hysa_apy_bps: int
    estimated_annual_yield_cents: int
    evidence: _StableCheckingEvidence


@dataclass(frozen=True)
class _W2Candidate:
    tax_year: int
    months_remaining: int
    start_month: str
    end_month: str
    annual_limit_cents: int
    contributed_ytd_cents: int
    room_remaining_cents: int
    monthly_transfer_cents: int
    monthly_saving_capacity_cents: int
    projected_annual_income_cents: int


@dataclass(frozen=True)
class _W3CashFlow:
    income_mtd_cents: int
    expense_mtd_cents: int
    savings_transfer_mtd_cents: int
    remaining_recurring_expense_cents: int
    surplus_cents: int


@dataclass(frozen=True)
class _W3Option:
    kind: str
    score_bps: int
    impact_cents: int
    amount_cents: int
    description: str
    action_phrase: str
    action_label: str
    action_tool: str
    action_params: dict[str, object]
    detail: str
    goal_id: str | None = None


@dataclass(frozen=True)
class _W3Candidate:
    cash_flow: _W3CashFlow
    best_option: _W3Option
    compared_options: tuple[_W3Option, ...]


@dataclass(frozen=True)
class _InvestmentCadence:
    account_id: str
    account_label: str
    monthly_amount_cents: int
    observed_months: tuple[str, ...]


@dataclass(frozen=True)
class _W4Candidate:
    goal_id: str
    goal_name: str
    goal_target_cents: int
    goal_date: date
    months_until_goal: int
    current_investment_balance_cents: int
    projected_end_balance_cents: int
    cadence: _InvestmentCadence
    source_account_id: str | None


def _money(cents: int) -> str:
    return fmt_dollars(cents_to_dollars(int(cents)))


def _parse_snapshot_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _account_label(row: sqlite3.Row) -> str:
    institution = str(row["institution_name"] or "").strip()
    account_name = str(row["account_name"] or "").strip()
    label = " ".join(part for part in (institution, account_name) if part)
    return label or str(row["id"])


def _personal_checking_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
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


def _has_active_hysa_transfer_flag(conn: sqlite3.Connection, *, account_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM hysa_transfer_flags
         WHERE account_id = ?
           AND status = 'active'
         LIMIT 1
        """,
        (account_id,),
    ).fetchone()
    return row is not None


def _has_high_apr_personal_card_debt(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND l.liability_type = 'credit'
           AND a.is_active = 1
           AND a.account_type = 'credit_card'
           AND COALESCE(a.is_business, 0) = 0
           AND ABS(COALESCE(a.balance_current_cents, 0)) > 0
           AND COALESCE(l.apr_purchase, 0) >= ?
           AND NOT EXISTS (
                SELECT 1
                  FROM account_aliases aa
                 WHERE aa.hash_account_id = a.id
           )
         LIMIT 1
        """,
        (_W1_HIGH_APR_DEBT_MIN,),
    ).fetchone()
    return row is not None


def _stable_checking_evidence(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    as_of: date,
    lookback_days: int,
) -> _StableCheckingEvidence | None:
    cutoff = as_of - timedelta(days=lookback_days)
    rows = conn.execute(
        """
        SELECT snapshot_date, balance_current_cents
          FROM balance_snapshots
         WHERE account_id = ?
           AND snapshot_date <= ?
           AND balance_current_cents IS NOT NULL
         ORDER BY snapshot_date ASC, created_at ASC
        """,
        (account_id, as_of.isoformat()),
    ).fetchall()
    if not rows:
        return None

    boundary: sqlite3.Row | None = None
    evidence_rows: list[sqlite3.Row] = []
    for row in rows:
        snapshot_day = _parse_snapshot_date(row["snapshot_date"])
        if snapshot_day is None:
            continue
        if snapshot_day <= cutoff:
            boundary = row
            continue
        evidence_rows.append(row)

    if boundary is None or not evidence_rows:
        return None
    evidence_rows.insert(0, boundary)
    parsed_dates = [
        parsed
        for row in evidence_rows
        if (parsed := _parse_snapshot_date(row["snapshot_date"])) is not None
    ]
    if not parsed_dates:
        return None
    if parsed_dates[-1] < as_of - timedelta(days=_W1_MAX_SNAPSHOT_STALENESS_DAYS):
        return None
    min_observed_cents = min(int(row["balance_current_cents"] or 0) for row in evidence_rows)
    return _StableCheckingEvidence(
        observed_since=parsed_dates[0],
        latest_snapshot_date=parsed_dates[-1],
        min_observed_balance_cents=min_observed_cents,
        evidence_points=len(evidence_rows),
    )


def _estimated_yield_cents(
    *,
    suggested_transfer_cents: int,
    current_apy_bps: int,
    hysa_apy_bps: int,
) -> int:
    return int(
        (
            Decimal(suggested_transfer_cents)
            * Decimal(hysa_apy_bps - current_apy_bps)
            / Decimal(10_000)
        ).quantize(_CENT, rounding=ROUND_HALF_UP)
    )


def _month_keys(as_of: date, months: int) -> list[str]:
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    cursor = end_of_last_complete_month.replace(day=1)
    values: list[str] = []
    for _ in range(months):
        values.append(cursor.strftime("%Y-%m"))
        cursor = (cursor - timedelta(days=1)).replace(day=1)
    values.reverse()
    return values


def _known_roth_ira_limit_cents(tax_year: int) -> int | None:
    return _W2_ROTH_IRA_LIMIT_CENTS_BY_YEAR.get(tax_year)


def _roth_full_contribution_phaseout_floor_cents(tax_year: int) -> int | None:
    return _W2_ROTH_FULL_CONTRIBUTION_PHASEOUT_FLOOR_CENTS_BY_YEAR.get(tax_year)


def _active_roth_target_exists(conn: sqlite3.Connection, *, tax_year: int) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM retirement_contribution_targets
         WHERE tax_year = ?
           AND account_type = 'roth_ira'
           AND status = 'active'
         LIMIT 1
        """,
        (tax_year,),
    ).fetchone()
    return row is not None


def _known_roth_contributed_ytd_cents(conn: sqlite3.Connection, *, tax_year: int) -> int:
    row = conn.execute(
        """
        SELECT MAX(COALESCE(contributed_ytd_cents, 0)) AS contributed_ytd_cents
          FROM retirement_contribution_targets
         WHERE tax_year = ?
           AND account_type = 'roth_ira'
        """,
        (tax_year,),
    ).fetchone()
    return int(row["contributed_ytd_cents"] or 0)


def _trailing_3mo_income_cents(conn: sqlite3.Connection, *, as_of: date) -> int | None:
    expected_months = _month_keys(as_of, 3)
    totals_by_month = {month: 0 for month in expected_months}
    for row in income_by_stream(conn, months=3, as_of=as_of):
        month = str(row["month"])
        if month in totals_by_month:
            totals_by_month[month] += int(row["total_cents"])
    if any(total <= 0 for total in totals_by_month.values()):
        return None
    return sum(totals_by_month.values())


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _months_between_floor(start: date, end: date) -> int:
    months = ((end.year - start.year) * 12) + (end.month - start.month)
    if end.day < start.day:
        months -= 1
    return max(months, 0)


def _current_investment_balance_cents(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(balance_current_cents), 0) AS total_cents
          FROM accounts
         WHERE is_active = 1
           AND account_type = 'investment'
           AND COALESCE(balance_current_cents, 0) > 0
        """
    ).fetchone()
    return int(row["total_cents"] or 0)


def _active_savings_automation_exists(conn: sqlite3.Connection, *, goal_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM savings_automations
         WHERE goal_id = ?
           AND status = 'active'
         LIMIT 1
        """,
        (goal_id,),
    ).fetchone()
    return row is not None


def _investment_cadence(conn: sqlite3.Connection, *, as_of: date) -> _InvestmentCadence | None:
    expected_months = _month_keys(as_of, _W4_LOOKBACK_MONTHS)
    rows = conn.execute(
        """
        SELECT a.id AS id,
               a.id AS account_id,
               a.institution_name,
               a.account_name,
               substr(t.date, 1, 7) AS month,
               COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          JOIN accounts a ON a.id = t.account_id
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE a.is_active = 1
           AND a.account_type = 'investment'
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents > 0
           AND t.date >= ?
           AND t.date <= ?
           AND (c.id IS NULL OR c.is_income = 0)
         GROUP BY a.id, a.institution_name, a.account_name, substr(t.date, 1, 7)
        """,
        (
            f"{expected_months[0]}-01",
            (as_of.replace(day=1) - timedelta(days=1)).isoformat(),
        ),
    ).fetchall()
    by_account: dict[str, dict[str, int]] = {}
    labels: dict[str, str] = {}
    for row in rows:
        account_id = str(row["account_id"])
        month = str(row["month"])
        by_account.setdefault(account_id, {})[month] = int(row["total_cents"] or 0)
        labels[account_id] = _account_label(row)

    candidates: list[_InvestmentCadence] = []
    for account_id, totals_by_month in by_account.items():
        month_values = [int(totals_by_month.get(month, 0)) for month in expected_months]
        if any(value <= 0 for value in month_values):
            continue
        monthly_amount_cents = int(
            (Decimal(sum(month_values)) / Decimal(len(month_values))).quantize(
                _CENT,
                rounding=ROUND_HALF_UP,
            )
        )
        if monthly_amount_cents <= 0:
            continue
        candidates.append(
            _InvestmentCadence(
                account_id=account_id,
                account_label=labels[account_id],
                monthly_amount_cents=monthly_amount_cents,
                observed_months=tuple(expected_months),
            )
        )

    if not candidates:
        return None
    return max(candidates, key=lambda cadence: (cadence.monthly_amount_cents, cadence.account_id))


def _default_checking_source_account_id(conn: sqlite3.Connection) -> str | None:
    rows = _personal_checking_rows(conn)
    if not rows:
        return None
    return str(rows[0]["id"])


def _monthly_equivalent_cents(amount_cents: int, frequency: str | None) -> int:
    frequency_value = str(frequency or "monthly").strip().lower()
    if frequency_value == "weekly":
        return int((Decimal(amount_cents) * Decimal(52) / Decimal(12)).quantize(_CENT, rounding=ROUND_HALF_UP))
    if frequency_value == "biweekly":
        return int((Decimal(amount_cents) * Decimal(26) / Decimal(12)).quantize(_CENT, rounding=ROUND_HALF_UP))
    if frequency_value == "quarterly":
        return int((Decimal(amount_cents) / Decimal(3)).quantize(_CENT, rounding=ROUND_HALF_UP))
    if frequency_value == "yearly":
        return int((Decimal(amount_cents) / Decimal(12)).quantize(_CENT, rounding=ROUND_HALF_UP))
    return int(amount_cents)


def _is_transfer_category_name(value: str | None) -> bool:
    normalized = " ".join(str(value or "").strip().lower().split())
    return normalized in _W3_TRANSFER_CATEGORY_NAMES or "transfer" in normalized


def _category_transfer_sql(alias: str = "c") -> str:
    return (
        f"(LOWER(COALESCE({alias}.name, '')) IN ("
        + ",".join("?" for _ in _W3_TRANSFER_CATEGORY_NAMES)
        + f") OR LOWER(COALESCE({alias}.name, '')) LIKE '%transfer%')"
    )


def _current_month_cash_flow(conn: sqlite3.Connection, *, as_of: date) -> _W3CashFlow | None:
    month_start = as_of.replace(day=1)
    transfer_filter = _category_transfer_sql()
    income_row = conn.execute(
        """
        SELECT COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE c.is_income = 1
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents > 0
           AND t.date >= ?
           AND t.date <= ?
        """,
        (month_start.isoformat(), as_of.isoformat()),
    ).fetchone()
    income_mtd_cents = int(income_row["total_cents"] or 0)
    if income_mtd_cents <= 0:
        return None

    expense_row = conn.execute(
        f"""
        SELECT COALESCE(SUM(ABS(t.amount_cents)), 0) AS total_cents
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN accounts a ON a.id = t.account_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND t.date >= ?
           AND t.date <= ?
           AND (c.id IS NULL OR c.is_income = 0)
           AND NOT {transfer_filter}
           AND (a.id IS NULL OR COALESCE(a.is_business, 0) = 0)
           AND (a.id IS NULL OR a.account_type NOT IN ('savings', 'investment'))
           AND (t.use_type IS NULL OR t.use_type = 'Personal')
        """,
        (
            month_start.isoformat(),
            as_of.isoformat(),
            *sorted(_W3_TRANSFER_CATEGORY_NAMES),
        ),
    ).fetchone()
    expense_mtd_cents = int(expense_row["total_cents"] or 0)

    savings_row = conn.execute(
        """
        SELECT COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          JOIN accounts a ON a.id = t.account_id
         WHERE t.is_active = 1
           AND t.amount_cents > 0
           AND t.date >= ?
           AND t.date <= ?
           AND (c.id IS NULL OR c.is_income = 0)
           AND a.is_active = 1
           AND a.account_type IN ('savings', 'investment')
           AND COALESCE(a.is_business, 0) = 0
        """,
        (month_start.isoformat(), as_of.isoformat()),
    ).fetchone()
    savings_transfer_mtd_cents = int(savings_row["total_cents"] or 0)

    recurring_rows = conn.execute(
        """
        SELECT rf.amount_cents, rf.frequency, rf.day_of_month, c.name AS category_name
          FROM recurring_flows rf
          LEFT JOIN categories c ON c.id = rf.category_id
          LEFT JOIN accounts a ON a.id = rf.account_id
         WHERE rf.is_active = 1
           AND rf.flow_type = 'expense'
           AND ABS(COALESCE(rf.amount_cents, 0)) > 0
           AND (rf.day_of_month IS NULL OR rf.day_of_month > ?)
           AND (
                rf.account_id IS NULL
                OR (
                    a.is_active = 1
                    AND COALESCE(a.is_business, 0) = 0
                    AND a.account_type IN ('checking', 'savings')
                )
           )
        """,
        (as_of.day,),
    ).fetchall()
    remaining_recurring_expense_cents = sum(
        _monthly_equivalent_cents(abs(int(row["amount_cents"] or 0)), row["frequency"])
        for row in recurring_rows
        if not _is_transfer_category_name(row["category_name"])
    )
    if expense_mtd_cents + remaining_recurring_expense_cents <= 0:
        return None

    surplus_cents = (
        income_mtd_cents
        - expense_mtd_cents
        - savings_transfer_mtd_cents
        - remaining_recurring_expense_cents
    )
    if surplus_cents < _W3_MIN_SURPLUS_CENTS:
        return None
    return _W3CashFlow(
        income_mtd_cents=income_mtd_cents,
        expense_mtd_cents=expense_mtd_cents,
        savings_transfer_mtd_cents=savings_transfer_mtd_cents,
        remaining_recurring_expense_cents=remaining_recurring_expense_cents,
        surplus_cents=surplus_cents,
    )


def _apr_to_bps(apr: float | int | str | None) -> int:
    if apr is None:
        return 0
    return int((Decimal(str(apr)) * Decimal(100)).quantize(_CENT, rounding=ROUND_HALF_UP))


def _bps_value_cents(amount_cents: int, bps: int) -> int:
    return int(
        (Decimal(amount_cents) * Decimal(bps) / Decimal(10_000)).quantize(
            _CENT,
            rounding=ROUND_HALF_UP,
        )
    )


def _has_active_card_paydown_flag(conn: sqlite3.Connection, *, account_id: str) -> bool:
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


def _w3_debt_option(conn: sqlite3.Connection, cash_flow: _W3CashFlow) -> _W3Option | None:
    rows = conn.execute(
        """
        SELECT a.id,
               a.institution_name,
               a.account_name,
               ABS(COALESCE(a.balance_current_cents, 0)) AS balance_cents,
               l.apr_purchase
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
           AND l.liability_type = 'credit'
           AND a.is_active = 1
           AND a.account_type = 'credit_card'
           AND COALESCE(a.is_business, 0) = 0
           AND ABS(COALESCE(a.balance_current_cents, 0)) > 0
           AND COALESCE(l.apr_purchase, 0) > 0
           AND NOT EXISTS (
                SELECT 1
                  FROM account_aliases aa
                 WHERE aa.hash_account_id = a.id
           )
         ORDER BY l.apr_purchase DESC, ABS(COALESCE(a.balance_current_cents, 0)) DESC, a.id
        """
    ).fetchall()
    for row in rows:
        account_id = str(row["id"])
        if _has_active_card_paydown_flag(conn, account_id=account_id):
            continue
        balance_cents = int(row["balance_cents"] or 0)
        amount_cents = min(cash_flow.surplus_cents, balance_cents)
        score_bps = _apr_to_bps(row["apr_purchase"])
        if amount_cents <= 0 or score_bps <= 0:
            continue
        impact_cents = _bps_value_cents(amount_cents, score_bps)
        if impact_cents <= 0:
            continue
        label = _account_label(row)
        apr_text = f"{Decimal(str(row['apr_purchase'])):.2f}%"
        return _W3Option(
            kind="debt_paydown",
            score_bps=score_bps,
            impact_cents=impact_cents,
            amount_cents=amount_cents,
            description=(
                f"Paying {_money(amount_cents)} toward {label} at {apr_text} APR is "
                f"the strongest math, saving about {_money(impact_cents)}/yr"
            ),
            action_phrase="flag that card for paydown",
            action_label="Flag surplus for card paydown",
            action_tool="flag_card_for_paydown",
            action_params={
                "account_id": account_id,
                "suggested_payment_cents": amount_cents,
                "cash_source_account_id": _default_checking_source_account_id(conn),
                "interest_saved_annual_cents": impact_cents,
                "reason": f"Use this month's unallocated surplus against {label} at {apr_text} APR.",
                "source": "agent",
                "dry_run": False,
            },
            detail=f"Debt option: {label} at {apr_text} APR, {_money(impact_cents)}/yr estimated interest avoided.",
        )
    return None


def _current_liquid_balance_cents(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(balance_current_cents), 0) AS total_cents
          FROM accounts
         WHERE is_active = 1
           AND account_type IN ('checking', 'savings')
           AND COALESCE(is_business, 0) = 0
           AND COALESCE(balance_current_cents, 0) > 0
        """
    ).fetchone()
    return int(row["total_cents"] or 0)


def _first_active_account_id(conn: sqlite3.Connection, *, account_type: str) -> str | None:
    row = conn.execute(
        """
        SELECT id
          FROM accounts
         WHERE is_active = 1
           AND account_type = ?
           AND COALESCE(is_business, 0) = 0
         ORDER BY COALESCE(balance_current_cents, 0) DESC, id
         LIMIT 1
        """,
        (account_type,),
    ).fetchone()
    return None if row is None else str(row["id"])


def _goal_current_balance_cents(conn: sqlite3.Connection, *, metric: str) -> int | None:
    if metric == "liquid_cash":
        return _current_liquid_balance_cents(conn)
    if metric == "investments":
        return _current_investment_balance_cents(conn)
    return None


def _goal_destination_account_id(conn: sqlite3.Connection, *, metric: str) -> str | None:
    if metric == "liquid_cash":
        return _first_active_account_id(conn, account_type="savings")
    if metric == "investments":
        return _first_active_account_id(conn, account_type="investment")
    return None


def _w3_goal_option(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
    cash_flow: _W3CashFlow,
) -> _W3Option | None:
    candidates: list[_W3Option] = []
    for goal in ctx.goals:
        if goal.direction != "up" or goal.target_cents is None:
            continue
        current_balance_cents = _goal_current_balance_cents(conn, metric=goal.metric)
        if current_balance_cents is None:
            continue
        gap_cents = int(goal.target_cents) - current_balance_cents
        if gap_cents <= 0:
            continue
        if _active_savings_automation_exists(conn, goal_id=goal.id):
            continue
        destination_account_id = _goal_destination_account_id(conn, metric=goal.metric)
        if destination_account_id is None:
            continue
        amount_cents = min(cash_flow.surplus_cents, gap_cents)
        if amount_cents <= 0:
            continue
        goal_date = _parse_iso_date(goal.deadline)
        if goal_date is not None:
            projected_end_balance_cents = current_balance_cents + (
                amount_cents * max(_months_between_floor(ctx.now.date(), goal_date), 1)
            )
        else:
            projected_end_balance_cents = current_balance_cents + amount_cents
        candidates.append(
            _W3Option(
                kind="goal_funding",
                score_bps=_W3_GOAL_PROGRESS_SCORE_BPS,
                impact_cents=amount_cents,
                amount_cents=amount_cents,
                description=(
                    f"Putting {_money(amount_cents)} toward {goal.name} closes part "
                    f"of a {_money(gap_cents)} gap"
                ),
                action_phrase="set up the goal automation",
                action_label="Set goal automation",
                action_tool="setup_savings_automation",
                action_params={
                    "goal_id": goal.id,
                    "amount_cents": amount_cents,
                    "start_date": ctx.now.date().isoformat(),
                    "cadence": "monthly",
                    "funding_method": "auto_transfer",
                    "day_of_month": ctx.now.date().day,
                    "source_account_id": _default_checking_source_account_id(conn),
                    "destination_account_id": destination_account_id,
                    "target_amount_cents": goal.target_cents,
                    "projected_end_balance_cents": projected_end_balance_cents,
                    "goal_date": None if goal_date is None else goal_date.isoformat(),
                    "reason": "Use this month's unallocated surplus to fund an active goal.",
                    "dry_run": False,
                },
                detail=f"Goal option: {goal.name} gap {_money(gap_cents)}.",
                goal_id=goal.id,
            )
        )
    if not candidates:
        return None
    return max(candidates, key=lambda option: (option.impact_cents, option.goal_id or ""))


def _best_w2_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _W2Candidate | None:
    as_of = ctx.now.date()
    if as_of.month < 7:
        return None
    if _has_high_apr_personal_card_debt(conn):
        return None

    tax_year = as_of.year
    if _active_roth_target_exists(conn, tax_year=tax_year):
        return None

    trailing_income_cents = _trailing_3mo_income_cents(conn, as_of=as_of)
    if trailing_income_cents is None:
        return None
    projected_annual_income_cents = int(
        (Decimal(trailing_income_cents) / Decimal(3) * Decimal(12)).quantize(
            _CENT,
            rounding=ROUND_HALF_UP,
        )
    )
    phaseout_floor_cents = _roth_full_contribution_phaseout_floor_cents(tax_year)
    annual_limit_for_year_cents = _known_roth_ira_limit_cents(tax_year)
    if phaseout_floor_cents is None or annual_limit_for_year_cents is None:
        return None
    if projected_annual_income_cents >= phaseout_floor_cents:
        return None

    annual_limit_cents = min(
        annual_limit_for_year_cents,
        projected_annual_income_cents,
    )
    contributed_ytd_cents = _known_roth_contributed_ytd_cents(conn, tax_year=tax_year)
    room_remaining_cents = annual_limit_cents - contributed_ytd_cents
    if room_remaining_cents <= 0:
        return None

    months_remaining = 12 - as_of.month + 1
    monthly_transfer_cents = room_remaining_cents // months_remaining
    if monthly_transfer_cents <= 0:
        return None

    monthly_income_cents = int(
        (Decimal(trailing_income_cents) / Decimal(3)).quantize(_CENT, rounding=ROUND_HALF_UP)
    )
    monthly_saving_capacity_cents = monthly_income_cents - int(ctx.trailing_3mo_avg_expense_cents)
    if monthly_saving_capacity_cents < monthly_transfer_cents:
        return None

    return _W2Candidate(
        tax_year=tax_year,
        months_remaining=months_remaining,
        start_month=f"{tax_year}-{as_of.month:02d}",
        end_month=f"{tax_year}-12",
        annual_limit_cents=annual_limit_cents,
        contributed_ytd_cents=contributed_ytd_cents,
        room_remaining_cents=room_remaining_cents,
        monthly_transfer_cents=monthly_transfer_cents,
        monthly_saving_capacity_cents=monthly_saving_capacity_cents,
        projected_annual_income_cents=projected_annual_income_cents,
    )


def _w3_retirement_option(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
    cash_flow: _W3CashFlow,
) -> _W3Option | None:
    candidate = _best_w2_candidate(conn, ctx)
    if candidate is None:
        return None
    amount_cents = min(cash_flow.surplus_cents, candidate.monthly_transfer_cents)
    if amount_cents <= 0:
        return None
    total_planned_cents = amount_cents * candidate.months_remaining
    if total_planned_cents > candidate.room_remaining_cents:
        amount_cents = candidate.room_remaining_cents // candidate.months_remaining
        total_planned_cents = amount_cents * candidate.months_remaining
    if amount_cents <= 0 or total_planned_cents <= 0:
        return None
    return _W3Option(
        kind="retirement_room",
        score_bps=_W3_ASSUMED_RETIREMENT_RETURN_BPS,
        impact_cents=total_planned_cents,
        amount_cents=amount_cents,
        description=(
            f"Using {_money(amount_cents)}/mo toward Roth IRA room would plan "
            f"{_money(total_planned_cents)} before year-end"
        ),
        action_phrase="set the Roth transfer target",
        action_label="Set monthly Roth transfer target",
        action_tool="setup_monthly_transfer_goal",
        action_params={
            "tax_year": str(candidate.tax_year),
            "monthly_transfer_cents": amount_cents,
            "room_remaining_cents": candidate.room_remaining_cents,
            "start_month": candidate.start_month,
            "end_month": candidate.end_month,
            "account_type": "roth_ira",
            "annual_limit_cents": candidate.annual_limit_cents,
            "contributed_ytd_cents": candidate.contributed_ytd_cents,
            "estimated_tax_savings_cents": None,
            "reason": "Use this month's unallocated surplus toward remaining Roth IRA room.",
            "update_monthly_plans": True,
            "dry_run": False,
        },
        detail=(
            f"Retirement option: {_money(candidate.room_remaining_cents)} Roth room remains "
            f"for {candidate.tax_year}."
        ),
    )


def _best_w3_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _W3Candidate | None:
    cash_flow = _current_month_cash_flow(conn, as_of=ctx.now.date())
    if cash_flow is None:
        return None

    options = tuple(
        option
        for option in (
            _w3_debt_option(conn, cash_flow),
            _w3_retirement_option(conn, ctx, cash_flow),
            _w3_goal_option(conn, ctx, cash_flow),
        )
        if option is not None
    )
    if not options:
        return None
    best_option = max(
        options,
        key=lambda option: (
            option.score_bps,
            option.impact_cents,
            option.amount_cents,
            option.kind,
        ),
    )
    return _W3Candidate(
        cash_flow=cash_flow,
        best_option=best_option,
        compared_options=options,
    )


def _best_w1_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _W1Candidate | None:
    if _has_high_apr_personal_card_debt(conn):
        return None

    as_of = ctx.now.date()
    retained_buffer_cents = max(
        int(ctx.trailing_3mo_avg_expense_cents),
        _W1_MIN_RETAINED_BUFFER_CENTS,
    )
    candidates: list[_W1Candidate] = []
    for row in _personal_checking_rows(conn):
        account_id = str(row["id"])
        if _has_active_hysa_transfer_flag(conn, account_id=account_id):
            continue
        evidence = _stable_checking_evidence(
            conn,
            account_id=account_id,
            as_of=as_of,
            lookback_days=_W1_LOOKBACK_DAYS,
        )
        if evidence is None:
            continue
        current_balance_cents = int(row["balance_current_cents"] or 0)
        stable_surplus_cents = evidence.min_observed_balance_cents - retained_buffer_cents
        current_surplus_cents = current_balance_cents - retained_buffer_cents
        suggested_transfer_cents = min(stable_surplus_cents, current_surplus_cents)
        if suggested_transfer_cents < _W1_MIN_STABLE_SURPLUS_CENTS:
            continue
        yield_cents = _estimated_yield_cents(
            suggested_transfer_cents=suggested_transfer_cents,
            current_apy_bps=_W1_CURRENT_CHECKING_APY_BPS,
            hysa_apy_bps=_W1_ASSUMED_HYSA_APY_BPS,
        )
        candidates.append(
            _W1Candidate(
                account_id=account_id,
                account_label=_account_label(row),
                current_balance_cents=current_balance_cents,
                retained_buffer_cents=retained_buffer_cents,
                minimum_balance_cents=retained_buffer_cents + suggested_transfer_cents,
                suggested_transfer_cents=suggested_transfer_cents,
                current_apy_bps=_W1_CURRENT_CHECKING_APY_BPS,
                hysa_apy_bps=_W1_ASSUMED_HYSA_APY_BPS,
                estimated_annual_yield_cents=yield_cents,
                evidence=evidence,
            )
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda candidate: (
            candidate.estimated_annual_yield_cents,
            candidate.suggested_transfer_cents,
            candidate.current_balance_cents,
        ),
    )


def _best_w4_candidate(conn: sqlite3.Connection, ctx: InterventionContext) -> _W4Candidate | None:
    as_of = ctx.now.date()
    cadence = _investment_cadence(conn, as_of=as_of)
    if cadence is None:
        return None

    current_balance_cents = _current_investment_balance_cents(conn)
    candidates: list[_W4Candidate] = []
    for goal in ctx.goals:
        if goal.metric != "investments" or goal.direction != "up" or goal.target_cents is None:
            continue
        goal_date = _parse_iso_date(goal.deadline)
        if goal_date is None:
            continue
        months_until_goal = _months_between_floor(as_of, goal_date)
        if months_until_goal < _W4_MIN_HORIZON_MONTHS:
            continue
        if current_balance_cents >= goal.target_cents:
            continue
        if _active_savings_automation_exists(conn, goal_id=goal.id):
            continue
        projected_end_balance_cents = current_balance_cents + (
            cadence.monthly_amount_cents * months_until_goal
        )
        if projected_end_balance_cents < goal.target_cents:
            continue
        candidates.append(
            _W4Candidate(
                goal_id=goal.id,
                goal_name=goal.name,
                goal_target_cents=goal.target_cents,
                goal_date=goal_date,
                months_until_goal=months_until_goal,
                current_investment_balance_cents=current_balance_cents,
                projected_end_balance_cents=projected_end_balance_cents,
                cadence=cadence,
                source_account_id=_default_checking_source_account_id(conn),
            )
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda candidate: (
            candidate.projected_end_balance_cents - candidate.goal_target_cents,
            candidate.cadence.monthly_amount_cents,
            candidate.goal_date,
        ),
    )


@register_pattern(
    id="W-1",
    move=Move.PRESCRIBE,
    tiers=(1,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=30),
    tool="flag_account_for_hysa_transfer",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.INVESTMENT),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
)
def evaluate_w1_surplus_cash_drag(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _best_w1_candidate(conn, ctx)
    if candidate is None:
        return None

    hysa_yield = _money(candidate.estimated_annual_yield_cents)
    transfer = _money(candidate.suggested_transfer_cents)
    hysa_apy = Decimal(candidate.hysa_apy_bps) / Decimal(100)
    return Intervention(
        pattern_id="W-1",
        move=Move.PRESCRIBE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"{_money(candidate.current_balance_cents)} has been sitting in "
            f"{candidate.account_label} for 3+ months. Using a {hysa_apy:.2f}% HYSA "
            f"assumption, moving {transfer} could earn roughly {hysa_yield}/yr."
        ),
        detail_bullets=(
            f"Stable observed balance since {candidate.evidence.observed_since.isoformat()}: "
            f"{_money(candidate.evidence.min_observed_balance_cents)}",
            f"Retained checking buffer: {_money(candidate.retained_buffer_cents)}",
            f"Latest balance snapshot: {candidate.evidence.latest_snapshot_date.isoformat()} "
            f"({candidate.evidence.evidence_points} points)",
            "Confirm the available HYSA rate before accepting the transfer flag.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Flag checking surplus for HYSA",
            tool="flag_account_for_hysa_transfer",
            params={
                "account_id": candidate.account_id,
                "suggested_transfer_cents": candidate.suggested_transfer_cents,
                "hysa_apy_bps": candidate.hysa_apy_bps,
                "current_apy_bps": candidate.current_apy_bps,
                "retained_buffer_cents": candidate.retained_buffer_cents,
                "minimum_balance_cents": candidate.minimum_balance_cents,
                "lookback_days": _W1_LOOKBACK_DAYS,
                "as_of": ctx.now.date().isoformat(),
                "reason": (
                    f"Stable checking surplus above the retained buffer since "
                    f"{candidate.evidence.observed_since.isoformat()}."
                ),
                "source": "agent",
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.estimated_annual_yield_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("W-1"),
    )


@register_pattern(
    id="W-2",
    move=Move.PRESCRIBE,
    tiers=(1, 2, 4),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=90),
    tool="setup_monthly_transfer_goal",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.INVESTMENT, CFPDomain.RETIREMENT),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
)
def evaluate_w2_roth_ira_contribution_prompt(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _best_w2_candidate(conn, ctx)
    if candidate is None:
        return None

    return Intervention(
        pattern_id="W-2",
        move=Move.PRESCRIBE,
        tiers=(1, 2, 4),
        priority=Priority.MEDIUM,
        headline=(
            f"{candidate.months_remaining} months from year-end, roughly "
            f"{_money(candidate.room_remaining_cents)} of Roth IRA room left. "
            f"At your savings rate you can max it - want me to suggest "
            f"{_money(candidate.monthly_transfer_cents)}/mo?"
        ),
        detail_bullets=(
            f"Projected annual income from the last 3 complete months: "
            f"{_money(candidate.projected_annual_income_cents)}.",
            f"Known Roth contributions this year: {_money(candidate.contributed_ytd_cents)}.",
            f"Trailing monthly saving capacity: {_money(candidate.monthly_saving_capacity_cents)}.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Set monthly Roth transfer target",
            tool="setup_monthly_transfer_goal",
            params={
                "tax_year": str(candidate.tax_year),
                "monthly_transfer_cents": candidate.monthly_transfer_cents,
                "room_remaining_cents": candidate.room_remaining_cents,
                "start_month": candidate.start_month,
                "end_month": candidate.end_month,
                "account_type": "roth_ira",
                "annual_limit_cents": candidate.annual_limit_cents,
                "contributed_ytd_cents": candidate.contributed_ytd_cents,
                "estimated_tax_savings_cents": None,
                "reason": "Q3/Q4 Roth IRA room remains and recent saving capacity can cover the monthly transfer.",
                "update_monthly_plans": True,
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.room_remaining_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("W-2"),
    )


@register_pattern(
    id="W-3",
    move=Move.COMPARE,
    tiers=(1, 4),
    priority=Priority.MEDIUM,
    tool=None,
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.INVESTMENT, CFPDomain.RETIREMENT),
    cfp_steps=(
        CFPProcessStep.ANALYZE,
        CFPProcessStep.DEVELOP,
        CFPProcessStep.PRESENT,
        CFPProcessStep.IMPLEMENT,
    ),
)
def evaluate_w3_surplus_deployment_decision(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _best_w3_candidate(conn, ctx)
    if candidate is None:
        return None

    cash_flow = candidate.cash_flow
    best = candidate.best_option
    return Intervention(
        pattern_id="W-3",
        move=Move.COMPARE,
        tiers=(1, 4),
        priority=Priority.MEDIUM,
        headline=(
            f"You have {_money(cash_flow.surplus_cents)} surplus this month. "
            f"{best.description}. Want to {best.action_phrase}?"
        ),
        detail_bullets=(
            f"Income month-to-date: {_money(cash_flow.income_mtd_cents)}.",
            (
                "Known expenses reserved/spent: "
                f"{_money(cash_flow.expense_mtd_cents + cash_flow.remaining_recurring_expense_cents)} "
                f"({_money(cash_flow.expense_mtd_cents)} spent, "
                f"{_money(cash_flow.remaining_recurring_expense_cents)} remaining recurring)."
            ),
            f"Savings/investment transfers already made: {_money(cash_flow.savings_transfer_mtd_cents)}.",
            *(option.detail for option in candidate.compared_options),
        ),
        tier4_ladder=f"Turn this month's surplus into {best.kind.replace('_', ' ')} progress.",
        tier4_is_fallback=False,
        action=InterventionAction(
            label=best.action_label,
            tool=best.action_tool,
            params=dict(best.action_params),
            build_stub=False,
        ),
        dollar_impact_cents=best.impact_cents,
        goal_link=best.goal_id,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("W-3"),
    )


@register_pattern(
    id="W-4",
    move=Move.COACH,
    tiers=(4,),
    priority=Priority.MEDIUM,
    cooldown=timedelta(days=90),
    tool="setup_savings_automation",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.INVESTMENT),
    cfp_steps=(
        CFPProcessStep.ANALYZE,
        CFPProcessStep.DEVELOP,
        CFPProcessStep.IMPLEMENT,
        CFPProcessStep.MONITOR,
    ),
)
def evaluate_w4_goal_aligned_investment_cadence(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _best_w4_candidate(conn, ctx)
    if candidate is None:
        return None

    start_date = ctx.now.date()
    return Intervention(
        pattern_id="W-4",
        move=Move.COACH,
        tiers=(4,),
        priority=Priority.MEDIUM,
        headline=(
            f"You're on pace for {_money(candidate.projected_end_balance_cents)} by "
            f"{candidate.goal_date.isoformat()}. Want to lock in an automatic "
            f"{_money(candidate.cadence.monthly_amount_cents)}/mo transfer so you don't "
            f"have to think about it?"
        ),
        detail_bullets=(
            f"Goal: {candidate.goal_name} target {_money(candidate.goal_target_cents)}.",
            f"Current investment balance: {_money(candidate.current_investment_balance_cents)}.",
            f"Observed investment deposits: {_money(candidate.cadence.monthly_amount_cents)}/mo "
            f"across {', '.join(candidate.cadence.observed_months)}.",
            f"Destination account: {candidate.cadence.account_label}.",
        ),
        tier4_ladder=f"Goal-linked automation for {candidate.goal_name}",
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Set savings automation",
            tool="setup_savings_automation",
            params={
                "goal_id": candidate.goal_id,
                "amount_cents": candidate.cadence.monthly_amount_cents,
                "start_date": start_date.isoformat(),
                "cadence": "monthly",
                "funding_method": "auto_transfer",
                "day_of_month": start_date.day,
                "source_account_id": candidate.source_account_id,
                "destination_account_id": candidate.cadence.account_id,
                "target_amount_cents": candidate.goal_target_cents,
                "projected_end_balance_cents": candidate.projected_end_balance_cents,
                "goal_date": candidate.goal_date.isoformat(),
                "reason": "Three complete months of investment deposits are already on pace for the long-horizon goal.",
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.cadence.monthly_amount_cents,
        goal_link=candidate.goal_id,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("W-4"),
    )
