from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import re
import sqlite3

from ..commands.common import fmt_dollars
from ..models import cents_to_dollars
from .context import InterventionContext
from .helpers import expense_filter_clause, income_by_stream, trailing_avg_expenses_cents
from .registry import (
    CFPDomain,
    CFPProcessStep,
    Intervention,
    InterventionAction,
    Move,
    Priority,
    register_pattern,
)

_C2_LOOKAHEAD_DAYS = 7
_C2_LOW_BALANCE_THRESHOLD_CENTS = 50_000
_C2_RECENT_CHARGE_LOOKBACK_DAYS = 14
_C3_MIN_DAYS_LEFT = 7
_C3_MIN_ROOM_RATIO = Decimal("0.30")
_C6_LOOKBACK_DAYS = 90
_C6_MIN_FEE_COUNT = 2
_C6_BUFFER_THRESHOLD_CENTS = 10_000
_FIXED_CATEGORY_TOKENS = {
    "childcare",
    "debt",
    "daycare",
    "fees",
    "groceries",
    "grocery",
    "health",
    "hoa",
    "insurance",
    "loan",
    "medical",
    "mortgage",
    "payment",
    "rent",
    "tax",
    "taxes",
    "transfer",
    "utility",
    "utilities",
}
_FIXED_CATEGORY_PHRASES = ("child care", "credit card", "student loan")
_DISCRETIONARY_CATEGORY_TOKENS = {
    "bar",
    "bars",
    "clothing",
    "coffee",
    "dining",
    "entertainment",
    "hobby",
    "hobbies",
    "lifestyle",
    "restaurant",
    "restaurants",
    "shopping",
    "travel",
}
_DISCRETIONARY_CATEGORY_PHRASES = (
    "personal care",
    "ride share",
    "rideshare",
    "food delivery",
)


@dataclass(frozen=True)
class _C6Candidate:
    account_id: str
    account_label: str
    fee_count: int
    fee_total_cents: int
    one_day_late_deposit_count: int
    latest_fee_date: date


def _category_tokens(name: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", name.lower()))


def _is_fixed_category(name: str) -> bool:
    lowered = name.lower()
    tokens = _category_tokens(lowered)
    return bool(tokens & _FIXED_CATEGORY_TOKENS) or any(phrase in lowered for phrase in _FIXED_CATEGORY_PHRASES)


def _is_discretionary_category(name: str) -> bool:
    lowered = name.lower()
    if _is_fixed_category(lowered):
        return False
    tokens = _category_tokens(lowered)
    return bool(tokens & _DISCRETIONARY_CATEGORY_TOKENS) or any(
        phrase in lowered for phrase in _DISCRETIONARY_CATEGORY_PHRASES
    )


def _parse_txn_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value or "")[:10])
    except ValueError:
        return None


def _account_label_from_row(row: sqlite3.Row) -> str:
    institution = str(row["institution_name"] or "").strip()
    account_name = str(row["account_name"] or "").strip()
    label = " ".join(part for part in (institution, account_name) if part)
    return label or str(row["account_id"])


def _contains_fee_marker(text: str) -> bool:
    lowered = text.lower()
    tokens = _category_tokens(lowered)
    return bool(tokens & {"fee", "fees", "charge", "charges"}) or "bank fee" in lowered


def _contains_overdraft_marker(text: str) -> bool:
    lowered = text.lower()
    tokens = _category_tokens(lowered)
    if any(phrase in lowered for phrase in ("overdraft", "overdrawn", "insufficient funds", "nonsufficient funds")):
        return True
    if "non sufficient funds" in lowered or "non-sufficient funds" in lowered:
        return True
    if "nsf" in tokens:
        return True
    if "od" in tokens and tokens & {"fee", "fees"}:
        return True
    returned_item = "returned item" in lowered or "returned payment" in lowered
    return returned_item and bool(tokens & {"fee", "fees"})


def _is_overdraft_fee_row(row: sqlite3.Row) -> bool:
    text = " ".join(
        str(row[key] or "")
        for key in ("description", "source_category", "category_name")
    )
    return _contains_overdraft_marker(text) and _contains_fee_marker(text)


def _is_deposit_like_credit_row(row: sqlite3.Row) -> bool:
    if int(row["is_income"] or 0) == 1:
        return True
    text = " ".join(
        str(row[key] or "")
        for key in ("description", "source_category", "category_name")
    )
    lowered = text.lower()
    tokens = _category_tokens(lowered)
    if tokens & {"paycheck", "payroll", "salary", "wage", "wages", "income"}:
        return True
    if "direct deposit" in lowered:
        return True
    if "deposit" in tokens and not tokens & {"refund", "refunded", "return", "returned", "reversal"}:
        return True
    return False


def _has_existing_c6_alert(
    conn: sqlite3.Connection,
    *,
    account_id: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM account_alert_rules
         WHERE rule_type = 'low_balance'
           AND account_id = ?
           AND status = 'active'
           AND threshold_cents >= ?
         LIMIT 1
        """,
        (account_id, _C6_BUFFER_THRESHOLD_CENTS),
    ).fetchone()
    return row is not None


def _has_next_day_deposit(
    conn: sqlite3.Connection,
    *,
    account_id: str,
    fee_date: date,
) -> bool:
    rows = conn.execute(
        """
        SELECT t.id, t.description,
               COALESCE(t.source_category, '') AS source_category,
               COALESCE(c.name, '') AS category_name,
               COALESCE(c.is_income, 0) AS is_income
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.account_id = ?
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents > 0
           AND date(t.date) = date(?, '+1 day')
         ORDER BY t.amount_cents DESC, t.id
        """,
        (account_id, fee_date.isoformat()),
    ).fetchall()
    return any(_is_deposit_like_credit_row(row) for row in rows)


def _find_c6_candidate(conn: sqlite3.Connection, *, as_of: date) -> _C6Candidate | None:
    start = (as_of - timedelta(days=_C6_LOOKBACK_DAYS)).isoformat()
    rows = conn.execute(
        """
        SELECT t.id, t.account_id, t.date, t.description, t.amount_cents,
               COALESCE(t.source_category, '') AS source_category,
               COALESCE(c.name, '') AS category_name,
               a.institution_name, a.account_name
          FROM transactions t
          JOIN accounts a ON a.id = t.account_id
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND t.date >= ?
           AND t.date <= ?
           AND a.is_active = 1
           AND a.account_type IN ('checking', 'savings')
           AND NOT EXISTS (
                SELECT 1
                  FROM account_aliases aa
                 WHERE aa.hash_account_id = a.id
           )
         ORDER BY t.date DESC, ABS(t.amount_cents) DESC, t.id
        """,
        (start, as_of.isoformat()),
    ).fetchall()

    fees_by_account: dict[str, list[tuple[sqlite3.Row, date]]] = {}
    for row in rows:
        if not _is_overdraft_fee_row(row):
            continue
        fee_date = _parse_txn_date(row["date"])
        if fee_date is None:
            continue
        fees_by_account.setdefault(str(row["account_id"]), []).append((row, fee_date))

    candidates: list[_C6Candidate] = []
    for account_id, fees in fees_by_account.items():
        if len(fees) < _C6_MIN_FEE_COUNT:
            continue
        if _has_existing_c6_alert(conn, account_id=account_id):
            continue
        fee_total_cents = sum(abs(int(row["amount_cents"] or 0)) for row, _fee_date in fees)
        one_day_late_deposits = sum(
            1
            for fee_date in {fee_date for _row, fee_date in fees}
            if _has_next_day_deposit(conn, account_id=account_id, fee_date=fee_date)
        )
        latest_fee_date = max(fee_date for _row, fee_date in fees)
        candidates.append(
            _C6Candidate(
                account_id=account_id,
                account_label=_account_label_from_row(fees[0][0]),
                fee_count=len(fees),
                fee_total_cents=fee_total_cents,
                one_day_late_deposit_count=one_day_late_deposits,
                latest_fee_date=latest_fee_date,
            )
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            item.fee_count,
            item.fee_total_cents,
            item.one_day_late_deposit_count,
            item.latest_fee_date,
            item.account_label,
            item.account_id,
        ),
    )


def _current_liquid_cash_cents(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(balance_current_cents), 0) AS total_cents
          FROM accounts a
         WHERE a.is_active = 1
           AND a.account_type IN ('checking', 'savings')
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
        """
    ).fetchone()
    return int(row["total_cents"] or 0)


def _personal_monthly_budget_status(
    conn: sqlite3.Connection,
    *,
    month_start: str,
    today: str,
) -> list[dict[str, object]]:
    budget_rows = conn.execute(
        """
        SELECT b.id, b.category_id, c.name AS category_name, b.amount_cents
          FROM budgets b
          JOIN categories c ON c.id = b.category_id
         WHERE b.period = 'monthly'
           AND b.use_type = 'Personal'
           AND b.amount_cents > 0
           AND b.effective_to IS NULL
           AND date(b.effective_from) <= date(?)
           AND b.id = (
                SELECT b2.id
                  FROM budgets b2
                 WHERE b2.category_id = b.category_id
                   AND b2.period = b.period
                   AND b2.use_type = b.use_type
                   AND b2.effective_to IS NULL
                   AND date(b2.effective_from) <= date(?)
                 ORDER BY date(b2.effective_from) DESC, b2.id DESC
                 LIMIT 1
           )
         ORDER BY b.amount_cents DESC, c.name ASC
        """,
        (today, today),
    ).fetchall()

    status: list[dict[str, object]] = []
    for row in budget_rows:
        category_name = str(row["category_name"])
        if not _is_discretionary_category(category_name):
            continue
        actual = conn.execute(
            """
            SELECT COALESCE(SUM(ABS(t.amount_cents)), 0) AS actual_cents
              FROM transactions t
             WHERE t.is_active = 1
               AND t.is_payment = 0
               AND t.amount_cents < 0
               AND (t.use_type = 'Personal' OR t.use_type IS NULL)
               AND t.category_id = ?
               AND t.date >= ?
               AND t.date <= ?
            """,
            (row["category_id"], month_start, today),
        ).fetchone()
        budget_cents = int(row["amount_cents"] or 0)
        actual_cents = int(actual["actual_cents"] or 0)
        status.append(
            {
                "budget_id": str(row["id"]),
                "category_id": str(row["category_id"]),
                "category_name": category_name,
                "budget_cents": budget_cents,
                "actual_cents": actual_cents,
                "remaining_cents": budget_cents - actual_cents,
            }
        )
    return status


def _month_text(months: Decimal) -> str:
    quantized = months.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    return f"{quantized}"


def _monthly_due_date(today: date, day_of_month: int) -> date | None:
    if day_of_month < 1 or day_of_month > 31:
        return None
    year = today.year
    month = today.month
    day = max(1, min(int(day_of_month), calendar.monthrange(year, month)[1]))
    candidate = date(year, month, day)
    if candidate < today:
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        day = max(1, min(int(day_of_month), calendar.monthrange(year, month)[1]))
        candidate = date(year, month, day)
    return candidate


def _current_month_expense_categories(
    conn: sqlite3.Connection,
    *,
    today: date,
    use_type: str = "Personal",
) -> list[str]:
    month_start = today.replace(day=1).isoformat()
    rows = conn.execute(
        f"""
        SELECT c.name AS category_name,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE {expense_filter_clause(use_type=use_type)}
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY c.name
         ORDER BY total_cents DESC, c.name
        """,
        (month_start, today.isoformat()),
    ).fetchall()
    return [str(row["category_name"]) for row in rows if str(row["category_name"] or "").strip()]


def _recent_discretionary_charge(conn: sqlite3.Connection, *, today: date) -> str:
    start = (today - timedelta(days=_C2_RECENT_CHARGE_LOOKBACK_DAYS)).isoformat()
    rows = conn.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM(t.description), ''), c.name) AS label,
               ABS(t.amount_cents) AS amount_cents,
               c.name AS category_name
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE {expense_filter_clause()}
           AND t.date >= ?
           AND t.date <= ?
           AND COALESCE(t.is_recurring, 0) = 0
         ORDER BY t.date DESC, amount_cents DESC, t.id
         LIMIT 20
        """,
        (start, today.isoformat()),
    ).fetchall()
    for row in rows:
        if not _is_discretionary_category(str(row["category_name"])):
            continue
        label = str(row["label"] or "").strip() or "discretionary spend"
        return f"{label} ({fmt_dollars(cents_to_dollars(int(row['amount_cents'] or 0)))})"
    return "discretionary spend"


def _anchored_monthly_flows(conn: sqlite3.Connection, *, today: date) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT rf.id, rf.name, rf.flow_type, ABS(rf.amount_cents) AS amount_cents,
               rf.day_of_month, c.name AS category_name
          FROM recurring_flows rf
          LEFT JOIN accounts a ON a.id = rf.account_id
          LEFT JOIN categories c ON c.id = rf.category_id
         WHERE rf.is_active = 1
           AND rf.frequency = 'monthly'
           AND rf.day_of_month IS NOT NULL
           AND ABS(rf.amount_cents) > 0
           AND (
                rf.account_id IS NULL
                OR (
                    a.is_active = 1
                    AND a.account_type IN ('checking', 'savings')
                    AND NOT EXISTS (
                        SELECT 1
                          FROM account_aliases aa
                         WHERE aa.hash_account_id = rf.account_id
                    )
                )
           )
        """
    ).fetchall()
    flows: list[dict[str, object]] = []
    for row in rows:
        due_date = _monthly_due_date(today, int(row["day_of_month"]))
        if due_date is None:
            continue
        days_until = (due_date - today).days
        amount_cents = int(row["amount_cents"] or 0)
        flow_type = str(row["flow_type"])
        if flow_type == "expense":
            fixed_label = str(row["category_name"] or row["name"] or "")
            if not _is_fixed_category(fixed_label):
                continue
        signed_amount_cents = amount_cents if flow_type == "income" else -amount_cents
        flows.append(
            {
                "id": str(row["id"]),
                "name": str(row["name"] or "Upcoming bill"),
                "flow_type": flow_type,
                "amount_cents": amount_cents,
                "signed_amount_cents": signed_amount_cents,
                "due_date": due_date,
                "days_until": days_until,
            }
        )
    return flows


def _projected_checking_savings_balance_cents(
    *,
    current_balance_cents: int,
    flows: list[dict[str, object]],
    through_days: int,
) -> int:
    projected_cents = current_balance_cents
    for flow in flows:
        days_until = int(flow["days_until"])
        if 1 <= days_until <= through_days:
            projected_cents += int(flow["signed_amount_cents"])
    return projected_cents


@register_pattern(
    id="C-2",
    move=Move.WARN,
    tiers=(1,),
    priority=Priority.HIGH,
    tool="set_spending_freeze_flag",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
)
def evaluate_c2_pre_bill_warning(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    today = ctx.now.date()
    current_balance_cents = _current_liquid_cash_cents(conn)
    flows = _anchored_monthly_flows(conn, today=today)
    candidates: list[dict[str, object]] = []
    for flow in flows:
        days_until = int(flow["days_until"])
        if flow["flow_type"] != "expense" or days_until < 1 or days_until > _C2_LOOKAHEAD_DAYS:
            continue
        remaining_cents = _projected_checking_savings_balance_cents(
            current_balance_cents=current_balance_cents,
            flows=flows,
            through_days=days_until,
        )
        if remaining_cents >= _C2_LOW_BALANCE_THRESHOLD_CENTS:
            continue
        candidates.append(
            {
                "id": str(flow["id"]),
                "name": str(flow["name"]),
                "amount_cents": int(flow["amount_cents"]),
                "due_date": flow["due_date"],
                "days_until": days_until,
                "remaining_cents": remaining_cents,
            }
        )

    if not candidates:
        return None

    candidate = min(
        candidates,
        key=lambda item: (
            int(item["days_until"]),
            int(item["remaining_cents"]),
            -int(item["amount_cents"]),
            str(item["id"]),
        ),
    )
    bill_name = str(candidate["name"])
    amount_cents = int(candidate["amount_cents"])
    due_date = candidate["due_date"]
    days_until = int(candidate["days_until"])
    remaining_cents = int(candidate["remaining_cents"])
    recent_charge = _recent_discretionary_charge(conn, today=today)
    days_text = f"in {days_until} day{'s' if days_until != 1 else ''}"
    hold_until = due_date.isoformat()
    action_params = {
        "scope": "discretionary",
        "hold_until": hold_until,
        "reason": f"Hold discretionary spending until {bill_name} clears",
        "bill_name": bill_name,
        "bill_amount_cents": amount_cents,
        "due_date": hold_until,
        "target_balance_after_cents": remaining_cents,
    }

    return Intervention(
        pattern_id="C-2",
        move=Move.WARN,
        tiers=(1,),
        priority=Priority.HIGH,
        headline=(
            f"{bill_name} ({fmt_dollars(cents_to_dollars(amount_cents))}) hits {days_text}. "
            f"After that you'll have {fmt_dollars(cents_to_dollars(remaining_cents))} in checking/savings. "
            f"Worth holding off on {recent_charge} until your next deposit."
        ),
        detail_bullets=(
            f"Current checking/savings balance: {fmt_dollars(cents_to_dollars(current_balance_cents))}",
            f"Projected post-bill balance: {fmt_dollars(cents_to_dollars(remaining_cents))}",
            f"Low-balance warning threshold: {fmt_dollars(cents_to_dollars(_C2_LOW_BALANCE_THRESHOLD_CENTS))}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Set temporary spending freeze",
            tool="set_spending_freeze_flag",
            params=action_params,
            build_stub=False,
        ),
        dollar_impact_cents=max(_C2_LOW_BALANCE_THRESHOLD_CENTS - remaining_cents, 0),
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("C-2"),
    )


@register_pattern(
    id="C-3",
    move=Move.PRESCRIBE,
    tiers=(1,),
    cooldown=timedelta(days=30),
    tool="budget_reallocate",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.DEVELOP, CFPProcessStep.IMPLEMENT),
)
def evaluate_c3_discretionary_cliff(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    today = ctx.now.date()
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    days_left = days_in_month - today.day
    if days_left < _C3_MIN_DAYS_LEFT:
        return None

    month_start = today.replace(day=1).isoformat()
    today_iso = today.isoformat()
    days_elapsed = max(today.day, 1)
    status_rows = _personal_monthly_budget_status(
        conn,
        month_start=month_start,
        today=today_iso,
    )

    underused_rows = [
        row
        for row in status_rows
        if int(row["remaining_cents"]) > 0
        and Decimal(int(row["remaining_cents"])) / Decimal(int(row["budget_cents"])) >= _C3_MIN_ROOM_RATIO
    ]
    if not underused_rows:
        return None

    candidates: list[dict[str, object]] = []
    for row in status_rows:
        budget_cents = int(row["budget_cents"])
        actual_cents = int(row["actual_cents"])
        if actual_cents < budget_cents:
            continue
        projected_cents = int(
            (Decimal(actual_cents) / Decimal(days_elapsed) * Decimal(days_in_month)).quantize(
                Decimal("1"),
                rounding=ROUND_HALF_UP,
            )
        )
        suggested_cents = max(projected_cents - budget_cents, actual_cents - budget_cents)
        if suggested_cents <= 0:
            continue
        funding_rows = [
            underused
            for underused in underused_rows
            if underused["category_id"] != row["category_id"]
            and int(underused["remaining_cents"]) >= suggested_cents
        ]
        if not funding_rows:
            continue
        funding_row = min(
            funding_rows,
            key=lambda item: (
                int(item["remaining_cents"]),
                str(item["category_name"]),
            ),
        )
        candidates.append(
            {
                "full": row,
                "funding": funding_row,
                "projected_cents": projected_cents,
                "suggested_cents": suggested_cents,
            }
        )

    if not candidates:
        return None

    candidate = max(
        candidates,
        key=lambda item: (
            int(item["suggested_cents"]),
            int(item["projected_cents"]),
            str(item["full"]["category_name"]),
        ),
    )
    full = candidate["full"]
    funding = candidate["funding"]
    suggested_cents = int(candidate["suggested_cents"])
    full_category = str(full["category_name"])
    funding_category = str(funding["category_name"])
    month_name = today.strftime("%B")

    return Intervention(
        pattern_id="C-3",
        move=Move.PRESCRIBE,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"Your {full_category} is fully spent with {days_left} days left. "
            f"Pulling {fmt_dollars(cents_to_dollars(suggested_cents))} from {funding_category} "
            f"(which has {fmt_dollars(cents_to_dollars(int(funding['remaining_cents'])))} to spare) "
            f"buys you the rest of {month_name}."
        ),
        detail_bullets=(
            f"{full_category} budget: {fmt_dollars(cents_to_dollars(int(full['budget_cents'])))}",
            f"{full_category} month-to-date spend: {fmt_dollars(cents_to_dollars(int(full['actual_cents'])))}",
            f"Projected {full_category} spend: {fmt_dollars(cents_to_dollars(int(candidate['projected_cents'])))}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label=f"Move budget room to {full_category}",
            tool="budget_reallocate",
            params={
                "from_category": funding_category,
                "to_category": full_category,
                "amount": cents_to_dollars(suggested_cents),
                "period": "monthly",
                "view": "personal",
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=0,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("C-3"),
    )


@register_pattern(
    id="C-1",
    move=Move.WARN,
    tiers=(1, 4),
    priority=Priority.HIGH,
    tool="spending_trends",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
)
def evaluate_c1_forward_burn(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    today = ctx.now.date()
    month_start = today.replace(day=1).isoformat()
    today_iso = today.isoformat()
    row = conn.execute(
        f"""
        SELECT COALESCE(SUM(ABS(t.amount_cents)), 0) AS mtd_spend_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE {expense_filter_clause(use_type="Personal")}
           AND t.date >= ?
           AND t.date <= ?
        """,
        (month_start, today_iso),
    ).fetchone()
    mtd_spend_cents = int(row["mtd_spend_cents"] or 0)
    if mtd_spend_cents <= 0:
        return None

    days_elapsed = max(today.day, 1)
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    remaining_days = max(days_in_month - days_elapsed, 0)
    mtd_daily_rate = Decimal(mtd_spend_cents) / Decimal(days_elapsed)
    projected_month_spend_cents = int(
        (mtd_daily_rate * Decimal(days_in_month)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )
    trailing_personal_avg_cents = trailing_avg_expenses_cents(conn, 6, as_of=today, use_type="Personal")
    if projected_month_spend_cents <= int(trailing_personal_avg_cents * 1.15):
        return None

    current_cash_cents = _current_liquid_cash_cents(conn)
    projected_end_balance_cents = current_cash_cents - int(
        (mtd_daily_rate * Decimal(remaining_days)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )
    if projected_end_balance_cents >= 0:
        return None

    if mtd_daily_rate <= 0:
        return None
    burn_days = max(0, int(Decimal(current_cash_cents) / mtd_daily_rate)) if current_cash_cents > 0 else 0
    burn_date = (today + timedelta(days=burn_days)).isoformat()
    overshoot_cents = max(projected_month_spend_cents - trailing_personal_avg_cents, 0)
    shortfall_cents = abs(min(projected_end_balance_cents, 0))
    action_params = {
        "months": 1,
        "view": "personal",
        "categories": _current_month_expense_categories(conn, today=today),
    }

    return Intervention(
        pattern_id="C-1",
        move=Move.WARN,
        tiers=(1, 4),
        priority=Priority.HIGH,
        headline=f"At this run rate you'll burn through your buffer by {burn_date}. Worth pulling back this week.",
        detail_bullets=(
            f"Projected month spend: {fmt_dollars(cents_to_dollars(projected_month_spend_cents))}",
            f"Trailing 6-month average: {fmt_dollars(cents_to_dollars(ctx.trailing_6mo_avg_expense_cents))}",
            f"Projected month-end cash: {fmt_dollars(cents_to_dollars(projected_end_balance_cents))}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Show current-month spending trends",
            tool="spending_trends",
            params=action_params,
            build_stub=False,
        ),
        dollar_impact_cents=max(overshoot_cents, shortfall_cents),
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("C-1"),
        goal_ladder_delta_cents=-max(overshoot_cents, shortfall_cents),
    )


@register_pattern(
    id="C-4",
    move=Move.DIAGNOSE,
    tiers=(1, 4),
    tool="spending_trends",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
)
def evaluate_c4_income_vs_expense_mtd(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    today = ctx.now.date()
    month_start = today.replace(day=1).isoformat()
    today_iso = today.isoformat()
    expense_row = conn.execute(
        f"""
        SELECT COALESCE(SUM(ABS(t.amount_cents)), 0) AS expense_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE {expense_filter_clause(use_type="Personal")}
           AND t.date >= ?
           AND t.date <= ?
        """,
        (month_start, today_iso),
    ).fetchone()
    income_row = conn.execute(
        """
        SELECT COALESCE(SUM(t.amount_cents), 0) AS income_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.amount_cents > 0
           AND t.is_payment = 0
           AND t.is_active = 1
           AND (t.use_type = 'Personal' OR t.use_type IS NULL)
           AND c.is_income = 1
           AND t.date >= ?
           AND t.date <= ?
        """,
        (month_start, today_iso),
    ).fetchone()
    expense_cents = int(expense_row["expense_cents"] or 0)
    income_cents = int(income_row["income_cents"] or 0)
    delta_cents = expense_cents - income_cents
    if delta_cents <= 0:
        return None
    action_params = {
        "months": 1,
        "view": "personal",
        "categories": _current_month_expense_categories(conn, today=today),
    }

    return Intervention(
        pattern_id="C-4",
        move=Move.DIAGNOSE,
        tiers=(1, 4),
        priority=Priority.MEDIUM,
        headline=(
            f"You've spent {fmt_dollars(cents_to_dollars(expense_cents))} against "
            f"{fmt_dollars(cents_to_dollars(income_cents))} of income so far this month. "
            f"Over by {fmt_dollars(cents_to_dollars(delta_cents))} and trending into savings."
        ),
        detail_bullets=(
            f"Month-to-date income: {fmt_dollars(cents_to_dollars(income_cents))}",
            f"Month-to-date expenses: {fmt_dollars(cents_to_dollars(expense_cents))}",
            f"Current MTD gap: {fmt_dollars(cents_to_dollars(delta_cents))}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Show current-month spending trends",
            tool="spending_trends",
            params=action_params,
            build_stub=False,
        ),
        dollar_impact_cents=delta_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("C-4"),
        goal_ladder_delta_cents=-delta_cents,
    )


@register_pattern(
    id="C-6",
    move=Move.PATTERN_CATCH,
    tiers=(1,),
    priority=Priority.HIGH,
    cooldown=timedelta(days=30),
    tool="set_low_balance_alert",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT, CFPProcessStep.MONITOR),
)
def evaluate_c6_late_deposit_overdraft(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_c6_candidate(conn, as_of=ctx.now.date())
    if candidate is None:
        return None

    fees = fmt_dollars(cents_to_dollars(candidate.fee_total_cents))
    buffer = fmt_dollars(cents_to_dollars(_C6_BUFFER_THRESHOLD_CENTS))
    if candidate.one_day_late_deposit_count == 1:
        deposit_sentence = "1 was a one-day-late deposit."
    else:
        deposit_sentence = f"{candidate.one_day_late_deposit_count} were one-day-late deposits."
    return Intervention(
        pattern_id="C-6",
        move=Move.PATTERN_CATCH,
        tiers=(1,),
        priority=Priority.HIGH,
        headline=(
            f"You paid {fees} in {candidate.fee_count} overdraft fees in the last 3 months. "
            f"{deposit_sentence} "
            f"A {buffer} buffer alert on {candidate.account_label} catches this before the fee."
        ),
        detail_bullets=(
            f"Account: {candidate.account_label}",
            f"Latest overdraft fee: {candidate.latest_fee_date.isoformat()}",
            f"Low-balance alert threshold: {buffer}",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Set overdraft buffer alert",
            tool="set_low_balance_alert",
            params={
                "account_id": candidate.account_id,
                "threshold_cents": _C6_BUFFER_THRESHOLD_CENTS,
                "channel": "telegram",
                "cooldown_hours": 24,
                "label": f"{candidate.account_label} overdraft buffer alert",
                "dry_run": False,
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.fee_total_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("C-6"),
    )


@register_pattern(
    id="C-5",
    move=Move.DIAGNOSE,
    tiers=(1, 4),
    cooldown=timedelta(days=90),
    tool="goal_status",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES,),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.MONITOR),
)
def evaluate_c5_buffer_health(conn: sqlite3.Connection, ctx: InterventionContext) -> Intervention | None:
    current_liquid_cents = _current_liquid_cash_cents(conn)
    target_cents = ctx.trailing_3mo_avg_expense_cents * 3
    if target_cents <= 0 or current_liquid_cents >= target_cents:
        return None

    income_rows = income_by_stream(conn, months=3, as_of=ctx.now.date())
    trailing_3mo_income_cents = sum(int(row["total_cents"]) for row in income_rows)
    monthly_savings_cents = int(
        (Decimal(trailing_3mo_income_cents - (ctx.trailing_3mo_avg_expense_cents * 3)) / Decimal(3)).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP,
        )
    )
    runway_months = (
        Decimal(current_liquid_cents) / Decimal(ctx.trailing_3mo_avg_expense_cents)
        if ctx.trailing_3mo_avg_expense_cents > 0
        else None
    )
    if monthly_savings_cents > 0:
        months_to_target = Decimal(target_cents - current_liquid_cents) / Decimal(monthly_savings_cents)
        headline = (
            f"Your 3-month emergency fund target is {fmt_dollars(cents_to_dollars(target_cents))}. "
            f"You have {fmt_dollars(cents_to_dollars(current_liquid_cents))}. "
            f"At your current saving pace you hit it in {_month_text(months_to_target)} months. "
            "{accelerator_suggestion}"
        )
    else:
        needed_monthly_cents = max(ctx.trailing_3mo_avg_expense_cents - max(monthly_savings_cents, 0), 0)
        headline = (
            f"Your 3-month emergency fund target is {fmt_dollars(cents_to_dollars(target_cents))}. "
            f"You have {fmt_dollars(cents_to_dollars(current_liquid_cents))}. "
            f"At current pace you're not gaining ground - worth finding "
            f"{fmt_dollars(cents_to_dollars(needed_monthly_cents))}/mo. "
            "{accelerator_suggestion}"
        )

    if any(goal.target_cents is not None for goal in ctx.goals):
        action = InterventionAction(
            label="Show goal status",
            tool="goal_status",
            params={},
            build_stub=False,
        )
    else:
        action = InterventionAction(
            label="Create emergency fund goal",
            tool="goal_set",
            params={
                "name": "3-month emergency fund",
                "metric": "liquid_cash",
                "target_cents": target_cents,
                "direction": "up",
            },
            build_stub=False,
        )

    detail_bullets = [
        f"Target gap: {fmt_dollars(cents_to_dollars(target_cents - current_liquid_cents))}",
    ]
    if runway_months is not None:
        detail_bullets.append(
            f"Current runway: {_month_text(runway_months)} months at your trailing expense pace"
        )
    detail_bullets.append(
        f"Monthly saving pace: {fmt_dollars(cents_to_dollars(monthly_savings_cents))}"
    )

    return Intervention(
        pattern_id="C-5",
        move=Move.DIAGNOSE,
        tiers=(1, 4),
        priority=Priority.MEDIUM,
        headline=headline,
        detail_bullets=tuple(detail_bullets),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=action,
        dollar_impact_cents=max(monthly_savings_cents, 0),
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("C-5"),
    )
