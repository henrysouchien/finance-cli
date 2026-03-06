"""Monthly planning commands."""

from __future__ import annotations

import calendar
import sqlite3
import uuid
from datetime import date
from typing import Any

from ..models import cents_to_dollars


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("plan", parents=[format_parent], help="Monthly planning")
    plan_sub = parser.add_subparsers(dest="plan_command", required=True)

    p_create = plan_sub.add_parser("create", parents=[format_parent], help="Create month plan")
    p_create.add_argument("--month", required=True)
    p_create.set_defaults(func=handle_create, command_name="plan.create")

    p_show = plan_sub.add_parser("show", parents=[format_parent], help="Show month plan")
    p_show.add_argument("--month")
    p_show.set_defaults(func=handle_show, command_name="plan.show")

    p_review = plan_sub.add_parser("review", parents=[format_parent], help="Review current month plan")
    p_review.set_defaults(func=handle_review, command_name="plan.review")


def _month_end(month: str) -> str:
    year, month_number = [int(v) for v in month.split("-")]
    last_day = calendar.monthrange(year, month_number)[1]
    return f"{year:04d}-{month_number:02d}-{last_day:02d}"


def _previous_months(target_month: str, count: int = 3) -> list[str]:
    year, month = [int(v) for v in target_month.split("-")]
    ref = date(year, month, 1)
    months = []
    for i in range(1, count + 1):
        y = ref.year
        m = ref.month - i
        while m <= 0:
            y -= 1
            m += 12
        months.append(f"{y:04d}-{m:02d}")
    return months


def _monthly_actuals(conn: sqlite3.Connection, month: str) -> tuple[int, int]:
    start = f"{month}-01"
    end = _month_end(month)
    row = conn.execute(
        """
        SELECT COALESCE(SUM(CASE WHEN amount_cents > 0 THEN amount_cents ELSE 0 END), 0) AS income_cents,
               COALESCE(SUM(CASE WHEN amount_cents < 0 THEN -amount_cents ELSE 0 END), 0) AS expense_cents
          FROM transactions
         WHERE is_active = 1
           AND is_payment = 0
           AND date >= ?
           AND date <= ?
        """,
        (start, end),
    ).fetchone()
    return int(row["income_cents"]), int(row["expense_cents"])


def _get_plan(conn: sqlite3.Connection, month: str):
    return conn.execute("SELECT * FROM monthly_plans WHERE month = ?", (month,)).fetchone()


def handle_create(args, conn: sqlite3.Connection) -> dict[str, Any]:
    hist_months = _previous_months(args.month, count=3)
    incomes = []
    expenses = []
    for month in hist_months:
        income_cents, expense_cents = _monthly_actuals(conn, month)
        if income_cents or expense_cents:
            incomes.append(income_cents)
            expenses.append(expense_cents)

    if incomes:
        expected_income = int(round(sum(incomes) / len(incomes)))
        expected_expenses = int(round(sum(expenses) / len(expenses)))
    else:
        expected_income = 0
        expected_expenses = 0

    savings_target = max(expected_income - expected_expenses, 0)
    investment_target = int(round(savings_target * 0.25))

    existing = _get_plan(conn, args.month)
    if existing:
        conn.execute(
            """
            UPDATE monthly_plans
               SET expected_income_cents = ?,
                   expected_expenses_cents = ?,
                   savings_target_cents = ?,
                   investment_target_cents = ?
             WHERE month = ?
            """,
            (expected_income, expected_expenses, savings_target, investment_target, args.month),
        )
        plan_id = existing["id"]
        created = False
    else:
        plan_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO monthly_plans (
                id,
                month,
                expected_income_cents,
                expected_expenses_cents,
                savings_target_cents,
                investment_target_cents,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (plan_id, args.month, expected_income, expected_expenses, savings_target, investment_target),
        )
        created = True

    conn.commit()

    return {
        "data": {
            "plan_id": plan_id,
            "month": args.month,
            "created": created,
            "expected_income_cents": expected_income,
            "expected_expenses_cents": expected_expenses,
            "savings_target_cents": savings_target,
            "investment_target_cents": investment_target,
        },
        "summary": {"total_plans": 1},
        "cli_report": f"Plan ready for {args.month}",
    }


def handle_show(args, conn: sqlite3.Connection) -> dict[str, Any]:
    month = args.month or date.today().strftime("%Y-%m")
    row = _get_plan(conn, month)
    if not row:
        raise ValueError(f"Plan for {month} not found")

    plan = dict(row)
    for key in ["expected_income_cents", "expected_expenses_cents", "savings_target_cents", "investment_target_cents"]:
        if plan[key] is not None:
            plan[key.replace("_cents", "")] = cents_to_dollars(int(plan[key]))

    return {
        "data": {"plan": plan},
        "summary": {"total_plans": 1},
        "cli_report": f"Plan {month} loaded",
    }


def handle_review(args, conn: sqlite3.Connection) -> dict[str, Any]:
    month = date.today().strftime("%Y-%m")
    row = _get_plan(conn, month)
    if not row:
        raise ValueError(f"Plan for {month} not found")

    actual_income, actual_expenses = _monthly_actuals(conn, month)
    expected_income = int(row["expected_income_cents"] or 0)
    expected_expenses = int(row["expected_expenses_cents"] or 0)
    expected_savings = int(row["savings_target_cents"] or 0)

    actual_savings = max(actual_income - actual_expenses, 0)

    review = {
        "month": month,
        "expected_income_cents": expected_income,
        "actual_income_cents": actual_income,
        "income_delta_cents": actual_income - expected_income,
        "expected_expenses_cents": expected_expenses,
        "actual_expenses_cents": actual_expenses,
        "expenses_delta_cents": actual_expenses - expected_expenses,
        "expected_savings_cents": expected_savings,
        "actual_savings_cents": actual_savings,
        "savings_delta_cents": actual_savings - expected_savings,
    }

    review_out = dict(review)
    for key, value in list(review.items()):
        if key.endswith("_cents"):
            review_out[key.replace("_cents", "")] = cents_to_dollars(int(value))

    return {
        "data": {"review": review_out},
        "summary": {"month": month},
        "cli_report": f"Savings delta: {review_out['savings_delta']:.2f}",
    }
