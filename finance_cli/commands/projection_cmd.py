"""Net worth projection command."""

from __future__ import annotations

import sqlite3
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from ..debt_calculator import load_debt_cards, project_interest
from ..models import cents_to_dollars
from .common import fmt_dollars


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser(
        "projection", parents=[format_parent], help="Net worth projection"
    )
    parser.add_argument("--months", type=int, default=12, help="Projection horizon in months")
    parser.set_defaults(func=handle_projection, command_name="projection")


def _query_balances(conn: sqlite3.Connection) -> dict[str, int]:
    """Query current balances by type, excluding aliases."""
    rows = conn.execute(
        """
        SELECT account_type,
               COALESCE(SUM(balance_current_cents), 0) AS total_cents
          FROM accounts a
         WHERE a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
         GROUP BY account_type
        """
    ).fetchall()

    liquid_cents = 0
    investment_cents = 0
    credit_card_debt_cents = 0
    loan_debt_cents = 0

    for row in rows:
        acct_type = str(row["account_type"] or "")
        total = int(row["total_cents"] or 0)
        if acct_type in {"checking", "savings"}:
            liquid_cents += total
        elif acct_type == "investment":
            investment_cents += total
        elif acct_type == "credit_card":
            credit_card_debt_cents += abs(total)
        elif acct_type == "loan":
            loan_debt_cents += abs(total)

    return {
        "liquid_cents": liquid_cents,
        "investment_cents": investment_cents,
        "credit_card_debt_cents": credit_card_debt_cents,
        "loan_debt_cents": loan_debt_cents,
    }


def _query_avg_income_expenses(conn: sqlite3.Connection) -> dict[str, int]:
    """Compute 3-month average income and expenses using complete calendar months."""
    rows = conn.execute(
        """
        SELECT strftime('%Y-%m', t.date) AS month,
               COALESCE(SUM(CASE WHEN t.amount_cents > 0 THEN t.amount_cents ELSE 0 END), 0) AS income_cents,
               COALESCE(SUM(CASE WHEN t.amount_cents < 0 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS expense_cents
          FROM transactions t
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.date >= date('now', 'start of month', '-3 months')
           AND t.date < date('now', 'start of month')
         GROUP BY strftime('%Y-%m', t.date)
         ORDER BY month
        """
    ).fetchall()

    if not rows:
        return {"avg_income_cents": 0, "avg_expense_cents": 0, "months_with_data": 0}

    total_income = sum(int(r["income_cents"]) for r in rows)
    total_expense = sum(int(r["expense_cents"]) for r in rows)
    n = len(rows)

    return {
        "avg_income_cents": int(total_income / n) if n else 0,
        "avg_expense_cents": int(total_expense / n) if n else 0,
        "months_with_data": n,
    }


def _milestone_months(max_months: int) -> list[int]:
    """Return milestone month numbers up to max_months."""
    milestones = [0, 3, 6, 12]
    return [m for m in milestones if m <= max_months]


def handle_projection(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Project net worth forward using current trends."""
    months = max(1, int(getattr(args, "months", 12) or 12))

    balances = _query_balances(conn)
    avg_flow = _query_avg_income_expenses(conn)

    liquid_cents = balances["liquid_cents"]
    investment_cents = balances["investment_cents"]
    credit_card_debt_cents = balances["credit_card_debt_cents"]
    loan_debt_cents = balances["loan_debt_cents"]

    avg_income_cents = avg_flow["avg_income_cents"]
    avg_expense_cents = avg_flow["avg_expense_cents"]
    net_savings_cents = avg_income_cents - avg_expense_cents

    # Debt trajectory: use project_interest for credit cards
    cards = load_debt_cards(conn, include_zero_balance=False)
    debt_schedule: dict[int, int] = {}
    if cards:
        proj = project_interest(cards, months=months, summary_only=True)
        for entry in proj.get("schedule", []):
            debt_schedule[int(entry["month"])] = int(entry["remaining_balance_cents"])

    # Investment growth: 7% annual, Decimal math
    monthly_rate = Decimal("7") / Decimal("100") / Decimal("12")

    milestones = _milestone_months(months)
    projection_rows: list[dict[str, Any]] = []

    for m in milestones:
        if m == 0:
            proj_liquid = liquid_cents
            proj_investments = investment_cents
            proj_cc_debt = credit_card_debt_cents
            proj_loan_debt = loan_debt_cents
        else:
            proj_liquid = liquid_cents + net_savings_cents * m
            proj_investments = int(
                (Decimal(investment_cents) * (1 + monthly_rate) ** m).quantize(
                    Decimal("1"), ROUND_HALF_UP
                )
            )
            proj_cc_debt = debt_schedule.get(m, credit_card_debt_cents)
            proj_loan_debt = loan_debt_cents  # held constant

        proj_net_worth = proj_liquid + proj_investments - proj_cc_debt - proj_loan_debt

        projection_rows.append({
            "month": m,
            "liquid_cash_cents": proj_liquid,
            "investments_cents": proj_investments,
            "credit_card_debt_cents": proj_cc_debt,
            "loan_debt_cents": proj_loan_debt,
            "net_worth_cents": proj_net_worth,
        })

    data = {
        "months": months,
        "current": {
            "liquid_cash_cents": liquid_cents,
            "investment_cents": investment_cents,
            "credit_card_debt_cents": credit_card_debt_cents,
            "loan_debt_cents": loan_debt_cents,
            "net_worth_cents": liquid_cents + investment_cents - credit_card_debt_cents - loan_debt_cents,
        },
        "avg_income_cents": avg_income_cents,
        "avg_expense_cents": avg_expense_cents,
        "net_savings_cents": net_savings_cents,
        "months_with_data": avg_flow["months_with_data"],
        "projection": projection_rows,
    }

    cli_report = _build_cli_report(data)

    return {
        "data": data,
        "summary": {
            "months": months,
            "current_net_worth": cents_to_dollars(data["current"]["net_worth_cents"]),
            "net_savings_monthly": cents_to_dollars(net_savings_cents),
        },
        "cli_report": cli_report,
    }


def _build_cli_report(data: dict[str, Any]) -> str:
    months = data["months"]
    current = data["current"]
    net_savings = data["net_savings_cents"]

    lines = [
        f"Net Worth Projection ({months} months)",
        "=" * 40,
        "",
        f"Current:                    {fmt_dollars(cents_to_dollars(current['net_worth_cents']))}",
        f"Monthly Net Savings:        {fmt_dollars(cents_to_dollars(net_savings))} (3-month avg)",
        "",
        f"{'Month':>5}    {'Liquid Cash':>12}    {'Investments':>12}    {'Debt':>12}    {'Net Worth':>12}",
        "-" * 70,
    ]

    for row in data["projection"]:
        m = row["month"]
        label = "Now" if m == 0 else str(m)
        total_debt = row["credit_card_debt_cents"] + row["loan_debt_cents"]
        lines.append(
            f"{label:>5}    "
            f"{fmt_dollars(cents_to_dollars(row['liquid_cash_cents'])):>12}    "
            f"{fmt_dollars(cents_to_dollars(row['investments_cents'])):>12}    "
            f"{fmt_dollars(cents_to_dollars(total_debt)):>12}    "
            f"{fmt_dollars(cents_to_dollars(row['net_worth_cents'])):>12}"
        )

    lines.extend([
        "",
        "Note: Projection uses 3-month avg cash flow, 7% annual investment growth,",
        "      and minimum-only debt payments. Actual results will vary.",
        "      Debt projection covers credit cards only; loans held constant.",
    ])

    return "\n".join(lines)
