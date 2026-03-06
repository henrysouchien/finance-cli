"""Financial health summary command."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..models import cents_to_dollars
from .common import fmt_dollars, use_type_filter


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("summary", parents=[format_parent], help="Financial health dashboard")
    parser.add_argument("--view", choices=["personal", "business", "all"], default="all")
    parser.set_defaults(func=handle_summary, command_name="summary")


def _query_balances(conn: sqlite3.Connection) -> dict[str, int]:
    """Query account balances by type, excluding aliases."""
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
    asset_cents = 0
    liability_cents = 0

    for row in rows:
        acct_type = str(row["account_type"] or "")
        total = int(row["total_cents"] or 0)
        if acct_type in {"checking", "savings"}:
            liquid_cents += total
            asset_cents += total
        elif acct_type == "investment":
            investment_cents += total
            asset_cents += total
        elif acct_type in {"credit_card", "loan"}:
            liability_cents += abs(total)
        else:
            # Unknown types counted as assets if positive
            if total > 0:
                asset_cents += total

    return {
        "liquid_cash_cents": liquid_cents,
        "investment_cents": investment_cents,
        "asset_cents": asset_cents,
        "liability_cents": liability_cents,
        "net_worth_cents": asset_cents - liability_cents,
    }


def _query_cash_flow(conn: sqlite3.Connection, view: str) -> dict[str, int]:
    """Query 30-day income and expenses."""
    ut_filter = use_type_filter(view)
    row = conn.execute(
        f"""
        SELECT COALESCE(SUM(CASE WHEN t.amount_cents > 0 THEN t.amount_cents ELSE 0 END), 0) AS income_cents,
               COALESCE(SUM(CASE WHEN t.amount_cents < 0 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS expense_cents
          FROM transactions t
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.date >= date('now', '-30 days')
           AND t.date <= date('now')
           {ut_filter}
        """
    ).fetchone()
    return {
        "income_30d_cents": int(row["income_cents"]),
        "expense_30d_cents": int(row["expense_cents"]),
    }


def _query_recurring_obligations(conn: sqlite3.Connection) -> int:
    """Sum active recurring expense flows normalized to monthly."""
    rows = conn.execute(
        """
        SELECT amount_cents, frequency
          FROM recurring_flows
         WHERE is_active = 1
           AND flow_type = 'expense'
        """
    ).fetchall()
    total = 0
    for row in rows:
        total += _month_equivalent(int(row["amount_cents"]), row["frequency"])
    return total


def _month_equivalent(amount_cents: int, frequency: str) -> int:
    if frequency == "weekly":
        return int(round(amount_cents * 52 / 12))
    if frequency == "biweekly":
        return int(round(amount_cents * 26 / 12))
    if frequency == "monthly":
        return amount_cents
    if frequency == "quarterly":
        return int(round(amount_cents / 3))
    if frequency == "yearly":
        return int(round(amount_cents / 12))
    return amount_cents


def _query_subscriptions(conn: sqlite3.Connection) -> int:
    """Sum active subscriptions normalized to monthly."""
    rows = conn.execute(
        """
        SELECT amount_cents, frequency
          FROM subscriptions
         WHERE is_active = 1
        """
    ).fetchall()
    total = 0
    for row in rows:
        total += _month_equivalent(int(row["amount_cents"]), row["frequency"] or "monthly")
    return total


def _query_debt_minimums(conn: sqlite3.Connection) -> int:
    """Sum liability minimum payments."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(COALESCE(minimum_payment_cents, next_monthly_payment_cents, 0)), 0) AS total_cents
          FROM liabilities
         WHERE is_active = 1
        """
    ).fetchone()
    return int(row["total_cents"])


def _query_data_health(conn: sqlite3.Connection) -> dict[str, Any]:
    """Count unreviewed and uncategorized transactions, plus freshness."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(CASE WHEN is_reviewed = 0 THEN 1 ELSE 0 END), 0) AS unreviewed,
               COALESCE(SUM(CASE WHEN category_id IS NULL THEN 1 ELSE 0 END), 0) AS uncategorized
          FROM transactions
         WHERE is_active = 1
        """
    ).fetchone()

    freshness = conn.execute(
        """
        SELECT MAX(date) AS latest_transaction_date
          FROM transactions
         WHERE is_active = 1
        """
    ).fetchone()

    balance_row = conn.execute(
        """
        SELECT MAX(balance_updated_at) AS latest_balance_refresh
          FROM accounts
         WHERE is_active = 1
           AND id NOT IN (SELECT hash_account_id FROM account_aliases)
        """
    ).fetchone()

    return {
        "unreviewed": int(row["unreviewed"]),
        "uncategorized": int(row["uncategorized"]),
        "latest_transaction_date": freshness["latest_transaction_date"] if freshness else None,
        "latest_balance_refresh": balance_row["latest_balance_refresh"] if balance_row else None,
    }


def _build_cli_report(data: dict[str, Any]) -> str:
    W = 26
    lines = ["Financial Health Summary", "=" * 40, ""]

    lines.append(f"  {'Net Worth:':<{W}s} {fmt_dollars(data['net_worth']):>12s}")
    lines.append(f"  {'Assets:':<{W}s} {fmt_dollars(data['assets']):>12s}")
    lines.append(f"  {'Liabilities:':<{W}s} {fmt_dollars(data['total_debt']):>12s}")
    lines.append("")

    lines.append("Cash & Investments:")
    lines.append(f"  {'Liquid Cash:':<{W}s} {fmt_dollars(data['liquid_cash']):>12s}")
    lines.append(f"  {'Investments:':<{W}s} {fmt_dollars(data['investments']):>12s}")
    lines.append(f"  {'Total Debt:':<{W}s} {fmt_dollars(data['total_debt']):>12s}")
    lines.append("")

    lines.append("Monthly Cash Flow (30d):")
    lines.append(f"  {'Income:':<{W}s} {fmt_dollars(data['income_30d']):>12s}")
    lines.append(f"  {'Expenses:':<{W}s} {fmt_dollars(data['expense_30d']):>12s}")
    sr = data.get("savings_rate")
    sr_str = f"{sr * 100:.0f}%" if sr is not None else "N/A"
    lines.append(f"  {'Savings Rate:':<{W}s} {sr_str:>12s}")
    lines.append("")

    lines.append("Risk Metrics:")
    dti = data.get("debt_to_income")
    dti_str = f"{dti:.1f}x" if dti is not None else "N/A"
    lines.append(f"  {'Debt-to-Income:':<{W}s} {dti_str:>12s}")
    em = data.get("emergency_fund_months")
    em_str = f"{em:.1f} months" if em is not None else "N/A"
    lines.append(f"  {'Emergency Fund:':<{W}s} {em_str:>12s}")
    lines.append("")

    lines.append(f"Fixed Monthly Obligations: {fmt_dollars(data['fixed_obligations'])}")
    lines.append(f"  {'Recurring Flows:':<{W}s} {fmt_dollars(data['recurring_flows']):>12s}")
    lines.append(f"  {'Debt Minimums:':<{W}s} {fmt_dollars(data['debt_minimums']):>12s}")
    lines.append(f"  {'Subscriptions:':<{W}s} {fmt_dollars(data['subscriptions']):>12s}")
    lines.append("")

    lines.append("Data Health:")
    lines.append(f"  {'Unreviewed:':<{W}s} {data['unreviewed']:>12d}")
    lines.append(f"  {'Uncategorized:':<{W}s} {data['uncategorized']:>12d}")

    ltd = data.get("latest_transaction_date")
    if ltd:
        lines.append(f"  {'Latest Transaction:':<{W}s} {ltd:>12s}")
    lbr = data.get("latest_balance_refresh")
    if lbr:
        lines.append(f"  {'Last Balance Refresh:':<{W}s} {str(lbr)[:10]:>12s}")

    return "\n".join(lines)


def handle_summary(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Financial health dashboard."""
    view = getattr(args, "view", "all")

    balances = _query_balances(conn)
    cash_flow = _query_cash_flow(conn, view)
    recurring_cents = _query_recurring_obligations(conn)
    subs_cents = _query_subscriptions(conn)
    debt_min_cents = _query_debt_minimums(conn)
    health = _query_data_health(conn)

    income_30d = cash_flow["income_30d_cents"]
    expense_30d = cash_flow["expense_30d_cents"]

    # Computed metrics with zero-denominator guards
    savings_rate = None
    if income_30d > 0:
        savings_rate = (income_30d - expense_30d) / income_30d

    debt_to_income = None
    if income_30d > 0:
        annual_income = income_30d * 12
        debt_to_income = balances["liability_cents"] / annual_income

    emergency_fund_months = None
    if expense_30d > 0:
        emergency_fund_months = balances["liquid_cash_cents"] / expense_30d

    fixed_obligations_cents = recurring_cents + debt_min_cents + subs_cents

    data = {
        "net_worth": cents_to_dollars(balances["net_worth_cents"]),
        "net_worth_cents": balances["net_worth_cents"],
        "assets": cents_to_dollars(balances["asset_cents"]),
        "assets_cents": balances["asset_cents"],
        "total_debt": cents_to_dollars(balances["liability_cents"]),
        "total_debt_cents": balances["liability_cents"],
        "liquid_cash": cents_to_dollars(balances["liquid_cash_cents"]),
        "liquid_cash_cents": balances["liquid_cash_cents"],
        "investments": cents_to_dollars(balances["investment_cents"]),
        "investments_cents": balances["investment_cents"],
        "income_30d": cents_to_dollars(income_30d),
        "income_30d_cents": income_30d,
        "expense_30d": cents_to_dollars(expense_30d),
        "expense_30d_cents": expense_30d,
        "savings_rate": savings_rate,
        "debt_to_income": debt_to_income,
        "emergency_fund_months": emergency_fund_months,
        "recurring_flows": cents_to_dollars(recurring_cents),
        "recurring_flows_cents": recurring_cents,
        "debt_minimums": cents_to_dollars(debt_min_cents),
        "debt_minimums_cents": debt_min_cents,
        "subscriptions": cents_to_dollars(subs_cents),
        "subscriptions_cents": subs_cents,
        "fixed_obligations": cents_to_dollars(fixed_obligations_cents),
        "fixed_obligations_cents": fixed_obligations_cents,
        "unreviewed": health["unreviewed"],
        "uncategorized": health["uncategorized"],
        "latest_transaction_date": health["latest_transaction_date"],
        "latest_balance_refresh": health["latest_balance_refresh"],
    }

    return {
        "data": data,
        "summary": {
            "net_worth": data["net_worth"],
            "savings_rate": savings_rate,
            "fixed_obligations": data["fixed_obligations"],
        },
        "cli_report": _build_cli_report(data),
    }
