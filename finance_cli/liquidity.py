"""Liquidity and cash-flow projection helpers."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

from .subscriptions import subscription_burn

LOOKBACK_WINDOW_DAYS = 90
DAYS_PER_MONTH = 30


def _use_type_filter_sql(view: str) -> str:
    if view == "business":
        return "AND t.use_type = 'Business'"
    if view == "personal":
        return "AND (t.use_type = 'Personal' OR t.use_type IS NULL)"
    return ""


def _month_equivalent_from_recurring(amount_cents: int, frequency: str) -> int:
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


def liquidity_snapshot(
    conn: sqlite3.Connection,
    forecast_days: int = 90,
    include_investments: bool = False,
    view: str = "all",
) -> dict:
    """Compute historical + projected liquidity snapshot.

    Output mixes:
    - transaction-derived trend metrics (90-day net, recurring, subscriptions)
    - account-balance state (liquid cash, credit/loan owed)
    - near-term liability obligations (`liabilities.next_payment_due_date`)

    See `docs/overview/PROJECT_GUIDE.md` and `docs/architecture/DESIGN.md`
    for command-level expectations.
    """
    today = date.today()
    start_90 = (today - timedelta(days=LOOKBACK_WINDOW_DAYS)).isoformat()
    end = today.isoformat()

    account_filter = ""
    if not include_investments:
        account_filter = " AND (a.account_type IS NULL OR a.account_type != 'investment')"
    use_type_filter = _use_type_filter_sql(view)

    net_row = conn.execute(
        f"""
        SELECT COALESCE(SUM(t.amount_cents), 0) AS net_cents,
               COALESCE(SUM(CASE WHEN t.amount_cents > 0 THEN t.amount_cents ELSE 0 END), 0) AS income_cents,
               COALESCE(SUM(CASE WHEN t.amount_cents < 0 THEN -t.amount_cents ELSE 0 END), 0) AS expense_cents
         FROM transactions t
          LEFT JOIN accounts a ON a.id = t.account_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date <= ?
           {use_type_filter}
           {account_filter}
        """,
        (start_90, end),
    ).fetchone()

    net_90 = int(net_row["net_cents"])
    avg_daily_net = net_90 / LOOKBACK_WINDOW_DAYS

    subs = subscription_burn(conn, view=view)
    recurring_rows = conn.execute(
        """
        SELECT flow_type, amount_cents, frequency
          FROM recurring_flows
         WHERE is_active = 1
        """
    ).fetchall()

    recurring_monthly_cents = 0
    for row in recurring_rows:
        month_eq = _month_equivalent_from_recurring(int(row["amount_cents"]), row["frequency"])
        if row["flow_type"] == "income":
            recurring_monthly_cents += month_eq
        else:
            recurring_monthly_cents -= month_eq

    recurring_daily_cents = recurring_monthly_cents / DAYS_PER_MONTH
    subs_daily_cents = -(subs["monthly_burn_cents"] / DAYS_PER_MONTH)

    projected_net_cents = int(round((avg_daily_net + recurring_daily_cents + subs_daily_cents) * forecast_days))

    balance_rows = conn.execute(
        """
        SELECT account_type,
               COALESCE(SUM(balance_current_cents), 0) AS total_balance_cents,
               COALESCE(SUM(CASE WHEN balance_current_cents IS NOT NULL THEN 1 ELSE 0 END), 0) AS accounts_with_balance
          FROM accounts
         WHERE is_active = 1
           AND id NOT IN (SELECT hash_account_id FROM account_aliases)
         GROUP BY account_type
        """
    ).fetchall()

    has_real_balances = any(int(row["accounts_with_balance"]) > 0 for row in balance_rows)
    liquid_balance_cents = 0
    credit_owed_cents = 0
    for row in balance_rows:
        account_type = str(row["account_type"] or "")
        total = int(row["total_balance_cents"] or 0)
        if account_type in {"checking", "savings"}:
            liquid_balance_cents += total
        # Credit-card and loan balances are treated as obligations in the
        # liquidity view, regardless of source sign conventions.
        if account_type in {"credit_card", "loan"}:
            credit_owed_cents += abs(total)

    liability_window_end = (today + timedelta(days=int(forecast_days))).isoformat()
    liability_row = conn.execute(
        """
        SELECT COALESCE(SUM(COALESCE(minimum_payment_cents, next_monthly_payment_cents, 0)), 0) AS due_cents,
               COUNT(*) AS due_count
          FROM liabilities
         WHERE is_active = 1
           AND next_payment_due_date IS NOT NULL
           AND next_payment_due_date >= ?
           AND next_payment_due_date <= ?
        """,
        (today.isoformat(), liability_window_end),
    ).fetchone()

    return {
        "window_days": LOOKBACK_WINDOW_DAYS,
        "forecast_days": int(forecast_days),
        "include_investments": bool(include_investments),
        "income_90d_cents": int(net_row["income_cents"]),
        "expense_90d_cents": int(net_row["expense_cents"]),
        "net_90d_cents": net_90,
        "avg_daily_net_cents": avg_daily_net,
        "subscriptions_monthly_burn_cents": subs["monthly_burn_cents"],
        "recurring_monthly_net_cents": recurring_monthly_cents,
        "projected_net_cents": projected_net_cents,
        "liquid_balance_cents": int(liquid_balance_cents),
        "credit_owed_cents": int(credit_owed_cents),
        "has_real_balances": bool(has_real_balances),
        "upcoming_liability_payments_cents": int(liability_row["due_cents"]),
        "upcoming_liability_payments_count": int(liability_row["due_count"]),
    }
