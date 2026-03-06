"""Spending trends command."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from ..models import cents_to_dollars
from .common import fmt_dollars, use_type_filter


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("spending", parents=[format_parent], help="Spending analysis")
    spending_sub = parser.add_subparsers(dest="spending_command", required=True)

    p_trends = spending_sub.add_parser("trends", parents=[format_parent], help="Monthly spending trends")
    p_trends.add_argument("--months", type=int, default=6)
    p_trends.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_trends.set_defaults(func=handle_trends, command_name="spending.trends")


def _trend_indicator(last_month_cents: int, prior_avg_cents: float) -> str:
    """Compare last month to average of prior months.

    Returns up-arrow if >110%, down-arrow if <90%, right-arrow otherwise.
    """
    if prior_avg_cents == 0:
        return "\u2192"  # no prior data
    ratio = last_month_cents / prior_avg_cents
    if ratio > 1.10:
        return "\u2191"
    if ratio < 0.90:
        return "\u2193"
    return "\u2192"


def _build_month_keys(months: int) -> list[str]:
    """Return list of YYYY-MM keys for the last N months (inclusive of current)."""
    today = date.today()
    keys: list[str] = []
    for i in range(months - 1, -1, -1):
        # Walk backwards i months from today
        d = today.replace(day=1)
        for _ in range(i):
            d = (d - timedelta(days=1)).replace(day=1)
        key = d.strftime("%Y-%m")
        if key not in keys:
            keys.append(key)
    return keys


def _month_label(ym: str) -> str:
    """Convert YYYY-MM to short month name (e.g. 'Jan')."""
    try:
        parts = ym.split("-")
        month_num = int(parts[1])
        names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        return names[month_num - 1]
    except (IndexError, ValueError):
        return ym


def handle_trends(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Monthly spending trends by category."""
    months = max(1, getattr(args, "months", 6))
    view = getattr(args, "view", "all")
    ut_filter = use_type_filter(view)

    month_keys = _build_month_keys(months)
    if not month_keys:
        return {
            "data": {"months": [], "categories": []},
            "summary": {"total_categories": 0},
            "cli_report": "No data for the requested period.",
        }

    start_date = month_keys[0] + "-01"

    rows = conn.execute(
        f"""
        SELECT c.name AS category,
               strftime('%Y-%m', t.date) AS month,
               COALESCE(SUM(ABS(t.amount_cents)), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND t.date >= ?
           {ut_filter}
         GROUP BY c.name, strftime('%Y-%m', t.date)
         ORDER BY c.name, month
        """,
        (start_date,),
    ).fetchall()

    # Build pivot: category -> {month -> cents}
    pivot: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        cat = row["category"]
        m = row["month"]
        if m in month_keys:
            pivot[cat][m] = int(row["total_cents"])

    # Sort categories by total spend descending
    cat_totals = {cat: sum(months_data.values()) for cat, months_data in pivot.items()}
    sorted_cats = sorted(cat_totals, key=lambda c: cat_totals[c], reverse=True)

    # Build output rows
    categories_out: list[dict[str, Any]] = []
    grand_totals: dict[str, int] = defaultdict(int)

    for cat in sorted_cats:
        month_values = {m: pivot[cat].get(m, 0) for m in month_keys}
        for m, v in month_values.items():
            grand_totals[m] += v

        # Compute trend
        last_month = month_keys[-1]
        last_val = month_values.get(last_month, 0)
        prior_months = [month_values.get(m, 0) for m in month_keys[:-1] if month_values.get(m, 0) > 0]
        prior_avg = sum(prior_months) / len(prior_months) if prior_months else 0
        trend = _trend_indicator(last_val, prior_avg)

        avg_cents = int(round(sum(month_values.values()) / len(month_keys)))

        cat_row = {
            "category": cat,
            "months": {m: cents_to_dollars(v) for m, v in month_values.items()},
            "months_cents": month_values,
            "average": cents_to_dollars(avg_cents),
            "average_cents": avg_cents,
            "trend": trend,
        }
        categories_out.append(cat_row)

    # Grand total row
    grand_last = grand_totals.get(month_keys[-1], 0) if month_keys else 0
    grand_prior = [grand_totals.get(m, 0) for m in month_keys[:-1] if grand_totals.get(m, 0) > 0]
    grand_prior_avg = sum(grand_prior) / len(grand_prior) if grand_prior else 0
    grand_trend = _trend_indicator(grand_last, grand_prior_avg)
    grand_avg = int(round(sum(grand_totals.values()) / len(month_keys))) if month_keys else 0

    data = {
        "months": month_keys,
        "categories": categories_out,
        "totals": {m: cents_to_dollars(grand_totals.get(m, 0)) for m in month_keys},
        "totals_cents": dict(grand_totals),
        "grand_average": cents_to_dollars(grand_avg),
        "grand_trend": grand_trend,
    }

    return {
        "data": data,
        "summary": {"total_categories": len(categories_out), "months": len(month_keys)},
        "cli_report": _build_cli_report(data, month_keys, categories_out, grand_totals, grand_avg, grand_trend),
    }


def _build_cli_report(
    data: dict,
    month_keys: list[str],
    categories: list[dict],
    grand_totals: dict[str, int],
    grand_avg: int,
    grand_trend: str,
) -> str:
    if not categories:
        return "No spending data for the requested period."

    col_w = 9
    cat_w = 28
    labels = [_month_label(m) for m in month_keys]

    header = f"{'':>{cat_w}s}"
    for lbl in labels:
        header += f"{lbl:>{col_w}s}"
    header += f"{'Avg':>{col_w}s}  Trend"

    lines = [f"Spending Trends (last {len(month_keys)} months)", header]

    for cat_row in categories:
        line = f"{cat_row['category'][:cat_w]:<{cat_w}s}"
        for m in month_keys:
            val = cat_row["months_cents"].get(m, 0)
            line += f"{fmt_dollars(cents_to_dollars(val)):>{col_w}s}"
        line += f"{fmt_dollars(cat_row['average']):>{col_w}s}  {cat_row['trend']}"
        lines.append(line)

    # Total row
    sep = "-" * len(lines[1])
    lines.append(sep)
    total_line = f"{'TOTAL':<{cat_w}s}"
    for m in month_keys:
        total_line += f"{fmt_dollars(cents_to_dollars(grand_totals.get(m, 0))):>{col_w}s}"
    total_line += f"{fmt_dollars(cents_to_dollars(grand_avg)):>{col_w}s}  {grand_trend}"
    lines.append(total_line)

    return "\n".join(lines)
