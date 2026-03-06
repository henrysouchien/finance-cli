"""Liability reporting commands."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..models import cents_to_dollars
from .common import fmt_dollars


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("liability", parents=[format_parent], help="Liability reporting")
    liab_sub = parser.add_subparsers(dest="liability_command", required=True)

    p_show = liab_sub.add_parser("show", parents=[format_parent], help="Show liabilities")
    p_show.add_argument("--type", choices=["credit", "student", "mortgage"])
    p_show.add_argument("--include-inactive", action="store_true")
    p_show.set_defaults(func=handle_show, command_name="liability.show")

    p_upcoming = liab_sub.add_parser("upcoming", parents=[format_parent], help="Upcoming liability payments")
    p_upcoming.add_argument("--days", type=int, default=30)
    p_upcoming.add_argument("--type", choices=["credit", "student", "mortgage"])
    p_upcoming.set_defaults(func=handle_upcoming, command_name="liability.upcoming")

    p_obligations = liab_sub.add_parser("obligations", parents=[format_parent], help="Fixed monthly obligations")
    p_obligations.set_defaults(func=handle_obligations, command_name="liability.obligations")


def _enrich_amount_fields(item: dict[str, Any]) -> None:
    for cents_key in [
        "last_payment_amount_cents",
        "last_statement_balance_cents",
        "minimum_payment_cents",
        "origination_principal_cents",
        "outstanding_interest_cents",
        "ytd_interest_paid_cents",
        "ytd_principal_paid_cents",
        "escrow_balance_cents",
        "next_monthly_payment_cents",
        "past_due_amount_cents",
        "current_late_fee_cents",
    ]:
        value = item.get(cents_key)
        if value is not None:
            item[cents_key.replace("_cents", "")] = cents_to_dollars(int(value))


def handle_show(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """List liabilities with optional type filter and inactive inclusion."""
    where = ["1=1"]
    params: list[Any] = []
    if not args.include_inactive:
        where.append("l.is_active = 1")
    if args.type:
        where.append("l.liability_type = ?")
        params.append(args.type)

    rows = conn.execute(
        f"""
        SELECT l.*, a.institution_name, a.account_name, a.account_type
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE {' AND '.join(where)}
         ORDER BY l.next_payment_due_date IS NULL, l.next_payment_due_date ASC, l.updated_at DESC
        """,
        tuple(params),
    ).fetchall()

    liabilities = []
    total_minimum_due_cents = 0
    for row in rows:
        item = dict(row)
        _enrich_amount_fields(item)
        minimum_due = int(item.get("minimum_payment_cents") or item.get("next_monthly_payment_cents") or 0)
        total_minimum_due_cents += minimum_due
        liabilities.append(item)

    return {
        "data": {
            "liabilities": liabilities,
            "total_minimum_due_cents": total_minimum_due_cents,
            "total_minimum_due": cents_to_dollars(total_minimum_due_cents),
        },
        "summary": {"total_liabilities": len(liabilities)},
        "cli_report": f"liabilities={len(liabilities)} total_minimum_due={cents_to_dollars(total_minimum_due_cents):.2f}",
    }


def handle_upcoming(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Return upcoming liability obligations within a forward day window."""
    if args.days < 1:
        raise ValueError("--days must be >= 1")

    where = [
        "l.is_active = 1",
        "l.next_payment_due_date IS NOT NULL",
        "l.next_payment_due_date >= date('now')",
        "l.next_payment_due_date <= date('now', ?)",
    ]
    params: list[Any] = [f"+{args.days} day"]

    if args.type:
        where.append("l.liability_type = ?")
        params.append(args.type)

    rows = conn.execute(
        f"""
        SELECT l.id, l.account_id, l.liability_type, l.next_payment_due_date,
               l.minimum_payment_cents, l.next_monthly_payment_cents,
               a.institution_name, a.account_name
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE {' AND '.join(where)}
         ORDER BY l.next_payment_due_date ASC, l.liability_type ASC
        """,
        tuple(params),
    ).fetchall()

    items = []
    total_due_cents = 0
    for row in rows:
        item = dict(row)
        due_cents = int(item.get("minimum_payment_cents") or item.get("next_monthly_payment_cents") or 0)
        item["payment_due_cents"] = due_cents
        item["payment_due"] = cents_to_dollars(due_cents)
        total_due_cents += due_cents
        items.append(item)

    return {
        "data": {
            "days": int(args.days),
            "upcoming": items,
            "total_due_cents": total_due_cents,
            "total_due": cents_to_dollars(total_due_cents),
        },
        "summary": {"total_upcoming": len(items)},
        "cli_report": _build_upcoming_cli_report(items, total_due_cents),
    }


def _build_upcoming_cli_report(items: list[dict], total_due_cents: int) -> str:
    if not items:
        return "No upcoming payments."
    lines: list[str] = []
    header = f"{'Due Date':<12} {'Institution':<20} {'Account':<25} {'Amount Due':>12}"
    lines.append(header)
    lines.append("-" * len(header))
    for item in items:
        due_date = (item.get("next_payment_due_date") or "")[:12]
        inst = (item.get("institution_name") or "")[:20]
        name = (item.get("account_name") or "")[:25]
        amount = fmt_dollars(cents_to_dollars(item.get("payment_due_cents", 0)))
        lines.append(f"{due_date:<12} {inst:<20} {name:<25} {amount:>12}")
    lines.append("-" * len(header))
    lines.append(f"{'Total Due:':<58} {fmt_dollars(cents_to_dollars(total_due_cents)):>12}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# obligations subcommand
# ---------------------------------------------------------------------------

def _month_equivalent(amount_cents: int, frequency: str) -> int:
    """Normalize an amount to monthly equivalent based on frequency."""
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


def handle_obligations(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Consolidated view of all fixed monthly obligations."""
    # 1. Recurring expense flows
    flow_rows = conn.execute(
        """
        SELECT name, amount_cents, frequency
          FROM recurring_flows
         WHERE is_active = 1
           AND flow_type = 'expense'
         ORDER BY amount_cents DESC
        """
    ).fetchall()

    recurring_items: list[dict[str, Any]] = []
    recurring_total_cents = 0
    for row in flow_rows:
        monthly = _month_equivalent(int(row["amount_cents"]), row["frequency"])
        recurring_items.append({
            "description": row["name"],
            "amount_cents": int(row["amount_cents"]),
            "frequency": row["frequency"],
            "monthly_cents": monthly,
            "monthly": cents_to_dollars(monthly),
        })
        recurring_total_cents += monthly

    # 2. Liability minimum payments
    liab_rows = conn.execute(
        """
        SELECT l.id, l.liability_type,
               COALESCE(l.minimum_payment_cents, l.next_monthly_payment_cents, 0) AS payment_cents,
               a.institution_name, a.account_name
          FROM liabilities l
          JOIN accounts a ON a.id = l.account_id
         WHERE l.is_active = 1
         ORDER BY COALESCE(l.minimum_payment_cents, l.next_monthly_payment_cents, 0) DESC
        """
    ).fetchall()

    debt_items: list[dict[str, Any]] = []
    debt_total_cents = 0
    for row in liab_rows:
        payment = int(row["payment_cents"])
        debt_items.append({
            "institution": row["institution_name"],
            "account": row["account_name"],
            "liability_type": row["liability_type"],
            "monthly_cents": payment,
            "monthly": cents_to_dollars(payment),
        })
        debt_total_cents += payment

    # 3. Active subscriptions
    sub_rows = conn.execute(
        """
        SELECT vendor_name, amount_cents, frequency
          FROM subscriptions
         WHERE is_active = 1
         ORDER BY amount_cents DESC
        """
    ).fetchall()

    sub_items: list[dict[str, Any]] = []
    sub_total_cents = 0
    for row in sub_rows:
        monthly = _month_equivalent(int(row["amount_cents"]), row["frequency"] or "monthly")
        sub_items.append({
            "vendor": row["vendor_name"],
            "amount_cents": int(row["amount_cents"]),
            "frequency": row["frequency"] or "monthly",
            "monthly_cents": monthly,
            "monthly": cents_to_dollars(monthly),
        })
        sub_total_cents += monthly

    grand_total_cents = recurring_total_cents + debt_total_cents + sub_total_cents

    data = {
        "recurring_flows": recurring_items,
        "recurring_total_cents": recurring_total_cents,
        "recurring_total": cents_to_dollars(recurring_total_cents),
        "debt_payments": debt_items,
        "debt_total_cents": debt_total_cents,
        "debt_total": cents_to_dollars(debt_total_cents),
        "subscriptions": sub_items,
        "subscription_total_cents": sub_total_cents,
        "subscription_total": cents_to_dollars(sub_total_cents),
        "grand_total_cents": grand_total_cents,
        "grand_total": cents_to_dollars(grand_total_cents),
    }

    return {
        "data": data,
        "summary": {
            "recurring_count": len(recurring_items),
            "debt_count": len(debt_items),
            "subscription_count": len(sub_items),
            "grand_total": data["grand_total"],
        },
        "cli_report": _build_obligations_cli_report(data),
    }


def _build_obligations_cli_report(data: dict[str, Any]) -> str:
    W = 36
    lines = [
        f"Fixed Monthly Obligations: {fmt_dollars(data['grand_total'])}/mo",
        "=" * 50,
        "",
    ]

    # Recurring flows section
    lines.append("Recurring Flows:")
    if data["recurring_flows"]:
        for item in data["recurring_flows"]:
            desc = (item["description"] or "Unknown")[:W]
            lines.append(f"  {desc:<{W}s} {fmt_dollars(item['monthly']):>12s}")
    else:
        lines.append("  (none)")
    lines.append(f"  {'Subtotal:':<{W}s} {fmt_dollars(data['recurring_total']):>12s}")
    lines.append("")

    # Debt minimum payments section
    lines.append("Debt Minimum Payments:")
    if data["debt_payments"]:
        for item in data["debt_payments"]:
            label = (item.get("institution") or item.get("account") or "Unknown")[:W]
            lines.append(f"  {label:<{W}s} {fmt_dollars(item['monthly']):>12s}")
    else:
        lines.append("  (none)")
    lines.append(f"  {'Subtotal:':<{W}s} {fmt_dollars(data['debt_total']):>12s}")
    lines.append("")

    # Subscriptions section
    lines.append("Subscriptions:")
    if data["subscriptions"]:
        for item in data["subscriptions"]:
            vendor = (item["vendor"] or "Unknown")[:W]
            lines.append(f"  {vendor:<{W}s} {fmt_dollars(item['monthly']):>12s}")
    else:
        lines.append("  (none)")
    lines.append(f"  {'Subtotal:':<{W}s} {fmt_dollars(data['subscription_total']):>12s}")

    return "\n".join(lines)
