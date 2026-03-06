"""Daily spending command."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..models import cents_to_dollars
from .common import fmt_dollars, today_iso, txn_row_to_dict, use_type_filter


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("daily", parents=[format_parent], help="Daily rundown")
    parser.add_argument("--date")
    parser.add_argument("--pending", action="store_true")
    parser.add_argument("--view", choices=["personal", "business", "all"], default="all")
    parser.set_defaults(func=handle_daily, command_name="daily")


def handle_daily(args, conn: sqlite3.Connection) -> dict[str, Any]:
    target_date = args.date or today_iso()
    view = getattr(args, "view", "all")

    where = ["t.date = ?", "t.is_active = 1"]
    view_clause = use_type_filter(view)
    params: list[Any] = [target_date]
    if args.pending:
        where.append("t.is_reviewed = 0")

    query = f"""
        SELECT t.*, c.name AS category_name,
               COALESCE(p.name, c.name) AS group_name,
               a.account_name
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN categories p ON p.id = c.parent_id
          LEFT JOIN accounts a ON a.id = t.account_id
         WHERE {' AND '.join(where)}
           {view_clause}
         ORDER BY t.created_at DESC
    """

    rows = conn.execute(query, tuple(params)).fetchall()
    txns = [txn_row_to_dict(row) for row in rows]
    total_cents = sum(int(r["amount_cents"]) for r in rows if int(r["is_payment"] or 0) == 0)
    unreviewed_count = conn.execute(
        f"""
        SELECT COUNT(*) AS n
          FROM transactions t
         WHERE t.date = ?
           AND t.is_active = 1
           AND t.is_reviewed = 0
           {view_clause}
        """,
        (target_date,),
    ).fetchone()["n"]
    data_range_row = conn.execute(
        f"""
        SELECT MIN(t.date) AS earliest, MAX(t.date) AS latest
          FROM transactions t
         WHERE t.is_active = 1
           {view_clause}
        """
    ).fetchone()
    data_range = {
        "earliest": data_range_row["earliest"],
        "latest": data_range_row["latest"],
    }

    if txns:
        total_dollars = cents_to_dollars(total_cents)
        header = f"{target_date} \u2014 {len(txns)} transaction{'s' if len(txns) != 1 else ''}, total: {fmt_dollars(total_dollars)}, unreviewed: {unreviewed_count}"
        cli_lines = [header, ""]
        for row in txns[:100]:
            desc = (row.get("description") or "")[:20].ljust(20)
            cat = (row.get("category_name") or "Uncategorized")[:16].ljust(16)
            acct = (row.get("account_name") or "")[:16].ljust(16)
            amt = fmt_dollars(row["amount"])
            cli_lines.append(f"  {desc}  {cat}  {acct}  {amt:>10s}")
        cli_report = "\n".join(cli_lines)
    elif data_range["earliest"] and data_range["latest"]:
        cli_report = (
            f"No transactions on {target_date} "
            f"(data range: {data_range['earliest']} to {data_range['latest']})"
        )
    else:
        cli_report = f"No transactions on {target_date} (data range: empty)"

    return {
        "data": {
            "date": target_date,
            "transactions": txns,
            "unreviewed_count": int(unreviewed_count),
            "data_range": data_range,
        },
        "summary": {
            "total_transactions": len(txns),
            "total_amount": total_cents / 100,
        },
        "cli_report": cli_report,
    }
