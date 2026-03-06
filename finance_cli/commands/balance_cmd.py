"""Balance and net worth commands."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..models import cents_to_dollars
from .common import fmt_dollars, use_type_filter


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("balance", parents=[format_parent], help="Balance and net worth")
    balance_sub = parser.add_subparsers(dest="balance_command", required=True)

    p_show = balance_sub.add_parser("show", parents=[format_parent], help="Show current balances by account")
    p_show.add_argument("--type", choices=["checking", "savings", "credit_card", "investment", "loan"])
    p_show.add_argument("--all", dest="show_all", action="store_true", help="Include accounts with all balance fields null")
    p_show.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_show.set_defaults(func=handle_show, command_name="balance.show")

    p_worth = balance_sub.add_parser("net-worth", parents=[format_parent], help="Calculate net worth")
    p_worth.add_argument(
        "--exclude-investments",
        action="store_true",
        help="Exclude investment accounts from net worth",
    )
    p_worth.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_worth.set_defaults(func=handle_net_worth, command_name="balance.net_worth")

    p_hist = balance_sub.add_parser("history", parents=[format_parent], help="Balance history for one account")
    p_hist.add_argument("--account", required=True)
    p_hist.add_argument("--days", type=int, default=90)
    p_hist.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_hist.set_defaults(func=handle_history, command_name="balance.history")


def _is_liability_account_type(account_type: str | None) -> bool:
    return (account_type or "") in {"credit_card", "loan"}


def _build_show_cli_report(
    accounts: list[dict], assets_cents: int, liabilities_cents: int
) -> str:
    lines: list[str] = []
    header = f"{'Institution':<20} {'Account':<25} {'Type':<12} {'Balance':>12}"
    lines.append(header)
    lines.append("-" * len(header))
    for a in accounts:
        bal = (
            a.get("balance_current")
            if a.get("balance_current") is not None
            else a.get("balance_available")
        )
        if bal is None:
            bal = (
                a.get("balance_current_cents")
                if a.get("balance_current_cents") is not None
                else a.get("balance_available_cents")
            )
        if bal is None:
            bal = 0
        if isinstance(bal, int):
            bal = cents_to_dollars(bal)
        inst = (a.get("institution_name") or "")[:20]
        name = (a.get("account_name") or "")[:25]
        atype = (a.get("account_type") or "")[:12]
        lines.append(f"{inst:<20} {name:<25} {atype:<12} {fmt_dollars(bal):>12}")
    lines.append("-" * len(header))
    lines.append(
        f"{'Assets:':<20} {fmt_dollars(cents_to_dollars(assets_cents)):>12}   "
        f"{'Liabilities:':<15} {fmt_dollars(cents_to_dollars(liabilities_cents)):>12}"
    )
    return "\n".join(lines)


def handle_show(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """List active accounts with balance fields and aggregate totals."""
    view = getattr(args, "view", "all")
    view_clause = use_type_filter(view)
    where = ["a.is_active = 1", "a.id NOT IN (SELECT hash_account_id FROM account_aliases)"]
    params: list[Any] = []
    if args.type:
        where.append("a.account_type = ?")
        params.append(args.type)
    if not args.show_all:
        where.append("(a.balance_current_cents IS NOT NULL OR a.balance_available_cents IS NOT NULL)")
    if view != "all":
        where.append(
            f"""
            EXISTS (
                SELECT 1
                  FROM transactions t
                 WHERE t.account_id = a.id
                   AND t.is_active = 1
                   {view_clause}
            )
            """
        )

    rows = conn.execute(
        f"""
        SELECT a.id, a.institution_name, a.account_name, a.account_type, a.card_ending,
               a.balance_current_cents, a.balance_available_cents, a.balance_limit_cents,
               a.iso_currency_code, a.unofficial_currency_code, a.balance_updated_at
          FROM accounts a
         WHERE {' AND '.join(where)}
         ORDER BY a.institution_name, a.account_name
        """,
        tuple(params),
    ).fetchall()

    accounts = []
    total_assets_cents = 0
    total_liabilities_cents = 0
    for row in rows:
        item = dict(row)
        current_cents = item["balance_current_cents"]
        available_cents = item["balance_available_cents"]
        limit_cents = item["balance_limit_cents"]

        if current_cents is not None:
            item["balance_current"] = cents_to_dollars(int(current_cents))
        if available_cents is not None:
            item["balance_available"] = cents_to_dollars(int(available_cents))
        if limit_cents is not None:
            item["balance_limit"] = cents_to_dollars(int(limit_cents))

        current_value = int(current_cents or 0)
        if _is_liability_account_type(item.get("account_type")):
            total_liabilities_cents += abs(current_value)
        else:
            total_assets_cents += current_value

        accounts.append(item)

    return {
        "data": {
            "accounts": accounts,
            "total_assets_cents": total_assets_cents,
            "total_liabilities_cents": total_liabilities_cents,
            "total_assets": cents_to_dollars(total_assets_cents),
            "total_liabilities": cents_to_dollars(total_liabilities_cents),
        },
        "summary": {"total_accounts": len(accounts)},
        "cli_report": _build_show_cli_report(accounts, total_assets_cents, total_liabilities_cents),
    }


_ACCOUNT_TYPE_DISPLAY = {
    "checking": "Checking",
    "savings": "Savings",
    "credit_card": "Credit Card",
    "investment": "Investment",
    "loan": "Loan",
    "unknown": "Other",
}


def _build_net_worth_cli_report(
    breakdown: list[dict], assets_cents: int, liabilities_cents: int, net_worth_cents: int
) -> str:
    lines = [f"Net Worth: {fmt_dollars(cents_to_dollars(net_worth_cents))}", ""]
    for entry in breakdown:
        account_type = entry["account_type"]
        display_name = _ACCOUNT_TYPE_DISPLAY.get(account_type, account_type)
        lines.append(f"  {display_name + ':':<15s} {fmt_dollars(entry['balance']):>12s}")
    lines.append("")
    lines.append(f"  {'Assets:':<15s} {fmt_dollars(cents_to_dollars(assets_cents)):>12s}")
    lines.append(f"  {'Liabilities:':<15s} {fmt_dollars(cents_to_dollars(liabilities_cents)):>12s}")
    return "\n".join(lines)


def handle_net_worth(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Compute net worth from current account balances."""
    view = getattr(args, "view", "all")
    view_clause = use_type_filter(view)
    where = [
        "a.is_active = 1",
        "a.balance_current_cents IS NOT NULL",
        "a.id NOT IN (SELECT hash_account_id FROM account_aliases)",
    ]
    if getattr(args, "exclude_investments", False):
        where.append("a.account_type != 'investment'")
    if view != "all":
        where.append(
            f"""
            EXISTS (
                SELECT 1
                  FROM transactions t
                 WHERE t.account_id = a.id
                   AND t.is_active = 1
                   {view_clause}
            )
            """
        )

    rows = conn.execute(
        f"""
        SELECT a.account_type, a.balance_current_cents
          FROM accounts a
         WHERE {' AND '.join(where)}
        """
    ).fetchall()

    assets_cents = 0
    liabilities_cents = 0
    by_type: dict[str, int] = {}
    for row in rows:
        account_type = str(row["account_type"] or "unknown")
        cents = int(row["balance_current_cents"] or 0)
        by_type[account_type] = by_type.get(account_type, 0) + cents
        if _is_liability_account_type(account_type):
            liabilities_cents += abs(cents)
        else:
            assets_cents += cents

    net_worth_cents = assets_cents - liabilities_cents
    breakdown = [
        {"account_type": account_type, "balance_cents": cents, "balance": cents_to_dollars(cents)}
        for account_type, cents in sorted(by_type.items())
    ]

    return {
        "data": {
            "exclude_investments": bool(getattr(args, "exclude_investments", False)),
            "assets_cents": assets_cents,
            "liabilities_cents": liabilities_cents,
            "net_worth_cents": net_worth_cents,
            "assets": cents_to_dollars(assets_cents),
            "liabilities": cents_to_dollars(liabilities_cents),
            "net_worth": cents_to_dollars(net_worth_cents),
            "breakdown": breakdown,
        },
        "summary": {"total_account_types": len(breakdown)},
        "cli_report": _build_net_worth_cli_report(breakdown, assets_cents, liabilities_cents, net_worth_cents),
    }


def handle_history(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Return per-day balance snapshots for one account."""
    if args.days < 1:
        raise ValueError("--days must be >= 1")

    account = conn.execute(
        """
        SELECT id, institution_name, account_name, account_type
          FROM accounts
         WHERE id = ?
        """,
        (args.account,),
    ).fetchone()
    if not account:
        raise ValueError(f"Account {args.account} not found")

    view = getattr(args, "view", "all")
    if view != "all":
        view_clause = use_type_filter(view)
        matching_txn = conn.execute(
            f"""
            SELECT 1
              FROM transactions t
             WHERE t.account_id = ?
               AND t.is_active = 1
               {view_clause}
             LIMIT 1
            """,
            (args.account,),
        ).fetchone()
        if not matching_txn:
            return {
                "data": {
                    "account": dict(account),
                    "days": int(args.days),
                    "history": [],
                },
                "summary": {"total_points": 0},
                "cli_report": "history_points=0",
            }

    rows = conn.execute(
        """
        SELECT snapshot_date, source, balance_current_cents, balance_available_cents, balance_limit_cents
          FROM balance_snapshots
         WHERE account_id = ?
           AND snapshot_date >= date('now', ?)
         ORDER BY snapshot_date ASC, created_at ASC
        """,
        (args.account, f"-{args.days - 1} day"),
    ).fetchall()

    history = []
    for row in rows:
        item = dict(row)
        for cents_key, amount_key in [
            ("balance_current_cents", "balance_current"),
            ("balance_available_cents", "balance_available"),
            ("balance_limit_cents", "balance_limit"),
        ]:
            cents_value = item.get(cents_key)
            if cents_value is not None:
                item[amount_key] = cents_to_dollars(int(cents_value))
        history.append(item)

    return {
        "data": {
            "account": dict(account),
            "days": int(args.days),
            "history": history,
        },
        "summary": {"total_points": len(history)},
        "cli_report": _build_history_cli_report(dict(account), history),
    }


def _build_history_cli_report(account: dict, history: list[dict]) -> str:
    inst = account.get("institution_name") or ""
    name = account.get("account_name") or ""
    lines: list[str] = [f"{inst} — {name}", ""]
    if not history:
        lines.append("No balance history found.")
        return "\n".join(lines)
    header = f"{'Date':<12} {'Source':<10} {'Balance':>12} {'Available':>12} {'Limit':>12}"
    lines.append(header)
    lines.append("-" * len(header))
    for item in history:
        snap_date = (item.get("snapshot_date") or "")[:12]
        source = (item.get("source") or "")[:10]
        bal = fmt_dollars(item["balance_current"]) if "balance_current" in item else "—"
        avail = fmt_dollars(item["balance_available"]) if "balance_available" in item else "—"
        limit = fmt_dollars(item["balance_limit"]) if "balance_limit" in item else "—"
        lines.append(f"{snap_date:<12} {source:<10} {bal:>12} {avail:>12} {limit:>12}")
    return "\n".join(lines)
