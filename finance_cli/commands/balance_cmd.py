"""Balance and net worth commands."""

from __future__ import annotations

from datetime import date
import sqlite3
from typing import Any
import uuid

from finance_cli.exceptions import NotFoundError, ValidationError

from ..models import cents_to_dollars, dollars_to_cents
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

    p_update = balance_sub.add_parser("update", parents=[format_parent], help="Record a manual balance snapshot")
    p_update.add_argument("--account", required=True, help="Account ID to update")
    p_update.add_argument("--current", help="Current balance in dollars")
    p_update.add_argument("--available", help="Available balance in dollars")
    p_update.add_argument("--limit", dest="balance_limit", help="Credit limit in dollars")
    p_update.add_argument("--date", dest="snapshot_date", help="Snapshot date (YYYY-MM-DD); defaults to today")
    p_update.add_argument("--dry-run", action="store_true", help="Preview without writing")
    p_update.set_defaults(func=handle_update, command_name="balance.update")


def _arg(args: Any, name: str, default: Any = None) -> Any:
    if isinstance(args, dict):
        return args.get(name, default)
    return getattr(args, name, default)


def _parse_optional_cents(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        raise ValidationError(f"{field_name} cannot be blank")
    try:
        return dollars_to_cents(text)
    except Exception as exc:
        raise ValidationError(f"{field_name} must be a dollar amount") from exc


def _normalize_snapshot_date(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return date.today().isoformat()
    text = str(value).strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValidationError("snapshot_date must be YYYY-MM-DD") from exc


def _fetch_active_canonical_account(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT *
          FROM accounts
         WHERE id = ?
        """,
        (account_id,),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    if int(row["is_active"] or 0) != 1:
        raise ValidationError(f"Account {account_id} is inactive")

    alias_row = conn.execute(
        """
        SELECT canonical_id
          FROM account_aliases
         WHERE hash_account_id = ?
        """,
        (account_id,),
    ).fetchone()
    if alias_row is not None:
        raise ValidationError(
            f"Account {account_id} is an alias source; update canonical account "
            f"{alias_row['canonical_id']} instead"
        )
    return row

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
    "manual_loans": "Manual Loans",
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
    view = args.get("view", "all") if isinstance(args, dict) else getattr(args, "view", "all")
    exclude_investments = (
        bool(args.get("exclude_investments", False))
        if isinstance(args, dict)
        else bool(getattr(args, "exclude_investments", False))
    )
    view_clause = use_type_filter(view)
    where = [
        "a.is_active = 1",
        "a.balance_current_cents IS NOT NULL",
        "a.id NOT IN (SELECT hash_account_id FROM account_aliases)",
    ]
    if exclude_investments:
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

    loan_view_clause = ""
    if view == "personal":
        loan_view_clause = "AND use_type = 'Personal'"
    elif view == "business":
        loan_view_clause = "AND use_type = 'Business'"

    manual_loan_row = conn.execute(
        f"""
        SELECT COALESCE(SUM(current_balance_cents), 0) AS total_cents
          FROM manual_loans
         WHERE is_active = 1
           {loan_view_clause}
        """
    ).fetchone()
    manual_loan_total_cents = int(manual_loan_row["total_cents"])
    liabilities_cents += manual_loan_total_cents
    if manual_loan_total_cents > 0:
        by_type["manual_loans"] = -manual_loan_total_cents

    net_worth_cents = assets_cents - liabilities_cents
    breakdown = [
        {"account_type": account_type, "balance_cents": cents, "balance": cents_to_dollars(cents)}
        for account_type, cents in sorted(by_type.items())
    ]

    return {
        "data": {
            "exclude_investments": exclude_investments,
            "assets_cents": assets_cents,
            "liabilities_cents": liabilities_cents,
            "manual_loans_cents": manual_loan_total_cents,
            "manual_loans": cents_to_dollars(manual_loan_total_cents),
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


def handle_update(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Record a manual balance update and matching daily snapshot."""
    dry_run = bool(_arg(args, "dry_run", default=False))
    account_id = str(_arg(args, "account", default="") or "").strip()
    if not account_id:
        raise ValidationError("account is required")

    provided = {
        "balance_current_cents": _parse_optional_cents(_arg(args, "current"), "current"),
        "balance_available_cents": _parse_optional_cents(_arg(args, "available"), "available"),
        "balance_limit_cents": _parse_optional_cents(_arg(args, "balance_limit"), "limit"),
    }
    if all(value is None for value in provided.values()):
        raise ValidationError("Provide at least one of current, available, or limit")

    snapshot_date = _normalize_snapshot_date(_arg(args, "snapshot_date"))
    account_before = _fetch_active_canonical_account(conn, account_id)
    old_values = {
        key: account_before[key]
        for key in ("balance_current_cents", "balance_available_cents", "balance_limit_cents")
    }
    new_values = {
        key: (value if value is not None else old_values[key])
        for key, value in provided.items()
    }
    snapshot_values = dict(provided)

    existing_snapshot = conn.execute(
        """
        SELECT id
          FROM balance_snapshots
         WHERE account_id = ?
           AND snapshot_date = ?
           AND source = 'manual'
        """,
        (account_id, snapshot_date),
    ).fetchone()
    snapshot_id = str(existing_snapshot["id"]) if existing_snapshot is not None else uuid.uuid4().hex

    conn.execute(
        """
        UPDATE accounts
           SET balance_current_cents = ?,
               balance_available_cents = ?,
               balance_limit_cents = ?,
               balance_updated_at = datetime('now'),
               updated_at = datetime('now')
         WHERE id = ?
        """,
        (
            new_values["balance_current_cents"],
            new_values["balance_available_cents"],
            new_values["balance_limit_cents"],
            account_id,
        ),
    )
    conn.execute(
        """
        INSERT INTO balance_snapshots (
            id, account_id, balance_current_cents, balance_available_cents,
            balance_limit_cents, source, snapshot_date
        ) VALUES (?, ?, ?, ?, ?, 'manual', ?)
        ON CONFLICT(account_id, snapshot_date, source) DO UPDATE SET
            balance_current_cents = COALESCE(
                excluded.balance_current_cents,
                balance_snapshots.balance_current_cents
            ),
            balance_available_cents = COALESCE(
                excluded.balance_available_cents,
                balance_snapshots.balance_available_cents
            ),
            balance_limit_cents = COALESCE(
                excluded.balance_limit_cents,
                balance_snapshots.balance_limit_cents
            ),
            created_at = datetime('now')
        """,
        (
            snapshot_id,
            account_id,
            snapshot_values["balance_current_cents"],
            snapshot_values["balance_available_cents"],
            snapshot_values["balance_limit_cents"],
            snapshot_date,
        ),
    )
    persisted_snapshot = conn.execute(
        """
        SELECT id, account_id, snapshot_date, source,
               balance_current_cents, balance_available_cents, balance_limit_cents
          FROM balance_snapshots
         WHERE account_id = ?
           AND snapshot_date = ?
           AND source = 'manual'
        """,
        (account_id, snapshot_date),
    ).fetchone()
    snapshot_record = dict(persisted_snapshot) if persisted_snapshot is not None else {
        "id": snapshot_id,
        "account_id": account_id,
        "snapshot_date": snapshot_date,
        "source": "manual",
        **snapshot_values,
    }

    changed_fields = [
        key.removeprefix("balance_").removesuffix("_cents")
        for key, value in provided.items()
        if value is not None and value != old_values[key]
    ]
    account_after = dict(account_before)
    account_after.update(new_values)
    for cents_key, amount_key in [
        ("balance_current_cents", "balance_current"),
        ("balance_available_cents", "balance_available"),
        ("balance_limit_cents", "balance_limit"),
    ]:
        cents_value = account_after.get(cents_key)
        account_after[amount_key] = cents_to_dollars(int(cents_value)) if cents_value is not None else None

    if dry_run:
        conn.rollback()
    else:
        conn.commit()

    data = {
        "account": account_after,
        "account_id": account_id,
        "snapshot": snapshot_record,
        "changed_fields": changed_fields,
        "updated_existing_snapshot": existing_snapshot is not None,
        **({"dry_run": True} if dry_run else {}),
    }
    current_text = (
        fmt_dollars(cents_to_dollars(int(new_values["balance_current_cents"])))
        if new_values["balance_current_cents"] is not None
        else "n/a"
    )
    prefix = "[DRY RUN] Would update" if dry_run else "Updated"
    return {
        "data": data,
        "summary": {
            "total_accounts": 1,
            "snapshots_upserted": 1,
            "changed_fields": len(changed_fields),
        },
        "cli_report": f"{prefix} balance for {account_id} on {snapshot_date}: current={current_text}",
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
