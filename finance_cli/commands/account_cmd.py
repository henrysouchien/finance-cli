"""Account management commands."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..models import cents_to_dollars
from .common import fmt_dollars

_ACCOUNT_TYPES = ("checking", "savings", "credit_card", "investment", "loan")
_ACCOUNT_TYPE_SET = set(_ACCOUNT_TYPES)
_STATUS_CHOICES = {"active", "inactive", "all"}


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("account", parents=[format_parent], help="Account management commands")
    account_sub = parser.add_subparsers(dest="account_command", required=True)

    p_list = account_sub.add_parser("list", parents=[format_parent], help="List accounts")
    p_list.add_argument("--status", choices=["active", "inactive", "all"], default="active")
    p_list.add_argument("--type", choices=list(_ACCOUNT_TYPES))
    p_list.add_argument("--institution")
    p_list.add_argument("--source", help="Free-text source filter (e.g. plaid, csv_import, pdf_import, manual, schwab)")
    p_list.set_defaults(func=handle_list, command_name="account.list")

    p_show = account_sub.add_parser("show", parents=[format_parent], help="Show account details")
    p_show.add_argument("id")
    p_show.set_defaults(func=handle_show, command_name="account.show")

    p_set_type = account_sub.add_parser("set-type", parents=[format_parent], help="Override account type")
    p_set_type.add_argument("id")
    p_set_type.add_argument("--type", required=True, choices=list(_ACCOUNT_TYPES))
    p_set_type.set_defaults(func=handle_set_type, command_name="account.set_type")

    p_set_business = account_sub.add_parser("set-business", parents=[format_parent], help="Set business account flag")
    p_set_business.add_argument("id")
    group = p_set_business.add_mutually_exclusive_group(required=True)
    group.add_argument("--business", action="store_true", help="Mark account as business")
    group.add_argument("--personal", action="store_true", help="Mark account as personal")
    p_set_business.add_argument(
        "--backfill",
        action="store_true",
        help="Also update linked active transactions based on the selected mode",
    )
    p_set_business.set_defaults(func=handle_set_business, command_name="account.set_business")

    p_deactivate = account_sub.add_parser("deactivate", parents=[format_parent], help="Deactivate account")
    p_deactivate.add_argument("id")
    p_deactivate.add_argument("--cascade", action="store_true", help="Also deactivate linked txns and auto-detected subscriptions")
    p_deactivate.add_argument("--force", action="store_true", help="Allow deactivation even if account is alias canonical target")
    p_deactivate.set_defaults(func=handle_deactivate, command_name="account.deactivate")

    p_activate = account_sub.add_parser("activate", parents=[format_parent], help="Activate account")
    p_activate.add_argument("id")
    p_activate.set_defaults(func=handle_activate, command_name="account.activate")


def _arg(args, *names: str, default: Any = None) -> Any:
    for name in names:
        if hasattr(args, name):
            return getattr(args, name)
    return default


def _normalize_account_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in _ACCOUNT_TYPE_SET:
        allowed = ", ".join(_ACCOUNT_TYPES)
        raise ValueError(f"Invalid account type '{value}'. Allowed: {allowed}")
    return normalized


def _normalize_status(value: Any) -> str:
    status = str(value or "active").strip().lower() or "active"
    if status not in _STATUS_CHOICES:
        raise ValueError("status must be one of: active, inactive, all")
    return status


def _account_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    account = dict(row)
    for cents_key, amount_key in [
        ("balance_current_cents", "balance_current"),
        ("balance_available_cents", "balance_available"),
        ("balance_limit_cents", "balance_limit"),
    ]:
        cents_value = account.get(cents_key)
        if cents_value is not None:
            account[amount_key] = cents_to_dollars(int(cents_value))
    account["is_active"] = int(account.get("is_active") or 0)
    if "is_business" in account:
        account["is_business"] = int(account.get("is_business") or 0)
    return account


def _fetch_account_or_raise(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
    if not row:
        raise ValueError(f"Account {account_id} not found")
    return row


def _linked_subscriptions(conn: sqlite3.Connection, account_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, vendor_name, amount_cents, frequency, is_active, is_auto_detected
          FROM subscriptions
         WHERE account_id = ?
         ORDER BY is_active DESC, vendor_name ASC, id ASC
        """,
        (account_id,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["is_active"] = int(item.get("is_active") or 0)
        item["is_auto_detected"] = int(item.get("is_auto_detected") or 0)
        item["amount"] = cents_to_dollars(int(item.get("amount_cents") or 0))
        out.append(item)
    return out


def _account_summary_row(row: sqlite3.Row) -> dict[str, Any]:
    balance_cents = row["balance_current_cents"]
    return {
        "id": row["id"],
        "institution": row["institution_name"],
        "name": row["account_name"],
        "type": row["account_type"],
        "card_ending": row["card_ending"],
        "balance_current_cents": balance_cents,
        "balance_current": cents_to_dollars(int(balance_cents)) if balance_cents is not None else None,
        "source": row["source"],
        "is_active": int(row["is_active"] or 0),
        "is_business": int(row["is_business"] or 0),
        "balance_updated_at": row["balance_updated_at"],
        "account_type_override": row["account_type_override"],
    }


def handle_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    status = _normalize_status(_arg(args, "status", default="active"))
    account_type = _arg(args, "type", "account_type")
    institution = str(_arg(args, "institution", default="") or "").strip()
    source = str(_arg(args, "source", default="") or "").strip()
    raw_is_business = _arg(args, "is_business", default=None)

    where = ["1=1"]
    params: list[Any] = []

    is_business: bool | None = None
    if raw_is_business is not None:
        if isinstance(raw_is_business, bool):
            is_business = raw_is_business
        elif isinstance(raw_is_business, (int, float)) and int(raw_is_business) in {0, 1}:
            is_business = bool(int(raw_is_business))
        else:
            normalized_flag = str(raw_is_business).strip().lower()
            if normalized_flag in {"1", "true", "yes"}:
                is_business = True
            elif normalized_flag in {"0", "false", "no"}:
                is_business = False
            else:
                raise ValueError("is_business must be true/false when provided")

    if status == "active":
        where.append("a.is_active = 1")
    elif status == "inactive":
        where.append("a.is_active = 0")

    if account_type:
        normalized_type = _normalize_account_type(account_type)
        where.append("a.account_type = ?")
        params.append(normalized_type)

    if institution:
        where.append("a.institution_name LIKE ? COLLATE NOCASE")
        params.append(f"%{institution}%")

    if source:
        where.append("COALESCE(a.source, '') LIKE ? COLLATE NOCASE")
        params.append(f"%{source}%")

    if is_business is not None:
        where.append("a.is_business = ?")
        params.append(1 if is_business else 0)

    rows = conn.execute(
        f"""
        SELECT
            a.id,
            a.institution_name,
            a.account_name,
            a.account_type,
            a.account_type_override,
            a.card_ending,
            a.balance_current_cents,
            a.source,
            a.is_active,
            a.is_business,
            a.balance_updated_at
          FROM accounts a
         WHERE {' AND '.join(where)}
         ORDER BY a.institution_name ASC, a.account_name ASC, a.id ASC
        """,
        tuple(params),
    ).fetchall()

    accounts = [_account_summary_row(row) for row in rows]
    active_count = sum(1 for item in accounts if int(item.get("is_active") or 0) == 1)
    inactive_count = len(accounts) - active_count

    if accounts:
        lines = [f"accounts={len(accounts)} active={active_count} inactive={inactive_count}"]
        for item in accounts[:200]:
            source_text = str(item.get("source") or "-")
            balance_value = item.get("balance_current")
            balance_text = fmt_dollars(balance_value) if balance_value is not None else "n/a"
            status_text = "active" if int(item.get("is_active") or 0) else "inactive"
            biz_text = "biz" if int(item.get("is_business") or 0) else "-"
            lines.append(
                f"  {str(item['institution'] or '')[:20]:20}  "
                f"{str(item['name'] or '')[:22]:22}  "
                f"{str(item['type'] or '')[:11]:11}  "
                f"{status_text:8}  "
                f"{source_text[:12]:12}  "
                f"{biz_text:3}  "
                f"{balance_text:>12}"
            )
        cli_report = "\n".join(lines)
    else:
        cli_report = "No accounts"

    return {
        "data": {
            "accounts": accounts,
            "filters": {
                "status": status,
                "account_type": account_type,
                "institution": institution or None,
                "source": source or None,
                "is_business": is_business,
            },
        },
        "summary": {
            "total_accounts": len(accounts),
            "active_accounts": active_count,
            "inactive_accounts": inactive_count,
        },
        "cli_report": cli_report,
    }


def handle_show(args, conn: sqlite3.Connection) -> dict[str, Any]:
    account_id = str(_arg(args, "id", default="") or "").strip()
    if not account_id:
        raise ValueError("id is required")

    account_row = _fetch_account_or_raise(conn, account_id)
    account = _account_to_dict(account_row)

    txn_stats = conn.execute(
        """
        SELECT COUNT(*) AS total_transactions,
               SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_transactions,
               SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS inactive_transactions,
               MIN(date) AS first_transaction_date,
               MAX(date) AS last_transaction_date
          FROM transactions
         WHERE account_id = ?
        """,
        (account_id,),
    ).fetchone()
    stats = {
        "total_transactions": int(txn_stats["total_transactions"] or 0),
        "active_transactions": int(txn_stats["active_transactions"] or 0),
        "inactive_transactions": int(txn_stats["inactive_transactions"] or 0),
        "first_transaction_date": txn_stats["first_transaction_date"],
        "last_transaction_date": txn_stats["last_transaction_date"],
    }

    lines = [
        f"id: {account['id']}",
        f"institution: {account.get('institution_name') or '-'}",
        f"name: {account.get('account_name') or '-'}",
        f"type: {account.get('account_type') or '-'}",
        f"type_override: {account.get('account_type_override') or '-'}",
        f"source: {account.get('source') or '-'}",
        f"is_active: {account.get('is_active')}",
        f"is_business: {account.get('is_business')}",
        f"balance_current: {fmt_dollars(account['balance_current']) if account.get('balance_current') is not None else 'n/a'}",
        (
            "transactions: "
            f"{stats['total_transactions']} "
            f"(active={stats['active_transactions']} inactive={stats['inactive_transactions']}) "
            f"range={stats['first_transaction_date'] or '-'}..{stats['last_transaction_date'] or '-'}"
        ),
    ]

    return {
        "data": {
            "account": account,
            "transaction_stats": stats,
        },
        "summary": {
            "total_accounts": 1,
            "total_transactions": stats["total_transactions"],
        },
        "cli_report": "\n".join(lines),
    }


def handle_set_type(args, conn: sqlite3.Connection) -> dict[str, Any]:
    account_id = str(_arg(args, "id", default="") or "").strip()
    if not account_id:
        raise ValueError("id is required")

    new_type = _normalize_account_type(_arg(args, "type", "account_type"))
    row = _fetch_account_or_raise(conn, account_id)
    old_type = str(row["account_type"])
    old_override = row["account_type_override"]

    conn.execute(
        """
        UPDATE accounts
           SET account_type = ?,
               account_type_override = ?,
               updated_at = datetime('now')
         WHERE id = ?
        """,
        (new_type, new_type, account_id),
    )
    conn.commit()

    changed = (old_type != new_type) or (old_override != new_type)
    return {
        "data": {
            "account_id": account_id,
            "old_type": old_type,
            "old_override": old_override,
            "new_type": new_type,
            "account_type_override": new_type,
            "override_set": True,
            "changed": changed,
        },
        "summary": {
            "total_accounts": 1,
            "changed": int(changed),
        },
        "cli_report": f"Account {account_id} type set to {new_type} (override set)",
    }


def handle_set_business(args, conn: sqlite3.Connection) -> dict[str, Any]:
    account_id = str(_arg(args, "id", default="") or "").strip()
    if not account_id:
        raise ValueError("id is required")

    as_business = bool(_arg(args, "business", default=False))
    as_personal = bool(_arg(args, "personal", default=False))
    if as_business == as_personal:
        raise ValueError("Specify exactly one of --business or --personal")

    backfill = bool(_arg(args, "backfill", default=False))
    new_flag = 1 if as_business else 0

    row = _fetch_account_or_raise(conn, account_id)
    old_flag = int(row["is_business"] or 0)

    conn.execute(
        """
        UPDATE accounts
           SET is_business = ?,
               updated_at = datetime('now')
         WHERE id = ?
        """,
        (new_flag, account_id),
    )

    backfilled_transactions = 0
    if backfill and new_flag == 1:
        backfilled_transactions = int(
            conn.execute(
                """
                UPDATE transactions
                   SET use_type = 'Business',
                       updated_at = datetime('now')
                 WHERE account_id = ?
                   AND use_type IS NULL
                   AND is_active = 1
                """,
                (account_id,),
            ).rowcount
            or 0
        )
    elif backfill and new_flag == 0:
        backfilled_transactions = int(
            conn.execute(
                """
                UPDATE transactions
                   SET use_type = NULL,
                       updated_at = datetime('now')
                 WHERE account_id = ?
                   AND use_type = 'Business'
                   AND is_active = 1
                """,
                (account_id,),
            ).rowcount
            or 0
        )

    conn.commit()
    account = _account_to_dict(_fetch_account_or_raise(conn, account_id))
    changed = old_flag != new_flag

    mode = "business" if new_flag else "personal"
    cli_report = f"Account {account_id} set {mode} (backfilled={backfilled_transactions})"

    return {
        "data": {
            "account": account,
            "account_id": account_id,
            "old_is_business": old_flag,
            "new_is_business": new_flag,
            "changed": changed,
            "backfill": {
                "enabled": backfill,
                "transactions_updated": backfilled_transactions,
            },
        },
        "summary": {
            "total_accounts": 1,
            "changed": int(changed),
            "transactions_updated": backfilled_transactions,
        },
        "cli_report": cli_report,
    }


def handle_deactivate(args, conn: sqlite3.Connection) -> dict[str, Any]:
    account_id = str(_arg(args, "id", default="") or "").strip()
    if not account_id:
        raise ValueError("id is required")

    cascade = bool(_arg(args, "cascade", default=False))
    force = bool(_arg(args, "force", default=False))

    row = _fetch_account_or_raise(conn, account_id)
    was_active = int(row["is_active"] or 0) == 1

    alias_rows = conn.execute(
        """
        SELECT hash_account_id
          FROM account_aliases
         WHERE canonical_id = ?
         ORDER BY hash_account_id ASC
        """,
        (account_id,),
    ).fetchall()
    alias_ids = [str(alias_row["hash_account_id"]) for alias_row in alias_rows]
    if alias_ids and not force:
        raise ValueError(
            f"Account {account_id} is canonical target for {len(alias_ids)} alias(es). "
            "Use --force to deactivate anyway."
        )

    account_rows_updated = 0
    if was_active:
        account_rows_updated = int(
            conn.execute(
                """
                UPDATE accounts
                   SET is_active = 0,
                       updated_at = datetime('now')
                 WHERE id = ?
                   AND is_active = 1
                """,
                (account_id,),
            ).rowcount
            or 0
        )

    deactivated_transactions = 0
    deactivated_subscriptions = 0
    if cascade:
        deactivated_transactions = int(
            conn.execute(
                """
                UPDATE transactions
                   SET is_active = 0,
                       updated_at = datetime('now')
                 WHERE account_id = ?
                   AND is_active = 1
                """,
                (account_id,),
            ).rowcount
            or 0
        )
        deactivated_subscriptions = int(
            conn.execute(
                """
                UPDATE subscriptions
                   SET is_active = 0
                 WHERE account_id = ?
                   AND is_active = 1
                   AND is_auto_detected = 1
                """,
                (account_id,),
            ).rowcount
            or 0
        )

    conn.commit()

    account = _account_to_dict(_fetch_account_or_raise(conn, account_id))
    linked_subscriptions = _linked_subscriptions(conn, account_id)

    cli_parts = [f"Account {account_id} deactivated" if account_rows_updated else f"Account {account_id} already inactive"]
    if alias_ids and force:
        cli_parts.append(f"forced_aliases={len(alias_ids)}")
    if cascade:
        cli_parts.append(f"cascade_txns={deactivated_transactions}")
        cli_parts.append(f"cascade_subs={deactivated_subscriptions}")
    if linked_subscriptions:
        cli_parts.append(f"linked_subscriptions={len(linked_subscriptions)}")

    return {
        "data": {
            "account": account,
            "account_id": account_id,
            "deactivated": bool(account_rows_updated),
            "already_inactive": not was_active,
            "cascade": {
                "enabled": cascade,
                "deactivated_transactions": deactivated_transactions,
                "deactivated_subscriptions": deactivated_subscriptions,
            },
            "alias_guard": {
                "canonical_alias_count": len(alias_ids),
                "canonical_alias_ids": alias_ids,
                "forced": force,
            },
            "linked_subscriptions": linked_subscriptions,
        },
        "summary": {
            "total_accounts": 1,
            "deactivated_accounts": int(bool(account_rows_updated)),
            "deactivated_transactions": deactivated_transactions,
            "deactivated_subscriptions": deactivated_subscriptions,
        },
        "cli_report": " ".join(cli_parts),
    }


def handle_activate(args, conn: sqlite3.Connection) -> dict[str, Any]:
    account_id = str(_arg(args, "id", default="") or "").strip()
    if not account_id:
        raise ValueError("id is required")

    row = _fetch_account_or_raise(conn, account_id)
    was_active = int(row["is_active"] or 0) == 1

    account_rows_updated = 0
    if not was_active:
        account_rows_updated = int(
            conn.execute(
                """
                UPDATE accounts
                   SET is_active = 1,
                       updated_at = datetime('now')
                 WHERE id = ?
                   AND is_active = 0
                """,
                (account_id,),
            ).rowcount
            or 0
        )
    conn.commit()

    account = _account_to_dict(_fetch_account_or_raise(conn, account_id))

    return {
        "data": {
            "account": account,
            "account_id": account_id,
            "activated": bool(account_rows_updated),
            "already_active": was_active,
        },
        "summary": {
            "total_accounts": 1,
            "activated_accounts": int(bool(account_rows_updated)),
        },
        "cli_report": (
            f"Account {account_id} activated"
            if account_rows_updated
            else f"Account {account_id} already active"
        ),
    }
