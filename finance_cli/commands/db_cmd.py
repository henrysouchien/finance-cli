"""Database backup and reset commands."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..config import get_db_path
from ..db import backup_database, wipe_runtime_data


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("db", parents=[format_parent], help="Database maintenance commands")
    db_sub = parser.add_subparsers(dest="db_command", required=True)

    p_status = db_sub.add_parser("status", parents=[format_parent], help="Show DB status overview")
    p_status.set_defaults(func=handle_status, command_name="db.status")

    p_backup = db_sub.add_parser("backup", parents=[format_parent], help="Create a timestamped DB backup")
    p_backup.add_argument(
        "--output",
        help="Backup destination file or directory (defaults to DB directory with timestamped filename)",
    )
    p_backup.set_defaults(func=handle_backup, command_name="db.backup")

    p_reset = db_sub.add_parser(
        "reset",
        parents=[format_parent],
        help="Wipe runtime data while optionally preserving Plaid item credentials",
    )
    p_reset.add_argument("--yes", action="store_true", help="Required confirmation for destructive reset")
    p_reset.add_argument(
        "--drop-plaid-items",
        action="store_true",
        help="Also delete plaid_items instead of preserving access-token references",
    )
    p_reset.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip pre-reset backup (not recommended)",
    )
    p_reset.add_argument(
        "--backup-output",
        help="Backup destination file or directory used before reset",
    )
    p_reset.set_defaults(func=handle_reset, command_name="db.reset")


def _as_path(raw: str | None) -> Path | None:
    value = str(raw or "").strip()
    return Path(value).expanduser() if value else None


def handle_backup(args, conn) -> dict[str, Any]:
    backup_path = backup_database(conn=conn, destination=_as_path(args.output))
    db_path = get_db_path().expanduser().resolve()
    size_bytes = int(backup_path.stat().st_size)

    return {
        "data": {
            "db_path": str(db_path),
            "backup_path": str(backup_path),
            "size_bytes": size_bytes,
        },
        "summary": {"size_bytes": size_bytes},
        "cli_report": f"Database backup created: {backup_path}",
    }


def handle_reset(args, conn) -> dict[str, Any]:
    if not args.yes:
        raise ValueError("db reset is destructive; rerun with --yes to proceed")

    preserve_plaid_items = not bool(args.drop_plaid_items)
    backup_path: Path | None = None
    if not args.no_backup:
        backup_path = backup_database(conn=conn, destination=_as_path(args.backup_output))

    wipe_report = wipe_runtime_data(conn, preserve_plaid_items=preserve_plaid_items)
    conn.commit()

    rows_affected = int(sum(wipe_report.values()))
    data: dict[str, Any] = {
        "preserve_plaid_items": preserve_plaid_items,
        "wipe_report": wipe_report,
        "rows_affected": rows_affected,
    }
    if backup_path is not None:
        data["backup_path"] = str(backup_path)

    return {
        "data": data,
        "summary": {
            "rows_affected": rows_affected,
            "preserve_plaid_items": preserve_plaid_items,
        },
        "cli_report": (
            f"Database reset complete: rows_affected={rows_affected} "
            f"preserve_plaid_items={preserve_plaid_items}"
        ),
    }


def handle_status(args, conn) -> dict[str, Any]:
    txn_counts_row = conn.execute(
        """
        SELECT COUNT(*) AS total_count,
               SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_count,
               SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS inactive_count
          FROM transactions
        """
    ).fetchone()
    total_count = int(txn_counts_row["total_count"] or 0)
    active_count = int(txn_counts_row["active_count"] or 0)
    inactive_count = int(txn_counts_row["inactive_count"] or 0)

    date_range_row = conn.execute(
        "SELECT MIN(date) AS earliest, MAX(date) AS latest FROM transactions WHERE is_active = 1"
    ).fetchone()
    earliest = date_range_row["earliest"]
    latest = date_range_row["latest"]

    accounts_row = conn.execute("SELECT COUNT(*) AS account_count FROM accounts WHERE is_active = 1").fetchone()
    account_count = int(accounts_row["account_count"] or 0)

    uncategorized_row = conn.execute(
        "SELECT COUNT(*) AS uncategorized_count FROM transactions WHERE is_active = 1 AND category_id IS NULL"
    ).fetchone()
    uncategorized_count = int(uncategorized_row["uncategorized_count"] or 0)

    source_rows = conn.execute(
        """
        SELECT COALESCE(category_source, 'uncategorized') AS category_source,
               COUNT(*) AS count
          FROM transactions
         WHERE is_active = 1
         GROUP BY COALESCE(category_source, 'uncategorized')
         ORDER BY count DESC, category_source ASC
        """
    ).fetchall()
    source_distribution = [
        {"category_source": row["category_source"], "count": int(row["count"])}
        for row in source_rows
    ]

    category_rows = conn.execute(
        """
        SELECT COALESCE(c.name, 'Uncategorized') AS category_name,
               COUNT(*) AS count
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
         GROUP BY COALESCE(c.name, 'Uncategorized')
         ORDER BY count DESC, category_name ASC
         LIMIT 15
        """
    ).fetchall()
    top_categories = [{"category_name": row["category_name"], "count": int(row["count"])} for row in category_rows]

    last_import_row = conn.execute("SELECT MAX(created_at) AS last_import_at FROM import_batches").fetchone()
    last_import_at = last_import_row["last_import_at"]

    payment_row = conn.execute(
        "SELECT COUNT(*) AS payment_count FROM transactions WHERE is_active = 1 AND is_payment = 1"
    ).fetchone()
    payment_count = int(payment_row["payment_count"] or 0)

    data = {
        "transaction_counts": {
            "total": total_count,
            "active": active_count,
            "inactive": inactive_count,
        },
        "date_range": {
            "earliest": earliest,
            "latest": latest,
        },
        "active_account_count": account_count,
        "uncategorized_count": uncategorized_count,
        "payment_count": payment_count,
        "category_source_distribution": source_distribution,
        "top_categories": top_categories,
        "last_import_at": last_import_at,
    }
    cli_report_lines = [
        "Database Status",
        "",
        f"  Transactions:  {active_count:,} active ({inactive_count:,} inactive)",
        f"  Date Range:    {earliest or '-'} to {latest or '-'}",
        f"  Accounts:      {account_count:,} active",
        f"  Uncategorized: {uncategorized_count:,}",
        f"  Payments:      {payment_count:,}",
        f"  Last Import:   {last_import_at or 'never'}",
    ]
    if top_categories:
        cli_report_lines.append("")
        cli_report_lines.append("Top Categories:")
        for cat in top_categories[:5]:
            cli_report_lines.append(f"  {cat['category_name']}: {cat['count']:,}")

    return {
        "data": data,
        "summary": {
            "total_transactions": total_count,
            "active_transactions": active_count,
            "active_accounts": account_count,
        },
        "cli_report": "\n".join(cli_report_lines),
    }
