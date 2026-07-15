"""Database backup and reset commands."""

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from typing import Any

from .. import config as config_module
from ..config import get_data_dir
from ..db import _connected_main_db_path, backup_database, wipe_runtime_data


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("db", parents=[format_parent], help="Database maintenance commands")
    db_sub = parser.add_subparsers(dest="db_command", required=True)

    p_status = db_sub.add_parser("status", parents=[format_parent], help="Show DB status overview")
    p_status.set_defaults(func=handle_status, command_name="db.status")

    p_backup = db_sub.add_parser("backup", parents=[format_parent], help="Create a timestamped backup bundle")
    p_backup.add_argument(
        "--output",
        help="Backup destination file or directory (defaults to the data backups/ directory)",
    )
    p_backup.add_argument("--user-id", help="User ID for encrypted backup key routing")
    backup_mode = p_backup.add_mutually_exclusive_group()
    backup_mode.add_argument("--portable", action="store_true", help="Embed db-dek.enc recovery payload when v3 is available")
    backup_mode.add_argument("--compact", action="store_true", help="Omit db-dek.enc recovery payload when v3 is available")
    p_backup.set_defaults(func=handle_backup, command_name="db.backup")

    p_backup_list = db_sub.add_parser("backup-list", parents=[format_parent], help="List recent backup bundles")
    p_backup_list.add_argument("--type", choices=["local", "offhost", "all"], default="all")
    p_backup_list.add_argument("--limit", type=int, default=20)
    p_backup_list.set_defaults(func=handle_backup_list, command_name="db.backup-list")

    p_verify = db_sub.add_parser("verify-backup", parents=[format_parent], help="Verify a backup bundle")
    p_verify.add_argument("path", help="Path to the backup bundle")
    p_verify.add_argument("--user-id", help="User ID for encrypted backup verification")
    p_verify.set_defaults(func=handle_verify_backup, command_name="db.verify-backup")

    p_restore = db_sub.add_parser(
        "restore",
        parents=[format_parent],
        help="Restore from a backup bundle (dry-run by default; if the live DB is corrupt, delete it first)",
    )
    p_restore.add_argument("--file", required=True, help="Path to the backup bundle")
    p_restore.add_argument("--user-id", help="User ID expected to own the backup")
    p_restore.add_argument("--yes", action="store_true", help="Apply the restore (without this, dry-run only)")
    p_restore.set_defaults(func=handle_restore, command_name="db.restore")

    p_prune = db_sub.add_parser("backup-prune", parents=[format_parent], help="Apply tiered retention to old backups")
    p_prune.add_argument("--yes", action="store_true", help="Actually delete backups (without this, dry-run only)")
    p_prune.add_argument("--user-id", help="User ID for encrypted backup key cleanup")
    p_prune.set_defaults(func=handle_backup_prune, command_name="db.backup-prune")

    p_export_preferences = db_sub.add_parser(
        "export-preferences",
        parents=[format_parent],
        help="Export preferences bundle (rules, budgets, goals, vendor memory, etc.)",
    )
    p_export_preferences.add_argument("--output", help="Output path for .tar.gz bundle")
    p_export_preferences.set_defaults(
        func=handle_export_preferences,
        command_name="db.export-preferences",
    )

    p_import_preferences = db_sub.add_parser(
        "import-preferences",
        parents=[format_parent],
        help="Import preferences from a .tar.gz bundle",
    )
    p_import_preferences.add_argument("--file", required=True, help="Path to the .tar.gz bundle")
    p_import_preferences.add_argument("--mode", choices=["merge", "overwrite"], default="merge")
    p_import_preferences.add_argument("--create-missing-categories", action="store_true")
    p_import_preferences.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preview changes without applying (default)",
    )
    p_import_preferences.add_argument(
        "--yes",
        action="store_true",
        help="Actually apply changes (disables dry-run)",
    )
    p_import_preferences.set_defaults(
        func=handle_import_preferences,
        command_name="db.import-preferences",
    )

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
        "--full",
        action="store_true",
        help="Also clear preference tables (budgets, goals, vendor_memory, manual_loans, category_mappings)",
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


def _backup_user_id(args) -> str:
    value = str(getattr(args, "user_id", "") or "").strip()
    if value:
        return value
    return str(config_module.default_user_id)


def handle_backup(args, conn) -> dict[str, Any]:
    from finance_cli.backup import create_backup

    user_id = _backup_user_id(args)
    result = create_backup(
        conn,
        destination=_as_path(args.output) if args.output else None,
        backup_type="local",
        user_id=user_id,
        portable=bool(getattr(args, "portable", False)),
        compact=bool(getattr(args, "compact", False)),
    )

    return {
        "data": {
            "backup_path": str(result.bundle_path),
            "bundle_path": str(result.bundle_path),
            "bundle_sha256": result.bundle_sha256,
            "size_bytes": result.bundle_size,
            "db_sha256": result.db_sha256,
            "migration_version": result.migration_ver,
            "duration_ms": result.duration_ms,
            "file_count": len(result.files),
        },
        "summary": {"size_bytes": result.bundle_size, "file_count": len(result.files)},
        "cli_report": (
            f"Backup created: {result.bundle_path} "
            f"({result.bundle_size:,} bytes, {len(result.files)} files)"
        ),
    }


def handle_export_preferences(args, conn) -> dict[str, Any]:
    from finance_cli.preferences import export_preferences

    result = export_preferences(
        conn,
        destination=_as_path(args.output) if getattr(args, "output", None) else None,
    )
    total_rows = int(sum(result.table_counts.values()))
    return {
        "data": {
            "bundle_path": str(result.bundle_path),
            "bundle_size": result.bundle_size,
            "table_counts": result.table_counts,
            "file_count": result.file_count,
            "categories_referenced": result.categories_referenced,
        },
        "summary": {
            "bundle_size": result.bundle_size,
            "total_rows": total_rows,
        },
        "cli_report": (
            f"Preferences exported to {result.bundle_path} "
            f"({result.bundle_size:,} bytes, "
            f"{total_rows} rows across {len(result.table_counts)} tables)"
        ),
    }


def handle_import_preferences(args, conn) -> dict[str, Any]:
    from finance_cli.preferences import import_preferences

    if args.mode == "overwrite" and not getattr(args, "yes", False):
        raise ValueError("overwrite mode requires --yes for safety (auto-backs up first)")

    dry_run = not getattr(args, "yes", False)
    result = import_preferences(
        Path(args.file),
        conn,
        mode=args.mode,
        create_missing_categories=getattr(args, "create_missing_categories", False),
        dry_run=dry_run,
    )
    total_imported = int(sum(result.tables_imported.values()))
    total_skipped = int(sum(result.tables_skipped.values()))
    report = (
        f"{'DRY RUN: ' if result.dry_run else ''}"
        f"Imported {total_imported} rows, skipped {total_skipped} conflicts "
        f"({len(result.categories_missing)} missing categories, "
        f"{result.accounts_unresolved} unresolved accounts)"
    )
    return {
        "data": {
            "dry_run": result.dry_run,
            "mode": result.mode,
            "tables_imported": result.tables_imported,
            "tables_skipped": result.tables_skipped,
            "categories_missing": result.categories_missing,
            "categories_created": result.categories_created,
            "accounts_resolved": result.accounts_resolved,
            "accounts_unresolved": result.accounts_unresolved,
            "files_copied": result.files_copied,
            "warnings": result.warnings,
        },
        "summary": {
            "total_imported": total_imported,
            "total_skipped": total_skipped,
        },
        "cli_report": report,
    }


def handle_reset(args, conn) -> dict[str, Any]:
    if not args.yes:
        raise ValueError("db reset is destructive; rerun with --yes to proceed")

    from finance_cli.backup import install_subscriber_lock_path, is_canonical_install_db_path
    from finance_cli.sync.subscriber_lock import acquire_install_lock_for_destructive_op

    db_path = _connected_main_db_path(conn)
    lock_context = (
        acquire_install_lock_for_destructive_op(install_subscriber_lock_path(), "reset")
        if db_path is not None and is_canonical_install_db_path(db_path)
        else nullcontext()
    )

    with lock_context:
        preserve_plaid_items = not bool(args.drop_plaid_items)
        backup_path: Path | None = None
        if not args.no_backup:
            backup_path = backup_database(conn=conn, destination=_as_path(args.backup_output))

        full = bool(getattr(args, "full", False))
        wipe_report = wipe_runtime_data(conn, preserve_plaid_items=preserve_plaid_items, full=full)
        conn.commit()

    rows_affected = int(sum(wipe_report.values()))
    data: dict[str, Any] = {
        "preserve_plaid_items": preserve_plaid_items,
        "full": full,
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


def handle_backup_list(args, conn) -> dict[str, Any]:
    from finance_cli.backup import list_backups

    backup_type = None if args.type == "all" else args.type
    entries = list_backups(conn, backup_type=backup_type, limit=args.limit)

    if not entries:
        cli_report = "No backups found"
    else:
        lines = [f"{'Created At':<20} {'Type':<14} {'Size (bytes)':>14}  Path"]
        for entry in entries:
            created_at = str(entry.get("created_at", ""))
            item_type = str(entry.get("backup_type", ""))
            size_bytes = int(entry.get("bundle_size") or 0)
            bundle_path = str(entry.get("bundle_path", ""))
            lines.append(f"{created_at:<20} {item_type:<14} {size_bytes:>14,}  {bundle_path}")
        cli_report = "\n".join(lines)

    return {
        "data": entries,
        "summary": {
            "count": len(entries),
            "backup_type": backup_type or "all",
            "limit": int(args.limit),
        },
        "cli_report": cli_report,
    }


def handle_verify_backup(args, conn) -> dict[str, Any]:
    from finance_cli.backup import verify_backup

    user_id = _backup_user_id(args)
    result = verify_backup(Path(args.path), conn=conn, user_id=user_id)
    status = "VALID" if result.valid else "INVALID"
    error_count = len(result.errors)
    warning_count = len(result.warnings)
    manifest = result.manifest if isinstance(result.manifest, dict) else {}
    manifest_files = manifest.get("files")
    file_count = len(manifest_files) if isinstance(manifest_files, list) else 0
    if manifest.get("schema_version") == 2:
        cli_report = (
            f"{status}: header OK, decrypt OK, signature "
            f"{'OK' if result.valid else 'FAILED'}, {file_count} files listed in manifest"
        )
        if not result.valid:
            cli_report = f"{status}: {error_count} errors, {warning_count} warnings"
    else:
        cli_report = f"{status}: {error_count} errors, {warning_count} warnings"

    return {
        "data": {
            "valid": result.valid,
            "manifest": result.manifest,
            "errors": result.errors,
            "warnings": result.warnings,
        },
        "summary": {
            "valid": result.valid,
            "error_count": error_count,
            "warning_count": warning_count,
        },
        "cli_report": cli_report,
    }


def handle_restore(args, conn) -> dict[str, Any]:
    from finance_cli.backup import restore_backup

    dry_run = not getattr(args, "yes", False)
    user_id = _backup_user_id(args)
    result = restore_backup(
        Path(args.file),
        dry_run=dry_run,
        conn=conn,
        expected_user_id=user_id,
        user_id=user_id,
    )
    status = "DRY RUN: would restore" if result.dry_run else ("Restored" if result.restored else "Restore skipped")
    return {
        "data": {
            "restored": result.restored,
            "dry_run": result.dry_run,
            "bundle_path": str(result.bundle_path),
            "warnings": result.warnings,
        },
        "summary": {
            "restored": result.restored,
            "dry_run": result.dry_run,
            "warning_count": len(result.warnings),
        },
        "cli_report": f"{status} from {result.bundle_path}"
        + (f" ({len(result.warnings)} warnings)" if result.warnings else ""),
    }


def handle_backup_prune(args, conn) -> dict[str, Any]:
    from finance_cli.backup import prune_backups

    user_id = _backup_user_id(args)
    result = prune_backups(conn, dry_run=not args.yes, data_dir=get_data_dir(), user_id=user_id)
    prefix = "DRY RUN: " if result.dry_run else ""
    return {
        "data": {
            "dry_run": result.dry_run,
            "kept": result.kept,
            "deleted": result.deleted,
            "deleted_paths": result.deleted_paths,
            "freed_bytes": result.freed_bytes,
            "scheduled_key_deletions": result.scheduled_key_deletions,
        },
        "summary": {
            "dry_run": result.dry_run,
            "kept": result.kept,
            "deleted": result.deleted,
            "freed_bytes": result.freed_bytes,
            "scheduled_key_deletions": result.scheduled_key_deletions,
        },
        "cli_report": (
            f"{prefix}Kept {result.kept}, deleted {result.deleted}, "
            f"freed {result.freed_bytes:,} bytes"
            + (
                f", scheduled key deletion for {result.scheduled_key_deletions} bundles"
                if result.scheduled_key_deletions
                else ""
            )
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
