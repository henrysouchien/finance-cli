"""CLI entry point for finance_cli."""

from __future__ import annotations

import argparse
import sys

from .commands import (
    account_cmd,
    balance_cmd,
    biz_cmd,
    budget,
    cat,
    daily,
    debt_cmd,
    db_cmd,
    dedup_cmd,
    export,
    goal_cmd,
    ingest,
    intervention_cmd,
    liability_cmd,
    liquidity_cmd,
    loan_cmd,
    monthly_cmd,
    notify_cmd,
    ops_cmd,
    plaid_cmd,
    plan,
    projection_cmd,
    provider_cmd,
    reminder_cmd,
    rules,
    setup_cmd,
    stripe_cmd,
    schwab_cmd,
    spending_cmd,
    subs,
    summary_cmd,
    triage_cmd,
    txn,
    weekly,
)
from .commands.common import error_envelope, print_envelope, success_envelope
from .config import auto_migrate_data, get_db_path, load_dotenv
from .db import connect, initialize_database
from .error_capture import capture_error
from .exceptions import FinanceCLIError
from .logging_config import setup_logging
from .migrate_legacy import migrate_legacy_source
from .sync.cli_proxy import local_sync_proxy_spec, run_local_sync_proxy


_COMMAND_DEFAULT_DB_ATTR = "uses_default_db"


class CLIParseError(Exception):
    """Raised when CLI argument parsing fails with non-zero exit status."""

    def __init__(self, message: str, exit_code: int = 2) -> None:
        super().__init__(message)
        self.exit_code = int(exit_code)


class SafeArgumentParser(argparse.ArgumentParser):
    """ArgumentParser that raises exceptions instead of exiting on parse errors."""

    def error(self, message: str) -> None:
        raise CLIParseError(message, exit_code=2)

    def exit(self, status: int = 0, message: str | None = None) -> None:
        if status == 0:
            super().exit(status=status, message=message)
            return
        detail = (message or "").strip() or "Argument parsing failed"
        raise CLIParseError(detail, exit_code=status)


def register_migrate_command(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("migrate", parents=[format_parent], help="Migrate legacy financial_system CSVs")
    parser.add_argument("--source", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.set_defaults(func=handle_migrate, command_name="migrate")


def handle_migrate(args, conn):
    summary = migrate_legacy_source(conn, source_dir=args.source, dry_run=args.dry_run)
    txn_inserted = sum(item["inserted"] for item in summary.get("transactions", []))
    cli_report = f"Migrated {len(summary.get('transactions', []))} files, inserted={txn_inserted}"
    return {
        "data": summary,
        "summary": {
            "total_transactions": txn_inserted,
            "total_amount": 0,
        },
        "cli_report": cli_report,
    }


def should_capture_cli_error(exc: Exception) -> bool:
    if getattr(exc, "_b3_captured", False):
        return False
    if isinstance(exc, FinanceCLIError):
        return int(getattr(exc, "http_status", 500) or 500) >= 500
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = SafeArgumentParser(
        prog="finance_cli",
        description="CashNerd: personal finance tools that build understanding over time.",
    )
    format_parent = SafeArgumentParser(add_help=False)
    format_parent.add_argument("--format", choices=["json", "cli"], default="json")

    subparsers = parser.add_subparsers(dest="command", required=True)
    txn.register(subparsers, format_parent)
    account_cmd.register(subparsers, format_parent)
    cat.register(subparsers, format_parent)
    daily.register(subparsers, format_parent)
    weekly.register(subparsers, format_parent)
    budget.register(subparsers, format_parent)
    export.register(subparsers, format_parent)
    ingest.register(subparsers, format_parent)
    db_cmd.register(subparsers, format_parent)
    dedup_cmd.register(subparsers, format_parent)
    subs.register(subparsers, format_parent)
    liquidity_cmd.register(subparsers, format_parent)
    balance_cmd.register(subparsers, format_parent)
    liability_cmd.register(subparsers, format_parent)
    loan_cmd.register(subparsers, format_parent)
    plan.register(subparsers, format_parent)
    plaid_cmd.register(subparsers, format_parent)
    stripe_cmd.register(subparsers, format_parent)
    schwab_cmd.register(subparsers, format_parent)
    provider_cmd.register(subparsers, format_parent)
    rules.register(subparsers, format_parent)
    setup_cmd.register(subparsers, format_parent)
    monthly_cmd.register(subparsers, format_parent)
    notify_cmd.register(subparsers, format_parent)
    debt_cmd.register(subparsers, format_parent)
    biz_cmd.register(subparsers, format_parent)
    summary_cmd.register(subparsers, format_parent)
    spending_cmd.register(subparsers, format_parent)
    projection_cmd.register(subparsers, format_parent)
    goal_cmd.register(subparsers, format_parent)
    intervention_cmd.register(subparsers, format_parent)
    reminder_cmd.register(subparsers, format_parent)
    triage_cmd.register(subparsers, format_parent)
    ops_cmd.register(subparsers, format_parent)
    register_migrate_command(subparsers, format_parent)

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    auto_migrate_data()
    setup_logging()
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except CLIParseError as exc:
        command_name = "unknown"
        if argv:
            head = argv[0].strip()
            if head:
                command_name = head
        envelope = error_envelope(command_name, str(exc))
        print_envelope(envelope, "json")
        return exc.exit_code

    try:
        proxy_spec = local_sync_proxy_spec(args)
        if proxy_spec is not None:
            result = run_local_sync_proxy(proxy_spec, args)
            envelope = success_envelope(
                command=args.command_name,
                data=result.get("data", {}),
                summary=result.get("summary"),
                cli_report=result.get("cli_report"),
            )
            print_envelope(envelope, args.format)
            return 0

        uses_default_db = bool(getattr(args, _COMMAND_DEFAULT_DB_ATTR, True))
        if uses_default_db:
            initialize_database()
        command_name = getattr(args, "command_name", "")
        if not uses_default_db:
            result = args.func(args, None)
        elif command_name == "db.restore":
            conn = connect()
            try:
                result = args.func(args, conn)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            with connect() as conn:
                result = args.func(args, conn)
        envelope = success_envelope(
            command=args.command_name,
            data=result.get("data", {}),
            summary=result.get("summary"),
            cli_report=result.get("cli_report"),
        )
        print_envelope(envelope, args.format)
        return 0
    except Exception as exc:
        command_name = getattr(args, "command_name", "unknown")
        if should_capture_cli_error(exc):
            capture_error(
                exc,
                source="cli",
                endpoint=command_name or "unknown",
                db_path=get_db_path(),
            )
        envelope = error_envelope(command_name, str(exc))
        output_format = getattr(args, "format", "json")
        print_envelope(envelope, output_format)
        return 1


if __name__ == "__main__":
    sys.exit(main())
