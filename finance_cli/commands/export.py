"""Export commands."""

from __future__ import annotations

from typing import Any

from ..exporters import export_monthly_summary_csv, export_transactions_csv, export_wave


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("export", parents=[format_parent], help="Export data")
    export_sub = parser.add_subparsers(dest="export_command", required=True)

    p_csv = export_sub.add_parser("csv", parents=[format_parent], help="Export transactions CSV")
    p_csv.add_argument("--from", dest="date_from")
    p_csv.add_argument("--to", dest="date_to")
    p_csv.add_argument("--category")
    p_csv.add_argument("--output", required=True)
    p_csv.set_defaults(func=handle_csv, command_name="export.csv")

    p_summary = export_sub.add_parser("summary", parents=[format_parent], help="Export monthly summary")
    p_summary.add_argument("--month", required=True)
    p_summary.add_argument("--output", required=True)
    p_summary.set_defaults(func=handle_summary, command_name="export.summary")

    p_wave = export_sub.add_parser("wave", parents=[format_parent], help="Export Wave accounting CSVs")
    p_wave.add_argument("--month", required=True)
    p_wave.add_argument("--output", required=True)
    p_wave.set_defaults(func=handle_wave, command_name="export.wave")

    p_sheets = export_sub.add_parser("sheets", parents=[format_parent], help="Export to Google Sheets")
    p_sheets.add_argument("--from", dest="date_from")
    p_sheets.add_argument("--to", dest="date_to")
    p_sheets.add_argument("--year")
    p_sheets.add_argument("--auth", action="store_true", help="Run OAuth setup only")
    sheets_target_group = p_sheets.add_mutually_exclusive_group()
    sheets_target_group.add_argument("--new", action="store_true", help="Create a new spreadsheet")
    sheets_target_group.add_argument("--spreadsheet-id", dest="spreadsheet_id")
    p_sheets.set_defaults(func=handle_sheets, command_name="export.sheets")


def handle_csv(args, conn) -> dict[str, Any]:
    count = export_transactions_csv(
        conn,
        output_path=args.output,
        date_from=args.date_from,
        date_to=args.date_to,
        category_name=args.category,
    )
    return {
        "data": {"output": args.output, "rows": count},
        "summary": {"total_transactions": count},
        "cli_report": f"Wrote {count} rows to {args.output}",
    }


def handle_summary(args, conn) -> dict[str, Any]:
    count = export_monthly_summary_csv(conn, month=args.month, output_path=args.output)
    return {
        "data": {"output": args.output, "rows": count, "month": args.month},
        "summary": {"total_categories": count},
        "cli_report": f"Wrote {count} summary rows to {args.output}",
    }


def handle_wave(args, conn) -> dict[str, Any]:
    report = export_wave(conn, month=args.month, output_dir=args.output)
    return {
        "data": report,
        "summary": {"total_transactions": int(report.get("rows", 0)), "total_files": len(report.get("files", []))},
        "cli_report": f"Wrote {len(report.get('files', []))} files to {args.output}",
    }


def handle_sheets(args, conn) -> dict[str, Any]:
    from ..sheets_export import export_to_sheets

    report = export_to_sheets(
        conn,
        date_from=getattr(args, "date_from", None),
        date_to=getattr(args, "date_to", None),
        year=getattr(args, "year", None),
        spreadsheet_id=getattr(args, "spreadsheet_id", None),
        force_new=bool(getattr(args, "new", False)),
        auth_only=bool(getattr(args, "auth", False)),
        interactive=bool(getattr(args, "interactive", True)),
    )

    if bool(getattr(args, "auth", False)):
        return {
            "data": report,
            "summary": {"auth_configured": True},
            "cli_report": "Google Sheets auth setup completed.",
        }

    successful_tabs = report.get("tabs", [])
    skipped_tabs = report.get("skipped_tabs", [])
    failed_tabs = report.get("failed_tabs", [])

    cli_report = (
        f"Exported {len(successful_tabs)} tab(s) to {report.get('spreadsheet_url')}"
        if report.get("spreadsheet_url")
        else f"Exported {len(successful_tabs)} tab(s) to Google Sheets"
    )
    if skipped_tabs:
        cli_report += f" (skipped: {', '.join(str(item) for item in skipped_tabs)})"
    if failed_tabs:
        cli_report += f" (failed: {', '.join(str(item.get('tab')) for item in failed_tabs)})"

    return {
        "data": report,
        "summary": {
            "spreadsheet_id": report.get("spreadsheet_id"),
            "total_tabs": len(successful_tabs),
            "skipped_tabs": len(skipped_tabs),
            "failed_tabs": len(failed_tabs),
        },
        "cli_report": cli_report,
    }
