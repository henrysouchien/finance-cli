"""Runtime error and agent issue triage commands."""

from __future__ import annotations

from typing import Any

_TRIAGE_STATUSES = ("open", "investigating", "resolved", "wontfix", "all")
_UPDATE_STATUSES = ("open", "investigating", "resolved", "wontfix")
_ERROR_SEVERITIES = ("critical", "error", "warning")


def register(subparsers, format_parent) -> None:
    error_parser = subparsers.add_parser(
        "error",
        parents=[format_parent],
        help="Inspect and update captured runtime errors",
    )
    error_sub = error_parser.add_subparsers(dest="error_command", required=True)

    p_error_list = error_sub.add_parser(
        "list",
        parents=[format_parent],
        help="List captured runtime errors",
    )
    p_error_list.add_argument("--status", choices=_TRIAGE_STATUSES, default="open")
    p_error_list.add_argument("--severity", choices=_ERROR_SEVERITIES)
    p_error_list.add_argument("--source")
    p_error_list.add_argument("--days", type=int, default=7)
    p_error_list.set_defaults(
        func=handle_error_list,
        command_name="error.list",
    )

    p_error_show = error_sub.add_parser(
        "show",
        parents=[format_parent],
        help="Show one captured runtime error with occurrence timeline",
    )
    p_error_show.add_argument("error_id")
    p_error_show.set_defaults(
        func=handle_error_show,
        command_name="error.show",
    )

    p_error_stats = error_sub.add_parser(
        "stats",
        parents=[format_parent],
        help="Summarize captured runtime errors by source and severity",
    )
    p_error_stats.add_argument("--days", type=int, default=30)
    p_error_stats.set_defaults(
        func=handle_error_stats,
        command_name="error.stats",
    )

    p_error_update = error_sub.add_parser(
        "update",
        parents=[format_parent],
        help="Update captured runtime error triage status",
    )
    p_error_update.add_argument("error_id")
    p_error_update.add_argument("--status", choices=_UPDATE_STATUSES, required=True)
    p_error_update.add_argument("--resolution")
    p_error_update.set_defaults(
        func=handle_error_update,
        command_name="error.update",
    )

    issue_parser = subparsers.add_parser(
        "issue",
        parents=[format_parent],
        help="Inspect and update agent-reported issues",
    )
    issue_sub = issue_parser.add_subparsers(dest="issue_command", required=True)

    p_issue_list = issue_sub.add_parser(
        "list",
        parents=[format_parent],
        help="List agent-reported issues",
    )
    p_issue_list.add_argument("--status", choices=_TRIAGE_STATUSES, default="open")
    p_issue_list.set_defaults(
        func=handle_issue_list,
        command_name="issue.list",
    )

    p_issue_update = issue_sub.add_parser(
        "update",
        parents=[format_parent],
        help="Update agent-reported issue triage status",
    )
    p_issue_update.add_argument("issue_id")
    p_issue_update.add_argument("--status", choices=_UPDATE_STATUSES, required=True)
    p_issue_update.add_argument("--resolution")
    p_issue_update.set_defaults(
        func=handle_issue_update,
        command_name="issue.update",
    )


def _mcp_server():
    from .. import mcp_server

    return mcp_server


def handle_error_list(args, conn) -> dict[str, Any]:
    return _mcp_server()._handle_error_list(args, conn)


def handle_error_show(args, conn) -> dict[str, Any]:
    return _mcp_server()._handle_error_show(args, conn)


def handle_error_stats(args, conn) -> dict[str, Any]:
    return _mcp_server()._handle_error_stats(args, conn)


def handle_error_update(args, conn) -> dict[str, Any]:
    return _mcp_server()._handle_error_update(args, conn)


def handle_issue_list(args, conn) -> dict[str, Any]:
    return _mcp_server()._handle_issue_list(args, conn)


def handle_issue_update(args, conn) -> dict[str, Any]:
    return _mcp_server()._handle_issue_update(args, conn)
