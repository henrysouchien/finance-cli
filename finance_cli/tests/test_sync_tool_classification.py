from __future__ import annotations

import asyncio

from finance_cli.mcp_server import mcp
from finance_cli.sync.tool_classification import (
    DB_WRITE_TOOLS,
    NO_SYNC_TOOLS,
    SERVER_PROXIED_MUTATING_TOOLS,
    SERVER_PROXIED_TOOLS,
)


def _live_mcp_tool_names() -> frozenset[str]:
    return frozenset(tool.name for tool in asyncio.run(mcp.list_tools(run_middleware=False)))


def test_sync_tool_sets_are_pairwise_disjoint() -> None:
    assert DB_WRITE_TOOLS.isdisjoint(SERVER_PROXIED_TOOLS)
    assert DB_WRITE_TOOLS.isdisjoint(NO_SYNC_TOOLS)
    assert SERVER_PROXIED_TOOLS.isdisjoint(NO_SYNC_TOOLS)


def test_sync_tool_sets_cover_every_live_tool() -> None:
    live = _live_mcp_tool_names()
    classified = DB_WRITE_TOOLS | SERVER_PROXIED_TOOLS | NO_SYNC_TOOLS

    assert not (live - classified)
    assert not (classified - live)


def test_sync_tool_sets_include_expected_special_cases() -> None:
    assert {"txn_add", "setup_init", "agent_memory_update", "balance_update"} <= DB_WRITE_TOOLS
    assert {"plaid_sync", "setup_status", "db_restore", "activate_skill"} <= SERVER_PROXIED_TOOLS
    assert {"plaid_sync", "db_restore"} <= SERVER_PROXIED_MUTATING_TOOLS
    assert {"setup_status", "activate_skill"} & SERVER_PROXIED_MUTATING_TOOLS == set()
    assert {
        "db_backup",
        "notify_test",
        "agent_session_write",
        "error_update",
        "issue_update",
        "finance_log_issue",
    } <= NO_SYNC_TOOLS


def test_diagnostic_issue_tools_do_not_sync() -> None:
    assert "error_update" not in DB_WRITE_TOOLS
    assert "issue_update" not in DB_WRITE_TOOLS
    assert "finance_log_issue" not in DB_WRITE_TOOLS
