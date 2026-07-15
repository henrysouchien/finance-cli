from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

import finance_cli.telegram_bot.approval as approval_mod
from finance_cli.db import connect, initialize_database
from finance_cli.gateway.tools import APPROVAL_REQUIRED_TOOLS
from finance_cli.tests.test_intervention_engine import _seed_intervention_log
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


@contextmanager
def _mcp_user_db(db_path: Path):
    import finance_cli.mcp_server as mcp_server

    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        yield mcp_server
    finally:
        reset_user_context(token)


def test_interventions_act_tool_updates_row_via_call_wrapper(db_path: Path) -> None:
    with connect(db_path) as conn:
        log_id = _seed_intervention_log(conn, pattern_id="D-1", fired_at="2026-04-01 12:00:00")

    with _mcp_user_db(db_path) as mcp_server:
        result = mcp_server.interventions_act(log_id=log_id)

    with connect(db_path) as conn:
        row = conn.execute("SELECT user_action, acted_at FROM intervention_log WHERE id = ?", (log_id,)).fetchone()

    assert result["data"]["id"] == log_id
    assert result["data"]["user_action"] == "acted"
    assert result["summary"]["action"] == "acted"
    assert row["user_action"] == "acted"
    assert row["acted_at"] is not None


def test_phase3_intervention_tools_require_approval_and_have_summaries() -> None:
    expected = {
        "interventions_act",
        "interventions_dismiss",
        "interventions_mute",
        "interventions_unmute",
    }

    assert expected.issubset(APPROVAL_REQUIRED_TOOLS)
    assert expected.issubset(set(approval_mod._TOOL_SUMMARIES))
