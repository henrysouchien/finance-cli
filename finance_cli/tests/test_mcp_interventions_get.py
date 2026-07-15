from __future__ import annotations

from argparse import Namespace
from contextlib import contextmanager
from pathlib import Path
import sqlite3

import pytest

import finance_cli.intervention_engine as intervention_engine
from finance_cli.commands import intervention_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.gateway.tools import BRIDGE_TOOLS, READ_ONLY_TOOLS
from finance_cli.tests.test_intervention_engine import _seed_account, _seed_credit_liability
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


def _seed_d1(db_path: Path) -> None:
    with connect(db_path) as conn:
        high = _seed_account(conn, account_type="credit_card", balance_cents=-90_000, institution_name="High")
        mid = _seed_account(conn, account_type="credit_card", balance_cents=-30_000, institution_name="Mid")
        low = _seed_account(conn, account_type="credit_card", balance_cents=-5_000, institution_name="Low")
        _seed_credit_liability(conn, account_id=high, apr_purchase=29.99, minimum_payment_cents=3_000)
        _seed_credit_liability(conn, account_id=mid, apr_purchase=19.99, minimum_payment_cents=500)
        _seed_credit_liability(conn, account_id=low, apr_purchase=9.99, minimum_payment_cents=200)


def test_interventions_get_tool_returns_clean_envelope_without_logging(db_path: Path) -> None:
    _seed_d1(db_path)

    with _mcp_user_db(db_path) as mcp_server:
        result = mcp_server.interventions_get(surface="agent_prompt")

    with connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]
    assert set(result) == {"data", "summary"}
    assert result["data"]["surface"] == "agent_prompt"
    assert result["data"]["interventions"]
    assert "log_surface" not in result["data"]
    assert "cli_report" not in result
    assert int(count) == 0


def test_interventions_get_defaults_to_agent_prompt(db_path: Path) -> None:
    _seed_d1(db_path)

    with _mcp_user_db(db_path) as mcp_server:
        result = mcp_server.interventions_get()

    assert result["data"]["surface"] == "agent_prompt"
    assert result["summary"]["surface"] == "agent_prompt"


def test_interventions_get_surface_override(db_path: Path) -> None:
    _seed_d1(db_path)

    with _mcp_user_db(db_path) as mcp_server:
        dashboard = mcp_server.interventions_get(surface="dashboard")
        action_queue = mcp_server.interventions_get(surface="action_queue")

    assert dashboard["data"]["surface"] == "dashboard"
    assert action_queue["data"]["surface"] == "action_queue"
    assert dashboard["summary"]["count"] <= action_queue["summary"]["count"]


def test_interventions_get_initializes_stale_mcp_connection_before_read_only(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        conn.execute("DROP TABLE user_strategy_preferences")
        conn.execute("DELETE FROM schema_version WHERE version = 68")
        conn.commit()

    with _mcp_user_db(db_path) as mcp_server:
        result = mcp_server.interventions_get(surface="action_queue")

    with connect(db_path) as conn:
        strategy_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'user_strategy_preferences'"
        ).fetchone()
        migration_row = conn.execute(
            "SELECT 1 FROM schema_version WHERE version = 68"
        ).fetchone()

    assert result["data"]["surface"] == "action_queue"
    assert strategy_table is not None
    assert migration_row is not None


def test_interventions_get_invalid_surface_returns_structured_error(db_path: Path) -> None:
    _seed_d1(db_path)

    with _mcp_user_db(db_path) as mcp_server:
        response = mcp_server.interventions_get(surface="invalid")

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "Unknown surface" in response["message"]
    assert {"name": "interventions_get", "args": {"surface": "agent_prompt"}} in response[
        "suggested_tool_calls"
    ]


def test_handle_get_query_only_blocks_accidental_writes(db_path: Path, monkeypatch) -> None:
    def write_during_evaluation(conn, surface, *, rules_path=None, log_to_surface=None, now=None):
        conn.execute(
            """
            INSERT INTO intervention_log (pattern_id, fired_at, surface, user_action, headline, payload)
            VALUES ('X-1', '2026-04-09 12:00:00', 'agent_prompt', 'pending', 'bad', '{}')
            """
        )
        raise AssertionError("write should fail before this point")

    monkeypatch.setattr(intervention_engine, "evaluate_for_surface", write_during_evaluation)

    with connect(db_path) as conn:
        with pytest.raises(sqlite3.OperationalError, match="readonly database"):
            intervention_cmd.handle_get(Namespace(surface="agent_prompt"), conn)


def test_interventions_get_tool_classification() -> None:
    assert "interventions_get" in READ_ONLY_TOOLS
    assert "interventions_get" not in BRIDGE_TOOLS
