from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.db import initialize_database
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


@pytest.fixture()
def mcp_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)

    import finance_cli.mcp_server as mcp_server

    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        yield mcp_server
    finally:
        reset_user_context(token)


def test_mcp_tool_exception_returns_structured_envelope(mcp_module) -> None:
    response = mcp_module.interventions_get(surface="invalid")

    assert response["status"] == "error"
    assert response["error_class"] == "ValueError"
    assert "Unknown surface" in response["message"]
    assert response["error"] == response["message"]
    assert response["names_correction"]["tool"] == "interventions_get"
    assert {"name": "interventions_get", "args": {"surface": "agent_prompt"}} in response[
        "suggested_tool_calls"
    ]


def test_mcp_returned_error_dict_is_normalized(mcp_module, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mcp_module, "_call", lambda *args, **kwargs: {"status": "error", "error": "bad account"})

    response = mcp_module.account_list()

    assert response["status"] == "error"
    assert response["error_class"] == "ToolReturnedError"
    assert response["message"] == "bad account"
    assert response["names_correction"]["tool"] == "account_list"
    assert {"name": "account_list", "args": {"include_inactive": True}} in response[
        "suggested_tool_calls"
    ]
