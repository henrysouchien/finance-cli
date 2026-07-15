from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.gateway import tools as gateway_tools
from finance_cli.interventions.context import build_context
from finance_cli.strategy_preferences import (
    clear_strategy_preference,
    get_strategy_preferences,
    set_strategy_preference,
)


NOW = datetime(2026, 4, 9, 12, 0, 0)


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def test_set_get_and_clear_strategy_preference(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = set_strategy_preference(
            conn,
            domain="debt",
            strategy="snowball",
            rationale="Small wins matter right now.",
            source="user",
            evidence={"conversation_id": "session-1"},
        )
        preference = result["data"]["by_domain"]["debt"]

        assert preference["strategy"] == "snowball"
        assert preference["rationale"] == "Small wins matter right now."
        assert preference["source"] == "user"
        assert preference["evidence"] == {"conversation_id": "session-1"}
        assert preference["created_at"]
        assert preference["updated_at"]

        all_preferences = get_strategy_preferences(conn)
        cleared = clear_strategy_preference(conn, domain="debt")
        after_clear = get_strategy_preferences(conn, domain="debt")

    assert all_preferences["summary"]["count"] == 1
    assert cleared["data"] == {"domain": "debt", "cleared": True}
    assert after_clear["data"]["preferences"] == []


def test_strategy_preference_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="domain must be one of"):
            set_strategy_preference(conn, domain="investing", strategy="avalanche")
        with pytest.raises(ValueError, match="strategy for debt must be one of"):
            set_strategy_preference(conn, domain="debt", strategy="fastest")
        with pytest.raises(ValueError, match="source must be one of"):
            set_strategy_preference(conn, domain="debt", strategy="avalanche", source="chat")
        with pytest.raises(ValueError, match="evidence must be a dict"):
            set_strategy_preference(conn, domain="debt", strategy="avalanche", evidence=["bad"])  # type: ignore[arg-type]


def test_build_context_loads_debt_strategy_preference(db_path: Path) -> None:
    with connect(db_path) as conn:
        set_strategy_preference(
            conn,
            domain="debt",
            strategy="snowball",
            rationale="The user explicitly chose momentum over interest savings.",
            source="user",
        )

        ctx = build_context(conn, now=NOW)

    assert ctx.strategy_prefs.debt_strategy == "snowball"
    assert ctx.strategy_prefs.debt_rationale == "The user explicitly chose momentum over interest savings."
    assert ctx.strategy_prefs.debt_source == "user"
    assert ctx.strategy_prefs.is_empty() is False


def test_strategy_preference_tools_are_classified_for_gateway_and_sync() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS, NO_SYNC_TOOLS

    assert "strategy_preference_get" in gateway_tools.READ_ONLY_TOOLS
    assert "strategy_preference_get" not in gateway_tools.BRIDGE_TOOLS
    assert "strategy_preference_get" in NO_SYNC_TOOLS

    assert {"strategy_preference_set", "strategy_preference_clear"} <= gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert {"strategy_preference_set", "strategy_preference_clear"} <= DB_WRITE_TOOLS
    assert {"strategy_preference_set", "strategy_preference_clear"} <= gateway_tools.COACH_DEBT_PAYOFF_AUTO_APPROVED
