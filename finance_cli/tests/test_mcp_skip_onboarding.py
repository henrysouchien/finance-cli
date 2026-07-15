from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.skill_state import SkillStateStore


def _init_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    return db_path


def _add_history(conn) -> None:
    conn.execute(
        """
        INSERT INTO accounts (id, institution_name, account_name, account_type, is_active)
        VALUES ('acct_1', 'Test Bank', 'Checking', 'checking', 1)
        """
    )
    conn.execute(
        """
        INSERT INTO transactions (id, account_id, date, description, amount_cents, source, is_active)
        VALUES ('txn_1', 'acct_1', '2026-04-01', 'Coffee', -500, 'manual', 1)
        """
    )
    conn.execute(
        """
        INSERT INTO transactions (id, account_id, date, description, amount_cents, source, is_active)
        VALUES ('txn_2', 'acct_1', '2026-05-01', 'Coffee', -600, 'manual', 1)
        """
    )


def _add_account(conn) -> None:
    conn.execute(
        """
        INSERT INTO accounts (id, institution_name, account_name, account_type, is_active)
        VALUES ('acct_1', 'Test Bank', 'Checking', 'checking', 1)
        """
    )


def test_skip_refused_before_phase_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _init_db(tmp_path, monkeypatch)
    from finance_cli.mcp_server import skip_onboarding

    result = skip_onboarding()

    assert result["status"] == "error"
    assert result["error_class"] == "ConflictError"
    assert "Connect a bank or upload a CSV" in result["message"]
    assert SkillStateStore(tmp_path / "skill_state.json").get("onboarding") == {}


def test_skip_refused_with_minimal_ack_but_no_account(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _init_db(tmp_path, monkeypatch)
    SkillStateStore(tmp_path / "skill_state.json").set(
        "onboarding",
        {"data_minimal_acknowledged": True},
    )
    from finance_cli.mcp_server import skip_onboarding

    result = skip_onboarding()

    assert result["status"] == "error"
    assert result["error_class"] == "ConflictError"
    assert "Connect a bank or upload a CSV" in result["message"]


def test_skip_allowed_after_phase_1_minimal_ack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _init_db(tmp_path, monkeypatch)
    SkillStateStore(tmp_path / "skill_state.json").set(
        "onboarding",
        {"data_minimal_acknowledged": True},
    )
    from finance_cli.mcp_server import skip_onboarding

    with connect(db_path) as conn:
        _add_account(conn)

    result = skip_onboarding()

    assert result["summary"] == {"skipped": True, "current_phase": "profile"}
    assert SkillStateStore(tmp_path / "skill_state.json").get("onboarding") == {
        "data_minimal_acknowledged": True,
        "onboarding_skipped": True,
    }


def test_skip_allowed_after_phase_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = _init_db(tmp_path, monkeypatch)
    from finance_cli.mcp_server import skip_onboarding

    with connect(db_path) as conn:
        _add_history(conn)

    result = skip_onboarding()

    assert result["summary"] == {"skipped": True, "current_phase": "profile"}
    assert SkillStateStore(tmp_path / "skill_state.json").get("onboarding") == {
        "onboarding_skipped": True,
    }
