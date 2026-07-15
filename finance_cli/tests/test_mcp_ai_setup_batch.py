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


def _seed_setup_data(conn) -> None:
    conn.execute(
        """
        INSERT INTO accounts (id, institution_name, account_name, account_type, balance_current_cents, is_active)
        VALUES ('acct_1', 'Test Bank', 'Checking', 'checking', 100000, 1)
        """
    )
    conn.execute(
        "INSERT INTO categories (id, name, is_income, is_system) VALUES ('cat_dining', 'Dining', 0, 0)"
    )
    conn.execute(
        "INSERT INTO categories (id, name, is_income, is_system) VALUES ('cat_grocery', 'Groceries', 0, 0)"
    )
    for month in ("01", "02", "03"):
        conn.execute(
            """
            INSERT INTO transactions (id, account_id, date, description, amount_cents, category_id, source, is_active)
            VALUES (?, 'acct_1', ?, 'Dining', -30000, 'cat_dining', 'manual', 1)
            """,
            (f"dining_{month}", f"2026-{month}-06"),
        )
        conn.execute(
            """
            INSERT INTO transactions (id, account_id, date, description, amount_cents, category_id, source, is_active)
            VALUES (?, 'acct_1', ?, 'Groceries', -50000, 'cat_grocery', 'manual', 1)
            """,
            (f"grocery_{month}", f"2026-{month}-07"),
        )


def test_ai_setup_batch_returns_stable_proposals(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = _init_db(tmp_path, monkeypatch)
    SkillStateStore(tmp_path / "skill_state.json").set(
        "onboarding",
        {"user_type": "salaried", "income_stability": "steady", "priority": "save_more"},
    )
    with connect(db_path) as conn:
        _seed_setup_data(conn)

    from finance_cli.mcp_server import ai_setup_batch

    first = ai_setup_batch()
    second = ai_setup_batch()

    assert first["summary"]["type"] == "ai_setup_batch"
    assert first["data"]["categorization_pending_count"] == 0
    assert first["data"]["budget_proposals"]
    assert first["data"]["goal_proposals"][0]["tool_name"] == "goal_set"
    first_ids = [item["id"] for item in first["data"]["budget_proposals"] + first["data"]["goal_proposals"]]
    second_ids = [item["id"] for item in second["data"]["budget_proposals"] + second["data"]["goal_proposals"]]
    assert first_ids == second_ids


def test_ai_setup_batch_does_not_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = _init_db(tmp_path, monkeypatch)
    SkillStateStore(tmp_path / "skill_state.json").set(
        "onboarding",
        {"user_type": "salaried", "income_stability": "steady", "priority": "save_more"},
    )
    with connect(db_path) as conn:
        _seed_setup_data(conn)
        before = {
            "budgets": conn.execute("SELECT COUNT(*) FROM budgets").fetchone()[0],
            "goals": conn.execute("SELECT COUNT(*) FROM goals").fetchone()[0],
        }

    from finance_cli.mcp_server import ai_setup_batch

    ai_setup_batch()

    with connect(db_path) as conn:
        after = {
            "budgets": conn.execute("SELECT COUNT(*) FROM budgets").fetchone()[0],
            "goals": conn.execute("SELECT COUNT(*) FROM goals").fetchone()[0],
        }

    assert after == before


def test_ai_setup_batch_contract_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _init_db(tmp_path, monkeypatch)
    SkillStateStore(tmp_path / "skill_state.json").set(
        "onboarding",
        {"user_type": "salaried", "income_stability": "steady", "priority": "spending_clarity"},
    )

    from finance_cli.mcp_server import ai_setup_batch

    result = ai_setup_batch()

    assert set(result["data"]) == {
        "summary",
        "categorization_pending_count",
        "budget_proposals",
        "goal_proposals",
        "rule_proposals",
        "debt_nudge",
    }
    assert result["data"]["summary"] == {
        "budget_proposals": 0,
        "goal_proposals": 0,
        "rule_proposals": 0,
    }
