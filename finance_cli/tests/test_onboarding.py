from __future__ import annotations

import sqlite3
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.gateway.tools import READ_ONLY_TOOLS
from finance_cli.onboarding import detect_user_state
from finance_cli.skill_state import SkillStateStore


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _make_store(tmp_path: Path) -> SkillStateStore:
    return SkillStateStore(tmp_path / "skill_state.json")


def _add_account(conn) -> None:
    conn.execute(
        """
        INSERT INTO accounts (id, institution_name, account_name, account_type, is_active)
        VALUES ('acct_1', 'Test Bank', 'Checking', 'checking', 1)
        """
    )


def _add_history(conn) -> None:
    _add_account(conn)
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


def test_detect_empty_db_no_state(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)

    with connect(db_path) as conn:
        result = detect_user_state(conn, store)

    assert result["data"]["is_new_user"] is True
    assert result["data"]["resume_checkpoint"] is None
    assert result["data"]["next_steps"][0] == {
        "step": "connect",
        "tool": "plaid_link",
        "args": {"wait": False, "include_balance": True, "include_liabilities": True},
        "instruction": "Connect your first bank or card account so CashNerd can build the ledger from real data.",
        "priority": 1,
    }
    assert result["data"]["phase_summary"] == "Phase 1: Connect accounts and import transactions."
    assert result["summary"]["next_step"] == "connect"


def test_detect_missing_tables(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    with sqlite3.connect(":memory:") as conn:
        result = detect_user_state(conn, store)

    assert result["data"]["is_new_user"] is True
    assert result["data"]["resume_checkpoint"] is None


def test_detect_has_data_no_state(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)

    with connect(db_path) as conn:
        _add_account(conn)
        result = detect_user_state(conn, store)

    assert result["data"]["is_new_user"] is False
    assert result["data"]["resume_checkpoint"] == "connect"


def test_detect_connect_complete_moves_to_profile(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)

    with connect(db_path) as conn:
        _add_history(conn)
        result = detect_user_state(conn, store)

    assert result["data"]["resume_checkpoint"] == "profile"
    assert result["data"]["next_steps"] == []
    assert result["data"]["phase_summary"] == "Phase 2: Capture your work type and income stability."


def test_detect_profile_fields_complete_moves_to_focus(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set(
        "onboarding",
        {
            "user_type": "salaried",
            "income_stability": "steady",
        },
    )

    with connect(db_path) as conn:
        _add_history(conn)
        result = detect_user_state(conn, store)

    assert result["data"]["resume_checkpoint"] == "focus"
    assert result["data"]["phase_summary"] == "Phase 3: Pick the first coaching priority."


def test_detect_focus_complete_moves_to_setup(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set(
        "onboarding",
        {
            "user_type": "salaried",
            "income_stability": "steady",
            "priority": "save_more",
        },
    )

    with connect(db_path) as conn:
        _add_history(conn)
        result = detect_user_state(conn, store)

    assert result["data"]["is_onboarding_complete"] is False
    assert result["data"]["resume_checkpoint"] == "setup"
    assert result["data"]["phase_summary"] == "Phase 4: Review starter setup proposals."


def test_detect_setup_acknowledged_is_onboarded(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set(
        "onboarding",
        {
            "user_type": "salaried",
            "income_stability": "steady",
            "priority": "save_more",
            "setup_acknowledged": True,
        },
    )

    with connect(db_path) as conn:
        _add_history(conn)
        result = detect_user_state(conn, store)

    assert result["data"]["is_onboarding_complete"] is True
    assert result["data"]["resume_checkpoint"] is None


def test_detect_data_connected(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set("onboarding", {"data_connected": True})

    with connect(db_path) as conn:
        result = detect_user_state(conn, store)

    assert result["data"]["resume_checkpoint"] == "connect"
    assert result["data"]["next_steps"][0]["step"] == "connect"
    assert result["data"]["next_steps"][0]["tool"] == "plaid_link"
    assert result["data"]["phase_summary"] == "Phase 1: Connect accounts and import transactions."


def test_detect_categorized_no_profile(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set(
        "onboarding",
        {
            "data_connected": True,
            "data_categorized": True,
        },
    )

    with connect(db_path) as conn:
        result = detect_user_state(conn, store)

    assert result["data"]["resume_checkpoint"] == "connect"
    assert result["data"]["next_steps"][0]["tool"] == "plaid_link"


def test_detect_retired_profile_flag_does_not_advance_without_data(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set(
        "onboarding",
        {
            "data_connected": True,
            "data_categorized": True,
            "profile_complete": True,
        },
    )

    with connect(db_path) as conn:
        result = detect_user_state(conn, store)

    assert result["data"]["resume_checkpoint"] == "connect"
    assert result["data"]["next_steps"][0]["tool"] == "plaid_link"


def test_detect_retired_assessment_flag_does_not_advance_without_data(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set(
        "onboarding",
        {
            "data_connected": True,
            "data_categorized": True,
            "profile_complete": True,
            "assessment_shown": True,
        },
    )

    with connect(db_path) as conn:
        result = detect_user_state(conn, store)

    assert result["data"]["resume_checkpoint"] == "connect"
    assert result["data"]["next_steps"][0]["tool"] == "plaid_link"


def test_detect_retired_setup_flag_does_not_advance_without_data(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set(
        "onboarding",
        {
            "data_connected": True,
            "data_categorized": True,
            "profile_complete": True,
            "assessment_shown": True,
            "setup_complete": True,
        },
    )

    with connect(db_path) as conn:
        result = detect_user_state(conn, store)

    assert result["data"]["resume_checkpoint"] == "connect"
    assert result["data"]["next_steps"][0]["tool"] == "plaid_link"


def test_detect_fully_complete(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set("onboarding", {"complete": True})

    with connect(db_path) as conn:
        _add_history(conn)
        result = detect_user_state(conn, store)

    assert result["data"]["is_onboarding_complete"] is True
    assert result["data"]["resume_checkpoint"] is None
    assert result["data"]["next_steps"] == []
    assert result["data"]["phase_summary"] == "Onboarding complete."


def test_detect_profile_from_state(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)
    store.set(
        "onboarding",
        {
            "user_type": "self_employed",
            "priority": "taxes",
            "profile": "aggressive",
        },
    )

    with connect(db_path) as conn:
        result = detect_user_state(conn, store)

    assert result["data"]["profile"] == {
        "name": None,
        "user_type": "self_employed",
        "income_stability": None,
        "priority": "taxes",
    }


def test_detect_plaid_pending_not_new(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    store = _make_store(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, status)
            VALUES ('plaid_1', 'item_1', 'Test Bank', 'pending')
            """
        )
        result = detect_user_state(conn, store)

    assert result["data"]["is_new_user"] is False
    assert result["data"]["resume_checkpoint"] == "connect"


def test_onboarding_detect_in_read_only_tools() -> None:
    assert "onboarding_detect" in READ_ONLY_TOOLS
