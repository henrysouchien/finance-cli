from __future__ import annotations

from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.onboarding_contract import (
    PhaseEvaluation,
    current_phase,
    connect_phase_complete,
    is_fully_onboarded,
    is_gate_open,
    sanitize_profile,
)


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


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


def test_connect_check_empty(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        assert connect_phase_complete(conn, {}) is False
        evaluation = PhaseEvaluation.build(conn, {})

    assert evaluation.current_phase.id.value == "connect"
    assert evaluation.entries[0].status.value == "in_progress"
    assert evaluation.entries[0].missing == ("account",)


def test_connect_check_with_data(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        _add_history(conn)
        assert connect_phase_complete(conn, {}) is True
        assert is_fully_onboarded(conn, {}) is False
        assert is_gate_open(conn, {}) is False
        evaluation = PhaseEvaluation.build(conn, {})

    assert evaluation.current_phase.id.value == "profile"
    assert [entry.status.value for entry in evaluation.entries] == [
        "complete",
        "in_progress",
        "pending",
        "pending",
    ]


def test_profile_check_requires_user_type(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        _add_history(conn)
        evaluation = PhaseEvaluation.build(conn, {"income_stability": "steady"})

    assert evaluation.current_phase.id.value == "profile"
    assert evaluation.entries[1].missing == ("user_type",)


def test_profile_check_requires_income_stability(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        _add_history(conn)
        evaluation = PhaseEvaluation.build(conn, {"user_type": "salaried"})

    assert evaluation.current_phase.id.value == "profile"
    assert evaluation.entries[1].missing == ("income_stability",)


def test_focus_check_priority(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        _add_history(conn)
        evaluation = PhaseEvaluation.build(
            conn,
            {
                "user_type": "self_employed",
                "income_stability": "variable",
                "priority": "taxes",
            },
        )

    assert evaluation.is_complete is False
    assert evaluation.current_phase.id.value == "setup"
    assert [entry.status.value for entry in evaluation.entries] == [
        "complete",
        "complete",
        "complete",
        "in_progress",
    ]
    assert evaluation.entries[3].missing == ("setup_acknowledged",)


def test_current_phase_progression(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        _add_history(conn)
        assert current_phase(conn, {}).id.value == "profile"
        assert current_phase(
            conn,
            {"user_type": "salaried", "income_stability": "steady"},
        ).id.value == "focus"
        assert current_phase(
            conn,
            {
                "user_type": "salaried",
                "income_stability": "steady",
                "priority": "save_more",
            },
        ).id.value == "setup"
        assert is_fully_onboarded(
            conn,
            {
                "user_type": "salaried",
                "income_stability": "steady",
                "priority": "save_more",
                "setup_acknowledged": True,
            },
        ) is True


def test_setup_check_acknowledged(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        _add_history(conn)
        evaluation = PhaseEvaluation.build(
            conn,
            {
                "user_type": "self_employed",
                "income_stability": "variable",
                "priority": "taxes",
                "setup_acknowledged": True,
            },
        )

    assert evaluation.is_complete is True
    assert [entry.status.value for entry in evaluation.entries] == [
        "complete",
        "complete",
        "complete",
        "complete",
    ]


def test_is_gate_open_skipped_with_phase1(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        _add_history(conn)
        assert is_gate_open(conn, {"onboarding_skipped": True}) is True
        assert is_fully_onboarded(conn, {"onboarding_skipped": True}) is False


def test_is_gate_open_skipped_without_phase1(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        assert is_gate_open(conn, {"onboarding_skipped": True}) is False
        assert is_fully_onboarded(conn, {"onboarding_skipped": True}) is False

def test_connect_check_minimal_ack(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        _add_account(conn)
        assert connect_phase_complete(conn, {"data_minimal_acknowledged": True}) is True


def test_is_fully_onboarded_back_compat_with_data(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        _add_history(conn)
        assert is_fully_onboarded(conn, {"complete": True}) is True


def test_is_fully_onboarded_back_compat_wiped(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        assert is_fully_onboarded(conn, {"complete": True}) is False
        assert is_gate_open(conn, {"complete": True}) is False


def test_sanitize_profile_allowlist() -> None:
    assert sanitize_profile(
        {
            "name": "Henry",
            "user_type": "self_employed",
            "income_stability": "variable",
            "priority": "taxes",
            "pending_link_token": "secret",
            "onboarding_skipped": True,
            "complete": True,
        }
    ) == {
        "name": "Henry",
        "user_type": "self_employed",
        "income_stability": "variable",
        "priority": "taxes",
    }
