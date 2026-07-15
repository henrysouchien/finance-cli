"""Derived onboarding phase contract.

The web shell, MCP onboarding detection, and gateway prompt routing should all
read the same phase checks.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Any, Callable


class PhaseStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"


class PhaseId(StrEnum):
    CONNECT = "connect"
    PROFILE = "profile"
    FOCUS = "focus"
    SETUP = "setup"


@dataclass(frozen=True)
class PhaseDefinition:
    id: PhaseId
    order: int
    verb_name: str
    completion_check: Callable[[sqlite3.Connection, dict[str, Any]], bool]
    missing_fields: Callable[[sqlite3.Connection, dict[str, Any]], list[str]]
    system_prompt_fragment_path: str


@dataclass(frozen=True)
class PhaseStatusEntry:
    phase: PhaseDefinition
    status: PhaseStatus
    missing: tuple[str, ...]


@dataclass(frozen=True)
class PhaseEvaluation:
    entries: tuple[PhaseStatusEntry, ...]
    current_phase: PhaseDefinition
    is_complete: bool
    profile: dict[str, Any]

    @classmethod
    def build(cls, conn: sqlite3.Connection, state: dict[str, Any]) -> "PhaseEvaluation":
        entries: list[PhaseStatusEntry] = []
        first_incomplete_seen = False
        current = PHASES[-1]
        complete_count = 0

        for phase in PHASES:
            missing = tuple(phase.missing_fields(conn, state))
            is_complete = not missing and phase.completion_check(conn, state)
            if is_complete:
                complete_count += 1
                status = PhaseStatus.COMPLETE
            elif not first_incomplete_seen:
                first_incomplete_seen = True
                current = phase
                status = PhaseStatus.IN_PROGRESS
            else:
                status = PhaseStatus.PENDING
            entries.append(PhaseStatusEntry(phase=phase, status=status, missing=missing))

        all_complete = complete_count == len(PHASES)
        if all_complete:
            current = PHASES[-1]
        return cls(
            entries=tuple(entries),
            current_phase=current,
            is_complete=all_complete,
            profile=sanitize_profile(state),
        )


def _row_value(row: Any, key: str, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        return row[key]
    except Exception:
        pass
    try:
        return row[index]
    except Exception:
        return default


def _count(conn: sqlite3.Connection, sql: str) -> int:
    try:
        row = conn.execute(sql).fetchone()
        return int(_row_value(row, "count", 0, 0) or 0)
    except sqlite3.OperationalError:
        return 0


def _months_of_history(conn: sqlite3.Connection) -> int:
    try:
        row = conn.execute(
            """
            SELECT MIN(date) AS min_date, MAX(date) AS max_date
              FROM transactions
             WHERE is_active = 1
               AND date IS NOT NULL
            """
        ).fetchone()
    except sqlite3.OperationalError:
        return 0

    min_raw = _row_value(row, "min_date", 0)
    max_raw = _row_value(row, "max_date", 1)
    if not min_raw or not max_raw:
        return 0
    try:
        min_d = date.fromisoformat(str(min_raw)[:10])
        max_d = date.fromisoformat(str(max_raw)[:10])
    except ValueError:
        return 0
    return max((max_d.year - min_d.year) * 12 + (max_d.month - min_d.month), 0)


def onboarding_signals(conn: sqlite3.Connection) -> dict[str, Any]:
    account_count = _count(conn, "SELECT COUNT(*) FROM accounts WHERE is_active = 1")
    plaid_count = _count(
        conn,
        "SELECT COUNT(*) FROM plaid_items WHERE status IN ('active', 'pending', 'error')",
    )
    txn_count = _count(conn, "SELECT COUNT(*) FROM transactions WHERE is_active = 1")
    categorized_count = (
        _count(
            conn,
            "SELECT COUNT(*) FROM transactions WHERE is_active = 1 AND category_id IS NOT NULL",
        )
        if txn_count > 0
        else 0
    )
    categorization_rate = round(categorized_count / txn_count, 3) if txn_count > 0 else 0.0
    vendor_memory_count = _count(conn, "SELECT COUNT(*) FROM vendor_memory WHERE is_enabled = 1")
    months = _months_of_history(conn)
    return {
        "has_accounts": account_count > 0,
        "has_plaid": plaid_count > 0,
        "has_transactions": txn_count > 0,
        "account_count": account_count,
        "plaid_count": plaid_count,
        "txn_count": txn_count,
        "transaction_count": txn_count,
        "months_of_history": months,
        "categorization_rate": categorization_rate,
        "vendor_memory_count": vendor_memory_count,
    }


def _connect_check(conn: sqlite3.Connection, state: dict[str, Any]) -> bool:
    account_count = _count(conn, "SELECT COUNT(*) FROM accounts WHERE is_active = 1")
    if account_count == 0:
        return False
    return _months_of_history(conn) >= 1 or bool(state.get("data_minimal_acknowledged"))


def _connect_missing(conn: sqlite3.Connection, state: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    account_count = _count(conn, "SELECT COUNT(*) FROM accounts WHERE is_active = 1")
    if account_count == 0:
        missing.append("account")
    elif _months_of_history(conn) < 1 and not bool(state.get("data_minimal_acknowledged")):
        missing.append("one_month_history_or_acknowledgment")
    return missing


def _state_has_text(state: dict[str, Any], key: str) -> bool:
    value = state.get(key)
    return isinstance(value, str) and bool(value.strip())


def _profile_check(conn: sqlite3.Connection, state: dict[str, Any]) -> bool:
    del conn
    return _state_has_text(state, "user_type") and _state_has_text(state, "income_stability")


def _profile_missing(conn: sqlite3.Connection, state: dict[str, Any]) -> list[str]:
    del conn
    missing: list[str] = []
    if not _state_has_text(state, "user_type"):
        missing.append("user_type")
    if not _state_has_text(state, "income_stability"):
        missing.append("income_stability")
    return missing


def _focus_check(conn: sqlite3.Connection, state: dict[str, Any]) -> bool:
    del conn
    return _state_has_text(state, "priority")


def _focus_missing(conn: sqlite3.Connection, state: dict[str, Any]) -> list[str]:
    del conn
    return [] if _state_has_text(state, "priority") else ["priority"]


def _setup_check(conn: sqlite3.Connection, state: dict[str, Any]) -> bool:
    del conn
    return bool(state.get("setup_acknowledged"))


def _setup_missing(conn: sqlite3.Connection, state: dict[str, Any]) -> list[str]:
    del conn
    return [] if bool(state.get("setup_acknowledged")) else ["setup_acknowledged"]


PHASES: tuple[PhaseDefinition, ...] = (
    PhaseDefinition(
        PhaseId.CONNECT,
        1,
        "Connecting your money",
        _connect_check,
        _connect_missing,
        "prompts/onboarding/phase_connect.md",
    ),
    PhaseDefinition(
        PhaseId.PROFILE,
        2,
        "Understanding your profile",
        _profile_check,
        _profile_missing,
        "prompts/onboarding/phase_profile.md",
    ),
    PhaseDefinition(
        PhaseId.FOCUS,
        3,
        "Picking your first focus",
        _focus_check,
        _focus_missing,
        "prompts/onboarding/phase_focus.md",
    ),
    PhaseDefinition(
        PhaseId.SETUP,
        4,
        "Building your starter setup",
        _setup_check,
        _setup_missing,
        "prompts/onboarding/phase_setup.md",
    ),
)


def connect_phase_complete(conn: sqlite3.Connection, state: dict[str, Any]) -> bool:
    return _connect_check(conn, state)


def is_fully_onboarded(conn: sqlite3.Connection, state: dict[str, Any]) -> bool:
    if state.get("complete") and _connect_check(conn, state):
        return True
    return PhaseEvaluation.build(conn, state).is_complete


def is_gate_open(conn: sqlite3.Connection, state: dict[str, Any]) -> bool:
    if state.get("onboarding_skipped") and _connect_check(conn, state):
        return True
    return is_fully_onboarded(conn, state)


def current_phase(conn: sqlite3.Connection, state: dict[str, Any]) -> PhaseDefinition:
    return PhaseEvaluation.build(conn, state).current_phase


def sanitize_profile(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": state.get("name"),
        "user_type": state.get("user_type"),
        "income_stability": state.get("income_stability"),
        "priority": state.get("priority"),
    }


def phase_entry_payload(entry: PhaseStatusEntry) -> dict[str, Any]:
    return {
        "id": entry.phase.id.value,
        "order": entry.phase.order,
        "verb_name": entry.phase.verb_name,
        "status": entry.status.value,
        "missing": list(entry.missing),
    }
