from __future__ import annotations

import time

import pytest

from finance_cli.storage_client import errors


def _expire_current_session(conn, local_storage_proxy) -> str:
    session_id = conn._session_id
    assert session_id is not None
    manager = local_storage_proxy.runtime.service.session_manager
    state = manager.get_session(session_id)
    state.in_transaction_observed = False
    state.last_used = time.monotonic() - local_storage_proxy.runtime.config.session_idle_timeout_seconds - 1
    manager.reap_once()
    return session_id


def test_session_expired_reopens_and_retries_outside_transaction(
    storage_connection_factory,
    local_storage_proxy,
) -> None:
    conn = storage_connection_factory()
    try:
        assert conn.execute("SELECT 1").fetchone() == (1,)
        old_session_id = _expire_current_session(conn, local_storage_proxy)

        assert conn.execute("SELECT 2").fetchone() == (2,)
        assert conn._session_id != old_session_id
    finally:
        conn.close()


def test_session_expired_does_not_retry_inside_transaction(
    storage_connection_factory,
    local_storage_proxy,
) -> None:
    conn = storage_connection_factory()
    try:
        conn.execute("CREATE TABLE recovery_txn (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("BEGIN IMMEDIATE")
        assert conn.in_transaction
        _expire_current_session(conn, local_storage_proxy)

        with pytest.raises(errors.SessionExpired):
            conn.execute("INSERT INTO recovery_txn (name) VALUES (?)", ("should_not_replay",))
    finally:
        conn.close()


def test_session_aborted_propagates_without_retry(
    storage_connection_factory,
    local_storage_proxy,
) -> None:
    conn = storage_connection_factory()
    try:
        assert conn.execute("SELECT 1").fetchone() == (1,)
        local_storage_proxy.runtime.service.session_manager.close_all(aborted=True)

        with pytest.raises(errors.SessionAborted):
            conn.execute("SELECT 2")
    finally:
        conn.close()
