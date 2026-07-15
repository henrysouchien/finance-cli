from __future__ import annotations

import asyncio
from contextlib import contextmanager
import sqlite3

import pytest

import finance_cli.db as db_module
from finance_cli import storage_files
from finance_cli.db import connect, initialize_database
from finance_cli.storage_client.connection import StorageConnection
from finance_cli.storage_lease import (
    LeaseMissingError,
    LeaseScope,
    LocalLease,
    RemoteLease,
    current_lease_scope,
)


class _SessionManager:
    def get_db_session(self):  # pragma: no cover - release is disabled in this test
        raise AssertionError("no PG access expected")


class _LeaseCursor:
    def __init__(self, manager: "_LeaseSessionManager") -> None:
        self._manager = manager
        self._row = None

    def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
        normalized = " ".join(query.split())
        if normalized == "SELECT storage_mode FROM users WHERE id = %s FOR UPDATE":
            self._row = {"storage_mode": self._manager.mode}
            return
        if normalized.startswith("INSERT INTO user_access_leases"):
            self._manager.leases.add(str(params[0]))
            self._row = None
            return
        if normalized == "DELETE FROM user_access_leases WHERE lease_id = %s":
            self._manager.leases.discard(str(params[0]))
            self._row = None
            return
        raise AssertionError(f"unexpected lease SQL: {normalized}")

    def fetchone(self):
        return self._row


class _LeaseConnection:
    def __init__(self, manager: "_LeaseSessionManager") -> None:
        self._manager = manager

    def cursor(self) -> _LeaseCursor:
        return _LeaseCursor(self._manager)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


class _LeaseSessionManager:
    def __init__(self, mode: str = "local") -> None:
        self.mode = mode
        self.leases: set[str] = set()

    @contextmanager
    def get_db_session(self):
        yield _LeaseConnection(self)


def test_db_connect_enforce_requires_active_scope(monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_LEASE_ENFORCE", "true")
    with pytest.raises(LeaseMissingError):
        connect(db_path=tmp_path / "finance.db", user_id="1")


def test_db_connect_reuses_active_scope_when_enforced(monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_LEASE_ENFORCE", "true")
    scope = LeaseScope(
        user_id="1",
        lease=LocalLease("00000000-0000-0000-0000-000000000001"),
        session_manager=_SessionManager(),
        owns_lease=False,
    )
    with scope:
        conn = connect(db_path=tmp_path / "finance.db", user_id="1")
        try:
            conn.execute("SELECT 1").fetchone()
        finally:
            conn.close()


def test_remote_db_connect_with_borrowed_scope_closes_on_context_exit(monkeypatch, tmp_path):
    monkeypatch.delenv("STORAGE_LEASE_ENFORCE", raising=False)
    manager = _LeaseSessionManager(mode="remote")
    remote_conn = StorageConnection("localhost:1", user_id="1", auth_provider=object())
    monkeypatch.setattr(
        db_module,
        "_storage_connection_for_dispatch",
        lambda **_kwargs: remote_conn,
    )
    scope = LeaseScope(
        user_id="1",
        lease=RemoteLease("00000000-0000-0000-0000-000000000004"),
        session_manager=manager,
        owns_lease=False,
    )

    with scope:
        with connect(
            db_path=tmp_path / "users" / "1" / "finance.db",
            expected_user_id="1",
            storage_session_manager=manager,
        ) as conn:
            assert conn is remote_conn
            assert not remote_conn._closed
            assert manager.leases == set()

        assert remote_conn._closed
        assert current_lease_scope() is scope
        assert manager.leases == set()

    assert current_lease_scope() is None


def test_storage_files_enforce_requires_active_scope(monkeypatch):
    monkeypatch.setenv("STORAGE_LEASE_ENFORCE", "true")
    with pytest.raises(LeaseMissingError):
        storage_files.list_files("localhost:1", user_id="1", product="finance_cli")


def test_leased_connection_with_exit_releases_lease(monkeypatch, tmp_path):
    monkeypatch.delenv("STORAGE_LEASE_ENFORCE", raising=False)
    manager = _LeaseSessionManager()
    db_path = tmp_path / "finance.db"

    with connect(db_path=db_path, user_id="1", storage_session_manager=manager) as conn:
        conn.execute("SELECT 1").fetchone()
        assert len(manager.leases) == 1

    assert manager.leases == set()
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1").fetchone()


def test_leased_connection_explicit_close_releases_lease(monkeypatch, tmp_path):
    monkeypatch.delenv("STORAGE_LEASE_ENFORCE", raising=False)
    manager = _LeaseSessionManager()
    conn = connect(db_path=tmp_path / "finance.db", user_id="1", storage_session_manager=manager)
    assert len(manager.leases) == 1

    conn.close()

    assert manager.leases == set()


def test_encrypted_leased_connection_uses_sqlcipher_factory(monkeypatch, tmp_path):
    monkeypatch.delenv("STORAGE_LEASE_ENFORCE", raising=False)
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "require")
    monkeypatch.setattr(db_module, "_default_storage_session_manager", lambda: None)
    monkeypatch.setattr(
        db_module.db_keys,
        "get_user_db_key",
        lambda _user_id, **_kwargs: b"\x11" * 32,
    )
    manager = _LeaseSessionManager()
    db_path = tmp_path / "users" / "2" / "finance.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    initialize_database(db_path)
    with connect(db_path=db_path) as conn:
        conn.execute("INSERT INTO tenant_marker (singleton, user_id) VALUES (1, '2')")
        conn.commit()

    with connect(
        db_path=db_path,
        expected_user_id="2",
        storage_session_manager=manager,
    ) as conn:
        row = conn.execute("SELECT user_id FROM tenant_marker WHERE singleton = 1").fetchone()
        assert row["user_id"] == "2"
        assert isinstance(conn, db_module._SQLCIPHER_CONNECTION_TYPE)
        assert len(manager.leases) == 1

    assert manager.leases == set()


def test_leased_storage_connection_with_exit_releases_lease(monkeypatch):
    conn = StorageConnection("localhost:1", user_id="1", auth_provider=object())
    events: list[str] = []
    monkeypatch.setattr(conn, "commit", lambda: events.append("commit"))
    conn._storage_lease_cleanup = lambda: events.append("lease")
    conn._close_on_context_exit = True

    with conn:
        pass

    assert events == ["commit", "lease"]
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_bind_context_carries_lease_scope_through_executor():
    async def run_bound() -> str | None:
        loop = asyncio.get_running_loop()
        scope = LeaseScope(
            user_id="1",
            lease=LocalLease("00000000-0000-0000-0000-000000000002"),
            session_manager=_SessionManager(),
            owns_lease=False,
        )
        with scope:
            bound = LeaseScope.bind_context(
                lambda: current_lease_scope().lease_id if current_lease_scope() else None
            )
            return await loop.run_in_executor(None, bound)

    assert asyncio.run(run_bound()) == "00000000-0000-0000-0000-000000000002"


def test_to_thread_automatically_propagates_lease_scope():
    async def run_to_thread() -> str | None:
        scope = LeaseScope(
            user_id="1",
            lease=LocalLease("00000000-0000-0000-0000-000000000003"),
            session_manager=_SessionManager(),
            owns_lease=False,
        )
        with scope:
            return await asyncio.to_thread(
                lambda: current_lease_scope().lease_id if current_lease_scope() else None
            )

    assert asyncio.run(run_to_thread()) == "00000000-0000-0000-0000-000000000003"
