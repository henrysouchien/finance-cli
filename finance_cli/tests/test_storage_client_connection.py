from __future__ import annotations

import sqlite3
import uuid

import pytest

from finance_cli.storage_client import StorageConnection, connect


def test_storage_connection_exposes_user_id() -> None:
    conn = StorageConnection.__new__(StorageConnection)
    conn._user_id = "alice"

    assert conn.user_id == "alice"


def test_connect_returns_storage_connection(storage_connection_factory, local_storage_proxy) -> None:
    conn = storage_connection_factory()

    try:
        assert isinstance(conn, StorageConnection)
        assert isinstance(
            connect(
                local_storage_proxy.target,
                user_id="synthetic-connect",
                auth_provider=conn._auth_provider,
            ),
            StorageConnection,
        )
    finally:
        conn.close()


def test_create_function_current_session_id_is_noop(storage_connection_factory) -> None:
    conn = storage_connection_factory()
    try:
        assert conn.create_function("current_session_id", 0, lambda: "x") is None
        assert conn.create_function("current_session_id", -1, lambda: "x") is None
    finally:
        conn.close()


def test_create_function_rejects_arbitrary_callbacks(storage_connection_factory) -> None:
    conn = storage_connection_factory()
    try:
        with pytest.raises(NotImplementedError, match="current_session_id pseudo-function"):
            conn.create_function("python_callback", 1, lambda value: value)
    finally:
        conn.close()


@pytest.mark.parametrize(
    "method_name",
    [
        "iterdump",
        "set_trace_callback",
        "set_progress_handler",
        "set_authorizer",
        "interrupt",
    ],
)
def test_unsupported_sqlite_callbacks_raise(storage_connection_factory, method_name: str) -> None:
    conn = storage_connection_factory()
    try:
        method = getattr(conn, method_name)
        with pytest.raises(NotImplementedError):
            method(None) if method_name != "interrupt" else method()
    finally:
        conn.close()


def test_close_is_idempotent_and_marks_connection_closed(storage_connection_factory) -> None:
    conn = storage_connection_factory()
    conn.execute("SELECT 1").fetchone()

    conn.close()
    conn.close()

    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_backup_returns_server_generated_path(storage_connection_factory) -> None:
    conn = storage_connection_factory(scopes=["admin"])
    try:
        conn.execute("CREATE TABLE backup_test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO backup_test (name) VALUES (?)", ("alpha",))
        conn.commit()

        path = conn.backup()

        assert isinstance(path, str)
        assert path.endswith(".db")
        assert "/backups/" in path
    finally:
        conn.close()


def test_backup_target_connection_form_is_not_supported(storage_connection_factory) -> None:
    conn = storage_connection_factory(scopes=["admin"])
    target_conn = sqlite3.connect(":memory:")
    try:
        with pytest.raises(NotImplementedError, match="target_conn-style backup"):
            conn.backup(target_conn)
    finally:
        target_conn.close()
        conn.close()


def test_context_manager_commits_and_keeps_connection_open(storage_connection_factory) -> None:
    user_id = f"synthetic-ctx-open-{uuid.uuid4().hex[:8]}"
    conn = storage_connection_factory(user_id=user_id)
    try:
        conn.execute("CREATE TABLE context_test (id INTEGER PRIMARY KEY, name TEXT)")

        with conn:
            conn.execute("INSERT INTO context_test (name) VALUES (?)", ("committed",))

        assert not conn._closed
        assert conn.execute("SELECT name FROM context_test").fetchone() == ("committed",)
    finally:
        conn.close()

    verifier = storage_connection_factory(user_id=user_id)
    try:
        assert verifier.execute("SELECT name FROM context_test").fetchone() == ("committed",)
    finally:
        verifier.close()
