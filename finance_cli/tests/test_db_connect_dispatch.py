from __future__ import annotations

from contextlib import contextmanager
import logging
from pathlib import Path
import sqlite3
from unittest.mock import patch

import pytest

from finance_cli import db as db_module
from finance_cli.storage_client import _dispatch as storage_dispatch
from finance_cli.storage_client.errors import MaintenanceModeError


class _FakeStorageConnection:
    def __init__(self) -> None:
        self.create_function_calls: list[tuple[object, ...]] = []
        self.row_factory = None

    def create_function(self, *args, **kwargs) -> None:
        self.create_function_calls.append((*args, kwargs))


class _ModeCursor:
    def __init__(self, manager: "_ModeSessionManager") -> None:
        self._manager = manager
        self._row = None

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        normalized = " ".join(query.split())
        assert normalized == "SELECT storage_mode FROM users WHERE id::text = %s"
        self._manager.query_count += 1
        user_id = str(params[0])
        mode = self._manager.modes.get(user_id)
        self._row = None if mode is None else {"storage_mode": mode}

    def fetchone(self):
        return self._row


class _ModeConnection:
    def __init__(self, manager: "_ModeSessionManager") -> None:
        self._manager = manager

    def cursor(self) -> _ModeCursor:
        return _ModeCursor(self._manager)


class _ModeSessionManager:
    def __init__(self, modes: dict[str, str]) -> None:
        self.modes = dict(modes)
        self.query_count = 0

    @contextmanager
    def get_db_session(self):
        yield _ModeConnection(self)


@pytest.fixture(autouse=True)
def _reset_dispatch(monkeypatch):
    storage_dispatch.clear_storage_mode_cache()
    monkeypatch.delenv("STORAGE_SERVER_URL", raising=False)
    monkeypatch.delenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", raising=False)
    monkeypatch.delenv("FINANCE_WEB_DATA_ROOT", raising=False)
    yield
    storage_dispatch.clear_storage_mode_cache()


def _user_db_path(tmp_path: Path, user_id: str) -> Path:
    data_root = tmp_path / "users"
    return data_root / user_id / "finance.db"


def _connect_with_storage_mock(tmp_path: Path, manager: _ModeSessionManager, **kwargs):
    fake = _FakeStorageConnection()
    with patch.object(db_module, "StorageConnection", return_value=fake) as ctor:
        conn = db_module.connect(
            db_path=kwargs.pop("db_path", tmp_path / "finance.db"),
            storage_session_manager=manager,
            **kwargs,
        )
    return conn, fake, ctor


@pytest.mark.parametrize("target_value", [None, ""])
def test_storage_server_url_unset_or_empty_falls_back_to_sqlite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_value: str | None,
) -> None:
    if target_value is None:
        monkeypatch.delenv("STORAGE_SERVER_URL", raising=False)
    else:
        monkeypatch.setenv("STORAGE_SERVER_URL", target_value)
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    manager = _ModeSessionManager({"alice": "remote"})

    conn, _fake, ctor = _connect_with_storage_mock(tmp_path, manager, user_id="alice")

    try:
        assert isinstance(conn, sqlite3.Connection)
        ctor.assert_not_called()
        assert manager.query_count == 0
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("enabled_value", "expect_remote"),
    [(None, False), ("false", False), ("TRUE", True)],
)
def test_client_enabled_flag_controls_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    enabled_value: str | None,
    expect_remote: bool,
) -> None:
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    if enabled_value is None:
        monkeypatch.delenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", raising=False)
    else:
        monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", enabled_value)
    manager = _ModeSessionManager({"alice": "remote"})

    conn, fake, ctor = _connect_with_storage_mock(tmp_path, manager, user_id="alice")

    if expect_remote:
        assert conn is fake
        assert fake.row_factory is sqlite3.Row
        ctor.assert_called_once()
    else:
        try:
            assert isinstance(conn, sqlite3.Connection)
            ctor.assert_not_called()
            assert manager.query_count == 0
        finally:
            conn.close()


def test_user_id_resolution_priority_expected_then_user_then_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_dispatch.clear_storage_mode_cache()
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path / "users"))
    manager = _ModeSessionManager(
        {
            "expected": "remote",
            "explicit": "remote",
            "parsed": "remote",
        }
    )

    path = _user_db_path(tmp_path, "parsed")
    conn, _fake, ctor = _connect_with_storage_mock(
        tmp_path,
        manager,
        db_path=path,
        expected_user_id="expected",
        user_id="explicit",
    )
    assert conn is not None
    assert ctor.call_args.kwargs["user_id"] == "expected"

    storage_dispatch.clear_storage_mode_cache()
    conn, _fake, ctor = _connect_with_storage_mock(
        tmp_path,
        manager,
        db_path=path,
        user_id="explicit",
    )
    assert conn is not None
    assert ctor.call_args.kwargs["user_id"] == "explicit"

    storage_dispatch.clear_storage_mode_cache()
    conn, _fake, ctor = _connect_with_storage_mock(tmp_path, manager, db_path=path)
    assert conn is not None
    assert ctor.call_args.kwargs["user_id"] == "parsed"


@pytest.mark.parametrize("mode", ["local", "remote", "migrating", "replaying"])
def test_storage_modes_route_or_raise(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    manager = _ModeSessionManager({"alice": mode})

    if mode in {"migrating", "replaying"}:
        with pytest.raises(MaintenanceModeError):
            _connect_with_storage_mock(tmp_path, manager, user_id="alice")
        assert "alice" not in storage_dispatch._storage_mode_cache
        return

    conn, fake, ctor = _connect_with_storage_mock(tmp_path, manager, user_id="alice")
    if mode == "remote":
        assert conn is fake
        ctor.assert_called_once()
    else:
        try:
            assert isinstance(conn, sqlite3.Connection)
            ctor.assert_not_called()
        finally:
            conn.close()


def test_storage_mode_cache_hit_miss_and_ttl_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    manager = _ModeSessionManager({"alice": "remote"})
    current_time = [100.0]
    monkeypatch.setattr(storage_dispatch.time, "monotonic", lambda: current_time[0])

    first, fake, _ctor = _connect_with_storage_mock(tmp_path, manager, user_id="alice")
    assert first is fake
    assert manager.query_count == 1

    manager.modes["alice"] = "local"
    second, fake, _ctor = _connect_with_storage_mock(tmp_path, manager, user_id="alice")
    assert second is fake
    assert manager.query_count == 1

    current_time[0] += 31.0
    third, _fake, ctor = _connect_with_storage_mock(tmp_path, manager, user_id="alice")
    try:
        assert isinstance(third, sqlite3.Connection)
        ctor.assert_not_called()
        assert manager.query_count == 2
    finally:
        third.close()


def test_session_manager_unavailable_falls_back_to_local_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    storage_dispatch.clear_storage_mode_cache()
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")

    conn = db_module.connect(
        db_path=tmp_path / "finance.db",
        user_id="alice",
        storage_session_manager=object(),
    )
    try:
        assert isinstance(conn, sqlite3.Connection)
    finally:
        conn.close()
    assert "storage_mode_lookup_unavailable fallback=local" in caplog.text


def test_dispatch_lookup_unavailable_counter_emits_per_failure_but_warning_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_dispatch.clear_storage_mode_cache()

    def raise_lookup_unavailable(*args, **kwargs):
        raise storage_dispatch.StorageModeLookupUnavailable("session_manager_unavailable")

    counter_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    warning_records: list[logging.LogRecord] = []

    def record_counter(*args, **kwargs):
        counter_calls.append((args, kwargs))

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            warning_records.append(record)

    monkeypatch.setattr(storage_dispatch, "_lookup_storage_mode", raise_lookup_unavailable)
    monkeypatch.setattr(storage_dispatch.storage_errors, "record_storage_client_error", record_counter)
    monkeypatch.setattr(storage_dispatch.log, "propagate", False)

    handler = _ListHandler(level=logging.WARNING)
    previous_level = storage_dispatch.log.level
    storage_dispatch.log.setLevel(logging.WARNING)
    storage_dispatch.log.addHandler(handler)
    try:
        for _ in range(3):
            assert storage_dispatch.storage_mode_for_user("alice", session_manager=object()) == "local"
    finally:
        storage_dispatch.log.removeHandler(handler)
        storage_dispatch.log.setLevel(previous_level)

    assert counter_calls == [
        (
            ("storage_dispatch", "LOOKUP_UNAVAILABLE"),
            {"reason": "session_manager_unavailable"},
        ),
        (
            ("storage_dispatch", "LOOKUP_UNAVAILABLE"),
            {"reason": "session_manager_unavailable"},
        ),
        (
            ("storage_dispatch", "LOOKUP_UNAVAILABLE"),
            {"reason": "session_manager_unavailable"},
        ),
    ]
    assert [record.getMessage() for record in warning_records] == [
        "storage_mode_lookup_unavailable fallback=local reason=session_manager_unavailable"
    ]


def test_unresolved_user_id_falls_back_to_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STORAGE_SERVER_URL", "storage.example:50051")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    manager = _ModeSessionManager({"alice": "remote"})

    conn, _fake, ctor = _connect_with_storage_mock(
        tmp_path,
        manager,
        db_path=tmp_path / "not-a-user-db.sqlite",
    )
    try:
        assert isinstance(conn, sqlite3.Connection)
        ctor.assert_not_called()
        assert manager.query_count == 0
    finally:
        conn.close()
