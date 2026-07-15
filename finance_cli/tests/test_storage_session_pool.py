from __future__ import annotations

import asyncio
import io
import json
import logging
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import Mock

import grpc
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from finance_cli import db as finance_db
from finance_cli.storage_client import errors as storage_errors
from finance_cli.storage_client import session_pool as session_pool_module
from finance_cli.storage_client._generated import storage_server_pb2 as pb2
from finance_cli.storage_client.connection import StorageConnection
from finance_cli.storage_client.session_pool import StorageSessionPool
from finance_cli.storage_client._session import SessionState


_REAL_RECORD_STORAGE_SESSION_POOL_EVENT = storage_errors.record_storage_session_pool_event


class FakeAuthProvider:
    def __init__(self, kid: str = "kid-1") -> None:
        self.kid = kid

    def get_token(self, product: str, user_id: str, scopes=None) -> str:
        del product, user_id, scopes
        return "token"


class FakeRpcError(grpc.RpcError):
    def __init__(
        self,
        storage_code: str,
        *,
        details: str | None = None,
        status: grpc.StatusCode = grpc.StatusCode.ABORTED,
    ) -> None:
        self._storage_code = storage_code
        self._details = details or storage_code
        self._status = status

    def trailing_metadata(self):
        return [("storage-server-error-code", self._storage_code)]

    def details(self):
        return self._details

    def code(self):
        return self._status


class FakeStub:
    def __init__(
        self,
        *,
        open_session_ids: list[str] | None = None,
        execute_outcomes: list[object] | None = None,
    ) -> None:
        self.open_session_ids = list(open_session_ids or ["session-1"])
        self.execute_outcomes = list(execute_outcomes or [])
        self.open_requests: list[pb2.OpenSessionRequest] = []
        self.execute_requests: list[pb2.ExecuteRequest] = []
        self.execute_session_ids: list[str] = []
        self.close_requests: list[pb2.CloseSessionRequest] = []

    def OpenSession(self, request, metadata=()):
        del metadata
        self.open_requests.append(request)
        if not self.open_session_ids:
            raise AssertionError("unexpected OpenSession call")
        return pb2.OpenSessionResponse(session_id=self.open_session_ids.pop(0))

    def Execute(self, request, metadata=()):
        del metadata
        self.execute_requests.append(request)
        self.execute_session_ids.append(str(request.session_id))
        outcome = self.execute_outcomes.pop(0) if self.execute_outcomes else _execute_response()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def CloseSession(self, request, metadata=()):
        del metadata
        self.close_requests.append(request)
        return pb2.CloseSessionResponse(closed=True)


@pytest.fixture(autouse=True)
def _reset_default_pool_and_metrics(monkeypatch):
    monkeypatch.setattr(session_pool_module, "_default_pool", None)
    monkeypatch.setattr(
        storage_errors,
        "record_storage_session_pool_event",
        lambda *args, **kwargs: None,
    )
    yield
    monkeypatch.setattr(session_pool_module, "_default_pool", None)


def test_01_empty_pool_returns_none_on_checkout() -> None:
    pool = StorageSessionPool()

    assert pool.checkout("target", "finance_cli", "alice", "kid") is None
    assert pool.size() == 0


def test_02_checkin_then_checkout_returns_same_session_id() -> None:
    pool = StorageSessionPool()

    assert pool.checkin("target", "finance_cli", "alice", "s1", "kid")

    assert pool.checkout("target", "finance_cli", "alice", "kid") == "s1"
    assert pool.size() == 0


def test_03_idle_timeout_evicts_lazy_on_checkout() -> None:
    pool = StorageSessionPool(idle_timeout_seconds=0.01, max_lifetime_seconds=100)
    assert pool.checkin("target", "finance_cli", "alice", "s1", "kid")

    time.sleep(0.02)

    assert pool.checkout("target", "finance_cli", "alice", "kid") is None
    assert pool.size() == 0


def test_04_max_lifetime_evicts_lazy_on_checkout() -> None:
    pool = StorageSessionPool(idle_timeout_seconds=100, max_lifetime_seconds=0.01)
    assert pool.checkin("target", "finance_cli", "alice", "s1", "kid")

    time.sleep(0.02)

    assert pool.checkout("target", "finance_cli", "alice", "kid") is None
    assert pool.size() == 0


def test_05_different_user_keys_do_not_collide() -> None:
    pool = StorageSessionPool()
    pool.checkin("target", "finance_cli", "alice", "alice-session", "kid")
    pool.checkin("target", "finance_cli", "bob", "bob-session", "kid")

    assert pool.checkout("target", "finance_cli", "alice", "kid") == "alice-session"
    assert pool.checkout("target", "finance_cli", "bob", "kid") == "bob-session"


def test_06_different_auth_kids_do_not_collide() -> None:
    pool = StorageSessionPool()
    pool.checkin("target", "finance_cli", "alice", "old-session", "old-kid")
    pool.checkin("target", "finance_cli", "alice", "new-session", "new-kid")

    assert pool.checkout("target", "finance_cli", "alice", "new-kid") == "new-session"
    assert pool.checkout("target", "finance_cli", "alice", "old-kid") == "old-session"


def test_07_evict_specific_session_and_all_user_sessions() -> None:
    pool = StorageSessionPool()
    pool.checkin("target", "finance_cli", "alice", "s1", "kid-a")
    pool.checkin("target", "finance_cli", "alice", "s2", "kid-a")
    pool.checkin("target", "finance_cli", "alice", "s3", "kid-b")
    pool.checkin("target", "finance_cli", "bob", "s4", "kid-a")

    pool.evict("target", "finance_cli", "alice", "kid-a", session_id="s1")

    assert pool.checkout("target", "finance_cli", "alice", "kid-a") == "s2"
    assert pool.evict_user("target", "finance_cli", "alice") == 1
    assert pool.checkout("target", "finance_cli", "alice", "kid-b") is None
    assert pool.checkout("target", "finance_cli", "bob", "kid-a") == "s4"


def test_08_close_all_calls_close_session_for_each_pooled_session() -> None:
    pool = StorageSessionPool()
    pool.checkin("target", "finance_cli", "alice", "s1", "kid")
    pool.checkin("target", "finance_cli", "alice", "s2", "kid")
    stub = Mock()
    stub.CloseSession.return_value = pb2.CloseSessionResponse(closed=True)
    stub_factory = Mock(return_value=stub)
    metadata_factory = Mock(return_value=(("authorization", "Bearer token"),))

    count = pool.close_all(stub_factory, metadata_factory)

    assert count == 2
    assert pool.size() == 0
    assert stub_factory.call_count == 1
    assert stub.CloseSession.call_count == 2
    assert metadata_factory.call_count == 2
    closed_ids = {
        call.args[0].session_id
        for call in stub.CloseSession.call_args_list
    }
    assert closed_ids == {"s1", "s2"}


def test_close_all_refuses_late_checkin_after_drain() -> None:
    pool = StorageSessionPool()

    count = pool.close_all(lambda target: Mock(), lambda product, user_id, auth_kid: ())

    assert count == 0
    assert pool._closed
    assert not pool.checkin("target", "finance_cli", "alice", "late-session", "kid")
    assert pool._sessions == {}
    assert pool.size() == 0


def test_get_default_pool_recreates_after_close_all() -> None:
    pool = session_pool_module.get_default_pool()

    pool.close_all(lambda target: Mock(), lambda product, user_id, auth_kid: ())
    recreated = session_pool_module.get_default_pool()

    assert recreated is not pool
    assert not recreated._closed


def test_09_concurrent_checkout_race_gives_one_thread_the_session() -> None:
    pool = StorageSessionPool()
    pool.checkin("target", "finance_cli", "alice", "s1", "kid")
    barrier = threading.Barrier(2)
    results: list[str | None] = []
    results_lock = threading.Lock()

    def worker() -> None:
        barrier.wait(timeout=5)
        result = pool.checkout("target", "finance_cli", "alice", "kid")
        with results_lock:
            results.append(result)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert sorted(results, key=lambda item: item or "") == [None, "s1"]
    assert pool.size() == 0


def test_10_overflow_on_checkin_returns_false_for_caller_hard_close() -> None:
    pool = StorageSessionPool(max_per_key=2)

    assert pool.checkin("target", "finance_cli", "alice", "s1", "kid")
    assert pool.checkin("target", "finance_cli", "alice", "s2", "kid")
    assert not pool.checkin("target", "finance_cli", "alice", "s3", "kid")
    assert pool.size() == 2


def test_11_thread_safety_stress_keeps_unique_checked_out_sessions() -> None:
    pool = StorageSessionPool(max_per_key=4)
    users = ("alice", "bob", "carol")
    for user in users:
        pool.checkin("target", "finance_cli", user, f"{user}-session", "kid")

    checked_out: set[str] = set()
    errors: list[BaseException] = []
    checked_out_lock = threading.Lock()

    def worker(offset: int) -> None:
        try:
            for index in range(100):
                user = users[(offset + index) % len(users)]
                session_id = pool.checkout("target", "finance_cli", user, "kid")
                if session_id is None:
                    continue
                with checked_out_lock:
                    assert session_id not in checked_out
                    checked_out.add(session_id)
                time.sleep(0)
                with checked_out_lock:
                    checked_out.remove(session_id)
                assert pool.checkin("target", "finance_cli", user, session_id, "kid")
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(offset,)) for offset in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert errors == []
    assert pool.size() == 3


def test_12_pragma_setters_mark_connection_tainted_and_hard_close() -> None:
    for sql in ("PRAGMA cache_size = 100", "PRAGMA cache_size(100)"):
        pool = StorageSessionPool()
        stub = FakeStub(open_session_ids=[f"{sql}-session"])
        conn = _make_connection(pool=pool, stub=stub)

        conn.execute(sql)
        conn.close()

        assert conn._session_id is None
        assert pool.size() == 0
        assert len(stub.close_requests) == 1


def test_13_create_temp_forms_mark_connection_tainted_and_hard_close() -> None:
    for sql in ("CREATE TEMP TABLE temp_items (id INTEGER)", "CREATE TABLE temp.temp_items (id INTEGER)"):
        pool = StorageSessionPool()
        stub = FakeStub(open_session_ids=[f"{len(sql)}-session"])
        conn = _make_connection(pool=pool, stub=stub)

        conn.execute(sql)
        conn.close()

        assert pool.size() == 0
        assert len(stub.close_requests) == 1


def test_14_normal_select_insert_update_do_not_taint_and_clean_close_pools() -> None:
    state = SessionState()
    for sql in ("SELECT 1", "INSERT INTO items VALUES (1)", "UPDATE items SET id = 2"):
        state.update_after_execute(sql, False, None)
        assert not state.tainted

    pool = StorageSessionPool()
    stub = FakeStub(open_session_ids=["clean-session"])
    conn = _make_connection(pool=pool, stub=stub)

    conn.execute("SELECT 1")
    conn.close()

    assert stub.close_requests == []
    assert pool.checkout("target", "finance_cli", "alice", "kid-1") == "clean-session"


def test_15_tainted_state_survives_commit_before_close() -> None:
    pool = StorageSessionPool()
    stub = FakeStub(
        open_session_ids=["tainted-session"],
        execute_outcomes=[
            _execute_response(in_transaction=True),
            _execute_response(in_transaction=True),
            _execute_response(in_transaction=False),
        ],
    )
    conn = _make_connection(pool=pool, stub=stub)

    conn.execute("BEGIN")
    conn.execute("PRAGMA cache_size = 100")
    conn.commit()
    conn.close()

    assert pool.size() == 0
    assert [request.session_id for request in stub.close_requests] == ["tainted-session"]


def test_16_clean_untainted_close_returns_to_pool_without_close_session_rpc() -> None:
    pool = StorageSessionPool()
    stub = FakeStub(open_session_ids=["s1"])
    conn = _make_connection(pool=pool, stub=stub)

    conn.execute("SELECT 1")
    conn.close()

    assert stub.close_requests == []
    assert pool.checkout("target", "finance_cli", "alice", "kid-1") == "s1"


def test_17_open_transaction_close_force_closes_and_does_not_pool() -> None:
    pool = StorageSessionPool()
    stub = FakeStub(
        open_session_ids=["dirty-session"],
        execute_outcomes=[_execute_response(in_transaction=True)],
    )
    conn = _make_connection(pool=pool, stub=stub)

    conn.execute("BEGIN")
    conn.close()

    assert pool.size() == 0
    assert [request.session_id for request in stub.close_requests] == ["dirty-session"]


def test_18_two_sequential_connections_reuse_session_without_second_open() -> None:
    pool = StorageSessionPool()
    stub = FakeStub(open_session_ids=["s1"])
    first = _make_connection(pool=pool, stub=stub)
    second = _make_connection(pool=pool, stub=stub)

    first.execute("SELECT 1")
    first.close()
    second.execute("SELECT 2")
    second.close()

    assert len(stub.open_requests) == 1
    assert stub.execute_session_ids == ["s1", "s1"]


def test_session_pool_does_not_reuse_session_across_connection_users() -> None:
    pool = StorageSessionPool()
    stub = FakeStub(open_session_ids=["alice-session", "bob-session"])
    alice = _make_connection(pool=pool, stub=stub, user_id="alice")
    bob = _make_connection(pool=pool, stub=stub, user_id="bob")

    alice.execute("SELECT 1")
    alice.close()
    bob.execute("SELECT 2")
    bob.close()

    assert [request.user_id for request in stub.open_requests] == ["alice", "bob"]
    assert stub.execute_session_ids == ["alice-session", "bob-session"]
    assert pool.checkout("target", "finance_cli", "alice", "kid-1") == "alice-session"
    assert pool.checkout("target", "finance_cli", "bob", "kid-1") == "bob-session"


def test_19_session_expired_evicts_reopens_and_retries() -> None:
    pool = StorageSessionPool()
    pool.checkin("target", "finance_cli", "alice", "expired-session", "kid-1")
    stub = FakeStub(
        open_session_ids=["new-session"],
        execute_outcomes=[
            FakeRpcError("SESSION_EXPIRED", status=grpc.StatusCode.UNAVAILABLE),
            _execute_response(value=2),
        ],
    )
    conn = _make_connection(pool=pool, stub=stub)

    assert conn.execute("SELECT 2").fetchone() == (2,)

    assert stub.execute_session_ids == [
        "expired-session",
        "new-session",
    ]
    assert len(stub.open_requests) == 1
    assert pool.size() == 0
    conn.close()


def test_session_expired_retry_terminal_error_clears_session_id_and_evicts_pool() -> None:
    pool = StorageSessionPool()
    pool.checkin("target", "finance_cli", "alice", "expired-session", "kid-1")
    stub = FakeStub(
        open_session_ids=["retry-session"],
        execute_outcomes=[
            FakeRpcError("SESSION_EXPIRED", status=grpc.StatusCode.UNAVAILABLE),
            FakeRpcError("SESSION_ABORTED"),
        ],
    )
    conn = _make_connection(pool=pool, stub=stub)

    with pytest.raises(storage_errors.SessionAborted):
        conn.execute("SELECT 2")

    assert stub.execute_session_ids == [
        "expired-session",
        "retry-session",
    ]
    assert len(stub.open_requests) == 1
    assert conn._session_id is None
    assert pool.size() == 0
    assert pool.checkout("target", "finance_cli", "alice", "kid-1") is None
    conn.close()
    assert stub.close_requests == []


def test_20_session_aborted_evicts_without_retry_and_clears_session_id() -> None:
    pool = StorageSessionPool()
    pool.checkin("target", "finance_cli", "alice", "aborted-session", "kid-1")
    stub = FakeStub(
        execute_outcomes=[FakeRpcError("SESSION_ABORTED")],
    )
    conn = _make_connection(pool=pool, stub=stub)

    with pytest.raises(storage_errors.SessionAborted):
        conn.execute("SELECT 1")

    assert len(stub.execute_requests) == 1
    assert len(stub.open_requests) == 0
    assert conn._session_id is None
    conn.close()
    assert stub.close_requests == []


def test_21_session_invalid_evicts_without_retry_and_clears_session_id() -> None:
    pool = StorageSessionPool()
    pool.checkin("target", "finance_cli", "alice", "invalid-session", "kid-1")
    stub = FakeStub(
        execute_outcomes=[FakeRpcError("SESSION_INVALID")],
    )
    conn = _make_connection(pool=pool, stub=stub)

    with pytest.raises(storage_errors.SessionInvalid):
        conn.execute("SELECT 1")

    assert len(stub.execute_requests) == 1
    assert len(stub.open_requests) == 0
    assert conn._session_id is None
    conn.close()
    assert stub.close_requests == []


def test_22_maintenance_mode_evicts_pool_for_resolved_and_queued_paths(
    monkeypatch,
    tmp_path: Path,
) -> None:
    pool = StorageSessionPool()
    monkeypatch.setattr(session_pool_module, "_default_pool", pool)
    monkeypatch.setenv("FINANCE_CLI_STORAGE_SESSION_POOL", "enabled")
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    monkeypatch.setenv("STORAGE_SERVER_URL", "target")

    pool.checkin("target", "finance_cli", "alice", "migrating-session", "kid")
    with pytest.raises(storage_errors.MaintenanceModeError):
        finance_db._storage_connection_for_dispatch(
            resolved=tmp_path / "finance.db",
            user_id=None,
            expected_user_id="alice",
            auth_provider=FakeAuthProvider(),
            storage_mode_override="migrating",
        )
    assert pool.size() == 0

    pool.checkin("target", "finance_cli", "alice", "queued-session", "kid")
    monkeypatch.setattr(
        finance_db.storage_lease,
        "acquire_or_route",
        lambda *args, **kwargs: finance_db.storage_lease.Queued(storage_mode="replaying"),
    )
    with pytest.raises(storage_errors.MaintenanceModeError):
        finance_db._acquire_connection_lease(
            resolved=tmp_path / "finance.db",
            user_id=None,
            expected_user_id="alice",
            session_manager=object(),
            product="finance_cli",
        )
    assert pool.size() == 0


def test_23_env_flag_disabled_preserves_no_pool_default_and_enabled_passes_pool(
    monkeypatch,
    tmp_path: Path,
) -> None:
    pool = StorageSessionPool()
    monkeypatch.setattr(session_pool_module, "_default_pool", pool)
    monkeypatch.setenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED", "true")
    monkeypatch.setenv("STORAGE_SERVER_URL", "target")
    monkeypatch.delenv("FINANCE_CLI_STORAGE_SESSION_POOL", raising=False)

    disabled_conn = finance_db._storage_connection_for_dispatch(
        resolved=tmp_path / "finance.db",
        user_id=None,
        expected_user_id="alice",
        auth_provider=FakeAuthProvider(),
        storage_mode_override="remote",
    )
    assert disabled_conn is not None
    assert disabled_conn._session_pool is None
    disabled_conn.close()

    monkeypatch.setenv("FINANCE_CLI_STORAGE_SESSION_POOL", "enabled")
    enabled_conn = finance_db._storage_connection_for_dispatch(
        resolved=tmp_path / "finance.db",
        user_id=None,
        expected_user_id="alice",
        auth_provider=FakeAuthProvider(),
        storage_mode_override="remote",
    )
    assert enabled_conn is not None
    assert enabled_conn._session_pool is pool
    enabled_conn.close()


def test_24_finance_web_lifespan_runs_storage_pool_drain_on_shutdown(monkeypatch) -> None:
    root = Path(__file__).resolve().parents[2]
    finance_web_path = root / "finance-web"
    if str(finance_web_path) not in sys.path:
        sys.path.insert(0, str(finance_web_path))
    from server import app as app_module

    calls: list[str] = []

    async def fake_drain() -> None:
        calls.append("drained")

    monkeypatch.setattr(app_module, "_drain_storage_session_pool", fake_drain)
    app = FastAPI(lifespan=app_module._lifespan)

    with TestClient(app):
        pass

    assert calls == ["drained"]


def test_25_mcp_remote_lifespan_drains_pool_on_shutdown(monkeypatch) -> None:
    import finance_cli.mcp_remote as mcp_remote

    events: list[str] = []
    original_lifespan = mcp_remote.mcp._lifespan
    original_installed = mcp_remote._STORAGE_POOL_LIFESPAN_INSTALLED

    @asynccontextmanager
    async def base_lifespan(_server):
        events.append("base-enter")
        try:
            yield
        finally:
            events.append("base-exit")

    async def fake_drain() -> None:
        events.append("drain")

    try:
        mcp_remote.mcp._lifespan = base_lifespan
        mcp_remote._STORAGE_POOL_LIFESPAN_INSTALLED = False
        monkeypatch.setattr(mcp_remote, "_drain_storage_session_pool", fake_drain)

        mcp_remote._install_storage_pool_lifespan()

        async def run() -> None:
            async with mcp_remote.mcp._lifespan(object()):
                events.append("inside")

        asyncio.run(run())
    finally:
        mcp_remote.mcp._lifespan = original_lifespan
        mcp_remote._STORAGE_POOL_LIFESPAN_INSTALLED = original_installed

    assert events == ["base-enter", "inside", "drain", "base-exit"]


def test_session_pool_metric_formatter_allows_outcome_and_drops_unknown_fields(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        storage_errors,
        "record_storage_session_pool_event",
        _REAL_RECORD_STORAGE_SESSION_POOL_EVENT,
    )
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.INFO)
    handler.setFormatter(storage_errors._JSONFormatter())
    logger = storage_errors._METRIC_LOGGER
    old_handlers = list(logger.handlers)
    old_level = logger.level
    old_propagate = logger.propagate
    for old_handler in old_handlers:
        logger.removeHandler(old_handler)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        storage_errors.record_storage_session_pool_event(
            "session_pool_checkin",
            user_id="1",
            outcome="pooled",
            pool_size=2,
            secret_credentials="foo",
        )
        handler.flush()
    finally:
        logger.removeHandler(handler)
        handler.close()
        for old_handler in old_handlers:
            logger.addHandler(old_handler)
        logger.setLevel(old_level)
        logger.propagate = old_propagate

    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "session_pool_checkin"
    assert payload["outcome"] == "pooled"
    assert "secret_credentials" not in payload


def _make_connection(
    *,
    pool: StorageSessionPool,
    stub: FakeStub,
    user_id: str = "alice",
    kid: str = "kid-1",
) -> StorageConnection:
    conn = StorageConnection(
        "target",
        user_id=user_id,
        auth_provider=FakeAuthProvider(kid),
        session_pool=pool,
    )
    conn._stub = stub
    return conn


def _execute_response(
    *,
    value: int = 1,
    in_transaction: bool = False,
) -> pb2.ExecuteResponse:
    return pb2.ExecuteResponse(
        rows=[pb2.Row(values=[pb2.SqlParam(integer=value)])],
        column_names=["value"],
        rowcount=-1,
        in_transaction=in_transaction,
    )
