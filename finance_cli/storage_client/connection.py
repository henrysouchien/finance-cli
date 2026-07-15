"""sqlite3.Connection-compatible client backed by the storage server proxy."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from typing import Any

import grpc

from . import _params, auth, channel, errors
from ._generated import storage_server_pb2 as pb2
from ._generated import storage_server_pb2_grpc as pb2_grpc
from ._session import SessionState, starts_implicit_dml
from .cursor import StorageCursor
from .session_pool import StorageSessionPool


_BACKUP_TARGET_CONN_MESSAGE = (
    "storage_client.backup expects target_path: str | None (server-generated path is returned). "
    "target_conn-style backup is not supported via the proxy. "
    "See PLAN_STORAGE_PHASE_4_CLIENT.md decision #12."
)
_CREATE_FUNCTION_MESSAGE = (
    "storage_client supports only the current_session_id pseudo-function; "
    "arbitrary Python callbacks cannot be remoted"
)
_UNSUPPORTED_ATTR_MESSAGE = (
    "storage_client does not support this sqlite3 connection callback API via the proxy"
)


class StorageConnection:
    """Drop-in sqlite3.Connection replacement for the storage server proxy."""

    def __init__(
        self,
        target: str,
        *,
        user_id: str,
        product: str = "finance_cli",
        scopes: list[str] | None = None,
        auth_provider=None,
        channel_pool=None,
        jwt_metadata_key: str = "authorization",
        session_pool: StorageSessionPool | None = None,
    ) -> None:
        self._target = target
        self._user_id = user_id
        self._product = product
        self._scopes = list(scopes or [])
        self._auth_provider = auth_provider or auth.get_default_provider()
        self._channel_pool = channel_pool or channel._default_pool
        self._jwt_metadata_key = jwt_metadata_key
        self._session_pool = session_pool
        self._session_id: str | None = None
        self._session = SessionState()
        self._channel: grpc.Channel | None = None
        self._stub: pb2_grpc.SqliteProxyStub | None = None
        self._row_factory = None
        self._closed = False
        self._close_on_context_exit = False

    @property
    def user_id(self) -> str:
        """Public accessor for the bound user_id."""
        return self._user_id

    def execute(self, sql: str, params: _params.Params = None) -> StorageCursor:
        cursor = self.cursor()
        return cursor.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Iterable[_params.Params]) -> StorageCursor:
        cursor = self.cursor()
        return cursor.executemany(sql, seq_of_params)

    def executescript(self, sql: str) -> StorageCursor:
        cursor = self.cursor()
        return cursor.executescript(sql)

    def commit(self) -> None:
        self._ensure_open()
        if self._session_id is None or not self._session.last_in_transaction:
            return
        response = self._execute_no_implicit("COMMIT", None)
        self._session.update_after_execute("COMMIT", response.in_transaction, response)

    def rollback(self) -> None:
        self._ensure_open()
        if self._session_id is None or not self._session.last_in_transaction:
            return
        response = self._execute_no_implicit("ROLLBACK", None)
        self._session.update_after_execute("ROLLBACK", response.in_transaction, response)

    def close(self) -> None:
        if self._closed:
            self._run_storage_lease_cleanup()
            return
        try:
            if self._session_id is not None:
                hard_close = True
                checkin_outcome: str | None = None
                if self._session_pool is not None:
                    transaction_clean = (
                        not self._session.last_in_transaction
                        and self._session.explicit_begin_depth == 0
                    )
                    if self._session.tainted:
                        checkin_outcome = "tainted_closed"
                    elif not transaction_clean:
                        checkin_outcome = "dirty_closed"
                    else:
                        pooled = self._session_pool.checkin(
                            self._target,
                            self._product,
                            self._user_id,
                            self._session_id,
                            self._auth_kid(),
                        )
                        if pooled:
                            hard_close = False
                            checkin_outcome = "pooled"
                        else:
                            checkin_outcome = "overflow_closed"
                    self._record_session_pool_event(
                        "session_pool_checkin",
                        outcome=checkin_outcome,
                    )
                if hard_close:
                    self._close_session_rpc(self._session_id)
        finally:
            self._closed = True
            self._session_id = None
            self._session.reset_for_reopen()
            self._run_storage_lease_cleanup()

    def cursor(self) -> StorageCursor:
        self._ensure_open()
        return StorageCursor(connection=self)

    def backup(self, target_path: str | None = None) -> str:
        self._ensure_open()
        if target_path is not None and not isinstance(target_path, str):
            raise NotImplementedError(_BACKUP_TARGET_CONN_MESSAGE)
        session_id = self._session_open()
        request = pb2.BackupDatabaseRequest(
            product=self._product,
            user_id=self._user_id,
            session_id=session_id,
        )
        try:
            response = self._stub_for_call().BackupDatabase(request, metadata=self._metadata())
        except grpc.RpcError as exc:
            raise errors.from_grpc_error(exc, rpc="BackupDatabase") from exc
        return str(response.path)

    def create_function(self, name: str, narg: int, callable: Any, *args, **kwargs) -> None:
        del callable, args, kwargs
        if str(name) == "current_session_id" and int(narg) in {-1, 0}:
            return None
        raise NotImplementedError(_CREATE_FUNCTION_MESSAGE)

    def iterdump(self, *args, **kwargs):
        raise NotImplementedError(_UNSUPPORTED_ATTR_MESSAGE)

    def set_trace_callback(self, *args, **kwargs):
        raise NotImplementedError(_UNSUPPORTED_ATTR_MESSAGE)

    def set_progress_handler(self, *args, **kwargs):
        raise NotImplementedError(_UNSUPPORTED_ATTR_MESSAGE)

    def set_authorizer(self, *args, **kwargs):
        raise NotImplementedError(_UNSUPPORTED_ATTR_MESSAGE)

    def interrupt(self):
        raise NotImplementedError(_UNSUPPORTED_ATTR_MESSAGE)

    def __enter__(self) -> "StorageConnection":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            if self._close_on_context_exit:
                self.close()
        return None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    @property
    def in_transaction(self) -> bool:
        return bool(self._session.last_in_transaction) if self._session_id is not None else False

    @property
    def row_factory(self):
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value) -> None:
        self._row_factory = value

    def _execute(self, sql: str, params: _params.Params = None) -> pb2.ExecuteResponse:
        self._ensure_open()
        if self._should_open_implicit_transaction(sql):
            begin_response = self._execute_no_implicit("BEGIN", None)
            self._session.update_after_execute("BEGIN", begin_response.in_transaction, begin_response)
        return self._execute_no_implicit(sql, params)

    def _execute_no_implicit(self, sql: str, params: _params.Params = None) -> pb2.ExecuteResponse:
        session_id = self._session_open()
        positional, named = _params.split_params(params)
        request = pb2.ExecuteRequest(session_id=session_id, sql=sql)
        request.positional.extend(positional)
        _params.copy_named_params(request.named, named)
        return self._call_execute_with_recovery("Execute", request, sql=sql)

    def _execute_many(self, sql: str, seq_of_params: Iterable[_params.Params]) -> pb2.ExecuteResponse:
        self._ensure_open()
        if self._should_open_implicit_transaction(sql):
            begin_response = self._execute_no_implicit("BEGIN", None)
            self._session.update_after_execute("BEGIN", begin_response.in_transaction, begin_response)
        session_id = self._session_open()
        request = pb2.ExecuteManyRequest(session_id=session_id, sql=sql)
        request.bindings.extend(_params.to_bindings(params) for params in seq_of_params)
        return self._call_execute_with_recovery("ExecuteMany", request, sql=sql)

    def _execute_script(self, sql: str) -> pb2.ExecuteScriptResponse:
        self._ensure_open()
        session_id = self._session_open()
        request = pb2.ExecuteScriptRequest(session_id=session_id, sql=sql)
        return self._call_execute_with_recovery("ExecuteScript", request, sql=sql)

    def _call_execute_with_recovery(self, rpc_name: str, request, *, sql: str):
        del sql
        stub = self._stub_for_call()
        rpc = getattr(stub, rpc_name)
        try:
            return rpc(request, metadata=self._metadata())
        except grpc.RpcError as exc:
            mapped = errors.from_grpc_error(exc, rpc=rpc_name)
            evictable = isinstance(
                mapped,
                (errors.SessionExpired, errors.SessionAborted, errors.SessionInvalid),
            )
            retry_expired = (
                isinstance(mapped, errors.SessionExpired)
                and self._session.should_retry_session_expired()
            )
            bad_session_id = self._session_id
            if evictable and self._session_pool is not None and bad_session_id is not None:
                reason = _session_evict_reason(mapped)
                self._session_pool.evict(
                    self._target,
                    self._product,
                    self._user_id,
                    self._auth_kid(),
                    session_id=bad_session_id,
                )
                self._record_session_pool_event(
                    "session_pool_evict",
                    reason=reason,
                    session_id=bad_session_id,
                )
            if not retry_expired:
                if evictable:
                    self._session_id = None
                    self._session.reset_for_reopen()
                raise mapped from exc
        self._session.reset_for_reopen()
        self._session_id = None
        new_session_id = self._session_open()
        request.session_id = new_session_id
        try:
            return rpc(request, metadata=self._metadata())
        except grpc.RpcError as inner_exc:
            inner_mapped = errors.from_grpc_error(inner_exc, rpc=rpc_name)
            inner_evictable = isinstance(
                inner_mapped,
                (errors.SessionExpired, errors.SessionAborted, errors.SessionInvalid),
            )
            if (
                inner_evictable
                and self._session_pool is not None
                and self._session_id is not None
            ):
                self._session_pool.evict(
                    self._target,
                    self._product,
                    self._user_id,
                    self._auth_kid(),
                    session_id=self._session_id,
                )
            if inner_evictable:
                self._session_id = None
                self._session.reset_for_reopen()
            raise inner_mapped from inner_exc

    def _session_open(self) -> str:
        self._ensure_open()
        if self._session_id is not None:
            return self._session_id
        if self._session_pool is not None:
            cached, reason, cached_age_us = self._session_pool.checkout_with_metadata(
                self._target,
                self._product,
                self._user_id,
                self._auth_kid(),
            )
            if cached is not None:
                self._session_id = cached
                self._session.session_id = cached
                self._session.last_kid = self._auth_kid()
                self._record_session_pool_event(
                    "session_pool_hit",
                    cached_age_us=cached_age_us,
                )
                return cached
            self._record_session_pool_event("session_pool_miss", reason=reason)
        request = pb2.OpenSessionRequest(user_id=self._user_id, product=self._product)
        try:
            response = self._stub_for_call().OpenSession(request, metadata=self._metadata())
        except grpc.RpcError as exc:
            raise errors.from_grpc_error(exc, rpc="OpenSession") from exc
        self._session_id = str(response.session_id)
        self._session.session_id = self._session_id
        self._session.last_kid = getattr(self._auth_provider, "kid", None)
        return self._session_id

    def _auth_kid(self) -> str | None:
        return getattr(self._auth_provider, "kid", None)

    def _metadata(self) -> tuple[tuple[str, str], ...]:
        token = self._auth_provider.get_token(self._product, self._user_id, self._scopes)
        key = self._jwt_metadata_key
        value = f"Bearer {token}" if key.lower() == "authorization" else str(token)
        return ((key, value),)

    def _stub_for_call(self) -> pb2_grpc.SqliteProxyStub:
        if self._stub is None:
            self._channel = self._channel_pool.get(self._target)
            self._stub = pb2_grpc.SqliteProxyStub(self._channel)
        return self._stub

    def _should_open_implicit_transaction(self, sql: str) -> bool:
        if not starts_implicit_dml(sql):
            return False
        return (
            self._session_id is None
            or (
                not self._session.last_in_transaction
                and self._session.explicit_begin_depth == 0
            )
        )

    def _ensure_open(self) -> None:
        if self._closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database.")

    def _close_session_rpc(self, session_id: str) -> None:
        request = pb2.CloseSessionRequest(session_id=session_id)
        try:
            self._stub_for_call().CloseSession(request, metadata=self._metadata())
        except grpc.RpcError as exc:
            errors.from_grpc_error(exc, rpc="CloseSession")
            pass

    def _record_session_pool_event(self, event_name: str, **fields: Any) -> None:
        if self._session_pool is None:
            return
        payload = {
            "user_id": self._user_id,
            "pool_size": self._session_pool.size(),
        }
        payload.update({key: value for key, value in fields.items() if value is not None})
        try:
            errors.record_storage_session_pool_event(event_name, **payload)
        except Exception:
            pass

    def _run_storage_lease_cleanup(self) -> None:
        cleanup = getattr(self, "_storage_lease_cleanup", None)
        if cleanup is None:
            return
        self._storage_lease_cleanup = None
        cleanup()


def _session_evict_reason(exc: errors.StorageClientError) -> str:
    if isinstance(exc, errors.SessionExpired):
        return "session_expired"
    if isinstance(exc, errors.SessionAborted):
        return "session_aborted"
    if isinstance(exc, errors.SessionInvalid):
        return "session_invalid"
    return "unknown"
