"""Canonical PostgreSQL-backed storage cutover leases for Phase 5.

This implementation lives in ``finance_cli`` because ``db.connect()`` and
CLI storage entrypoints are defined here; finance-web re-exports it from
``server.storage_lease`` for component-layout compatibility. psycopg2-specific
adapters are loaded lazily and remain optional so CLI-only environments without
PostgreSQL dependencies can still import the module. Canonical plan reference:
``docs/planning/PLAN_STORAGE_PHASE_5_CUTOVER.md`` decision #1.
"""

from __future__ import annotations

import atexit
import contextlib
import contextvars
from dataclasses import dataclass
from datetime import datetime
import logging
import os
import sys
import threading
import time
from typing import Any, Callable
from uuid import UUID, uuid4

log = logging.getLogger(__name__)

_JSON_ADAPTER_UNSET = object()
_json_adapter: Any = _JSON_ADAPTER_UNSET
_VALID_STORAGE_MODES = frozenset({"local", "remote", "migrating", "replaying"})
_QUEUE_STORAGE_MODES = frozenset({"migrating", "replaying"})
_LEASE_HEARTBEAT_INTERVAL_S = 30.0
_LEASE_HEARTBEAT_MIN_AGE_S = 60.0
_STALE_LEASE_SECONDS = 300


class LeaseMissingError(RuntimeError):
    """Raised when storage access occurs without an active lease in enforce mode."""


class LeaseUnavailableError(RuntimeError):
    """Raised when the lease table cannot be used for a required operation."""


class LeaseQueuedError(LeaseUnavailableError):
    """Raised when access is blocked by migrating/replaying storage_mode."""


@dataclass(frozen=True)
class LocalLease:
    lease_id: str
    storage_mode: str = "local"


@dataclass(frozen=True)
class RemoteLease:
    lease_id: str
    storage_mode: str = "remote"


@dataclass(frozen=True)
class Queued:
    storage_mode: str


AcquireResult = LocalLease | RemoteLease | Queued


@dataclass(frozen=True)
class SuspectLease:
    lease_id: str
    user_id: int
    started_at: datetime | None
    last_heartbeat_at: datetime | None
    operation: str | None
    holder_pid: int | None
    storage_mode: str


@dataclass(frozen=True)
class CleanupResult:
    deleted_count: int
    suspect_leases: tuple[SuspectLease, ...]


@dataclass(frozen=True)
class RemainingLease:
    lease_id: str
    started_at: datetime | None
    last_heartbeat_at: datetime | None
    operation: str | None
    holder_pid: int | None


@dataclass(frozen=True)
class DrainResult:
    success: bool
    timed_out: bool
    remaining_leases: tuple[RemainingLease, ...]
    elapsed_s: float


_lease_context: contextvars.ContextVar["LeaseScope | None"] = contextvars.ContextVar(
    "_lease_context",
    default=None,
)


def lease_enforcement_enabled() -> bool:
    return str(os.getenv("STORAGE_LEASE_ENFORCE") or "").strip().lower() == "true"


def current_lease_scope() -> "LeaseScope | None":
    return _lease_context.get()


def require_active_lease(*, user_id: str | int | None = None, resource: str = "storage") -> "LeaseScope":
    scope = _lease_context.get()
    if scope is None:
        raise LeaseMissingError(f"{resource} access requires an active storage lease")
    if user_id is not None and str(scope.user_id) != str(user_id):
        raise LeaseMissingError(
            f"{resource} access for user {user_id!r} is outside active lease for user {scope.user_id!r}"
        )
    return scope


def enforce_active_lease_if_required(
    *,
    user_id: str | int | None = None,
    resource: str = "storage",
) -> "LeaseScope | None":
    scope = _lease_context.get()
    if lease_enforcement_enabled():
        return require_active_lease(user_id=user_id, resource=resource)
    return scope


@contextlib.contextmanager
def optional_lease_scope(
    user_id: str | int | None,
    *,
    session_manager=None,
    operation: str = "request",
    metadata: dict[str, Any] | None = None,
    heartbeat: bool = False,
):
    """Acquire a lease when PG/session_manager is available, otherwise no-op unless enforced."""

    if user_id is None or current_lease_scope() is not None:
        yield current_lease_scope()
        return
    manager = session_manager if session_manager is not None else _default_session_manager()
    if manager is None:
        if lease_enforcement_enabled():
            raise LeaseMissingError("storage access requires an active storage lease")
        yield None
        return
    try:
        with LeaseScope.acquire(
            user_id,
            session_manager=manager,
            operation=operation,
            metadata=metadata,
            heartbeat=heartbeat,
        ) as scope:
            if isinstance(scope, Queued):
                raise LeaseQueuedError(f"user {user_id!r} is in storage_mode={scope.storage_mode}")
            yield scope
    except LeaseQueuedError:
        raise
    except LeaseUnavailableError:
        if lease_enforcement_enabled():
            raise
        yield None


def acquire_or_route(
    user_id: str | int,
    *,
    session_manager,
    on_queue: Callable[[Any, str], None] | None = None,
    operation: str = "request",
    metadata: dict[str, Any] | None = None,
) -> AcquireResult:
    """Atomically read ``users.storage_mode`` and acquire a lease if routable."""

    pg_user_id = _coerce_pg_user_id(user_id)
    if session_manager is None or not hasattr(session_manager, "get_db_session"):
        raise LeaseUnavailableError("session_manager_unavailable")

    lease_id = str(uuid4())
    pid = os.getpid()
    command = " ".join(sys.argv)[:1000] if sys.argv else None
    metadata_value = _jsonb(metadata)

    try:
        session_ctx = session_manager.get_db_session()
    except Exception as exc:
        raise LeaseUnavailableError("session_manager_unavailable") from exc
    if not hasattr(session_ctx, "__enter__") or not hasattr(session_ctx, "__exit__"):
        raise LeaseUnavailableError("session_manager_unavailable")

    with session_ctx as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT storage_mode FROM users WHERE id = %s FOR UPDATE",
                (pg_user_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise LeaseUnavailableError(f"user_not_found:{pg_user_id}")
            mode = _extract_value(row, "storage_mode", 0)
            storage_mode = str(mode or "local").strip().lower()
            if storage_mode not in _VALID_STORAGE_MODES:
                storage_mode = "local"

            if storage_mode in _QUEUE_STORAGE_MODES:
                if on_queue is not None:
                    on_queue(conn, storage_mode)
                _commit(conn)
                return Queued(storage_mode=storage_mode)

            cursor.execute(
                """
                INSERT INTO user_access_leases (
                    lease_id,
                    user_id,
                    pid,
                    started_at,
                    last_heartbeat_at,
                    operation,
                    holder_pid,
                    command,
                    metadata
                )
                VALUES (%s, %s, %s, NOW(), NOW(), %s, %s, %s, %s)
                """,
                (
                    lease_id,
                    pg_user_id,
                    pid,
                    str(operation or "request"),
                    pid,
                    command,
                    metadata_value,
                ),
            )
            _commit(conn)
        except Exception:
            _rollback(conn)
            raise

    if storage_mode == "remote":
        return RemoteLease(lease_id=lease_id)
    return LocalLease(lease_id=lease_id)


def release_user_lease(lease_id: str | UUID | None, *, session_manager) -> None:
    """Release a lease. The operation is idempotent."""

    if not lease_id or session_manager is None or not hasattr(session_manager, "get_db_session"):
        return
    with session_manager.get_db_session() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM user_access_leases WHERE lease_id = %s",
                (str(lease_id),),
            )
            _commit(conn)
        except Exception:
            _rollback(conn)
            raise


class LeaseHeartbeat:
    """Per-process heartbeat manager for active long-lived leases."""

    _instance: "LeaseHeartbeat | None" = None
    _instance_lock = threading.Lock()

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._leases: dict[str, tuple[Any, float]] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @classmethod
    def instance(cls) -> "LeaseHeartbeat":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
                atexit.register(cls._instance.shutdown)
            return cls._instance

    def register(self, lease_id: str | UUID, *, session_manager) -> None:
        if not lease_id or session_manager is None or not hasattr(session_manager, "get_db_session"):
            return
        with self._lock:
            self._leases[str(lease_id)] = (session_manager, time.monotonic())
            if self._thread is None or not self._thread.is_alive():
                self._stop.clear()
                self._thread = threading.Thread(
                    target=self._run,
                    name="storage-lease-heartbeat",
                    daemon=True,
                )
                self._thread.start()

    def unregister(self, lease_id: str | UUID | None) -> None:
        if not lease_id:
            return
        with self._lock:
            self._leases.pop(str(lease_id), None)

    def heartbeat_once(self) -> None:
        now = time.monotonic()
        grouped: dict[int, tuple[Any, list[str]]] = {}
        with self._lock:
            for lease_id, (session_manager, added_at) in self._leases.items():
                if now - added_at < _LEASE_HEARTBEAT_MIN_AGE_S:
                    continue
                key = id(session_manager)
                if key not in grouped:
                    grouped[key] = (session_manager, [])
                grouped[key][1].append(lease_id)

        for session_manager, lease_ids in grouped.values():
            if lease_ids:
                self._heartbeat_batch(session_manager, lease_ids)

    def shutdown(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=1.0)

    def _run(self) -> None:
        while not self._stop.wait(_LEASE_HEARTBEAT_INTERVAL_S):
            with self._lock:
                if not self._leases:
                    return
            try:
                self.heartbeat_once()
            except Exception:
                log.exception("storage_lease_heartbeat_failed")

    @staticmethod
    def _heartbeat_batch(session_manager, lease_ids: list[str]) -> None:
        with session_manager.get_db_session() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    UPDATE user_access_leases
                    SET last_heartbeat_at = NOW()
                    WHERE lease_id = ANY(%s::uuid[])
                    """,
                    (lease_ids,),
                )
                _commit(conn)
            except Exception:
                _rollback(conn)
                raise


class LeaseScope:
    """ContextVar-backed lease scope.

    ``LeaseScope`` can wrap an existing ``AcquireResult`` or acquire its own
    lease via ``LeaseScope.acquire(...)``. It releases owned leases on exit.
    """

    def __init__(
        self,
        *,
        user_id: str | int,
        lease: LocalLease | RemoteLease,
        session_manager,
        owns_lease: bool = True,
        heartbeat: bool = False,
    ) -> None:
        self.user_id = str(user_id)
        self.lease_id = str(lease.lease_id)
        self.storage_mode = str(lease.storage_mode)
        self.session_manager = session_manager
        self.owns_lease = bool(owns_lease)
        self.heartbeat = bool(heartbeat)
        self._token: contextvars.Token["LeaseScope | None"] | None = None
        self._closed = False

    @classmethod
    @contextlib.contextmanager
    def acquire(
        cls,
        user_id: str | int,
        *,
        session_manager,
        on_queue: Callable[[Any, str], None] | None = None,
        operation: str = "request",
        metadata: dict[str, Any] | None = None,
        heartbeat: bool | None = None,
    ):
        result = acquire_or_route(
            user_id,
            session_manager=session_manager,
            on_queue=on_queue,
            operation=operation,
            metadata=metadata,
        )
        if isinstance(result, Queued):
            yield result
            return
        scope = cls(
            user_id=user_id,
            lease=result,
            session_manager=session_manager,
            owns_lease=True,
            heartbeat=(operation != "request" if heartbeat is None else heartbeat),
        )
        with scope:
            yield scope

    def __enter__(self) -> "LeaseScope":
        self._token = _lease_context.set(self)
        if self.heartbeat:
            LeaseHeartbeat.instance().register(self.lease_id, session_manager=self.session_manager)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return wrapper

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self.heartbeat:
            LeaseHeartbeat.instance().unregister(self.lease_id)
        try:
            if self.owns_lease:
                release_user_lease(self.lease_id, session_manager=self.session_manager)
        finally:
            if self._token is not None:
                _lease_context.reset(self._token)
                self._token = None

    @staticmethod
    def run_in_context(callable_, *args, **kwargs):
        """Run now inside the current copied ContextVar context.

        Call this from inside the lease scope. For executor submission, prefer
        ``LeaseScope.bind_context(...)`` so the callable is pre-bound before it
        crosses a thread or loop boundary.
        """

        ctx = contextvars.copy_context()
        return ctx.run(callable_, *args, **kwargs)

    @staticmethod
    def bind_context(callable_, *args, **kwargs):
        """Return a callable bound to the current ContextVar context."""

        ctx = contextvars.copy_context()

        def bound(*later_args, **later_kwargs):
            call_kwargs = dict(kwargs)
            call_kwargs.update(later_kwargs)
            return ctx.run(callable_, *args, *later_args, **call_kwargs)

        return bound


def _cleanup_stale_leases(*, session_manager, cutover_aware: bool = True) -> CleanupResult:
    """Delete stale housekeeping leases and return cutover suspects."""

    with session_manager.get_db_session() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    l.lease_id,
                    l.user_id,
                    l.started_at,
                    l.last_heartbeat_at,
                    l.operation,
                    l.holder_pid,
                    u.storage_mode
                FROM user_access_leases l
                JOIN users u ON u.id = l.user_id
                WHERE l.last_heartbeat_at < NOW() - interval '300 seconds'
                ORDER BY l.last_heartbeat_at ASC
                """
            )
            rows = cursor.fetchall()
            suspects: list[SuspectLease] = []
            deletable: list[str] = []
            for row in rows:
                mode = str(_extract_value(row, "storage_mode", 6) or "local")
                lease_id = str(_extract_value(row, "lease_id", 0))
                if cutover_aware and mode in _QUEUE_STORAGE_MODES:
                    suspects.append(
                        SuspectLease(
                            lease_id=lease_id,
                            user_id=int(_extract_value(row, "user_id", 1)),
                            started_at=_extract_value(row, "started_at", 2),
                            last_heartbeat_at=_extract_value(row, "last_heartbeat_at", 3),
                            operation=_extract_value(row, "operation", 4),
                            holder_pid=_extract_value(row, "holder_pid", 5),
                            storage_mode=mode,
                        )
                    )
                else:
                    deletable.append(lease_id)

            deleted_count = 0
            if deletable:
                cursor.execute(
                    "DELETE FROM user_access_leases WHERE lease_id = ANY(%s::uuid[])",
                    (deletable,),
                )
                deleted_count = int(getattr(cursor, "rowcount", 0) or 0)
            _commit(conn)
            return CleanupResult(
                deleted_count=deleted_count,
                suspect_leases=tuple(suspects),
            )
        except Exception:
            _rollback(conn)
            raise


def drain_until_empty(
    user_id: str | int,
    *,
    session_manager,
    timeout_s: float = 600,
) -> DrainResult:
    """Poll active leases until the user is drained or the timeout expires."""

    pg_user_id = _coerce_pg_user_id(user_id)
    started = time.monotonic()
    deadline = started + float(timeout_s)
    remaining: tuple[RemainingLease, ...] = ()
    while True:
        remaining = _remaining_leases(pg_user_id, session_manager=session_manager)
        if not remaining:
            return DrainResult(
                success=True,
                timed_out=False,
                remaining_leases=(),
                elapsed_s=time.monotonic() - started,
            )
        if time.monotonic() >= deadline:
            log.warning(
                "storage_lease_drain_timeout",
                extra={
                    "user_id": pg_user_id,
                    "remaining_leases": [lease.__dict__ for lease in remaining],
                },
            )
            return DrainResult(
                success=False,
                timed_out=True,
                remaining_leases=remaining,
                elapsed_s=time.monotonic() - started,
            )
        time.sleep(min(0.25, max(deadline - time.monotonic(), 0.0)))


def _remaining_leases(user_id: int, *, session_manager) -> tuple[RemainingLease, ...]:
    own_lease_id = None
    scope = _lease_context.get()
    if scope is not None and str(scope.user_id) == str(user_id):
        own_lease_id = scope.lease_id
    with session_manager.get_db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT lease_id, started_at, last_heartbeat_at, operation, holder_pid
            FROM user_access_leases
            WHERE user_id = %s
              AND (%s IS NULL OR lease_id <> %s::uuid)
            ORDER BY started_at ASC
            """,
            (user_id, own_lease_id, own_lease_id),
        )
        rows = cursor.fetchall()
    return tuple(
        RemainingLease(
            lease_id=str(_extract_value(row, "lease_id", 0)),
            started_at=_extract_value(row, "started_at", 1),
            last_heartbeat_at=_extract_value(row, "last_heartbeat_at", 2),
            operation=_extract_value(row, "operation", 3),
            holder_pid=_extract_value(row, "holder_pid", 4),
        )
        for row in rows
    )


def _coerce_pg_user_id(user_id: str | int) -> int:
    try:
        return int(user_id)
    except (TypeError, ValueError) as exc:
        raise LeaseUnavailableError(f"user_id_not_bigint:{user_id!r}") from exc


def _jsonb(value: dict[str, Any] | None):
    if value is None:
        return None
    json_adapter = _psycopg_json_adapter()
    if json_adapter is None:
        return value
    return json_adapter(value)


def _psycopg_json_adapter():
    global _json_adapter
    if _json_adapter is _JSON_ADAPTER_UNSET:
        try:  # pragma: no cover - exercised in environments with psycopg2 installed
            from psycopg2.extras import Json
        except Exception:  # pragma: no cover
            _json_adapter = None
        else:
            _json_adapter = Json
    return _json_adapter


def _extract_value(row: Any, key: str, index: int) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    try:
        return row[key]
    except Exception:
        pass
    try:
        return row[index]
    except Exception:
        return getattr(row, key, None)


def _default_session_manager():
    try:
        from finance_cli.storage_client import _dispatch as storage_dispatch

        return storage_dispatch._default_session_manager()
    except Exception:
        return None


def _commit(conn) -> None:
    commit = getattr(conn, "commit", None)
    if callable(commit):
        commit()


def _rollback(conn) -> None:
    rollback = getattr(conn, "rollback", None)
    if callable(rollback):
        rollback()


__all__ = [
    "AcquireResult",
    "CleanupResult",
    "DrainResult",
    "LeaseHeartbeat",
    "LeaseMissingError",
    "LeaseQueuedError",
    "LeaseScope",
    "LeaseUnavailableError",
    "LocalLease",
    "Queued",
    "RemainingLease",
    "RemoteLease",
    "SuspectLease",
    "_cleanup_stale_leases",
    "_lease_context",
    "acquire_or_route",
    "current_lease_scope",
    "drain_until_empty",
    "enforce_active_lease_if_required",
    "lease_enforcement_enabled",
    "optional_lease_scope",
    "release_user_lease",
    "require_active_lease",
]
