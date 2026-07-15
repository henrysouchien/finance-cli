"""Process-local warm session pool for storage server connections."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Callable

import grpc

from ._generated import storage_server_pb2 as pb2

_DEFAULT_IDLE_TIMEOUT_SECONDS = 240.0
_DEFAULT_MAX_LIFETIME_SECONDS = 1500.0
_DEFAULT_MAX_PER_KEY = 4
_MAX_PER_KEY_ENV = "FINANCE_CLI_STORAGE_SESSION_POOL_MAX_PER_KEY"

_PoolKey = tuple[str, str, str, str | None]


@dataclass(frozen=True)
class _PooledSession:
    session_id: str
    last_used_monotonic: float
    opened_at_monotonic: float


class StorageSessionPool:
    """Process-global pool of warm gRPC sessions keyed by user and auth kid."""

    def __init__(
        self,
        *,
        idle_timeout_seconds: float = _DEFAULT_IDLE_TIMEOUT_SECONDS,
        max_lifetime_seconds: float = _DEFAULT_MAX_LIFETIME_SECONDS,
        max_per_key: int | None = None,
    ) -> None:
        self._sessions: dict[_PoolKey, list[_PooledSession]] = {}
        self._checked_out_opened_at: dict[tuple[_PoolKey, str], float] = {}
        self._lock = threading.Lock()
        self._idle_timeout = float(idle_timeout_seconds)
        self._max_lifetime = float(max_lifetime_seconds)
        self._max_per_key = _resolve_max_per_key(max_per_key)
        self._closed = False

    def checkout(
        self,
        target: str,
        product: str,
        user_id: str,
        auth_kid: str | None,
    ) -> str | None:
        """Return and remove an available session for the key, or None."""

        session_id, _reason, _cached_age_us = self.checkout_with_metadata(
            target,
            product,
            user_id,
            auth_kid,
        )
        return session_id

    def checkout_with_metadata(
        self,
        target: str,
        product: str,
        user_id: str,
        auth_kid: str | None,
    ) -> tuple[str | None, str, int | None]:
        """Checkout variant used by telemetry callers.

        Returns ``(session_id, reason, cached_age_us)`` where ``reason`` is
        ``hit``, ``cold``, ``idle_expired``, or ``max_lifetime``.
        """

        key = _key(target, product, user_id, auth_kid)
        now = time.monotonic()
        with self._lock:
            sessions = self._sessions.get(key)
            if not sessions:
                return None, "cold", None

            usable: list[_PooledSession] = []
            expired_reasons: list[str] = []
            for session in sessions:
                reason = self._expiry_reason(session, now)
                if reason is None:
                    usable.append(session)
                else:
                    expired_reasons.append(reason)

            if not usable:
                self._sessions.pop(key, None)
                reason = (
                    "max_lifetime"
                    if "max_lifetime" in expired_reasons
                    else "idle_expired"
                )
                return None, reason, None

            chosen = usable.pop()
            if usable:
                self._sessions[key] = usable
            else:
                self._sessions.pop(key, None)
            self._checked_out_opened_at[(key, chosen.session_id)] = chosen.opened_at_monotonic
            cached_age_us = max(int((now - chosen.opened_at_monotonic) * 1_000_000), 0)
            return chosen.session_id, "hit", cached_age_us

    def checkin(
        self,
        target: str,
        product: str,
        user_id: str,
        session_id: str,
        auth_kid: str | None,
    ) -> bool:
        """Return a session to the pool.

        Returns False when the key already has ``max_per_key`` available
        sessions; the caller must hard-close that session.
        """

        key = _key(target, product, user_id, auth_kid)
        normalized_session_id = str(session_id)
        now = time.monotonic()
        with self._lock:
            if self._closed:
                return False
            opened_at = self._checked_out_opened_at.pop((key, normalized_session_id), now)
            sessions = [
                session
                for session in self._sessions.get(key, [])
                if session.session_id != normalized_session_id
                and self._expiry_reason(session, now) is None
            ]
            if len(sessions) >= self._max_per_key:
                self._sessions[key] = sessions
                return False
            sessions.append(
                _PooledSession(
                    session_id=normalized_session_id,
                    last_used_monotonic=now,
                    opened_at_monotonic=opened_at,
                )
            )
            self._sessions[key] = sessions
            return True

    def evict(
        self,
        target: str,
        product: str,
        user_id: str,
        auth_kid: str | None,
        session_id: str | None = None,
    ) -> None:
        """Remove a specific pooled session, or every session for the key."""

        key = _key(target, product, user_id, auth_kid)
        normalized_session_id = str(session_id) if session_id is not None else None
        with self._lock:
            if normalized_session_id is None:
                self._sessions.pop(key, None)
                for checked_key in list(self._checked_out_opened_at):
                    if checked_key[0] == key:
                        self._checked_out_opened_at.pop(checked_key, None)
                return
            sessions = self._sessions.get(key)
            if sessions is not None:
                kept = [
                    session
                    for session in sessions
                    if session.session_id != normalized_session_id
                ]
                if kept:
                    self._sessions[key] = kept
                else:
                    self._sessions.pop(key, None)
            self._checked_out_opened_at.pop((key, normalized_session_id), None)

    def evict_user(self, target: str, product: str, user_id: str) -> int:
        """Drop all local entries for a user across auth kids without RPCs."""

        normalized_target = str(target)
        normalized_product = str(product)
        normalized_user_id = str(user_id)
        count = 0
        with self._lock:
            for key in list(self._sessions):
                if key[:3] != (normalized_target, normalized_product, normalized_user_id):
                    continue
                count += len(self._sessions.pop(key, []))
            for checked_key in list(self._checked_out_opened_at):
                key, _session_id = checked_key
                if key[:3] == (normalized_target, normalized_product, normalized_user_id):
                    self._checked_out_opened_at.pop(checked_key, None)
        return count

    def close_all(
        self,
        stub_factory: Callable[[str], object],
        metadata_factory: Callable[[str, str, str | None], tuple[tuple[str, str], ...]],
    ) -> int:
        """Drain the pool and best-effort CloseSession every pooled session."""

        with self._lock:
            self._closed = True
            drained = [
                (key, session)
                for key, sessions in self._sessions.items()
                for session in sessions
            ]
            self._sessions.clear()

        stubs: dict[str, object] = {}
        for key, session in drained:
            target, product, user_id, auth_kid = key
            try:
                stub = stubs.get(target)
                if stub is None:
                    stub = stub_factory(target)
                    stubs[target] = stub
                metadata = metadata_factory(product, user_id, auth_kid)
                stub.CloseSession(  # type: ignore[attr-defined]
                    pb2.CloseSessionRequest(session_id=session.session_id),
                    metadata=metadata,
                )
            except grpc.RpcError:
                continue
            except Exception:
                continue
        return len(drained)

    def size(self) -> int:
        """Return the total available pooled session count."""

        with self._lock:
            return sum(len(sessions) for sessions in self._sessions.values())

    def _expiry_reason(self, session: _PooledSession, now: float) -> str | None:
        if now - session.opened_at_monotonic > self._max_lifetime:
            return "max_lifetime"
        if now - session.last_used_monotonic > self._idle_timeout:
            return "idle_expired"
        return None


_default_pool: StorageSessionPool | None = None
_default_pool_lock = threading.Lock()


def get_default_pool() -> StorageSessionPool:
    global _default_pool
    with _default_pool_lock:
        if _default_pool is None or _default_pool._closed:
            _default_pool = StorageSessionPool()
        return _default_pool


def _key(target: str, product: str, user_id: str, auth_kid: str | None) -> _PoolKey:
    return (str(target), str(product), str(user_id), None if auth_kid is None else str(auth_kid))


def _resolve_max_per_key(max_per_key: int | None) -> int:
    if max_per_key is not None:
        return max(int(max_per_key), 0)
    raw = str(os.getenv(_MAX_PER_KEY_ENV, str(_DEFAULT_MAX_PER_KEY))).strip()
    try:
        return max(int(raw), 0)
    except ValueError:
        return _DEFAULT_MAX_PER_KEY
