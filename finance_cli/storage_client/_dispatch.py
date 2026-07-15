"""Shared storage-mode dispatch helpers for Phase 4 client routing."""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

STORAGE_MODE_CACHE_TTL_SECONDS = 30.0
_VALID_STORAGE_MODES = frozenset({"local", "remote", "migrating", "replaying"})

_cache_lock = threading.RLock()
_storage_mode_cache: dict[str, tuple[str, float]] = {}
_warned_lookup_unavailable = False


class StorageModeLookupUnavailable(RuntimeError):
    """Raised internally when PostgreSQL storage-mode lookup is not wired."""


def _storage_errors():
    from . import errors as storage_errors

    return storage_errors


def __getattr__(name: str) -> Any:
    if name == "storage_errors":
        value = _storage_errors()
        globals()[name] = value
        return value
    raise AttributeError(name)


def storage_server_target() -> str:
    return str(os.getenv("STORAGE_SERVER_URL") or "").strip()


def storage_client_enabled() -> bool:
    return str(os.getenv("FINANCE_CLI_STORAGE_CLIENT_ENABLED") or "").strip().lower() == "true"


def storage_mode_for_user(
    user_id: str | int,
    *,
    session_manager: Any | None = None,
    now: float | None = None,
) -> str:
    """Return the user's storage_mode, falling back to local when PG is unavailable."""

    normalized_user_id = str(user_id).strip()
    if not normalized_user_id:
        return "local"

    now_value = time.monotonic() if now is None else float(now)
    with _cache_lock:
        cached = _storage_mode_cache.get(normalized_user_id)
        if cached is not None:
            mode, expires_at = cached
            if expires_at > now_value:
                return mode

    try:
        mode = _lookup_storage_mode(normalized_user_id, session_manager=session_manager)
    except StorageModeLookupUnavailable as exc:
        _storage_errors().record_storage_client_error(
            "storage_dispatch",
            "LOOKUP_UNAVAILABLE",
            reason=str(exc),
        )
        _warn_lookup_unavailable_once(exc)
        return "local"

    with _cache_lock:
        _storage_mode_cache[normalized_user_id] = (
            mode,
            now_value + STORAGE_MODE_CACHE_TTL_SECONDS,
        )
    return mode


def remote_file_target_for_user(
    user_id: str | int,
    *,
    session_manager: Any | None = None,
) -> str | None:
    """Return STORAGE_SERVER_URL when file writes should dispatch remotely."""

    target = storage_server_target()
    if not target or not storage_client_enabled():
        return None
    if storage_mode_for_user(user_id, session_manager=session_manager) != "remote":
        return None
    return target


def invalidate_storage_mode(user_id: str | int) -> None:
    with _cache_lock:
        _storage_mode_cache.pop(str(user_id).strip(), None)


def clear_storage_mode_cache() -> None:
    global _warned_lookup_unavailable
    with _cache_lock:
        _storage_mode_cache.clear()
        _warned_lookup_unavailable = False


def user_id_from_data_dir(data_dir: Path | str | None) -> str | None:
    if data_dir is None:
        return None
    try:
        from finance_cli.user_provisioning import user_id_from_db_path

        return user_id_from_db_path(Path(data_dir).expanduser().resolve() / "finance.db")
    except Exception:
        return None


def user_id_from_user_file_path(path: Path | str | None) -> str | None:
    if path is None:
        return None
    resolved = Path(path).expanduser().resolve()
    if resolved.name == "finance.db":
        user_dir = resolved.parent
    elif resolved.parent.name in {"uploads", "sessions", "mcp_cache", "backups"}:
        user_dir = resolved.parent.parent
    else:
        user_dir = resolved.parent
    return user_id_from_data_dir(user_dir)


def _lookup_storage_mode(user_id: str, *, session_manager: Any | None) -> str:
    manager = session_manager if session_manager is not None else _default_session_manager()
    if manager is None or not hasattr(manager, "get_db_session"):
        raise StorageModeLookupUnavailable("session_manager_unavailable")

    try:
        with manager.get_db_session() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT storage_mode FROM users WHERE id::text = %s",
                (user_id,),
            )
            row = cursor.fetchone()
    except Exception as exc:
        raise StorageModeLookupUnavailable(exc.__class__.__name__) from exc

    mode = _extract_storage_mode(row)
    if mode not in _VALID_STORAGE_MODES:
        return "local"
    return mode


def _default_session_manager() -> Any | None:
    try:
        from app_platform.db.session import SessionManager
    except Exception:
        return None
    try:
        return SessionManager._get_default_manager()
    except Exception:
        return None


def _extract_storage_mode(row: Any) -> str:
    if row is None:
        return "local"
    if isinstance(row, dict):
        return str(row.get("storage_mode") or "local").strip().lower()
    try:
        return str(row["storage_mode"] or "local").strip().lower()
    except Exception:
        pass
    try:
        return str(row[0] or "local").strip().lower()
    except Exception:
        return "local"


def _warn_lookup_unavailable_once(exc: Exception) -> None:
    global _warned_lookup_unavailable
    with _cache_lock:
        if _warned_lookup_unavailable:
            return
        _warned_lookup_unavailable = True
    log.warning("storage_mode_lookup_unavailable fallback=local reason=%s", exc)
