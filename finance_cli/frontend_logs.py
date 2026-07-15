"""Frontend log storage helpers."""

from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .db import _resolve_connection_user_id, connect
from .redaction import redact_text
from . import storage_lease as storage_lease  # noqa: F401

ALLOWED_LEVELS = {"warn", "error"}
MAX_MESSAGE_LENGTH = 2000
MAX_META_KEYS = 20
MAX_META_STRING_LENGTH = 200
MAX_METADATA_JSON_LENGTH = 4000


def _coerce_db_path(db_path: str | Path | None) -> str | None:
    if db_path is None:
        return None
    if isinstance(db_path, str) and (db_path == ":memory:" or db_path.startswith("file:")):
        return db_path
    return str(Path(db_path).expanduser().resolve())


def _truncate_string(value: Any) -> str:
    return redact_text(str(value or ""))[:MAX_META_STRING_LENGTH]


def _is_error_like_mapping(value: Mapping[str, Any]) -> bool:
    return "name" in value and "message" in value


def _safe_json_dumps(value: Any) -> str | None:
    try:
        return json.dumps(value, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        return None


def _sanitize_meta_value(value: Any) -> Any:
    if isinstance(value, BaseException):
        return {
            "name": _truncate_string(value.__class__.__name__),
            "message": _truncate_string(str(value)),
        }
    if isinstance(value, str):
        return _truncate_string(value)
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int,)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping) and _is_error_like_mapping(value):
        return {
            "name": _truncate_string(value.get("name")),
            "message": _truncate_string(value.get("message")),
        }
    if isinstance(value, (Mapping, list, tuple)):
        serialized = _safe_json_dumps(value)
        return (
            "[unserializable]"
            if serialized is None
            else redact_text(serialized)[:MAX_META_STRING_LENGTH]
        )
    serialized = _safe_json_dumps(value)
    return "[unserializable]" if serialized is None else redact_text(serialized)[:MAX_META_STRING_LENGTH]


def _sanitize_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not metadata:
        return None

    clean: dict[str, Any] = {}
    for key, value in list(metadata.items())[:MAX_META_KEYS]:
        clean[str(key)] = _sanitize_meta_value(value)

    serialized = json.dumps(clean, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(serialized) > MAX_METADATA_JSON_LENGTH:
        return {"_truncated": True, "_keys": len(clean)}
    return clean or None


def record_frontend_log(
    db_path: str | Path | None,
    level: str,
    namespace: str | None,
    message: str,
    page: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    """Append a frontend log entry to per-user SQLite."""
    if level not in ALLOWED_LEVELS:
        return

    resolved_db_path = _coerce_db_path(db_path)
    if resolved_db_path is None:
        return

    clean_message = redact_text(str(message or ""))[:MAX_MESSAGE_LENGTH]
    clean_namespace = redact_text(str(namespace)) if namespace is not None else None
    clean_page = redact_text(str(page)) if page is not None else None
    clean_metadata = _sanitize_metadata(metadata)

    try:
        if resolved_db_path.startswith("file:"):
            conn = sqlite3.connect(resolved_db_path, uri=True, timeout=5.0)
            conn.execute("PRAGMA busy_timeout = 5000")
        else:
            resolved_path = Path(resolved_db_path).expanduser().resolve()
            try:
                user_id = _resolve_connection_user_id(resolved_path)
            except ValueError:
                conn = sqlite3.connect(str(resolved_path), timeout=5.0)
                conn.execute("PRAGMA busy_timeout = 5000")
            else:
                # Phase 5 Batch A: frontend log writes are covered by the
                # request-scoped lease from dependencies.get_user_conn.
                conn = connect(
                    db_path=resolved_path,
                    busy_timeout=5000,
                    check_same_thread=True,
                    user_id=user_id,
                )
        with conn:
            conn.execute(
                """
                INSERT INTO frontend_logs (
                    level,
                    namespace,
                    message,
                    page,
                    metadata
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    level,
                    clean_namespace,
                    clean_message,
                    clean_page,
                    json.dumps(clean_metadata, sort_keys=True, separators=(",", ":"), allow_nan=False)
                    if clean_metadata
                    else None,
                ),
            )
            conn.commit()
    except Exception:
        return


def prune_frontend_logs(conn: sqlite3.Connection, retention_days: int = 30) -> None:
    """Delete frontend logs older than the configured retention window."""
    conn.execute(
        """
        DELETE FROM frontend_logs
        WHERE created_at < datetime('now', ?)
        """,
        (f"-{int(retention_days)} days",),
    )


__all__ = ["record_frontend_log", "prune_frontend_logs"]
