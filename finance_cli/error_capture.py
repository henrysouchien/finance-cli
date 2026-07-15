"""Runtime error capture for observability."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
import re
import sqlite3
import traceback as traceback_lib
import uuid
from pathlib import Path
from typing import Any, Mapping

try:
    import alerts

    _HAS_ALERTS = True
except ImportError:  # pragma: no cover - optional dependency
    alerts = None  # type: ignore[assignment]
    _HAS_ALERTS = False

from .perf import get_request_id
from .db import COMPAT_ROW_FACTORY, _resolve_connection_user_id, connect
from .redaction import redact_text as _redact
from . import storage_lease as storage_lease  # noqa: F401

log = logging.getLogger(__name__)

ALLOWED_CONTEXT_KEYS = {
    "request_id",
    "tool_name",
    "tool_input_keys",
    "route",
    "method",
    "status_code",
    "duration_ms",
    "model",
    "batch_size",
    "environment",
    "release_sha",
    "tenant_expected_user_id",
    "tenant_actual_user_id",
    "tenant_db_path",
    "tenant_reason",
}

_ALLOWED_SEVERITIES = {"critical", "error", "warning"}
_REOPENABLE_STATUSES = {"resolved", "wontfix"}
_ALERTABLE_SEVERITIES = {"critical", "error"}
_MAX_MESSAGE_LEN = 2000
_MAX_TRACEBACK_LEN = 10000
_TRACEBACK_FRAME_RE = re.compile(r'File "([^"]+)", line \d+, in ([^\s]+)')


def capture_error(
    exc: Exception,
    *,
    source: str,
    endpoint: str | None = None,
    severity: str = "error",
    context: Mapping[str, Any] | None = None,
    db_path: str | Path | None = None,
    pg_pool: Any = None,
) -> str | None:
    """Capture an exception to SQLite/PG or structured logs."""
    if getattr(exc, "_b3_captured", False):
        return None

    payload = _build_payload(
        exc,
        source=source,
        endpoint=endpoint,
        severity=severity,
        context=context,
    )

    try:
        if db_path is not None:
            error_id = _write_sqlite(db_path, payload)
        elif pg_pool is not None:
            error_id = _write_postgres(pg_pool, payload)
        else:
            _log_fallback(payload)
            error_id = None
    except Exception as db_exc:
        _log_fallback(payload, db_exc=db_exc)
        error_id = None

    try:
        setattr(exc, "_b3_captured", True)
    except Exception:
        pass
    return error_id


def _error_fingerprint(
    source: str,
    endpoint: str | None,
    error_type: str,
    traceback_str: str,
) -> str:
    """Return a stable fingerprint for a runtime error."""
    first_frame = _extract_first_app_frame(traceback_str) or "unknown"
    raw = f"{source}:{endpoint or 'unknown'}:{error_type}:{first_frame}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _extract_first_app_frame(tb: str) -> str | None:
    """Return ``module:function`` for the first finance_cli/server frame."""
    for line in str(tb or "").splitlines():
        match = _TRACEBACK_FRAME_RE.search(line)
        if match is None:
            continue
        raw_path = match.group(1).replace("\\", "/")
        function_name = match.group(2)
        rel_path: str | None = None
        if "finance_cli/" in raw_path:
            rel_path = raw_path.split("finance_cli/", 1)[1]
        elif "server/" in raw_path:
            rel_path = raw_path.split("server/", 1)[1]
        if rel_path is None or not rel_path.endswith(".py"):
            continue
        module_name = rel_path[:-3].replace("/", ".")
        return f"{module_name}:{function_name}"
    return None


def _write_sqlite(db_path: str | Path | None, payload: dict[str, Any]) -> str | None:
    resolved_db_path = _coerce_db_path(db_path)
    if resolved_db_path is None:
        _log_fallback(payload)
        return None

    conn = _sqlite_connect(resolved_db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        error_id = _write_error_rows_sqlite(conn, payload)
        conn.commit()
        return error_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _write_error_rows_sqlite(conn: sqlite3.Connection, payload: Mapping[str, Any]) -> str:
    fingerprint = str(payload["fingerprint"])
    existing = conn.execute(
        """
        SELECT id, status, occurrence_count
        FROM errors
        WHERE fingerprint = ?
        """,
        (fingerprint,),
    ).fetchone()

    error_id = str(existing["id"]) if existing is not None else str(payload["id"])
    is_new_fingerprint = existing is None
    was_reopened = bool(existing is not None and existing["status"] in _REOPENABLE_STATUSES)

    conn.execute(
        """
        INSERT INTO errors (
            id,
            fingerprint,
            severity,
            source,
            endpoint,
            error_type,
            message,
            traceback,
            context,
            user_id,
            request_id,
            environment,
            release_sha
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fingerprint) DO UPDATE SET
            occurrence_count = occurrence_count + 1,
            last_seen = datetime('now'),
            message = excluded.message,
            traceback = excluded.traceback,
            context = excluded.context,
            user_id = excluded.user_id,
            request_id = excluded.request_id,
            environment = excluded.environment,
            release_sha = excluded.release_sha,
            status = CASE
                WHEN status IN ('resolved', 'wontfix') THEN 'open'
                ELSE status
            END,
            resolved_at = CASE
                WHEN status IN ('resolved', 'wontfix') THEN NULL
                ELSE resolved_at
            END,
            resolution = CASE
                WHEN status IN ('resolved', 'wontfix') THEN NULL
                ELSE resolution
            END
        """,
        (
            error_id,
            fingerprint,
            payload["severity"],
            payload["source"],
            payload["endpoint"],
            payload["error_type"],
            payload["message"],
            payload["traceback"],
            payload["context_json"],
            payload["user_id"],
            payload["request_id"],
            payload["environment"],
            payload["release_sha"],
        ),
    )

    row = conn.execute(
        """
        SELECT *
        FROM errors
        WHERE fingerprint = ?
        """,
        (fingerprint,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to load upserted error for fingerprint={fingerprint}")

    resolved_error_id = str(row["id"])
    conn.execute(
        """
        INSERT INTO error_occurrences (
            error_id,
            request_id,
            user_id,
            context
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            resolved_error_id,
            payload["request_id"],
            payload["user_id"],
            payload["context_json"],
        ),
    )

    error_row = dict(row)
    _maybe_alert(error_row, is_new_fingerprint, was_reopened, conn)
    return resolved_error_id


def _write_postgres(pg_pool: Any, payload: dict[str, Any]) -> str | None:
    with _pg_connection(pg_pool) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, status, occurrence_count
                FROM errors
                WHERE fingerprint = %s
                FOR UPDATE
                """,
                (payload["fingerprint"],),
            )
            existing = cursor.fetchone()
            error_id = str(existing[0]) if existing is not None else str(payload["id"])
            is_new_fingerprint = existing is None
            was_reopened = bool(existing is not None and existing[1] in _REOPENABLE_STATUSES)

            cursor.execute(
                """
                INSERT INTO errors (
                    id,
                    fingerprint,
                    severity,
                    source,
                    endpoint,
                    error_type,
                    message,
                    traceback,
                    context,
                    user_id,
                    request_id,
                    environment,
                    release_sha
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                ON CONFLICT(fingerprint) DO UPDATE SET
                    occurrence_count = errors.occurrence_count + 1,
                    last_seen = now(),
                    message = EXCLUDED.message,
                    traceback = EXCLUDED.traceback,
                    context = EXCLUDED.context,
                    user_id = EXCLUDED.user_id,
                    request_id = EXCLUDED.request_id,
                    environment = EXCLUDED.environment,
                    release_sha = EXCLUDED.release_sha,
                    status = CASE
                        WHEN errors.status IN ('resolved', 'wontfix') THEN 'open'
                        ELSE errors.status
                    END,
                    resolved_at = CASE
                        WHEN errors.status IN ('resolved', 'wontfix') THEN NULL
                        ELSE errors.resolved_at
                    END,
                    resolution = CASE
                        WHEN errors.status IN ('resolved', 'wontfix') THEN NULL
                        ELSE errors.resolution
                    END
                """,
                (
                    error_id,
                    payload["fingerprint"],
                    payload["severity"],
                    payload["source"],
                    payload["endpoint"],
                    payload["error_type"],
                    payload["message"],
                    payload["traceback"],
                    payload["context_json"],
                    payload["user_id"],
                    payload["request_id"],
                    payload["environment"],
                    payload["release_sha"],
                ),
            )

            cursor.execute(
                """
                SELECT
                    id,
                    fingerprint,
                    severity,
                    source,
                    endpoint,
                    error_type,
                    message,
                    traceback,
                    context,
                    user_id,
                    request_id,
                    environment,
                    release_sha,
                    status,
                    resolved_at,
                    resolution,
                    occurrence_count,
                    first_seen,
                    last_seen
                FROM errors
                WHERE fingerprint = %s
                """,
                (payload["fingerprint"],),
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(
                    f"Failed to load upserted PG error for fingerprint={payload['fingerprint']}"
                )
            resolved_error_id = str(row[0])

            cursor.execute(
                """
                INSERT INTO error_occurrences (
                    error_id,
                    request_id,
                    user_id,
                    context
                )
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                (
                    resolved_error_id,
                    payload["request_id"],
                    payload["user_id"],
                    payload["context_json"],
                ),
            )

            error_row = {
                "id": row[0],
                "fingerprint": row[1],
                "severity": row[2],
                "source": row[3],
                "endpoint": row[4],
                "error_type": row[5],
                "message": row[6],
                "traceback": row[7],
                "context": row[8],
                "user_id": row[9],
                "request_id": row[10],
                "environment": row[11],
                "release_sha": row[12],
                "status": row[13],
                "resolved_at": row[14],
                "resolution": row[15],
                "occurrence_count": row[16],
                "first_seen": row[17],
                "last_seen": row[18],
            }
            _maybe_alert(error_row, is_new_fingerprint, was_reopened, conn)
            conn.commit()
            return resolved_error_id
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _build_payload(
    exc: Exception,
    *,
    source: str,
    endpoint: str | None,
    severity: str,
    context: Mapping[str, Any] | None,
) -> dict[str, Any]:
    raw_traceback = "".join(
        traceback_lib.format_exception(type(exc), exc, exc.__traceback__)
    )
    error_type = type(exc).__name__
    filtered_context = _filter_context(context)
    request_id = _request_id_from_context(filtered_context)
    payload = {
        "id": uuid.uuid4().hex,
        "severity": severity if severity in _ALLOWED_SEVERITIES else "error",
        "source": source,
        "endpoint": endpoint,
        "error_type": error_type,
        "message": _truncate(_redact(str(exc) or error_type), _MAX_MESSAGE_LEN) or error_type,
        "traceback": _truncate(_redact(raw_traceback), _MAX_TRACEBACK_LEN) or None,
        "context": filtered_context,
        "context_json": _json_dumps(filtered_context),
        "user_id": None,
        "request_id": request_id,
        "environment": _environment_from_context(filtered_context),
        "release_sha": _release_sha_from_context(filtered_context),
    }
    payload["fingerprint"] = _error_fingerprint(
        source,
        endpoint,
        error_type,
        raw_traceback,
    )
    return payload


def _filter_context(context: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(context, Mapping):
        return None

    filtered: dict[str, Any] = {}
    for key in ALLOWED_CONTEXT_KEYS:
        if key not in context:
            continue
        value = context[key]
        if key == "tool_input_keys":
            normalized_keys = _normalize_tool_input_keys(value)
            if normalized_keys:
                filtered[key] = normalized_keys
            continue
        if key in {"status_code", "duration_ms", "batch_size"}:
            if isinstance(value, bool) or not isinstance(value, int):
                continue
            filtered[key] = int(value)
            continue
        if not isinstance(value, str):
            continue
        cleaned = _truncate(_redact(value.strip()), 200)
        if cleaned:
            filtered[key] = cleaned

    return filtered or None


def _normalize_tool_input_keys(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        raw_values = value.keys()
    elif isinstance(value, (list, tuple, set, frozenset)):
        raw_values = value
    else:
        return []

    normalized: list[str] = []
    for item in raw_values:
        if item is None:
            continue
        text = _truncate(_redact(str(item).strip()), 100)
        if not text:
            continue
        normalized.append(text)
    return normalized


def _request_id_from_context(context: Mapping[str, Any] | None) -> str | None:
    if context and isinstance(context.get("request_id"), str):
        request_id = str(context["request_id"]).strip()
        if request_id:
            return request_id
    return get_request_id()


def _environment_from_context(context: Mapping[str, Any] | None) -> str:
    raw_value: str | None = None
    if context and isinstance(context.get("environment"), str):
        raw_value = context["environment"]
    elif os.getenv("PYTEST_CURRENT_TEST"):
        raw_value = "test"
    else:
        raw_value = (
            os.getenv("FINANCE_CLI_ENV")
            or os.getenv("APP_ENV")
            or os.getenv("ENV")
            or "production"
        )

    normalized = str(raw_value or "production").strip().lower()
    if normalized in {"prod", "production"}:
        return "production"
    if normalized in {"dev", "development"}:
        return "development"
    if normalized == "test":
        return "test"
    return "production"


def _release_sha_from_context(context: Mapping[str, Any] | None) -> str | None:
    if context and isinstance(context.get("release_sha"), str):
        release_sha = str(context["release_sha"]).strip()
        if release_sha:
            return release_sha

    for env_key in ("RELEASE_SHA", "GIT_SHA", "COMMIT_SHA"):
        value = str(os.getenv(env_key) or "").strip()
        if value:
            return value
    return None


def _maybe_alert(
    error_row: dict[str, Any],
    is_new_fingerprint: bool,
    was_reopened: bool,
    conn: Any,
) -> None:
    """Record/send alerts for new, reopened, or spiking errors."""
    fingerprint = str(error_row["fingerprint"])
    reason: str | None = None

    if is_new_fingerprint or was_reopened:
        occurrence_count = int(error_row.get("occurrence_count", 1) or 1)
        reason = "new_error" if is_new_fingerprint else "reopened"
        window_key = f"new_fp:{fingerprint}:{occurrence_count}"
        if not _try_record_alert(conn, fingerprint, reason, window_key):
            return
    elif (
        str(error_row.get("status") or "") == "open"
        and str(error_row.get("severity") or "") in _ALERTABLE_SEVERITIES
    ):
        recent_count = _count_recent_occurrences(conn, str(error_row["id"]), hours=1)
        if recent_count <= 10:
            return
        hour_bucket = datetime.now(timezone.utc).strftime("%Y%m%d%H")
        reason = f"rate_spike ({recent_count}/hr)"
        window_key = f"rate:{fingerprint}:{hour_bucket}"
        if not _try_record_alert(conn, fingerprint, reason, window_key):
            return
    else:
        return

    _send_alert(error_row, reason or "new_error", conn)


def _try_record_alert(conn: Any, fingerprint: str, reason: str, window_key: str) -> bool:
    """Insert an alert row if the unique window has not already fired."""
    if isinstance(conn, sqlite3.Connection):
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO error_alerts (fingerprint, alert_reason, window_key)
            VALUES (?, ?, ?)
            """,
            (fingerprint, reason, window_key),
        )
        return cursor.rowcount > 0

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO error_alerts (fingerprint, alert_reason, window_key)
            VALUES (%s, %s, %s)
            ON CONFLICT(window_key) DO NOTHING
            """,
            (fingerprint, reason, window_key),
        )
        return cursor.rowcount > 0
    finally:
        cursor.close()


def prune_errors(conn: Any) -> None:
    """Delete aged error rows and child records."""
    cutoff_30 = _cutoff_value(conn, days=30)
    cutoff_90 = _cutoff_value(conn, days=90)

    if isinstance(conn, sqlite3.Connection):
        conn.execute(
            "DELETE FROM error_occurrences WHERE created_at < ?",
            (cutoff_30,),
        )
        conn.execute(
            "DELETE FROM error_alerts WHERE created_at < ?",
            (cutoff_30,),
        )
        conn.execute(
            """
            DELETE FROM errors
            WHERE status IN ('resolved', 'wontfix')
              AND last_seen < ?
            """,
            (cutoff_90,),
        )
        return

    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM error_occurrences WHERE created_at < %s", (cutoff_30,))
        cursor.execute("DELETE FROM error_alerts WHERE created_at < %s", (cutoff_30,))
        cursor.execute(
            """
            DELETE FROM errors
            WHERE status IN ('resolved', 'wontfix')
              AND last_seen < %s
            """,
            (cutoff_90,),
        )
    finally:
        cursor.close()


def _count_recent_occurrences(conn: Any, error_id: str, *, hours: int) -> int:
    cutoff = _cutoff_value(conn, hours=hours)
    if isinstance(conn, sqlite3.Connection):
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM error_occurrences
            WHERE error_id = ?
              AND created_at >= ?
            """,
            (error_id, cutoff),
        ).fetchone()
        return int(row[0] if row is not None else 0)

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM error_occurrences
            WHERE error_id = %s
              AND created_at >= %s
            """,
            (error_id, cutoff),
        )
        row = cursor.fetchone()
        return int(row[0] if row is not None else 0)
    finally:
        cursor.close()


def _cutoff_value(conn: Any, *, days: int = 0, hours: int = 0) -> Any:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days, hours=hours)
    if isinstance(conn, sqlite3.Connection):
        return cutoff.strftime("%Y-%m-%d %H:%M:%S")
    return cutoff


def _send_alert(error_row: Mapping[str, Any], reason: str, conn: Any = None) -> None:
    if not _HAS_ALERTS:
        return

    severity = str(error_row.get("severity") or "error")
    if severity not in _ALERTABLE_SEVERITIES:
        return

    endpoint = str(error_row.get("endpoint") or "unknown")
    source = str(error_row.get("source") or "unknown")
    error_type = str(error_row.get("error_type") or "Error")
    message = _truncate(_redact(str(error_row.get("message") or "")), 200)
    if severity == "critical":
        body = f"CRITICAL: {error_type} in {source}/{endpoint}\n{message}"
    else:
        body = f"ERROR: {error_type} in {endpoint} [{reason}]"

    creds: dict[str, str] = {}
    if isinstance(conn, sqlite3.Connection):
        from .notification_utils import resolve_notification_creds

        creds = resolve_notification_creds(conn, "telegram")

    try:
        alerts.send(body, channel="telegram", **creds)
    except Exception as exc:  # pragma: no cover - optional dependency path
        log.warning(
            "error alert notification failed",
            extra={"source": source, "endpoint": endpoint, "error": _redact(str(exc))[:200]},
        )


def _log_fallback(payload: Mapping[str, Any], db_exc: Exception | None = None) -> None:
    extra: dict[str, Any] = {
        "fingerprint": payload.get("fingerprint"),
        "original_error_type": payload.get("error_type"),
        "original_message": payload.get("message"),
        "source": payload.get("source"),
        "endpoint": payload.get("endpoint"),
        "severity": payload.get("severity"),
        "request_id": payload.get("request_id"),
    }
    if payload.get("context") is not None:
        extra["context"] = payload.get("context")
    if db_exc is not None:
        extra["db_error"] = _truncate(_redact(str(db_exc)), 200)
    log.error("error_capture_fallback", extra=extra)


def _truncate(text: str, limit: int) -> str:
    return str(text or "")[: int(limit)]


def _json_dumps(payload: Mapping[str, Any] | None) -> str | None:
    if not payload:
        return None
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _coerce_db_path(db_path: str | Path | None) -> str | None:
    if db_path is None:
        return None
    if isinstance(db_path, str) and (db_path == ":memory:" or db_path.startswith("file:")):
        return db_path
    return str(Path(db_path).expanduser().resolve())


def _sqlite_connect(db_path: str) -> sqlite3.Connection:
    connect_kwargs: dict[str, Any] = {"uri": db_path.startswith("file:")}
    if db_path.startswith("file:"):
        conn = sqlite3.connect(db_path, **connect_kwargs)
    else:
        resolved = Path(db_path).expanduser().resolve()
        try:
            user_id = _resolve_connection_user_id(resolved)
        except ValueError:
            conn = sqlite3.connect(str(resolved))
        else:
            # Phase 5 Batch A: web error capture runs inside the request-scoped
            # lease established by dependencies.get_user_conn.
            conn = connect(
                db_path=resolved,
                check_same_thread=True,
                user_id=user_id,
            )
    conn.row_factory = COMPAT_ROW_FACTORY
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@contextmanager
def _pg_connection(pg_pool: Any):
    if hasattr(pg_pool, "connection"):
        with pg_pool.connection() as conn:
            yield conn
        return
    if hasattr(pg_pool, "connect"):
        with pg_pool.connect() as conn:
            yield conn
        return
    if hasattr(pg_pool, "getconn") and hasattr(pg_pool, "putconn"):
        conn = pg_pool.getconn()
        try:
            yield conn
        finally:
            pg_pool.putconn(conn)
        return
    if hasattr(pg_pool, "cursor"):
        yield pg_pool
        return
    raise TypeError(f"Unsupported pg_pool type: {type(pg_pool)!r}")


__all__ = [
    "ALLOWED_CONTEXT_KEYS",
    "capture_error",
    "prune_errors",
    "_error_fingerprint",
    "_extract_first_app_frame",
    "_maybe_alert",
    "_redact",
    "_try_record_alert",
]
