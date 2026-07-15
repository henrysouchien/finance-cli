"""Shared helpers for internal tool-invocation operation logging."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import time
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def current_changelog_id(conn: Any) -> int:
    row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM _sync_changelog").fetchone()
    return int((row[0] if row else 0) or 0)


def operation_json(value: dict[str, Any] | None) -> str:
    return json.dumps(value or {}, sort_keys=True, separators=(",", ":"), default=str)


def tool_request_metadata(
    *,
    arguments: dict[str, Any],
    mutating: bool,
    upload_size_bytes: int | None = None,
) -> dict[str, Any]:
    visible_keys = sorted(
        str(key)
        for key in arguments
        if not str(key).startswith("__") and str(key) != "bundle_path"
    )
    metadata: dict[str, Any] = {
        "argument_count": len(visible_keys),
        "argument_keys": visible_keys,
        "mutating": bool(mutating),
        "upload": upload_size_bytes is not None,
    }
    if upload_size_bytes is not None:
        metadata["upload_size_bytes"] = upload_size_bytes
    return metadata


def operation_result_metadata(envelope: Any) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        return {"result_type": type(envelope).__name__}
    data = envelope.get("data")
    summary = envelope.get("summary")
    return {
        "data_keys": sorted(str(key) for key in data.keys()) if isinstance(data, dict) else [],
        "has_errors": operation_error_metadata(envelope) is not None,
        "result_keys": sorted(str(key) for key in envelope.keys()),
        "summary_keys": sorted(str(key) for key in summary.keys()) if isinstance(summary, dict) else [],
    }


def operation_error_metadata(envelope: Any) -> dict[str, Any] | None:
    if not isinstance(envelope, dict):
        return None

    messages: list[str] = []
    summary = envelope.get("summary")
    if isinstance(summary, dict):
        summary_errors = summary.get("errors")
        if isinstance(summary_errors, list):
            messages.extend(str(item) for item in summary_errors[:5])
        elif summary_errors:
            messages.append(str(summary_errors))
        if summary.get("error"):
            messages.append(str(summary.get("error")))

    data = envelope.get("data")
    if isinstance(data, dict):
        for key in ("error", "sync_auth_error", "sync_conflict"):
            if data.get(key):
                messages.append(str(data.get(key)))

    if not messages:
        return None
    return {"messages": [message[:500] for message in messages[:5]], "source": "envelope"}


def exception_error_metadata(exc: BaseException) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "exception_type": exc.__class__.__name__,
        "message": str(getattr(exc, "detail", exc))[:500],
        "source": "exception",
    }
    if exc.__cause__ is not None:
        payload["cause_type"] = exc.__cause__.__class__.__name__
        payload["cause_message"] = str(exc.__cause__)[:500]
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        payload["status_code"] = int(status_code)
    return payload


def record_operation_log(
    conn: Any,
    *,
    op_type: str,
    surface: str,
    tool_name: str,
    status: str,
    started_at: str,
    started_monotonic: float,
    start_changelog_id: int,
    end_changelog_id: int,
    request_metadata: dict[str, Any],
    result_metadata: dict[str, Any] | None = None,
    error_metadata: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> None:
    finished_at = utc_now_iso()
    duration_ms = max(0, int(round((time.monotonic() - started_monotonic) * 1000)))
    conn.execute(
        """
        INSERT INTO _operation_log (
            op_type,
            surface,
            tool_name,
            status,
            started_at,
            finished_at,
            duration_ms,
            start_changelog_id,
            end_changelog_id,
            request_json,
            result_json,
            error_json,
            idempotency_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            op_type,
            surface,
            tool_name,
            status,
            started_at,
            finished_at,
            duration_ms,
            int(start_changelog_id),
            int(end_changelog_id),
            operation_json(request_metadata),
            operation_json(result_metadata),
            operation_json(error_metadata) if error_metadata else None,
            idempotency_key,
        ),
    )
