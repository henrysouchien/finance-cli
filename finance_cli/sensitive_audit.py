"""Helpers for append-only security-sensitive audit events."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

SENSITIVE_ACTOR_TYPES = frozenset({"user", "system", "admin", "agent"})
SENSITIVE_SURFACES = frozenset({"web", "mcp", "cli", "sync", "telegram", "cron"})
SENSITIVE_OUTCOMES = frozenset({"started", "succeeded", "failed", "denied"})

_SECRET_KEY_FRAGMENTS = (
    "api_key",
    "apikey",
    "authorization",
    "bot_token",
    "cookie",
    "credential",
    "password",
    "secret",
    "session",
    "token",
)
_PATH_KEY_FRAGMENTS = ("path", "file", "dir")


def audit_hash(value: object | None) -> str | None:
    """Return a stable SHA-256 hash for an audit identifier."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sanitize_audit_details(value: Any, *, key: str | None = None) -> Any:
    """Redact known secret/path fields before storing details JSON."""
    if value is None or isinstance(value, (bool, int, float)):
        return value

    key_lower = str(key or "").lower()
    if any(fragment in key_lower for fragment in _SECRET_KEY_FRAGMENTS):
        return "[redacted]"

    if isinstance(value, Path):
        return {"sha256": audit_hash(str(value))}

    if isinstance(value, str):
        if any(fragment in key_lower for fragment in _PATH_KEY_FRAGMENTS):
            return {"sha256": audit_hash(value)}
        return value

    if isinstance(value, dict):
        return {str(item_key): sanitize_audit_details(item_value, key=str(item_key)) for item_key, item_value in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [sanitize_audit_details(item, key=key) for item in value]

    return str(value)


def details_json(details: dict[str, Any] | None) -> str:
    sanitized = sanitize_audit_details(details or {})
    return json.dumps(sanitized, sort_keys=True, separators=(",", ":"), default=str)


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _normalize_choice(value: str | None, allowed: frozenset[str], default: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in allowed else default


def _row_hash_payload(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)


def compute_row_hash(row: dict[str, Any]) -> str:
    return hashlib.sha256(_row_hash_payload(row).encode("utf-8")).hexdigest()


def build_audit_row(
    *,
    user_id: object | None,
    actor_type: str = "user",
    actor_id: object | None = None,
    event_type: str,
    target_type: str | None = None,
    target_id: object | None = None,
    surface: str = "mcp",
    outcome: str = "succeeded",
    request_id: str | None = None,
    session_id: str | None = None,
    ip: str | None = None,
    user_agent: str | None = None,
    details: dict[str, Any] | None = None,
    prev_hash: str | None = None,
    ts: str | None = None,
) -> dict[str, Any]:
    event = str(event_type or "").strip()
    if not event:
        raise ValueError("event_type is required")

    details_text = details_json(details)
    row = {
        "ts": ts or _utc_now_text(),
        "user_id": None if user_id is None else str(user_id),
        "actor_type": _normalize_choice(actor_type, SENSITIVE_ACTOR_TYPES, "user"),
        "actor_id_hash": audit_hash(actor_id),
        "event_type": event,
        "target_type": None if target_type is None else str(target_type),
        "target_id_hash": audit_hash(target_id),
        "surface": _normalize_choice(surface, SENSITIVE_SURFACES, "mcp"),
        "outcome": _normalize_choice(outcome, SENSITIVE_OUTCOMES, "succeeded"),
        "request_id": None if request_id is None else str(request_id),
        "session_id_hash": audit_hash(session_id),
        "ip_hash": audit_hash(ip),
        "user_agent_hash": audit_hash(user_agent),
        "details": details_text,
        "prev_hash": prev_hash,
    }
    row["row_hash"] = compute_row_hash(row)
    return row


def previous_sqlite_row_hash(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT row_hash FROM sensitive_audit_events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    return str(row["row_hash"] if isinstance(row, sqlite3.Row) else row[0])


def record_sqlite_sensitive_audit_event(
    conn: sqlite3.Connection,
    *,
    user_id: object | None,
    actor_type: str = "user",
    actor_id: object | None = None,
    event_type: str,
    target_type: str | None = None,
    target_id: object | None = None,
    surface: str = "mcp",
    outcome: str = "succeeded",
    request_id: str | None = None,
    session_id: str | None = None,
    ip: str | None = None,
    user_agent: str | None = None,
    details: dict[str, Any] | None = None,
    raise_errors: bool = False,
) -> str | None:
    """Insert a SQLite audit event and return its row hash.

    Audit writes are best-effort by default so they never break the sensitive
    operation they are observing.
    """
    try:
        row = build_audit_row(
            user_id=user_id,
            actor_type=actor_type,
            actor_id=actor_id,
            event_type=event_type,
            target_type=target_type,
            target_id=target_id,
            surface=surface,
            outcome=outcome,
            request_id=request_id,
            session_id=session_id,
            ip=ip,
            user_agent=user_agent,
            details=details,
            prev_hash=previous_sqlite_row_hash(conn),
        )
        conn.execute(
            """
            INSERT INTO sensitive_audit_events (
                ts,
                user_id,
                actor_type,
                actor_id_hash,
                event_type,
                target_type,
                target_id_hash,
                surface,
                outcome,
                request_id,
                session_id_hash,
                ip_hash,
                user_agent_hash,
                details,
                prev_hash,
                row_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, json(?), ?, ?)
            """,
            (
                row["ts"],
                row["user_id"],
                row["actor_type"],
                row["actor_id_hash"],
                row["event_type"],
                row["target_type"],
                row["target_id_hash"],
                row["surface"],
                row["outcome"],
                row["request_id"],
                row["session_id_hash"],
                row["ip_hash"],
                row["user_agent_hash"],
                row["details"],
                row["prev_hash"],
                row["row_hash"],
            ),
        )
        return str(row["row_hash"])
    except Exception:
        if raise_errors:
            raise
        log.warning("sensitive_audit_sqlite_write_failed event_type=%s", event_type, exc_info=True)
        return None
