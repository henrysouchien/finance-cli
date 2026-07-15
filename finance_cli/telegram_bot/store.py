"""Persistence helpers for Telegram bot chat history and request metrics."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from finance_cli.db import _connected_main_db_path, connect, initialize_database
from finance_cli.error_capture import capture_error
from finance_cli.storage_lease import enforce_active_lease_if_required

log = logging.getLogger(__name__)

_HISTORY_RESET_SENTINEL = "[HISTORY_RESET]"
_COMPACTION_SENTINEL = "[COMPACTION]"
_TOOL_ONLY_TURN_SENTINEL = "[Tool-only turn]"


def build_tool_only_turn_message(tool_calls: list[dict[str, Any]]) -> str:
    """Build a compact assistant history marker for turns that only called tools."""

    tool_parts: list[str] = []
    for tool_call in tool_calls:
        tool_name = str(tool_call.get("tool_name") or "unknown_tool")
        status = "error" if bool(tool_call.get("is_error")) else "ok"
        tool_parts.append(f"{tool_name} ({status})")
    tools_text = ", ".join(tool_parts) if tool_parts else "none"
    return (
        f"{_TOOL_ONLY_TURN_SENTINEL}\n"
        "The assistant completed this turn by calling tools but did not emit a final text response.\n"
        f"Tools used: {tools_text}."
    )


@dataclass
class RequestMetrics:
    request_id: str
    session_id: str
    model: str
    bot_session_id: str | None = None
    start_time: float = field(default_factory=time.time)
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_tokens: int = 0
    cache_read_tokens: int = 0
    estimated_cost: float = 0.0
    tool_call_count: int = 0
    error: str | None = None
    tool_calls: list[dict[str, Any]] = field(default_factory=list)

    @property
    def latency_ms(self) -> int:
        return max(0, int((time.time() - self.start_time) * 1000))


class BotStore:
    """SQLite-backed persistence for Telegram bot state."""

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def startup(self) -> None:
        if self._conn is not None:
            return
        # Phase 5 Batch A: web/Telegram callers establish a LeaseScope before
        # BotStore startup; db.connect reuses that scope for all bot_* tables.
        enforce_active_lease_if_required(resource="telegram_bot.store")
        initialize_database(self._db_path)
        self._conn = connect(self._db_path)

    def shutdown(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None

    @property
    def db_path(self) -> Path | None:
        if self._db_path is not None:
            return Path(self._db_path).expanduser().resolve()
        if self._conn is None:
            return None
        return _connected_main_db_path(self._conn)

    def save_user_message(
        self,
        content: str,
        request_id: str,
        bot_session_id: str | None = None,
    ) -> None:
        self._save_message(
            role="user",
            content=content,
            request_id=request_id,
            bot_session_id=bot_session_id,
        )

    def save_assistant_message(
        self,
        content: str,
        request_id: str,
        bot_session_id: str | None = None,
    ) -> None:
        self._save_message(
            role="assistant",
            content=content,
            request_id=request_id,
            bot_session_id=bot_session_id,
        )

    def save_request(self, metrics: RequestMetrics) -> None:
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO bot_requests (
                        request_id,
                        session_id,
                        model,
                        bot_session_id,
                        input_tokens,
                        output_tokens,
                        cache_creation_tokens,
                        cache_read_tokens,
                        estimated_cost,
                        tool_call_count,
                        latency_ms,
                        error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        metrics.request_id,
                        metrics.session_id,
                        metrics.model,
                        metrics.bot_session_id,
                        metrics.input_tokens,
                        metrics.output_tokens,
                        metrics.cache_creation_tokens,
                        metrics.cache_read_tokens,
                        metrics.estimated_cost,
                        metrics.tool_call_count,
                        metrics.latency_ms,
                        metrics.error,
                    ),
                )
                if metrics.tool_calls:
                    conn.executemany(
                        """
                        INSERT INTO bot_tool_calls (
                            request_id,
                            tool_name,
                            server,
                            duration_ms,
                            is_error,
                            result_bytes
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                metrics.request_id,
                                tool_call["tool_name"],
                                tool_call.get("server"),
                                int(tool_call["duration_ms"]),
                                int(bool(tool_call["is_error"])),
                                int(tool_call["result_bytes"]),
                            )
                            for tool_call in metrics.tool_calls
                        ],
                    )
        except Exception as exc:
            self._capture_store_error("save_request", exc)
            log.warning("Failed to persist Telegram bot request %s: %s", metrics.request_id, exc)

    def start_session(self, session_id: str, message_time: float = 0.0) -> None:
        """Insert a new bot_sessions row."""
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                if message_time > 0:
                    conn.execute(
                        """
                        INSERT INTO bot_sessions (session_id, started_at, last_activity_at)
                        VALUES (?, datetime(?, 'unixepoch'), datetime(?, 'unixepoch'))
                        """,
                        (session_id, message_time, message_time),
                    )
                else:
                    conn.execute(
                        "INSERT INTO bot_sessions (session_id) VALUES (?)",
                        (session_id,),
                    )
        except Exception as exc:
            self._capture_store_error("start_session", exc)
            log.warning("Failed to start bot session %s: %s", session_id, exc)

    def end_session(self, session_id: str, reason: str, ended_at_time: float = 0.0) -> None:
        """Close a bot session and compute aggregate stats."""
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                conn.execute(
                    """
                    UPDATE bot_sessions
                    SET ended_at = CASE
                            WHEN ? > 0 THEN datetime(?, 'unixepoch')
                            ELSE last_activity_at
                        END,
                        end_reason = ?,
                        message_count = (
                            SELECT COUNT(*)
                            FROM bot_chat_messages
                            WHERE bot_session_id = ?
                        ),
                        request_count = (
                            SELECT COUNT(*)
                            FROM bot_requests
                            WHERE bot_session_id = ?
                        ),
                        total_cost = (
                            SELECT COALESCE(SUM(estimated_cost), 0.0)
                            FROM bot_requests
                            WHERE bot_session_id = ?
                        )
                    WHERE session_id = ?
                    """,
                    (
                        ended_at_time,
                        ended_at_time,
                        reason,
                        session_id,
                        session_id,
                        session_id,
                        session_id,
                    ),
                )
        except Exception as exc:
            self._capture_store_error("end_session", exc)
            log.warning("Failed to end bot session %s: %s", session_id, exc)

    def update_session_activity(self, session_id: str, message_time: float = 0.0) -> None:
        """Update last_activity_at for a bot session."""
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                if message_time > 0:
                    conn.execute(
                        """
                        UPDATE bot_sessions
                        SET last_activity_at = datetime(?, 'unixepoch')
                        WHERE session_id = ?
                        """,
                        (message_time, session_id),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE bot_sessions
                        SET last_activity_at = datetime('now')
                        WHERE session_id = ?
                        """,
                        (session_id,),
                    )
        except Exception as exc:
            self._capture_store_error("update_session_activity", exc)
            log.warning("Failed to update bot session activity %s: %s", session_id, exc)

    def close_all_open_sessions(self, reason: str = "restart") -> int:
        """Close all sessions with ended_at IS NULL."""
        conn = self._conn
        if conn is None:
            return 0

        try:
            rows = conn.execute(
                "SELECT session_id FROM bot_sessions WHERE ended_at IS NULL"
            ).fetchall()
        except Exception as exc:
            self._capture_store_error("close_all_open_sessions", exc)
            log.warning("Failed to list open bot sessions: %s", exc)
            return 0

        for row in rows:
            self.end_session(str(row["session_id"]), reason)
        return len(rows)

    def get_last_session(self) -> dict[str, Any] | None:
        """Return the most recently closed bot session with messages."""
        conn = self._conn
        if conn is None:
            return None

        try:
            row = conn.execute(
                """
                SELECT *
                FROM bot_sessions
                WHERE ended_at IS NOT NULL AND message_count > 0
                ORDER BY ended_at DESC
                LIMIT 1
                """
            ).fetchone()
        except Exception as exc:
            self._capture_store_error("get_last_session", exc)
            log.warning("Failed to load last bot session: %s", exc)
            return None

        return dict(row) if row is not None else None

    def load_recent_messages(self, limit: int = 40) -> list[dict[str, str]]:
        conn = self._conn
        if conn is None:
            return []

        try:
            rows = conn.execute(
                """
                WITH last_reset AS (
                    SELECT COALESCE(MAX(id), 0) AS reset_id
                    FROM bot_chat_messages
                    WHERE role = 'assistant' AND content = ?
                )
                SELECT role, content
                FROM (
                    SELECT m.id, m.role, m.content
                    FROM bot_chat_messages AS m
                    CROSS JOIN last_reset
                    WHERE m.id > last_reset.reset_id
                      AND (
                          (m.request_id IS NOT NULL
                           AND m.request_id != ?
                           AND EXISTS (
                               SELECT 1
                               FROM bot_requests AS r
                               WHERE r.request_id = m.request_id
                                 AND r.error IS NULL
                           )
                           AND EXISTS (
                               SELECT 1
                               FROM bot_chat_messages AS paired
                               WHERE paired.request_id = m.request_id
                                 AND paired.role = 'assistant'
                                 AND paired.content != ?
                                 AND paired.compacted_at IS NULL
                           )
                           AND EXISTS (
                               SELECT 1
                               FROM bot_chat_messages AS paired
                               WHERE paired.request_id = m.request_id
                                 AND paired.role = 'user'
                                 AND paired.compacted_at IS NULL
                           )
                          )
                          OR m.request_id = ?
                      )
                      AND m.compacted_at IS NULL
                    ORDER BY m.id DESC
                    LIMIT ?
                )
                ORDER BY id ASC
                """,
                (
                    _HISTORY_RESET_SENTINEL,
                    _COMPACTION_SENTINEL,
                    _HISTORY_RESET_SENTINEL,
                    _COMPACTION_SENTINEL,
                    max(0, limit),
                ),
            ).fetchall()
        except Exception as exc:
            self._capture_store_error("load_recent_messages", exc)
            log.warning("Failed to load Telegram bot chat history: %s", exc)
            return []

        return [{"role": str(row["role"]), "content": str(row["content"])} for row in rows]

    def count_recent_messages(self) -> int:
        conn = self._conn
        if conn is None:
            return 0

        try:
            row = conn.execute(
                """
                WITH last_reset AS (
                    SELECT COALESCE(MAX(id), 0) AS reset_id
                    FROM bot_chat_messages
                    WHERE role = 'assistant' AND content = ?
                )
                SELECT COUNT(*) AS message_count
                FROM bot_chat_messages AS m
                CROSS JOIN last_reset
                WHERE m.id > last_reset.reset_id
                  AND (
                      (m.request_id IS NOT NULL
                       AND m.request_id != ?
                       AND EXISTS (
                           SELECT 1
                           FROM bot_requests AS r
                           WHERE r.request_id = m.request_id
                             AND r.error IS NULL
                       )
                       AND EXISTS (
                           SELECT 1
                           FROM bot_chat_messages AS paired
                           WHERE paired.request_id = m.request_id
                             AND paired.role = 'assistant'
                             AND paired.content != ?
                             AND paired.compacted_at IS NULL
                       )
                       AND EXISTS (
                           SELECT 1
                           FROM bot_chat_messages AS paired
                           WHERE paired.request_id = m.request_id
                             AND paired.role = 'user'
                             AND paired.compacted_at IS NULL
                       )
                      )
                      OR m.request_id = ?
                  )
                  AND m.compacted_at IS NULL
                """,
                (
                    _HISTORY_RESET_SENTINEL,
                    _COMPACTION_SENTINEL,
                    _HISTORY_RESET_SENTINEL,
                    _COMPACTION_SENTINEL,
                ),
            ).fetchone()
        except Exception as exc:
            self._capture_store_error("count_recent_messages", exc)
            log.warning("Failed to count Telegram bot chat history: %s", exc)
            return 0

        return int(row["message_count"] if row is not None else 0)

    def delete_request_messages(self, request_id: str) -> None:
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                conn.execute(
                    "DELETE FROM bot_chat_messages WHERE request_id = ?",
                    (request_id,),
                )
        except Exception as exc:
            self._capture_store_error("delete_request_messages", exc)
            log.warning(
                "Failed to delete Telegram bot request messages %s: %s",
                request_id,
                exc,
            )

    def save_compaction(
        self,
        summary: str,
        keep_recent: int,
        bot_session_id: str | None = None,
    ) -> None:
        """Mark old messages as compacted and save summary pair.

        The summary pair gets auto-increment IDs after the kept recent rows,
        so restart reload order is [recent..., summary] — acceptable since
        the summary provides context regardless of position.
        Uses the same success-paired predicate as load_recent_messages() to find
        the cutoff — don't count orphaned failed-request rows.
        """
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                row = conn.execute(
                    """
                    WITH last_reset AS (
                        SELECT COALESCE(MAX(id), 0) AS reset_id
                        FROM bot_chat_messages
                        WHERE role = 'assistant' AND content = ?
                    )
                    SELECT m.id FROM bot_chat_messages AS m
                    CROSS JOIN last_reset
                    WHERE m.id > last_reset.reset_id
                      AND m.compacted_at IS NULL
                      AND m.request_id IS NOT NULL
                      AND (
                          (m.request_id != ?
                           AND EXISTS (
                               SELECT 1 FROM bot_requests AS r
                               WHERE r.request_id = m.request_id AND r.error IS NULL
                           )
                           AND EXISTS (
                               SELECT 1 FROM bot_chat_messages AS p
                               WHERE p.request_id = m.request_id AND p.role = 'assistant'
                                 AND p.content != ? AND p.compacted_at IS NULL
                           )
                           AND EXISTS (
                               SELECT 1 FROM bot_chat_messages AS p
                               WHERE p.request_id = m.request_id AND p.role = 'user'
                                 AND p.compacted_at IS NULL
                           )
                          )
                          OR m.request_id = ?
                      )
                    ORDER BY m.id DESC
                    LIMIT 1 OFFSET ?
                    """,
                    (
                        _HISTORY_RESET_SENTINEL,
                        _COMPACTION_SENTINEL,
                        _HISTORY_RESET_SENTINEL,
                        _COMPACTION_SENTINEL,
                        keep_recent,
                    ),
                ).fetchone()

                if row is not None:
                    reset_row = conn.execute(
                        "SELECT COALESCE(MAX(id), 0) AS reset_id FROM bot_chat_messages "
                        "WHERE role = 'assistant' AND content = ?",
                        (_HISTORY_RESET_SENTINEL,),
                    ).fetchone()
                    reset_id = reset_row["reset_id"] if reset_row else 0
                    conn.execute(
                        """
                        UPDATE bot_chat_messages
                        SET compacted_at = datetime('now')
                        WHERE id <= ? AND id > ? AND compacted_at IS NULL
                        """,
                        (row["id"], reset_id),
                    )

                conn.execute(
                    """
                    INSERT INTO bot_chat_messages (role, content, request_id, bot_session_id)
                    VALUES ('user', ?, ?, ?)
                    """,
                    (f"[Previous conversation summary]\n{summary}", _COMPACTION_SENTINEL, bot_session_id),
                )
                conn.execute(
                    """
                    INSERT INTO bot_chat_messages (role, content, request_id, bot_session_id)
                    VALUES ('assistant', ?, ?, ?)
                    """,
                    (
                        "Understood. I have the context from our previous conversation.",
                        _COMPACTION_SENTINEL,
                        bot_session_id,
                    ),
                )
        except Exception as exc:
            self._capture_store_error("save_compaction", exc)
            log.warning("Failed to persist compaction: %s", exc)

    def mark_history_reset(self, bot_session_id: str | None = None) -> None:
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO bot_chat_messages (role, content, request_id, bot_session_id)
                    VALUES ('assistant', ?, NULL, ?)
                    """,
                    (_HISTORY_RESET_SENTINEL, bot_session_id),
                )
        except Exception as exc:
            self._capture_store_error("mark_history_reset", exc)
            log.warning("Failed to persist Telegram bot history reset: %s", exc)

    def _save_message(
        self,
        *,
        role: str,
        content: str,
        request_id: str | None,
        bot_session_id: str | None = None,
    ) -> None:
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO bot_chat_messages (role, content, request_id, bot_session_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (role, content, request_id, bot_session_id),
                )
        except Exception as exc:
            self._capture_store_error(f"save_{role}_message", exc)
            log.warning("Failed to persist Telegram bot %s message: %s", role, exc)

    def _capture_store_error(self, method_name: str, exc: Exception) -> None:
        capture_error(
            exc,
            source="telegram",
            endpoint=f"store_{method_name}",
            db_path=self.db_path,
        )


__all__ = ["BotStore", "RequestMetrics", "build_tool_only_turn_message"]
