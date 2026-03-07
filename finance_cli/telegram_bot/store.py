"""Persistence helpers for Telegram bot chat history and request metrics."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from finance_cli.db import connect, initialize_database

log = logging.getLogger(__name__)

_HISTORY_RESET_SENTINEL = "[HISTORY_RESET]"


@dataclass
class RequestMetrics:
    request_id: str
    session_id: str
    model: str
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
        initialize_database(self._db_path)
        self._conn = connect(self._db_path)

    def shutdown(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None

    def save_user_message(self, content: str, request_id: str) -> None:
        self._save_message(role="user", content=content, request_id=request_id)

    def save_assistant_message(self, content: str, request_id: str) -> None:
        self._save_message(role="assistant", content=content, request_id=request_id)

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
                        input_tokens,
                        output_tokens,
                        cache_creation_tokens,
                        cache_read_tokens,
                        estimated_cost,
                        tool_call_count,
                        latency_ms,
                        error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        metrics.request_id,
                        metrics.session_id,
                        metrics.model,
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
            log.warning("Failed to persist Telegram bot request %s: %s", metrics.request_id, exc)

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
                      AND m.request_id IS NOT NULL
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
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM bot_chat_messages AS paired
                          WHERE paired.request_id = m.request_id
                            AND paired.role = 'user'
                      )
                    ORDER BY m.id DESC
                    LIMIT ?
                )
                ORDER BY id ASC
                """,
                (_HISTORY_RESET_SENTINEL, _HISTORY_RESET_SENTINEL, max(0, limit)),
            ).fetchall()
        except Exception as exc:
            log.warning("Failed to load Telegram bot chat history: %s", exc)
            return []

        return [{"role": str(row["role"]), "content": str(row["content"])} for row in rows]

    def mark_history_reset(self) -> None:
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO bot_chat_messages (role, content, request_id)
                    VALUES ('assistant', ?, NULL)
                    """,
                    (_HISTORY_RESET_SENTINEL,),
                )
        except Exception as exc:
            log.warning("Failed to persist Telegram bot history reset: %s", exc)

    def _save_message(self, *, role: str, content: str, request_id: str | None) -> None:
        conn = self._conn
        if conn is None:
            return

        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO bot_chat_messages (role, content, request_id)
                    VALUES (?, ?, ?)
                    """,
                    (role, content, request_id),
                )
        except Exception as exc:
            log.warning("Failed to persist Telegram bot %s message: %s", role, exc)


__all__ = ["BotStore", "RequestMetrics"]
