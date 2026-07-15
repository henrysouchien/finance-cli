"""Performance helpers and correlation context for observability."""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Iterable

from .config import get_db_path
from .db import _resolve_connection_user_id, connect

log = logging.getLogger(__name__)

_request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_request_id_var",
    default=None,
)
_session_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_session_id_var",
    default=None,
)
_conversation_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_conversation_id_var",
    default=None,
)
_query_sample_db_path_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_query_sample_db_path_var",
    default=None,
)

_STRING_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")
_NUMERIC_LITERAL_RE = re.compile(
    r"(?<![\w.])[-+]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][-+]?\d+)?\b"
)
_WHITESPACE_RE = re.compile(r"\s+")
_MAX_SQL_FINGERPRINT_LEN = 200


def get_request_id() -> str | None:
    """Return the current request identifier."""
    return _request_id_var.get()


def set_request_id(value: str | None) -> contextvars.Token[str | None]:
    """Set the current request identifier."""
    return _request_id_var.set(value)


def get_session_id() -> str | None:
    """Return the current session identifier."""
    return _session_id_var.get()


def set_session_id(value: str | None) -> contextvars.Token[str | None]:
    """Set the current session identifier."""
    return _session_id_var.set(value)


def get_conversation_id() -> str | None:
    """Return the current conversation identifier."""
    return _conversation_id_var.get()


def set_conversation_id(value: str | None) -> contextvars.Token[str | None]:
    """Set the current conversation identifier."""
    return _conversation_id_var.set(value)


def _normalize_sql(sql: str) -> str:
    """Return a cardinality-safe SQL fingerprint."""
    normalized = _STRING_LITERAL_RE.sub("?", str(sql))
    normalized = _NUMERIC_LITERAL_RE.sub("?", normalized)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized[:_MAX_SQL_FINGERPRINT_LEN]


def _json_tags(tags: dict[str, Any] | None) -> str | None:
    if tags is None:
        return None
    return json.dumps(tags, sort_keys=True, separators=(",", ":"))


def _coerce_db_path(db_path: str | Path | None) -> str | None:
    if db_path is None:
        return None
    if isinstance(db_path, str) and (db_path == ":memory:" or db_path.startswith("file:")):
        return db_path
    return str(Path(db_path).expanduser().resolve())


def _open_perf_connection(db_path: str) -> sqlite3.Connection:
    if db_path == ":memory:":
        return sqlite3.connect(db_path)
    if db_path.startswith("file:"):
        return sqlite3.connect(db_path, uri=True)

    resolved = Path(db_path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(resolved)
    try:
        user_id = _resolve_connection_user_id(resolved)
    except ValueError:
        return sqlite3.connect(str(resolved))
    return connect(
        db_path=resolved,
        check_same_thread=True,
        user_id=user_id,
    )


def _record_query_sample(fingerprint: str, elapsed_ms: int) -> None:
    """Record a slow query sample using an isolated connection."""
    db_path = _query_sample_db_path_var.get() or _coerce_db_path(get_db_path())
    if db_path is None:
        return
    try:
        with _open_perf_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO perf_samples (
                    source,
                    metric,
                    value_ms,
                    is_error,
                    request_id,
                    tags
                )
                VALUES (?, ?, ?, 0, ?, ?)
                """,
                (
                    "query",
                    f"query.{fingerprint[:80]}",
                    int(elapsed_ms),
                    _request_id_var.get(None),
                    _json_tags({"sql_fingerprint": fingerprint}),
                ),
            )
            conn.commit()
    except Exception:
        return


def _record_perf_sample(
    db_path: str | Path | None,
    source: str,
    metric: str,
    value_ms: int,
    tags: dict[str, Any] | None = None,
    is_error: bool = False,
) -> None:
    """Record a perf sample using an isolated connection."""
    resolved_db_path = _coerce_db_path(db_path)
    if resolved_db_path is None:
        return
    try:
        with _open_perf_connection(resolved_db_path) as conn:
            conn.execute(
                """
                INSERT INTO perf_samples (
                    source,
                    metric,
                    value_ms,
                    is_error,
                    request_id,
                    tags
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source,
                    metric,
                    int(value_ms),
                    1 if is_error else 0,
                    _request_id_var.get(None),
                    _json_tags(tags),
                ),
            )
            conn.commit()
    except Exception:
        return


async def _record_perf_sample_async(
    db_path: str | Path | None,
    source: str,
    metric: str,
    value_ms: int,
    tags: dict[str, Any] | None = None,
    is_error: bool = False,
) -> None:
    """Async wrapper for perf sample recording."""
    if db_path is None:
        return
    await asyncio.to_thread(
        _record_perf_sample,
        db_path,
        source,
        metric,
        value_ms,
        tags,
        is_error,
    )


def prune_perf_samples(conn: sqlite3.Connection, retention_days: int = 30) -> None:
    """Delete perf samples older than the configured retention window."""
    conn.execute(
        """
        DELETE FROM perf_samples
        WHERE created_at < datetime('now', ?)
        """,
        (f"-{int(retention_days)} days",),
    )


def _slow_query_threshold_ms() -> int:
    raw_value = str(os.getenv("FINANCE_CLI_SLOW_QUERY_MS", "0")).strip()
    try:
        return max(int(raw_value), 0)
    except ValueError:
        return 0


class TimedCursor(sqlite3.Cursor):
    """Cursor that records slow query samples."""

    def _timed(self, method: str, sql: str, func: Callable[[], Any]) -> Any:
        threshold_ms = _slow_query_threshold_ms()
        if threshold_ms <= 0:
            return func()

        start = time.perf_counter()
        try:
            return func()
        finally:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            if elapsed_ms <= threshold_ms:
                return
            fingerprint = _normalize_sql(sql)
            log.warning(
                "slow query",
                extra={
                    "sql_fingerprint": fingerprint,
                    "duration_ms": elapsed_ms,
                    "method": method,
                    "request_id": _request_id_var.get(None),
                },
            )
            db_path = getattr(self.connection, "_perf_db_path", None)
            token = _query_sample_db_path_var.set(db_path) if db_path else None
            try:
                _record_query_sample(fingerprint, elapsed_ms)
            finally:
                if token is not None:
                    _query_sample_db_path_var.reset(token)

    def execute(
        self,
        sql: str,
        parameters: Iterable[Any] | None = None,
    ) -> "TimedCursor":
        params = () if parameters is None else parameters
        parent_execute = super().execute
        return self._timed("execute", sql, lambda: parent_execute(sql, params))

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterable[Iterable[Any]],
    ) -> "TimedCursor":
        parent_executemany = super().executemany
        return self._timed(
            "executemany",
            sql,
            lambda: parent_executemany(sql, seq_of_parameters),
        )

    def executescript(self, sql_script: str) -> "TimedCursor":
        parent_executescript = super().executescript
        return self._timed(
            "executescript",
            sql_script,
            lambda: parent_executescript(sql_script),
        )


class TimedConnection(sqlite3.Connection):
    """Connection that returns timed cursors and times convenience methods."""

    def __init__(self, database: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(database, *args, **kwargs)
        self._perf_db_path = _coerce_db_path(database)

    def cursor(self, factory: type[sqlite3.Cursor] | None = None) -> TimedCursor:
        chosen_factory = TimedCursor if factory is None else factory
        return super().cursor(factory=chosen_factory)

    def execute(
        self,
        sql: str,
        parameters: Iterable[Any] | None = None,
    ) -> TimedCursor:
        return self.cursor().execute(sql, parameters)

    def executemany(
        self,
        sql: str,
        seq_of_parameters: Iterable[Iterable[Any]],
    ) -> TimedCursor:
        return self.cursor().executemany(sql, seq_of_parameters)

    def executescript(self, sql_script: str) -> TimedCursor:
        return self.cursor().executescript(sql_script)


__all__ = [
    "TimedConnection",
    "TimedCursor",
    "_conversation_id_var",
    "_normalize_sql",
    "_record_perf_sample",
    "_record_perf_sample_async",
    "_record_query_sample",
    "_request_id_var",
    "_session_id_var",
    "get_conversation_id",
    "get_request_id",
    "get_session_id",
    "prune_perf_samples",
    "set_conversation_id",
    "set_request_id",
    "set_session_id",
]
