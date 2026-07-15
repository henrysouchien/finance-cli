"""Client-side change-feed subscriber for row-level replication."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from finance_cli import crypto_envelope
from finance_cli.db import connect
from finance_cli.storage_lease import optional_lease_scope
from finance_cli.sync_protocol import CHANGELOG_TABLES, SYNCED_SIDECAR_FILES

from .config import CASHNERD_DB_PATH, CASHNERD_DATA_DIR
from .exceptions import SyncAuthError, SyncDegradedError

if TYPE_CHECKING:
    from .engine import SyncEngine

logger = logging.getLogger(__name__)


def _quote_identifier(identifier: str) -> str:
    value = str(identifier or "").strip()
    if not value.replace("_", "").isalnum() or value[:1].isdigit():
        raise SyncDegradedError(f"unsafe SQLite identifier: {identifier!r}")
    return f'"{value}"'


def _primary_key_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({_quote_identifier(table_name)})").fetchall()
    pk_rows = sorted(
        (row for row in rows if int(row["pk"] or 0) > 0),
        key=lambda row: int(row["pk"] or 0),
    )
    return [str(row["name"]) for row in pk_rows]


def _apply_upsert(conn: sqlite3.Connection, table_name: str, row_json: str | None) -> None:
    row = json.loads(str(row_json or "{}"))
    if not isinstance(row, dict) or not row:
        raise SyncDegradedError(f"invalid row payload for {table_name}")
    columns = [str(column) for column in row]
    pk_columns = _primary_key_columns(conn, table_name)
    if not pk_columns:
        raise SyncDegradedError(f"table {table_name!r} has no primary key")
    placeholders = ", ".join("?" for _ in columns)
    column_list = ", ".join(_quote_identifier(column) for column in columns)
    conflict_target = ", ".join(_quote_identifier(column) for column in pk_columns)
    assignments = ", ".join(
        f"{_quote_identifier(column)} = excluded.{_quote_identifier(column)}"
        for column in columns
        if column not in pk_columns
    )
    if assignments:
        sql = (
            f"INSERT INTO {_quote_identifier(table_name)} ({column_list}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT({conflict_target}) DO UPDATE SET {assignments}"
        )
    else:
        sql = (
            f"INSERT INTO {_quote_identifier(table_name)} ({column_list}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT({conflict_target}) DO NOTHING"
        )
    conn.execute(sql, [row[column] for column in columns])


def _apply_delete(conn: sqlite3.Connection, table_name: str, pk_json: str | None) -> None:
    pk = json.loads(str(pk_json or "{}"))
    if not isinstance(pk, dict) or not pk:
        raise SyncDegradedError(f"invalid PK payload for {table_name}")
    where_clause = " AND ".join(f"{_quote_identifier(column)} = ?" for column in pk)
    conn.execute(
        f"DELETE FROM {_quote_identifier(table_name)} WHERE {where_clause}",
        [pk[column] for column in pk],
    )


def _target_path_for_meta_key(key: str) -> Path:
    normalized = str(key or "").strip()
    if normalized in SYNCED_SIDECAR_FILES:
        return CASHNERD_DATA_DIR / normalized
    if normalized.startswith("session:"):
        session_name = normalized.split(":", 1)[1]
        candidate = Path(session_name)
        if (
            not session_name
            or candidate.name != session_name
            or candidate.is_absolute()
            or ".." in candidate.parts
        ):
            raise SyncDegradedError(f"invalid session meta key: {key!r}")
        return CASHNERD_DATA_DIR / "sessions" / session_name
    raise SyncDegradedError(f"unknown meta key: {key!r}")


def _write_file_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(content)
        tmp_path = Path(handle.name)
    os.replace(tmp_path, path)


class ChangeFeedSubscriber:
    def __init__(self, engine: SyncEngine) -> None:
        self._engine = engine
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.is_running:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="cashnerd-change-feed")

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        backoff = 1.0
        try:
            while not self._stop.is_set():
                try:
                    await self._subscribe_once()
                    backoff = 1.0
                except asyncio.CancelledError:
                    raise
                except SyncAuthError:
                    await self._engine.refresh_credentials()
                except SyncDegradedError:
                    self._engine.mark_subscriber_degraded()
                    return
                except (httpx.HTTPError, ConnectionError, TimeoutError):
                    await asyncio.sleep(min(backoff, 60.0))
                    backoff = min(backoff * 2.0, 60.0)
        finally:
            self._task = None

    async def _subscribe_once(self) -> None:
        since = self._engine.last_applied_op_id
        token = await self._engine.get_sync_token()
        headers = {"Authorization": f"Bearer {token}"}
        if self._engine.schema_version is not None:
            headers["X-CashNerd-Schema-Version"] = str(self._engine.schema_version)
        reset_epoch = str(getattr(self._engine, "reset_epoch", "") or "").strip()
        if reset_epoch:
            headers["X-CashNerd-Reset-Epoch"] = reset_epoch
        current_event = "op"
        data_lines: list[str] = []

        async with self._engine.stream_request(
            "GET",
            f"{self._engine.server_url.rstrip('/')}/api/sync/subscribe",
            params={"since": since},
            headers=headers,
            timeout=None,
        ) as response:
            if response.status_code == 401:
                raise SyncAuthError("sync token rejected")
            response.raise_for_status()

            async for line in response.aiter_lines():
                if self._stop.is_set():
                    return
                if line == "":
                    await self._dispatch_event(current_event, data_lines)
                    current_event = "op"
                    data_lines = []
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    current_event = line.split(":", 1)[1].strip() or "op"
                    continue
                if line.startswith("data:"):
                    data_lines.append(line[5:].strip())

    async def _dispatch_event(self, event_name: str, data_lines: list[str]) -> None:
        if not data_lines:
            return
        payload = json.loads("\n".join(data_lines))
        if event_name == "heartbeat":
            return
        if event_name == "error":
            code = str(payload.get("code") or "").strip()
            if code == "auth_expired":
                raise SyncAuthError("sync token expired")
            raise SyncDegradedError(f"subscriber error: {code or 'unknown'}")
        if event_name != "op":
            raise SyncDegradedError(f"unknown SSE event: {event_name}")
        install_id = self._engine.install_id
        if not install_id:
            raise SyncDegradedError("install_id is empty during op dispatch; echo filter is unsafe")
        if str(payload.get("origin_session_id") or "") == install_id:
            self._engine.bump_last_applied(int(payload["id"]))
            return
        await self._apply_op(payload)

    async def _apply_op(self, payload: dict[str, Any]) -> None:
        table_name = str(payload.get("table") or "").strip()
        op_type = str(payload.get("op") or "").strip().upper()
        if table_name not in CHANGELOG_TABLES:
            logger.error("subscriber received non-changelog table op: %s", table_name)
            raise SyncDegradedError(f"op for non-changelog table {table_name!r}")
        if op_type not in {"INSERT", "UPDATE", "DELETE"}:
            raise SyncDegradedError(f"unknown op type {op_type!r}")

        meta_effect: tuple[str, str | None] | None = None
        user_id = str(getattr(self._engine, "user_id", "") or "") or None
        with optional_lease_scope(
            user_id,
            operation="sync_subscriber",
            metadata={"source": "sync.subscriber._apply_op"},
            heartbeat=True,
        ):
            conn = connect(
                CASHNERD_DB_PATH,
                busy_timeout=5000,
                check_same_thread=False,
                session_id="__STREAM__",
            )
            try:
                conn.execute("BEGIN IMMEDIATE")
                if op_type in {"INSERT", "UPDATE"}:
                    _apply_upsert(conn, table_name, payload.get("new_json"))
                else:
                    _apply_delete(conn, table_name, payload.get("pk_json"))

                if table_name == "_meta_state":
                    meta_row_json = payload.get("new_json") if op_type != "DELETE" else payload.get("old_json")
                    meta_row = json.loads(str(meta_row_json or "{}"))
                    if not isinstance(meta_row, dict):
                        raise SyncDegradedError("invalid _meta_state payload")
                    meta_effect = (str(meta_row.get("key") or ""), meta_row.get("sha256"))

                conn.execute(
                    "UPDATE sync_state SET last_applied_op_id = ? WHERE id = 0",
                    (int(payload["id"]),),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

        if meta_effect is not None:
            await self._apply_meta_effect(*meta_effect)

    async def _apply_meta_effect(self, key: str, sha256: str | None) -> None:
        normalized = str(key or "").strip()
        if normalized == "db-dek.enc":
            if sha256 is None:
                return
            content = await self._engine.fetch_sidecar_content(key, sha256)
            if content is None:
                return
            user_id = str(self._engine.user_id or "").strip()
            if not user_id:
                raise SyncDegradedError("missing user_id for db-dek.enc meta update")
            await asyncio.to_thread(
                crypto_envelope.install_db_dek_blob,
                user_id,
                content,
                data_dir=CASHNERD_DATA_DIR,
            )
            return
        path = _target_path_for_meta_key(key)
        if sha256 is None:
            path.unlink(missing_ok=True)
            return
        content = await self._engine.fetch_sidecar_content(key, sha256)
        if content is None:
            return
        await asyncio.to_thread(_write_file_atomic, path, content)
