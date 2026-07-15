"""Core local sync engine."""

from __future__ import annotations

import asyncio
import io
import json
import os
import shutil
import sqlite3
import tarfile
import tempfile
import webbrowser
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote
from uuid import uuid4

import httpx

from finance_cli import crypto_envelope
from finance_cli.db import _install_id_var, connect
from finance_cli.exceptions import EngagementRequiredError
from finance_cli.lazy_imports import LazyModule
from finance_cli.operation_log import (
    current_changelog_id as operation_current_changelog_id,
    record_operation_log,
)
from finance_cli import sync_protocol
from finance_cli.storage_client import _dispatch as storage_dispatch
from finance_cli.storage_lease import optional_lease_scope

from .auth import LocalAuth
from .bootstrap_lock import InstallBootstrapLock
from .config import (
    CASHNERD_AGENT_MEMORY_PATH,
    CASHNERD_DATA_DIR,
    CASHNERD_DB_PATH,
    CASHNERD_DIR,
    CASHNERD_PENDING_CHANGESET_PATH,
    CASHNERD_RULES_PATH,
    CASHNERD_SKILL_STATE_PATH,
    CASHNERD_SYNC_LOG_PATH,
    SyncConfig,
    ensure_dirs,
    save_config,
)
from .exceptions import (
    SyncAuthError,
    SyncCatchupFailedError,
    SyncConflictError,
    SyncSchemaMismatchError,
    SyncServerUnreachableError,
)
from .subscriber import ChangeFeedSubscriber
from .subscriber_lock import InstallSubscriberLock

storage_files = LazyModule("finance_cli.storage_files")


def _proxy_timeout(tool_name: str, args: dict[str, Any]) -> float:
    if tool_name in {"db_import_preferences", "db_restore"}:
        return 300.0
    if tool_name == "plaid_link":
        try:
            arg_timeout = int(args.get("timeout") or 300)
        except (TypeError, ValueError):
            arg_timeout = 300
        return float(arg_timeout + 30)
    if tool_name == "plaid_exchange":
        try:
            arg_timeout = int(args.get("timeout") or 300)
        except (TypeError, ValueError):
            arg_timeout = 300
        return float(arg_timeout + 30)
    if tool_name == "setup_connect":
        try:
            arg_timeout = int(args.get("timeout") or 300)
        except (TypeError, ValueError):
            arg_timeout = 300
        skip_sync = bool(args.get("skip_sync", False))
        return float(arg_timeout + 30) if skip_sync else float(arg_timeout + 300)
    if tool_name == "monthly_run":
        if bool(args.get("sync", False)) and bool(args.get("ai", False)):
            return 600.0
        return 300.0
    if tool_name in {
        "plaid_sync",
        "plaid_balance_refresh",
        "stripe_sync",
        "schwab_sync",
    }:
        return 180.0
    return 120.0


_UPLOAD_PROXY_TOOLS = frozenset({"db_import_preferences", "db_restore"})


def _as_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _append_warning_payload(payload: dict[str, Any], message: str) -> dict[str, Any]:
    envelope = dict(payload)
    summary = dict(envelope.get("summary", {}))
    warnings = list(summary.get("warnings") or [])
    warnings.append(message)
    summary["warnings"] = warnings
    envelope["summary"] = summary
    envelope.setdefault("data", {})
    return envelope


def _plaid_link_session(envelope: dict[str, Any]) -> dict[str, Any]:
    data = envelope.get("data")
    if not isinstance(data, dict):
        return {}
    session = data.get("session")
    return dict(session) if isinstance(session, dict) else {}


def _envelope_error_messages(envelope: dict[str, Any]) -> list[str]:
    messages: list[str] = []
    summary = envelope.get("summary")
    if isinstance(summary, dict):
        errors = summary.get("errors")
        if isinstance(errors, list):
            messages.extend(str(item) for item in errors if str(item).strip())
        elif isinstance(errors, str) and errors.strip():
            messages.append(errors.strip())
    data = envelope.get("data")
    if isinstance(data, dict):
        error = data.get("error")
        if isinstance(error, str) and error.strip() and error.strip() not in messages:
            messages.append(error.strip())
    return messages


def _response_json(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _response_error_code(body: dict[str, Any]) -> str:
    detail = body.get("detail")
    if isinstance(detail, dict):
        return str(detail.get("error") or "").strip()
    return str(body.get("error") or "").strip()


def _response_error_message(body: dict[str, Any]) -> str:
    detail = body.get("detail")
    if isinstance(detail, str):
        return detail.strip()
    if isinstance(detail, dict):
        for key in ("message", "detail", "error"):
            value = detail.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    error = body.get("error")
    return str(error).strip() if isinstance(error, str) else ""


def _raise_engagement_required(body: dict[str, Any]) -> None:
    message = _response_error_message(body) or (
        "This CashNerd server-side operation requires an active engagement."
    )
    raise EngagementRequiredError(message)


def _fsync_directory(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class SyncEngine:
    """Pull and push local SQLite changes against the authoritative server."""

    def __init__(
        self,
        config: SyncConfig,
        auth: LocalAuth,
        *,
        http_client: httpx.AsyncClient | None = None,
        browser_opener: Callable[[str], Any] | None = None,
    ) -> None:
        self._config = config
        self._auth = auth
        self._http_client = http_client
        self._browser_opener = browser_opener or webbrowser.open
        self._install_subscriber_lock = InstallSubscriberLock(
            CASHNERD_DIR / "subscriber.lock"
        )
        self._subscriber = ChangeFeedSubscriber(self)
        self._install_id_cache = self._read_current_sync_state_sync()[1]
        if self._install_id_cache:
            _install_id_var.set(self._install_id_cache)

    @property
    def server_url(self) -> str:
        return self._config.server_url

    @property
    def schema_version(self) -> int | None:
        if self._config.schema_version is not None:
            return int(self._config.schema_version)
        if not CASHNERD_DB_PATH.exists():
            return None
        try:
            with connect(CASHNERD_DB_PATH, check_same_thread=False) as conn:
                row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        except sqlite3.Error:
            return None
        value = int((row[0] if row else 0) or 0)
        return value or None

    @property
    def install_id(self) -> str:
        if self._install_id_cache:
            return self._install_id_cache
        _op_id, install_id, _status = self._read_current_sync_state_sync()
        if install_id:
            self._install_id_cache = install_id
        return install_id

    @property
    def last_applied_op_id(self) -> int:
        return self._read_current_sync_state_sync()[0]

    @property
    def reset_epoch(self) -> str:
        return self._read_current_reset_epoch_sync()

    def current_changelog_id(self) -> int:
        if not CASHNERD_DB_PATH.exists():
            return 0
        with optional_lease_scope(
            self._config.user_id,
            operation="sync",
            metadata={"source": "sync.engine.current_changelog_id"},
        ):
            with connect(
                CASHNERD_DB_PATH,
                busy_timeout=5000,
                check_same_thread=False,
                expected_user_id=self._config.user_id,
            ) as conn:
                return operation_current_changelog_id(conn)

    def record_tool_operation(
        self,
        *,
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
        if not CASHNERD_DB_PATH.exists():
            return
        with optional_lease_scope(
            self._config.user_id,
            operation="sync",
            metadata={"source": "sync.engine.record_tool_operation"},
        ):
            with connect(
                CASHNERD_DB_PATH,
                busy_timeout=5000,
                check_same_thread=False,
                expected_user_id=self._config.user_id,
            ) as conn:
                if (
                    conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_operation_log'"
                    ).fetchone()
                    is None
                ):
                    return
                record_operation_log(
                    conn,
                    op_type="tool_invocation",
                    surface=surface,
                    tool_name=tool_name,
                    status=status,
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    start_changelog_id=start_changelog_id,
                    end_changelog_id=end_changelog_id,
                    request_metadata=request_metadata,
                    result_metadata=result_metadata,
                    error_metadata=error_metadata,
                    idempotency_key=idempotency_key,
                )
                conn.commit()

    @property
    def subscriber_status(self) -> str:
        return self._read_current_sync_state_sync()[2]

    @property
    def user_id(self) -> str | None:
        return self._config.user_id

    @property
    def subscriber_is_running(self) -> bool:
        return self._subscriber.is_running

    async def pull(self) -> bool:
        """Ensure the local DB is up to date."""
        ensure_dirs()
        if not CASHNERD_DB_PATH.exists() or self._config.schema_version is None:
            await self.force_pull()
            return True

        try:
            token = await self._get_sync_token_with_browser_oauth()
            response = await self._request(
                "GET",
                f"{self._config.server_url.rstrip('/')}/api/sync/schema-version",
                headers={"Authorization": f"Bearer {token}"},
            )
        except SyncServerUnreachableError:
            self._log_event("pull_offline", {"mode": "using_local_db"})
            return False

        if response.status_code == 401:
            self._auth.invalidate_sync_token()
            await self.force_pull()
            return True

        response.raise_for_status()
        body = response.json()
        server_schema_version = int(body["schema_version"])
        if self._config.schema_version != server_schema_version:
            await self.force_pull()
        server_reset_epoch = str(body.get("reset_epoch") or "").strip()
        if server_reset_epoch and self.reset_epoch != server_reset_epoch:
            await self._force_pull_strict()
        return True

    async def push(self) -> dict[str, Any]:
        ensure_dirs()
        self._clear_downstream_only_changelog()
        pending_payload = self._read_pending_changeset()
        if pending_payload is not None:
            payload = pending_payload
            changeset = list(payload.get("changeset") or [])
            client_changelog_max_id = int(payload.get("client_changelog_max_id") or 0)
            if client_changelog_max_id <= 0:
                current_changeset, current_max_id = self._read_changeset_with_cursor()
                if current_changeset == changeset:
                    client_changelog_max_id = current_max_id
        else:
            changeset, client_changelog_max_id = self._read_changeset_with_cursor()

        if not changeset:
            self._delete_pending_changeset()
            return {"status": "no_changes"}

        if pending_payload is None:
            payload = {
                "push_id": uuid4().hex,
                "pull_timestamp": self._config.last_sync_ts,
                "schema_version": self._config.schema_version,
                "last_seen_op_id": self.last_applied_op_id,
                "install_id": self.install_id,
                "changeset": changeset,
                "client_changelog_max_id": client_changelog_max_id,
            }
            self._write_pending_changeset(payload)

        retried_after_catchup = False
        while True:
            try:
                response = await self._post_push(payload, retry_auth=True)
            except SyncServerUnreachableError:
                self._write_pending_changeset(payload)
                self._log_event("push_offline", {"changes": len(changeset)})
                raise

            if response.status_code == 409:
                self._delete_pending_changeset()
                raise SyncConflictError(response.json())
            if response.status_code == 412:
                body = response.json()
                if "server_op_id" in body:
                    if retried_after_catchup:
                        self.mark_subscriber_degraded()
                        raise SyncCatchupFailedError(
                            "subscriber could not catch up before push"
                        )
                    server_op_id = int(body["server_op_id"])
                    try:
                        await self._wait_for_subscriber(server_op_id, timeout=60.0)
                    except (asyncio.TimeoutError, SyncCatchupFailedError) as exc:
                        self.mark_subscriber_degraded()
                        raise SyncCatchupFailedError(
                            "subscriber could not catch up before push"
                        ) from exc
                    retried_after_catchup = True
                    continue
                self._delete_pending_changeset()
                raise SyncSchemaMismatchError(
                    body.get("server_schema_version"),
                    body.get("client_schema_version"),
                )
            if response.status_code == 401:
                self._delete_pending_changeset()
                raise SyncAuthError("Sync session was rejected by the server")
            if response.status_code == 403:
                detail = _response_json(response)
                if _response_error_code(detail) == "engagement_required":
                    self._delete_pending_changeset()
                    _raise_engagement_required(detail)

            if 400 <= response.status_code < 500:
                self._delete_pending_changeset()
            if response.status_code >= 500:
                self._write_pending_changeset(payload)
            response.raise_for_status()
            body = response.json()
            self._clear_changelog(max_id=client_changelog_max_id)
            self._delete_pending_changeset()
            new_pull_timestamp = body.get("new_pull_timestamp")
            if new_pull_timestamp:
                self._config.last_sync_ts = str(new_pull_timestamp)
            save_config(self._config)
            self._log_event("push_applied", {"changes": len(changeset)})
            return body

    async def force_pull(self) -> None:
        await self._force_pull_inner(strict=False)

    async def _force_pull_strict(self) -> None:
        await self._force_pull_inner(strict=True)

    async def _force_pull_inner(self, *, strict: bool) -> None:
        ensure_dirs()
        credential = await self._get_credential_with_browser_oauth()
        try:
            response = await self._request(
                "POST",
                f"{self._config.server_url.rstrip('/')}/api/sync/auth",
                json={"credential": credential},
            )
        except SyncServerUnreachableError:
            if strict or not CASHNERD_DB_PATH.exists():
                raise
            self._log_event("force_pull_offline", {"mode": "using_local_db"})
            return

        if response.status_code == 401:
            raise SyncAuthError("Google credential was rejected by the sync server")
        response.raise_for_status()

        sync_token = str(response.headers.get("X-CashNerd-Sync-Token") or "").strip()
        if sync_token:
            self._auth.record_sync_session(
                token=sync_token,
                user_id=response.headers.get("X-CashNerd-User-Id"),
            )

        snapshot_op_id = int(response.headers.get("X-CashNerd-Op-Id") or 0)
        tar_bytes = response.content
        snapshot_user_id = str(
            response.headers.get("X-CashNerd-User-Id") or self._config.user_id or ""
        ).strip()
        effective_install_id = await asyncio.to_thread(
            self._locked_commit_snapshot,
            tar_bytes,
            snapshot_op_id,
            snapshot_user_id,
            strict,
        )
        self._install_id_cache = effective_install_id
        _install_id_var.set(effective_install_id)
        self._rebuild_fts()

        self._config.user_id = (
            str(
                response.headers.get("X-CashNerd-User-Id") or self._config.user_id or ""
            ).strip()
            or None
        )
        self._config.last_sync_ts = (
            str(response.headers.get("X-CashNerd-Pull-Timestamp") or "").strip() or None
        )
        schema_header = response.headers.get("X-CashNerd-Schema-Version")
        self._config.schema_version = (
            int(schema_header)
            if schema_header is not None
            else self._config.schema_version
        )
        save_config(self._config)
        self._log_event(
            "pull_applied",
            {
                "schema_version": self._config.schema_version,
                "user_id": self._config.user_id,
            },
        )
        self.start_subscriber()

    async def proxy_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any] | None = None,
        *,
        wait_for_subscriber: bool = True,
    ) -> dict[str, Any]:
        args = dict(arguments or {})
        if tool_name == "plaid_link" and (
            _as_bool(args.get("wait")) or _as_bool(args.get("open_browser"))
        ):
            return await self._proxy_plaid_link_handoff(
                args, wait_for_subscriber=wait_for_subscriber
            )
        return await self._proxy_tool_once(
            tool_name, args, wait_for_subscriber=wait_for_subscriber
        )

    async def _proxy_tool_once(
        self,
        tool_name: str,
        args: dict[str, Any],
        *,
        wait_for_subscriber: bool = True,
    ) -> dict[str, Any]:
        restore_live = tool_name == "db_restore" and not _as_bool(
            args.get("dry_run"), default=True
        )
        if tool_name in _UPLOAD_PROXY_TOOLS:
            bundle_path = Path(str(args.get("bundle_path") or "")).expanduser()
            if not bundle_path.is_file():
                label = "Backup" if tool_name == "db_restore" else "Preferences"
                message = f"{label} bundle not found: {bundle_path}"
                return {"data": {"error": message}, "summary": {"errors": [message]}}
            if restore_live:
                await self.stop_subscriber()
            response = await self._post_proxy_tool_upload(
                tool_name, args, bundle_path=bundle_path, retry_auth=True
            )
        else:
            response = await self._post_proxy_tool(tool_name, args, retry_auth=True)
        if response.status_code == 401:
            self._auth.invalidate_sync_token()
            if restore_live:
                self.start_subscriber()
            raise SyncAuthError("Sync session was rejected by the server")
        if response.status_code == 403:
            detail = _response_json(response)
            if _response_error_code(detail) == "engagement_required":
                if restore_live:
                    self.start_subscriber()
                _raise_engagement_required(detail)
        if response.status_code == 501:
            detail = _response_json(response)
            if _response_error_code(detail) == "sync_unsupported_for_remote":
                message = (
                    "Remote storage sync is not enabled on this server yet. "
                    "Ask the CashNerd server operator to enable remote change-feed sync."
                )
                return {"data": {"error": message}, "summary": {"warnings": [message]}}
            server_message = _response_error_message(detail)
            if server_message and (
                "multipart upload" in server_message
                or "reset/rebootstrap" in server_message
            ):
                message = server_message
            else:
                message = (
                    "This tool requires a server-side upload path that is not enabled yet, "
                    "so it is still unavailable via local sync."
                )
            if restore_live:
                self.start_subscriber()
            return {"data": {"error": message}, "summary": {"warnings": [message]}}

        try:
            response.raise_for_status()
        except Exception:
            if restore_live:
                self.start_subscriber()
            raise
        body = response.json()
        target_op_id = body.pop("_op_id", None) if isinstance(body, dict) else None
        envelope = (
            body.get("result")
            if isinstance(body, dict) and isinstance(body.get("result"), dict)
            else body
        )
        if not isinstance(envelope, dict):
            return {"data": {}, "summary": {}}
        if tool_name == "db_restore":
            return await self._handle_restore_rebootstrap(
                envelope, restore_live=restore_live
            )
        if target_op_id is not None and wait_for_subscriber:
            try:
                await self._wait_for_subscriber(int(target_op_id), timeout=30.0)
            except (asyncio.TimeoutError, SyncCatchupFailedError):
                envelope = _append_warning_payload(
                    envelope,
                    "Server mutation succeeded but local subscriber hasn't caught up within 30s; next tool call may see stale data briefly.",
                )
        return envelope

    async def _handle_restore_rebootstrap(
        self,
        envelope: dict[str, Any],
        *,
        restore_live: bool,
    ) -> dict[str, Any]:
        data = envelope.get("data") if isinstance(envelope.get("data"), dict) else {}
        sync_reset = data.get("_sync_reset") if isinstance(data, dict) else None
        if not isinstance(sync_reset, dict) or not sync_reset.get(
            "local_rebootstrap_required"
        ):
            if restore_live:
                self.start_subscriber()
            return envelope

        try:
            await self._force_pull_strict()
        except Exception as exc:
            self.mark_subscriber_degraded()
            message = (
                "Server restore succeeded, but local rebootstrap failed. "
                f"A fresh pull is required before tools can run: {exc}"
            )
            return {
                "data": {
                    "error": message,
                    "restore_result": envelope,
                },
                "summary": {
                    "errors": [message],
                    "local_rebootstrap_required": True,
                },
            }

        data["local_rebootstrapped"] = True
        summary = envelope.get("summary")
        if not isinstance(summary, dict):
            summary = {}
            envelope["summary"] = summary
        summary["local_rebootstrapped"] = True
        return envelope

    async def _proxy_plaid_link_handoff(
        self,
        args: dict[str, Any],
        *,
        wait_for_subscriber: bool = True,
    ) -> dict[str, Any]:
        requested_wait = _as_bool(args.get("wait"))
        requested_open = _as_bool(args.get("open_browser"))
        server_args = dict(args)
        server_args["wait"] = False
        server_args["open_browser"] = False

        envelope = await self._proxy_tool_once(
            "plaid_link",
            server_args,
            wait_for_subscriber=wait_for_subscriber,
        )
        session = _plaid_link_session(envelope)
        hosted_url = str(session.get("hosted_link_url") or "").strip()
        if requested_open and hosted_url:
            self._browser_opener(hosted_url)
        elif requested_open:
            envelope = _append_warning_payload(
                envelope,
                "Plaid Link session did not include a hosted_link_url to open.",
            )

        if not requested_wait:
            return envelope

        if not requested_open:
            return _append_warning_payload(
                envelope,
                "wait=True via local sync requires open_browser=True so the user can complete Plaid Link before the exchange; returning hosted link session without waiting.",
            )

        link_token = str(session.get("link_token") or "").strip()
        if not link_token:
            return _append_warning_payload(
                envelope,
                "Plaid Link session did not include a link_token, so local sync could not complete the exchange.",
            )

        exchange_args: dict[str, Any] = {
            "link_token": link_token,
            "requested_products": session.get("requested_products"),
            "timeout": args.get("timeout"),
            "allow_duplicate_institution": _as_bool(args.get("allow_duplicate")),
        }
        exchange = await self._proxy_tool_once(
            "plaid_exchange",
            exchange_args,
            wait_for_subscriber=wait_for_subscriber,
        )
        merged = dict(envelope)
        data = dict(merged.get("data") or {})
        merged["data"] = data
        summary = dict(merged.get("summary") or {})
        summary["waited"] = True
        exchange_errors = _envelope_error_messages(exchange)
        if exchange_errors:
            data["exchange_error"] = exchange
            summary["linked"] = False
            summary["errors"] = exchange_errors
            merged["summary"] = summary
            return merged

        data["linked_item"] = dict(exchange.get("data") or {})
        merged["summary"] = summary
        return merged

    async def get_sync_token(self) -> str:
        return await self._get_sync_token_with_browser_oauth()

    async def refresh_credentials(self) -> None:
        self._auth.invalidate_sync_token()
        await self._get_sync_token_with_browser_oauth(force_refresh=True)

    @asynccontextmanager
    async def stream_request(
        self, method: str, url: str, **kwargs: Any
    ) -> AsyncIterator[httpx.Response]:
        client = self._http_client
        owns_client = client is None
        if client is None:
            client = httpx.AsyncClient(timeout=kwargs.pop("timeout", None))
        try:
            async with client.stream(method, url, **kwargs) as response:
                yield response
        except httpx.HTTPError as exc:
            raise SyncServerUnreachableError(str(exc)) from exc
        finally:
            if owns_client:
                await client.aclose()

    async def fetch_sidecar_content(self, key: str, sha256: str) -> bytes | None:
        response = await self._fetch_sidecar_content(key, sha256, retry_auth=True)
        if response.status_code == 409:
            return None
        if response.status_code == 401:
            raise SyncAuthError("Sync session was rejected by the server")
        response.raise_for_status()
        return bytes(response.content)

    def try_acquire_install_subscriber_lock(self) -> bool:
        return self._install_subscriber_lock.try_acquire()

    def release_install_subscriber_lock(self) -> None:
        self._install_subscriber_lock.release()

    def start_subscriber(self) -> bool:
        if not self._install_subscriber_lock.is_held:
            return False
        if (
            not CASHNERD_DB_PATH.exists()
            or not self.install_id
            or self.subscriber_status != "healthy"
        ):
            return False
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False
        self._subscriber.start()
        return True

    async def stop_subscriber(self) -> None:
        await self._subscriber.stop()

    def bump_last_applied(self, op_id: int) -> None:
        if not CASHNERD_DB_PATH.exists():
            return
        with connect(
            CASHNERD_DB_PATH,
            busy_timeout=5000,
            check_same_thread=False,
            session_id="__STREAM__",
        ) as conn:
            conn.execute(
                """
                UPDATE sync_state
                   SET last_applied_op_id = CASE
                       WHEN last_applied_op_id > ? THEN last_applied_op_id
                       ELSE ?
                   END
                 WHERE id = 0
                """,
                (int(op_id), int(op_id)),
            )
            conn.commit()

    def mark_subscriber_degraded(self) -> None:
        self._set_subscriber_status("degraded")

    def mark_subscriber_healthy(self) -> None:
        self._set_subscriber_status("healthy")

    async def _post_push(
        self, payload: dict[str, Any], *, retry_auth: bool
    ) -> httpx.Response:
        token = await self._get_sync_token_with_browser_oauth(
            force_refresh=not retry_auth
        )
        response = await self._request(
            "POST",
            f"{self._config.server_url.rstrip('/')}/api/sync/push",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )
        if response.status_code == 401 and retry_auth:
            self._auth.invalidate_sync_token()
            return await self._post_push(payload, retry_auth=False)
        return response

    async def _fetch_sidecar_content(
        self, key: str, sha256: str, *, retry_auth: bool
    ) -> httpx.Response:
        token = await self._get_sync_token_with_browser_oauth(
            force_refresh=not retry_auth
        )
        response = await self._request(
            "GET",
            f"{self._config.server_url.rstrip('/')}/api/sync/meta/{quote(str(key), safe='')}",
            headers={"Authorization": f"Bearer {token}"},
            params={"sha": sha256},
        )
        if response.status_code == 401 and retry_auth:
            self._auth.invalidate_sync_token()
            return await self._fetch_sidecar_content(key, sha256, retry_auth=False)
        return response

    async def _post_proxy_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        retry_auth: bool,
    ) -> httpx.Response:
        token = await self._get_sync_token_with_browser_oauth(
            force_refresh=not retry_auth
        )
        response = await self._request(
            "POST",
            f"{self._config.server_url.rstrip('/')}/api/sync/proxy-tool",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "tool_name": tool_name,
                "arguments": arguments,
            },
            timeout=_proxy_timeout(tool_name, arguments),
        )
        if response.status_code == 401 and retry_auth:
            self._auth.invalidate_sync_token()
            return await self._post_proxy_tool(tool_name, arguments, retry_auth=False)
        return response

    async def _post_proxy_tool_upload(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        bundle_path: Path,
        retry_auth: bool,
    ) -> httpx.Response:
        token = await self._get_sync_token_with_browser_oauth(
            force_refresh=not retry_auth
        )
        upload_arguments = dict(arguments)
        upload_arguments.pop("bundle_path", None)
        with bundle_path.open("rb") as handle:
            response = await self._request(
                "POST",
                f"{self._config.server_url.rstrip('/')}/api/sync/proxy-tool-upload",
                headers={"Authorization": f"Bearer {token}"},
                data={
                    "tool_name": tool_name,
                    "arguments": json.dumps(
                        upload_arguments, separators=(",", ":"), sort_keys=True
                    ),
                },
                files={"bundle": (bundle_path.name, handle, "application/gzip")},
                timeout=_proxy_timeout(tool_name, arguments),
            )
        if response.status_code == 401 and retry_auth:
            self._auth.invalidate_sync_token()
            return await self._post_proxy_tool_upload(
                tool_name,
                arguments,
                bundle_path=bundle_path,
                retry_auth=False,
            )
        return response

    def _read_changeset(self) -> list[dict[str, Any]]:
        return self._read_changeset_with_cursor()[0]

    def _read_changeset_with_cursor(self) -> tuple[list[dict[str, Any]], int]:
        if not CASHNERD_DB_PATH.exists():
            return [], 0
        with sqlite3.connect(str(CASHNERD_DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, table_name, op, pk_json, old_json, new_json
                  FROM _sync_changelog
                 ORDER BY id
                """
            ).fetchall()

        changeset: list[dict[str, Any]] = []
        max_id = 0
        for row in rows:
            max_id = max(max_id, int(row["id"] or 0))
            table_name = str(row["table_name"])
            if table_name in sync_protocol.DOWNSTREAM_ONLY_TABLES:
                continue
            entry: dict[str, Any] = {
                "table": table_name,
                "op": str(row["op"]),
                "pk": json.loads(str(row["pk_json"])),
            }
            op = str(row["op"]).upper()
            if op == "INSERT":
                entry["values"] = json.loads(str(row["new_json"] or "{}"))
            elif op == "UPDATE":
                entry["old_values"] = json.loads(str(row["old_json"] or "{}"))
                entry["new_values"] = json.loads(str(row["new_json"] or "{}"))
            elif op == "DELETE":
                entry["old_values"] = json.loads(str(row["old_json"] or "{}"))
            changeset.append(entry)
        return changeset, max_id

    def _read_pending_changeset(self) -> dict[str, Any] | None:
        if not CASHNERD_PENDING_CHANGESET_PATH.exists():
            return None
        payload = json.loads(
            CASHNERD_PENDING_CHANGESET_PATH.read_text(encoding="utf-8")
        )
        if not isinstance(payload, dict):
            raise ValueError("Pending changeset payload is invalid")
        return payload

    def _write_pending_changeset(self, payload: dict[str, Any]) -> None:
        CASHNERD_PENDING_CHANGESET_PATH.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        CASHNERD_PENDING_CHANGESET_PATH.chmod(0o600)

    def _delete_pending_changeset(self) -> None:
        CASHNERD_PENDING_CHANGESET_PATH.unlink(missing_ok=True)

    def _log_event(self, event: str, details: dict[str, Any]) -> None:
        records: list[dict[str, Any]]
        if CASHNERD_SYNC_LOG_PATH.exists():
            try:
                records = json.loads(CASHNERD_SYNC_LOG_PATH.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                records = []
        else:
            records = []
        records.append({"event": event, "details": details})
        CASHNERD_SYNC_LOG_PATH.write_text(
            json.dumps(records, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        CASHNERD_SYNC_LOG_PATH.chmod(0o600)

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        client = self._http_client
        owns_client = client is None
        if client is None:
            client = httpx.AsyncClient(timeout=60.0)
        try:
            return await client.request(method, url, **kwargs)
        except httpx.HTTPError as exc:
            raise SyncServerUnreachableError(str(exc)) from exc
        finally:
            if owns_client:
                await client.aclose()

    async def _get_credential_with_browser_oauth(self) -> str:
        return await self._with_browser_oauth_retry(self._auth.get_credential)

    async def _get_sync_token_with_browser_oauth(
        self, *, force_refresh: bool = False
    ) -> str:
        return await self._with_browser_oauth_retry(
            lambda: self._auth.get_sync_token(force_refresh=force_refresh)
        )

    async def _with_browser_oauth_retry(
        self, getter: Callable[[], Awaitable[str]]
    ) -> str:
        try:
            return await getter()
        except SyncAuthError as exc:
            await self._run_browser_oauth(exc)
        return await getter()

    async def _run_browser_oauth(self, original_exc: SyncAuthError) -> None:
        run_browser_oauth = getattr(self._auth, "run_browser_oauth", None)
        if not callable(run_browser_oauth):
            raise original_exc
        try:
            await run_browser_oauth()
        except SyncAuthError:
            raise
        except Exception as exc:
            raise SyncAuthError(f"Automatic browser OAuth failed: {exc}") from exc

    async def _wait_for_subscriber(self, target_op_id: int, timeout: float) -> None:
        if target_op_id <= 0:
            return
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            last_applied_op_id, _install_id, subscriber_status = (
                self._read_current_sync_state_sync()
            )
            if last_applied_op_id >= target_op_id:
                return
            if subscriber_status != "healthy":
                raise SyncCatchupFailedError("subscriber is degraded")
            if asyncio.get_running_loop().time() >= deadline:
                raise asyncio.TimeoutError
            await asyncio.sleep(0.1)

    def _read_current_sync_state_sync(self) -> tuple[int, str, str]:
        if not CASHNERD_DB_PATH.exists():
            return 0, "", "healthy"
        try:
            with connect(CASHNERD_DB_PATH, check_same_thread=False) as conn:
                row = conn.execute(
                    """
                    SELECT last_applied_op_id, install_id, subscriber_status
                      FROM sync_state
                     WHERE id = 0
                    """
                ).fetchone()
        except sqlite3.Error:
            return 0, "", "healthy"
        if row is None:
            return 0, "", "healthy"
        return (
            int(row[0] or 0),
            str(row[1] or ""),
            str(row[2] or "healthy"),
        )

    def _read_current_reset_epoch_sync(self) -> str:
        if not CASHNERD_DB_PATH.exists():
            return ""
        try:
            with optional_lease_scope(
                self._config.user_id,
                operation="sync",
                metadata={"source": "sync.engine.read_reset_epoch"},
            ):
                with connect(CASHNERD_DB_PATH, check_same_thread=False) as conn:
                    row = conn.execute(
                        """
                        SELECT reset_epoch
                          FROM sync_reset_state
                         WHERE id = 0
                        """
                    ).fetchone()
        except sqlite3.Error:
            return ""
        return str((row[0] if row else "") or "").strip()

    def _set_subscriber_status(self, status: str) -> None:
        if not CASHNERD_DB_PATH.exists():
            return
        with connect(
            CASHNERD_DB_PATH,
            busy_timeout=5000,
            check_same_thread=False,
            session_id="__STREAM__",
        ) as conn:
            try:
                conn.execute(
                    "UPDATE sync_state SET subscriber_status = ? WHERE id = 0",
                    (status,),
                )
            except sqlite3.OperationalError as exc:
                if "no such table" in str(exc).lower():
                    return
                raise
            conn.commit()

    def _managed_snapshot_paths(self) -> list[Path]:
        return [
            CASHNERD_DB_PATH,
            CASHNERD_DB_PATH.with_name(f"{CASHNERD_DB_PATH.name}-wal"),
            CASHNERD_DB_PATH.with_name(f"{CASHNERD_DB_PATH.name}-shm"),
            CASHNERD_RULES_PATH,
            CASHNERD_SKILL_STATE_PATH,
            CASHNERD_AGENT_MEMORY_PATH,
            CASHNERD_DATA_DIR / "sessions",
        ]

    def _extract_to_staging_sync(self, tar_bytes: bytes) -> Path:
        staging = Path(tempfile.mkdtemp(prefix=".bootstrap-staging-", dir=CASHNERD_DIR))
        try:
            with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
                try:
                    tar.extractall(staging, filter="data")
                except TypeError:
                    tar.extractall(staging)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        return staging

    def _commit_staged_files_sync(self, staging: Path, *, user_id: str) -> None:
        backup_sessions = CASHNERD_DATA_DIR / "sessions.old"
        target_sessions = CASHNERD_DATA_DIR / "sessions"
        staged_sessions = staging / "sessions"
        remote_sessions_target = storage_dispatch.remote_file_target_for_user(user_id)
        staged_db = staging / "finance.db"
        wal_path = CASHNERD_DB_PATH.with_name(f"{CASHNERD_DB_PATH.name}-wal")
        shm_path = CASHNERD_DB_PATH.with_name(f"{CASHNERD_DB_PATH.name}-shm")
        db_committed = False

        if remote_sessions_target is None and backup_sessions.exists():
            if target_sessions.exists():
                shutil.rmtree(backup_sessions, ignore_errors=True)
            else:
                os.rename(backup_sessions, target_sessions)

        try:
            for name in sync_protocol.SYNCED_SIDECAR_FILES:
                src = staging / name
                dst = CASHNERD_DATA_DIR / name
                if src.exists():
                    if name == "db-dek.enc":
                        if not user_id:
                            raise ValueError(
                                "Cannot install db-dek.enc snapshot without a user_id"
                            )
                        crypto_envelope.install_db_dek_blob(
                            user_id,
                            src.read_bytes(),
                            data_dir=CASHNERD_DATA_DIR,
                        )
                        src.unlink(missing_ok=True)
                    else:
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(src, dst)
                else:
                    if name != "db-dek.enc":
                        dst.unlink(missing_ok=True)

            if remote_sessions_target is not None and staged_sessions.exists():
                with optional_lease_scope(
                    user_id,
                    operation="sync",
                    metadata={"source": "sync.engine.commit_staged_sessions"},
                ):
                    for session_path in sorted(staged_sessions.rglob("*")):
                        if not session_path.is_file():
                            continue
                        relative = session_path.relative_to(staged_sessions)
                        storage_files.write_file(
                            remote_sessions_target,
                            user_id=user_id,
                            product="finance_cli",
                            relative_path=f"sessions/{relative.as_posix()}",
                            content=session_path.read_bytes(),
                        )
                shutil.rmtree(staged_sessions, ignore_errors=True)
            elif target_sessions.exists():
                if backup_sessions.exists():
                    shutil.rmtree(backup_sessions, ignore_errors=True)
                os.rename(target_sessions, backup_sessions)
            if remote_sessions_target is None and staged_sessions.exists():
                os.rename(staged_sessions, target_sessions)

            wal_path.unlink(missing_ok=True)
            shm_path.unlink(missing_ok=True)
            os.replace(staged_db, CASHNERD_DB_PATH)
            wal_path.unlink(missing_ok=True)
            shm_path.unlink(missing_ok=True)
            _fsync_directory(CASHNERD_DATA_DIR)
            db_committed = True
        finally:
            if backup_sessions.exists() and (db_committed or target_sessions.exists()):
                shutil.rmtree(backup_sessions, ignore_errors=True)
            shutil.rmtree(staging, ignore_errors=True)

    def _locked_commit_snapshot(
        self,
        tar_bytes: bytes,
        snapshot_op_id: int,
        user_id: str,
        strict: bool = False,
    ) -> str:
        with InstallBootstrapLock(CASHNERD_DIR / "bootstrap.lock"):
            current_op_id, current_install_id, _status = (
                self._read_current_sync_state_sync()
            )
            if not strict and snapshot_op_id <= current_op_id and current_install_id:
                return current_install_id

            effective_install_id = current_install_id or str(uuid4())
            staging = self._extract_to_staging_sync(tar_bytes)
            staged_db = staging / "finance.db"
            with sqlite3.connect(str(staged_db)) as conn:
                conn.execute("PRAGMA journal_mode=DELETE")
                conn.execute("DELETE FROM _sync_changelog")
                conn.execute(
                    """
                    INSERT OR REPLACE INTO sync_state
                        (id, last_applied_op_id, install_id, subscriber_status)
                    VALUES (0, ?, ?, 'healthy')
                    """,
                    (int(snapshot_op_id), effective_install_id),
                )
                conn.commit()
            self._commit_staged_files_sync(staging, user_id=user_id)
            return effective_install_id

    def _clear_changelog(self, *, max_id: int | None = None) -> None:
        if not CASHNERD_DB_PATH.exists():
            return
        with sqlite3.connect(str(CASHNERD_DB_PATH)) as conn:
            try:
                if max_id is None:
                    conn.execute("DELETE FROM _sync_changelog")
                else:
                    conn.execute(
                        "DELETE FROM _sync_changelog WHERE id <= ?", (int(max_id),)
                    )
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc).lower():
                    raise
            conn.commit()

    def _clear_downstream_only_changelog(self) -> None:
        if not CASHNERD_DB_PATH.exists() or not sync_protocol.DOWNSTREAM_ONLY_TABLES:
            return
        placeholders = ", ".join("?" for _ in sync_protocol.DOWNSTREAM_ONLY_TABLES)
        with sqlite3.connect(str(CASHNERD_DB_PATH)) as conn:
            try:
                conn.execute(
                    f"DELETE FROM _sync_changelog WHERE table_name IN ({placeholders})",
                    tuple(sorted(sync_protocol.DOWNSTREAM_ONLY_TABLES)),
                )
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc).lower():
                    raise
            conn.commit()

    def _rebuild_fts(self) -> None:
        if not CASHNERD_DB_PATH.exists():
            return
        with sqlite3.connect(str(CASHNERD_DB_PATH)) as conn:
            try:
                conn.execute("INSERT INTO txn_fts(txn_fts) VALUES('rebuild')")
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc).lower():
                    raise
            conn.commit()
