"""FastMCP middleware for local pull/push sync behavior."""

from __future__ import annotations

import json
import time
from typing import Any

import mcp.types as mt
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

from finance_cli.operation_log import (
    exception_error_metadata,
    operation_error_metadata,
    operation_result_metadata,
    tool_request_metadata,
    utc_now_iso,
)

from . import tool_classification
from .engine import SyncEngine
from .exceptions import (
    SyncAuthError,
    SyncCatchupFailedError,
    SyncConflictError,
    SyncServerUnreachableError,
)


def _base_payload() -> dict[str, Any]:
    return {"data": {}, "summary": {}}


def _payload_from_result(result: ToolResult) -> dict[str, Any]:
    if isinstance(result.structured_content, dict):
        payload = dict(result.structured_content)
        payload.setdefault("data", {})
        payload.setdefault("summary", {})
        return payload
    for item in result.content:
        if isinstance(item, mt.TextContent):
            try:
                payload = json.loads(item.text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                payload.setdefault("data", {})
                payload.setdefault("summary", {})
                return payload
    return _base_payload()


def _tool_result_from_payload(payload: dict[str, Any]) -> ToolResult:
    return ToolResult(
        content=[mt.TextContent(type="text", text=json.dumps(payload))],
        structured_content=payload,
    )


def _error_result(message: str) -> ToolResult:
    payload = {
        "data": {"error": message},
        "summary": {"errors": [message]},
    }
    return _tool_result_from_payload(payload)


def _append_warning(result: ToolResult, message: str) -> ToolResult:
    payload = _payload_from_result(result)
    summary = payload.setdefault("summary", {})
    warnings = list(summary.get("warnings") or [])
    warnings.append(message)
    summary["warnings"] = warnings
    return _tool_result_from_payload(payload)


def _append_error(result: ToolResult, message: str) -> ToolResult:
    payload = _payload_from_result(result)
    payload.setdefault("data", {})["sync_auth_error"] = message
    summary = payload.setdefault("summary", {})
    errors = list(summary.get("errors") or [])
    errors.append(message)
    summary["errors"] = errors
    return _tool_result_from_payload(payload)


def _current_changelog_id(engine: SyncEngine) -> int:
    reader = getattr(engine, "current_changelog_id", None)
    if not callable(reader):
        return 0
    return int(reader())


def _record_tool_result(
    engine: SyncEngine,
    *,
    tool_name: str,
    arguments: dict[str, Any],
    started_at: str,
    started_monotonic: float,
    start_changelog_id: int,
    end_changelog_id: int,
    result: ToolResult,
) -> None:
    recorder = getattr(engine, "record_tool_operation", None)
    if not callable(recorder):
        return
    payload = _payload_from_result(result)
    error_metadata = operation_error_metadata(payload)
    recorder(
        surface="local_mcp",
        tool_name=tool_name,
        status="error" if error_metadata else "success",
        started_at=started_at,
        started_monotonic=started_monotonic,
        start_changelog_id=start_changelog_id,
        end_changelog_id=end_changelog_id,
        request_metadata=tool_request_metadata(
            arguments=arguments,
            mutating=tool_name in tool_classification._derived("DB_WRITE_TOOLS"),
        ),
        result_metadata=operation_result_metadata(payload),
        error_metadata=error_metadata,
    )


def _record_tool_exception(
    engine: SyncEngine,
    *,
    tool_name: str,
    arguments: dict[str, Any],
    started_at: str,
    started_monotonic: float,
    start_changelog_id: int,
    end_changelog_id: int,
    exc: BaseException,
) -> None:
    recorder = getattr(engine, "record_tool_operation", None)
    if not callable(recorder):
        return
    recorder(
        surface="local_mcp",
        tool_name=tool_name,
        status="error",
        started_at=started_at,
        started_monotonic=started_monotonic,
        start_changelog_id=start_changelog_id,
        end_changelog_id=end_changelog_id,
        request_metadata=tool_request_metadata(
            arguments=arguments,
            mutating=tool_name in tool_classification._derived("DB_WRITE_TOOLS"),
        ),
        error_metadata=exception_error_metadata(exc),
    )


def _conflict_result(result: ToolResult, exc: SyncConflictError) -> ToolResult:
    payload = _payload_from_result(result)
    payload.setdefault("data", {})["sync_conflict"] = exc.details
    summary = payload.setdefault("summary", {})
    warnings = list(summary.get("warnings") or [])
    warnings.append("Server state changed since the last pull. The local DB was refreshed from the authoritative copy.")
    summary["warnings"] = warnings
    return _tool_result_from_payload(payload)


class SyncMiddleware(Middleware):
    def __init__(self, engine: SyncEngine):
        self._engine = engine
        self._session_pulled = False

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        tool_name = context.message.name
        arguments = dict(context.message.arguments or {})
        started_at = utc_now_iso()
        started_monotonic = time.monotonic()

        recovery_error = await self._ensure_subscriber_ready()
        if recovery_error is not None:
            op_id = _current_changelog_id(self._engine)
            _record_tool_result(
                self._engine,
                tool_name=tool_name,
                arguments=arguments,
                started_at=started_at,
                started_monotonic=started_monotonic,
                start_changelog_id=op_id,
                end_changelog_id=op_id,
                result=recovery_error,
            )
            return recovery_error

        if tool_name in tool_classification._derived("SERVER_PROXIED_TOOLS"):
            try:
                payload = await self._engine.proxy_tool(tool_name, arguments)
            except SyncAuthError as exc:
                return _error_result(str(exc))
            return _tool_result_from_payload(payload)

        pull_fresh = True
        if not self._session_pulled:
            try:
                pull_fresh = await self._engine.pull()
            except SyncAuthError as exc:
                result = _error_result(str(exc))
                op_id = _current_changelog_id(self._engine)
                _record_tool_result(
                    self._engine,
                    tool_name=tool_name,
                    arguments=arguments,
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    start_changelog_id=op_id,
                    end_changelog_id=op_id,
                    result=result,
                )
                return result
            self._session_pulled = True
            self._maybe_start_subscriber()

        start_op_id = _current_changelog_id(self._engine)
        try:
            result = await call_next(context)
        except Exception as exc:
            end_op_id = _current_changelog_id(self._engine)
            _record_tool_exception(
                self._engine,
                tool_name=tool_name,
                arguments=arguments,
                started_at=started_at,
                started_monotonic=started_monotonic,
                start_changelog_id=start_op_id,
                end_changelog_id=end_op_id,
                exc=exc,
            )
            raise
        end_op_id = _current_changelog_id(self._engine)

        if not pull_fresh:
            result = _append_warning(
                result,
                "Sync server is unreachable. Using the local database, which may be stale.",
            )

        if tool_name in tool_classification._derived("DB_WRITE_TOOLS"):
            try:
                await self._engine.push()
            except SyncConflictError as exc:
                await self._engine.force_pull()
                self._maybe_start_subscriber()
                result = _conflict_result(result, exc)
            except SyncAuthError as exc:
                result = _append_error(result, str(exc))
            except SyncCatchupFailedError:
                result = _append_warning(
                    result,
                    "Local changes were kept, but the subscriber could not catch up to the server before push. A full pull will run on the next tool call.",
                )
            except SyncServerUnreachableError:
                result = _append_warning(
                    result,
                    "Local changes were kept, but the sync server is unreachable. They will remain pending until connectivity returns.",
                )

        _record_tool_result(
            self._engine,
            tool_name=tool_name,
            arguments=arguments,
            started_at=started_at,
            started_monotonic=started_monotonic,
            start_changelog_id=start_op_id,
            end_changelog_id=end_op_id,
            result=result,
        )
        return result

    async def _ensure_subscriber_ready(self) -> ToolResult | None:
        if getattr(self._engine, "subscriber_status", "healthy") == "degraded":
            try:
                await self._engine._force_pull_strict()
            except SyncAuthError as exc:
                return _error_result(str(exc))
            except SyncServerUnreachableError:
                return _error_result(
                    "Sync subscriber is degraded and the sync server is unreachable. A fresh pull is required before tools can run.",
                )
            mark_healthy = getattr(self._engine, "mark_subscriber_healthy", None)
            if callable(mark_healthy):
                mark_healthy()

        try_acquire = getattr(self._engine, "try_acquire_install_subscriber_lock", None)
        if callable(try_acquire) and try_acquire():
            self._maybe_start_subscriber()
        return None

    def _maybe_start_subscriber(self) -> None:
        start = getattr(self._engine, "start_subscriber", None)
        if callable(start):
            start()
