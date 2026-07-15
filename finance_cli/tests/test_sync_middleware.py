from __future__ import annotations

import asyncio
import json

import mcp.types as mt
import pytest
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools.tool import ToolResult

from finance_cli.sync.exceptions import (
    SyncAuthError,
    SyncCatchupFailedError,
    SyncConflictError,
    SyncServerUnreachableError,
)
from finance_cli.sync.middleware import SyncMiddleware


def _context(name: str) -> MiddlewareContext[mt.CallToolRequestParams]:
    return MiddlewareContext(message=mt.CallToolRequestParams(name=name, arguments={"x": 1}))


def _tool_result(payload: dict[str, object] | None = None) -> ToolResult:
    body = payload or {"data": {"ok": True}, "summary": {}}
    return ToolResult(
        content=[mt.TextContent(type="text", text=json.dumps(body))],
        structured_content=body,
    )


class FakeEngine:
    def __init__(self) -> None:
        self.pull_calls = 0
        self.push_calls = 0
        self.force_pull_calls = 0
        self.strict_pull_calls = 0
        self.proxy_calls: list[tuple[str, dict[str, object]]] = []
        self.pull_result = True
        self.pull_error: Exception | None = None
        self.push_error: Exception | None = None
        self.proxy_error: Exception | None = None
        self.subscriber_status = "healthy"
        self.try_acquire_calls = 0
        self.try_acquire_result = True
        self.start_subscriber_calls = 0
        self.mark_subscriber_healthy_calls = 0
        self.current_op_id = 0
        self.clear_op_on_push = False
        self.operation_records: list[dict[str, object]] = []

    async def pull(self) -> bool:
        self.pull_calls += 1
        if self.pull_error is not None:
            raise self.pull_error
        return self.pull_result

    async def push(self) -> dict[str, object]:
        self.push_calls += 1
        if self.push_error is not None:
            raise self.push_error
        if self.clear_op_on_push:
            self.current_op_id = 0
        return {"status": "applied"}

    async def force_pull(self) -> None:
        self.force_pull_calls += 1

    async def _force_pull_strict(self) -> None:
        self.strict_pull_calls += 1
        if self.pull_error is not None:
            raise self.pull_error

    async def proxy_tool(self, tool_name: str, arguments: dict[str, object] | None = None) -> dict[str, object]:
        if self.proxy_error is not None:
            raise self.proxy_error
        self.proxy_calls.append((tool_name, dict(arguments or {})))
        return {"data": {"proxied": tool_name}, "summary": {}}

    def try_acquire_install_subscriber_lock(self) -> bool:
        self.try_acquire_calls += 1
        return self.try_acquire_result

    def start_subscriber(self) -> None:
        self.start_subscriber_calls += 1

    def mark_subscriber_healthy(self) -> None:
        self.mark_subscriber_healthy_calls += 1
        self.subscriber_status = "healthy"

    def current_changelog_id(self) -> int:
        return self.current_op_id

    def record_tool_operation(self, **kwargs) -> None:
        self.operation_records.append(dict(kwargs))


def test_middleware_routes_proxied_tools_without_local_execution() -> None:
    engine = FakeEngine()
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        raise AssertionError("proxied tools must not run locally")

    result = asyncio.run(middleware.on_call_tool(_context("plaid_sync"), call_next))

    assert engine.try_acquire_calls == 1
    assert engine.proxy_calls == [("plaid_sync", {"x": 1})]
    assert result.structured_content["data"]["proxied"] == "plaid_sync"


def test_middleware_pulls_once_pushes_after_db_write_and_retries_subscriber_start() -> None:
    engine = FakeEngine()
    middleware = SyncMiddleware(engine)
    local_calls: list[str] = []

    async def call_next(context):
        local_calls.append(context.message.name)
        return _tool_result()

    asyncio.run(middleware.on_call_tool(_context("txn_add"), call_next))
    asyncio.run(middleware.on_call_tool(_context("txn_list"), call_next))

    assert engine.pull_calls == 1
    assert engine.push_calls == 1
    assert engine.try_acquire_calls == 2
    assert engine.start_subscriber_calls >= 2
    assert local_calls == ["txn_add", "txn_list"]


def test_middleware_records_read_only_local_tool_operation() -> None:
    engine = FakeEngine()
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        return _tool_result({"data": {"items": []}, "summary": {"count": 0}})

    asyncio.run(middleware.on_call_tool(_context("txn_list"), call_next))

    record = engine.operation_records[-1]
    assert record["surface"] == "local_mcp"
    assert record["tool_name"] == "txn_list"
    assert record["status"] == "success"
    assert record["start_changelog_id"] == 0
    assert record["end_changelog_id"] == 0
    assert record["request_metadata"] == {
        "argument_count": 1,
        "argument_keys": ["x"],
        "mutating": False,
        "upload": False,
    }
    assert record["result_metadata"] == {
        "data_keys": ["items"],
        "has_errors": False,
        "result_keys": ["data", "summary"],
        "summary_keys": ["count"],
    }


def test_middleware_records_db_write_changelog_range_before_push_clears_it() -> None:
    engine = FakeEngine()
    engine.clear_op_on_push = True
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        engine.current_op_id = 12
        return _tool_result({"data": {"ok": True}, "summary": {}})

    asyncio.run(middleware.on_call_tool(_context("txn_add"), call_next))

    record = engine.operation_records[-1]
    assert engine.current_op_id == 0
    assert record["tool_name"] == "txn_add"
    assert record["status"] == "success"
    assert record["start_changelog_id"] == 0
    assert record["end_changelog_id"] == 12
    assert record["request_metadata"]["mutating"] is True


def test_middleware_records_local_tool_exception_before_reraising() -> None:
    engine = FakeEngine()
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(middleware.on_call_tool(_context("txn_list"), call_next))

    record = engine.operation_records[-1]
    assert record["tool_name"] == "txn_list"
    assert record["status"] == "error"
    assert record["error_metadata"] == {
        "exception_type": "RuntimeError",
        "message": "boom",
        "source": "exception",
    }


def test_middleware_returns_auth_error_before_tool_execution() -> None:
    engine = FakeEngine()
    engine.pull_error = SyncAuthError("Not authenticated. Run: python3 -m finance_cli.sync.login")
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        raise AssertionError("tool should not execute when pull auth fails")

    result = asyncio.run(middleware.on_call_tool(_context("txn_list"), call_next))

    assert result.structured_content["data"]["error"].startswith("Not authenticated.")
    assert "python3 -m finance_cli.sync.login" in result.structured_content["summary"]["errors"][0]


def test_middleware_appends_pull_warning_when_using_stale_local_db() -> None:
    engine = FakeEngine()
    engine.pull_result = False
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        return _tool_result()

    result = asyncio.run(middleware.on_call_tool(_context("txn_list"), call_next))

    assert "stale" in result.structured_content["summary"]["warnings"][0]


def test_middleware_replaces_result_with_conflict_details() -> None:
    engine = FakeEngine()
    engine.push_error = SyncConflictError({"status": "conflict"})
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        return _tool_result({"data": {"ok": True}, "summary": {}})

    result = asyncio.run(middleware.on_call_tool(_context("txn_add"), call_next))

    assert engine.force_pull_calls == 1
    assert result.structured_content["data"]["sync_conflict"]["status"] == "conflict"


def test_middleware_appends_offline_warning_after_write_push_failure() -> None:
    engine = FakeEngine()
    engine.push_error = SyncServerUnreachableError("offline")
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        return _tool_result({"data": {"ok": True}, "summary": {}})

    result = asyncio.run(middleware.on_call_tool(_context("txn_add"), call_next))

    assert "pending" in result.structured_content["summary"]["warnings"][0]


def test_middleware_appends_auth_error_after_write_push_failure() -> None:
    engine = FakeEngine()
    engine.push_error = SyncAuthError("Not authenticated. Run: python3 -m finance_cli.sync.login")
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        return _tool_result({"data": {"ok": True}, "summary": {}})

    result = asyncio.run(middleware.on_call_tool(_context("txn_add"), call_next))

    assert result.structured_content["data"]["ok"] is True
    assert "sync_auth_error" in result.structured_content["data"]
    assert "python3 -m finance_cli.sync.login" in result.structured_content["summary"]["errors"][0]


def test_middleware_returns_auth_error_for_proxied_tool() -> None:
    engine = FakeEngine()
    engine.proxy_error = SyncAuthError("Not authenticated. Run: python3 -m finance_cli.sync.login")
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        raise AssertionError("proxied tool should not execute locally")

    result = asyncio.run(middleware.on_call_tool(_context("plaid_sync"), call_next))

    assert result.structured_content["data"]["error"].startswith("Not authenticated.")


def test_middleware_degraded_status_forces_strict_pull_before_tool() -> None:
    engine = FakeEngine()
    engine.subscriber_status = "degraded"
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        return _tool_result()

    result = asyncio.run(middleware.on_call_tool(_context("txn_list"), call_next))

    assert engine.strict_pull_calls == 1
    assert engine.mark_subscriber_healthy_calls == 1
    assert result.structured_content["data"]["ok"] is True


def test_middleware_returns_error_when_degraded_recovery_is_offline() -> None:
    engine = FakeEngine()
    engine.subscriber_status = "degraded"
    engine.pull_error = SyncServerUnreachableError("offline")
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        raise AssertionError("tool should not execute when degraded recovery fails")

    result = asyncio.run(middleware.on_call_tool(_context("txn_list"), call_next))

    assert "degraded" in result.structured_content["data"]["error"]


def test_middleware_warns_when_push_catchup_fails() -> None:
    engine = FakeEngine()
    engine.push_error = SyncCatchupFailedError("catchup failed")
    middleware = SyncMiddleware(engine)

    async def call_next(_context):
        return _tool_result({"data": {"ok": True}, "summary": {}})

    result = asyncio.run(middleware.on_call_tool(_context("txn_add"), call_next))

    assert "full pull" in result.structured_content["summary"]["warnings"][0]
