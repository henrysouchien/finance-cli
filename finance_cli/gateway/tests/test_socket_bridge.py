from __future__ import annotations

import asyncio
import copy
import inspect
import importlib
import json
import os
import sys
import tempfile
from typing import Any

import pytest

from finance_cli.gateway.socket_bridge import (
    FinanceBridgeServer,
    build_client_module_source,
    build_client_source,
)

_TMP_DIR = "/tmp" if os.path.isdir("/tmp") else None


class FakeMcpClient:
    def __init__(
        self,
        *,
        handlers: dict[str, Any] | None = None,
        tool_definitions: list[dict[str, Any]] | None = None,
    ) -> None:
        self._handlers = dict(handlers or {})
        self._tool_definitions = copy.deepcopy(tool_definitions or [])
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get_tool_definitions(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self._tool_definitions)

    async def call_tool(
        self,
        name: str,
        tool_input: dict[str, Any],
    ) -> tuple[Any | None, dict[str, Any] | None]:
        self.calls.append((name, dict(tool_input)))
        handler = self._handlers.get(name)
        if handler is None:
            return None, {"code": "unknown_tool", "message": f"Unknown tool: {name}"}
        result = handler(tool_input)
        if inspect.isawaitable(result):
            result = await result
        if (
            isinstance(result, tuple)
            and len(result) == 2
            and (result[1] is None or isinstance(result[1], dict))
        ):
            return result
        return result, None


async def _socket_request(
    socket_path: str,
    payload: dict[str, Any] | None = None,
    *,
    raw: bytes | None = None,
) -> dict[str, Any]:
    reader, writer = await asyncio.open_unix_connection(socket_path)
    try:
        if raw is not None:
            writer.write(raw)
        else:
            writer.write(json.dumps(payload).encode("utf-8") + b"\n")
        await writer.drain()
        line = await reader.readline()
        return json.loads(line.decode("utf-8"))
    finally:
        writer.close()
        await writer.wait_closed()


def _socket_path(tmpdir: str) -> str:
    return os.path.join(tmpdir, "_finance.sock")


def test_successful_tool_call() -> None:
    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
            mcp = FakeMcpClient(
                handlers={"txn_list": lambda params: {"data": {"transactions": [params]}}},
                tool_definitions=[{"name": "txn_list", "description": "List transactions"}],
            )
            bridge = FinanceBridgeServer(
                _socket_path(tmpdir),
                mcp,
                None,
                frozenset({"txn_list"}),
            )
            await bridge.start()
            try:
                response = await _socket_request(
                    _socket_path(tmpdir),
                    {"tool_name": "txn_list", "params": {"limit": 20}},
                )
            finally:
                await bridge.stop()

            assert response == {"result": {"data": {"transactions": [{"limit": 20}]}}}
            assert mcp.calls == [("txn_list", {"limit": 20})]

    asyncio.run(_run())


def test_disallowed_tool_rejected() -> None:
    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
            mcp = FakeMcpClient(
                handlers={"txn_list": lambda params: {"ok": params}},
                tool_definitions=[{"name": "txn_list", "description": "List transactions"}],
            )
            bridge = FinanceBridgeServer(
                _socket_path(tmpdir),
                mcp,
                None,
                frozenset({"db_status"}),
            )
            await bridge.start()
            try:
                response = await _socket_request(
                    _socket_path(tmpdir),
                    {"tool_name": "txn_list", "params": {"limit": 20}},
                )
            finally:
                await bridge.stop()

            assert "error" in response
            assert "Tool not allowed: txn_list" in response["error"]
            assert mcp.calls == []

    asyncio.run(_run())


def test_list_tools() -> None:
    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
            mcp = FakeMcpClient(
                tool_definitions=[
                    {"name": "txn_list", "description": "List transactions"},
                    {"name": "db_status", "description": "Show database status"},
                    {"name": "db_backup", "description": "Create backup"},
                ]
            )
            bridge = FinanceBridgeServer(
                _socket_path(tmpdir),
                mcp,
                None,
                frozenset({"txn_list", "db_status"}),
            )
            await bridge.start()
            try:
                response = await _socket_request(
                    _socket_path(tmpdir),
                    {"tool_name": "__list_tools__", "params": {}},
                )
            finally:
                await bridge.stop()

            assert response == {
                "result": [
                    {"name": "db_status", "description": "Show database status"},
                    {"name": "txn_list", "description": "List transactions"},
                ]
            }

    asyncio.run(_run())


def test_user_paths_injected() -> None:
    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
            mcp = FakeMcpClient(
                handlers={"txn_list": lambda params: {"ok": True}},
                tool_definitions=[{"name": "txn_list", "description": "List transactions"}],
            )
            bridge = FinanceBridgeServer(
                _socket_path(tmpdir),
                mcp,
                {
                    "_user_db_path": "/server/db.sqlite3",
                    "_user_rules_path": "/server/rules.yaml",
                },
                frozenset({"txn_list"}),
            )
            await bridge.start()
            try:
                response = await _socket_request(
                    _socket_path(tmpdir),
                    {
                        "tool_name": "txn_list",
                        "params": {
                            "limit": 5,
                            "_user_db_path": "/client/override.sqlite3",
                            "_user_uploads_dir": "/client/uploads",
                            "_request_id": "req-123",
                            "_session_id": "session-123",
                            "_approval_reason": "ignore this",
                        },
                    },
                )
            finally:
                await bridge.stop()

            assert response == {"result": {"ok": True}}
            assert mcp.calls == [
                (
                    "txn_list",
                    {
                        "limit": 5,
                        "_user_db_path": "/server/db.sqlite3",
                        "_user_rules_path": "/server/rules.yaml",
                    },
                )
            ]

    asyncio.run(_run())


def test_concurrent_requests() -> None:
    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
            async def handler(params: dict[str, Any]) -> dict[str, Any]:
                await asyncio.sleep(0.01 * (params["value"] % 3))
                return {"value": params["value"]}

            mcp = FakeMcpClient(
                handlers={"echo": handler},
                tool_definitions=[{"name": "echo", "description": "Echo values"}],
            )
            bridge = FinanceBridgeServer(
                _socket_path(tmpdir),
                mcp,
                None,
                frozenset({"echo"}),
            )
            await bridge.start()
            try:
                responses = await asyncio.gather(
                    *[
                        _socket_request(
                            _socket_path(tmpdir),
                            {"tool_name": "echo", "params": {"value": value}},
                        )
                        for value in range(8)
                    ]
                )
            finally:
                await bridge.stop()

            assert sorted(item["result"]["value"] for item in responses) == list(range(8))
            assert len(mcp.calls) == 8

    asyncio.run(_run())


def test_tool_timeout() -> None:
    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
            async def handler(_: dict[str, Any]) -> dict[str, Any]:
                await asyncio.sleep(0.05)
                return {"ok": True}

            mcp = FakeMcpClient(
                handlers={"slow_tool": handler},
                tool_definitions=[{"name": "slow_tool", "description": "Slow tool"}],
            )
            bridge = FinanceBridgeServer(
                _socket_path(tmpdir),
                mcp,
                None,
                frozenset({"slow_tool"}),
                tool_timeout=0.01,
            )
            await bridge.start()
            try:
                response = await _socket_request(
                    _socket_path(tmpdir),
                    {"tool_name": "slow_tool", "params": {}},
                )
            finally:
                await bridge.stop()

            assert "error" in response
            assert "timed out" in response["error"]

    asyncio.run(_run())


def test_server_cleanup() -> None:
    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
            mcp = FakeMcpClient()
            socket_path = _socket_path(tmpdir)
            bridge = FinanceBridgeServer(
                socket_path,
                mcp,
                None,
                frozenset(),
            )
            await bridge.start()
            assert os.path.exists(socket_path)
            await bridge.stop()
            assert not os.path.exists(socket_path)

    asyncio.run(_run())


def test_malformed_request() -> None:
    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
            mcp = FakeMcpClient(
                handlers={"db_status": lambda params: {"ok": params}},
                tool_definitions=[{"name": "db_status", "description": "Database status"}],
            )
            bridge = FinanceBridgeServer(
                _socket_path(tmpdir),
                mcp,
                None,
                frozenset({"db_status"}),
            )
            await bridge.start()
            try:
                bad_response = await _socket_request(_socket_path(tmpdir), raw=b"{bad json}\n")
                good_response = await _socket_request(
                    _socket_path(tmpdir),
                    {"tool_name": "db_status", "params": {"verbose": True}},
                )
            finally:
                await bridge.stop()

            assert "error" in bad_response
            assert good_response == {"result": {"ok": {"verbose": True}}}

    asyncio.run(_run())


def test_client_roundtrip() -> None:
    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
            mcp = FakeMcpClient(
                handlers={"db_status": lambda params: {"ok": params}},
                tool_definitions=[{"name": "db_status", "description": "Database status"}],
            )
            bridge = FinanceBridgeServer(
                _socket_path(tmpdir),
                mcp,
                None,
                frozenset({"db_status"}),
            )
            await bridge.start()
            try:
                namespace = {"_os": os, "_WORK_DIR": tmpdir}
                exec(build_client_source(), namespace)
                finance = namespace["_finance"]

                result = await asyncio.to_thread(finance.call, "db_status", verbose=True)
                tools = await asyncio.to_thread(finance.tools)
            finally:
                await bridge.stop()

            assert result == {"ok": {"verbose": True}}
            assert tools == [{"name": "db_status", "description": "Database status"}]

    asyncio.run(_run())


def test_client_connection_failure() -> None:
    with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
        namespace = {"_os": os, "_WORK_DIR": tmpdir}
        exec(build_client_source(), namespace)
        finance = namespace["_finance"]

        with pytest.raises(RuntimeError, match="finance bridge unavailable"):
            finance.call("db_status")


def test_client_source_valid_python() -> None:
    source = build_client_source()
    code = compile(source, "<finance_bridge>", "exec")
    namespace = {"_os": os, "_WORK_DIR": "/tmp/example"}
    exec(code, namespace)

    finance = namespace["_finance"]
    assert finance.__class__.__name__ == "FinanceClient"
    assert finance._path == "/tmp/example/_finance.sock"

    module_source = build_client_module_source()
    module_code = compile(module_source, "<finance_client>", "exec")
    module_namespace = {"__file__": "/tmp/example/finance_client.py"}
    exec(module_code, module_namespace)

    module_finance = module_namespace["FinanceClient"]()
    assert module_finance.__class__.__name__ == "FinanceClient"
    assert module_finance._path == "/tmp/example/_finance.sock"


def test_module_file_importable() -> None:
    with tempfile.TemporaryDirectory(prefix="fcb_", dir=_TMP_DIR) as tmpdir:
        module_path = os.path.join(tmpdir, "finance_client.py")
        with open(module_path, "w", encoding="utf-8") as handle:
            handle.write(build_client_module_source())

        importlib.invalidate_caches()
        sys.path.insert(0, tmpdir)
        sys.modules.pop("finance_client", None)
        try:
            finance_client = importlib.import_module("finance_client")
            finance = finance_client.FinanceClient()

            assert finance_client.FinanceClient.__name__ == "FinanceClient"
            assert finance._path == os.path.join(tmpdir, "_finance.sock")
            assert os.path.samefile(finance_client.__file__, module_path)
        finally:
            sys.modules.pop("finance_client", None)
            sys.path.remove(tmpdir)
