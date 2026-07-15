"""Unix socket bridge for exposing finance MCP tools inside code execution."""
from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from agent_gateway import McpClientManager

_REQUEST_LIMIT = 1_048_576
_LIST_TOOLS = "__list_tools__"
_SERVER_RESERVED_ARG_KEYS = {"_approval_reason", "_request_id", "_session_id"}


def build_tool_catalog(
    mcp_client: McpClientManager,
    allowed_tools: frozenset[str],
) -> list[dict[str, str]]:
    """Return a compact catalog for the tools exposed through the bridge."""
    catalog: list[dict[str, str]] = []
    for tool_def in mcp_client.get_tool_definitions():
        name = tool_def.get("name")
        if not isinstance(name, str) or name not in allowed_tools:
            continue
        description = tool_def.get("description")
        catalog.append(
            {
                "name": name,
                "description": description.strip() if isinstance(description, str) else "",
            }
        )
    return sorted(catalog, key=lambda item: item["name"])


def build_client_source() -> str:
    """Return stdlib-only Python source for the in-sandbox finance client."""
    return """\
import json as _json
import socket as _socket

class FinanceClient:
    def __init__(self, socket_path=None, timeout=60):
        self._path = socket_path or _os.path.join(_WORK_DIR, "_finance.sock")
        self._timeout = timeout

    def call(self, tool_name, **params):
        request = _json.dumps(
            {"tool_name": tool_name, "params": params},
            separators=(",", ":"),
        ).encode("utf-8") + b"\\n"
        buffer = bytearray()
        try:
            with _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM) as sock:
                sock.settimeout(self._timeout)
                sock.connect(self._path)
                sock.sendall(request)
                while b"\\n" not in buffer:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    buffer.extend(chunk)
        except FileNotFoundError as exc:
            raise RuntimeError(f"finance bridge unavailable: {self._path}") from exc
        except TimeoutError as exc:
            raise RuntimeError(f"finance bridge timed out: {tool_name}") from exc
        except OSError as exc:
            raise RuntimeError(f"finance bridge request failed: {exc}") from exc

        if not buffer:
            raise RuntimeError("finance bridge returned no response")
        line = bytes(buffer).split(b"\\n", 1)[0]
        try:
            response = _json.loads(line.decode("utf-8"))
        except Exception as exc:
            raise RuntimeError("finance bridge returned invalid JSON") from exc
        if "error" in response:
            raise RuntimeError(str(response["error"]))
        return response.get("result")

    def tools(self):
        return self.call("__list_tools__")

_finance = FinanceClient()
"""


def build_client_module_source() -> str:
    """Return stdlib-only Python module source for the importable sandbox client."""
    return '''\
"""Finance bridge client for code execution sandbox."""
import json
import os
import socket


class FinanceClient:
    def __init__(self, socket_path=None, timeout=60):
        self._path = socket_path or os.path.join(os.path.dirname(__file__), "_finance.sock")
        self._timeout = timeout

    def call(self, tool_name, **params):
        request = json.dumps(
            {"tool_name": tool_name, "params": params},
            separators=(",", ":"),
        ).encode("utf-8") + b"\\n"
        buffer = bytearray()
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(self._timeout)
                sock.connect(self._path)
                sock.sendall(request)
                while b"\\n" not in buffer:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    buffer.extend(chunk)
        except FileNotFoundError as exc:
            raise RuntimeError(f"finance bridge unavailable: {self._path}") from exc
        except TimeoutError as exc:
            raise RuntimeError(f"finance bridge timed out: {tool_name}") from exc
        except OSError as exc:
            raise RuntimeError(f"finance bridge request failed: {exc}") from exc

        if not buffer:
            raise RuntimeError("finance bridge returned no response")
        line = bytes(buffer).split(b"\\n", 1)[0]
        try:
            response = json.loads(line.decode("utf-8"))
        except Exception as exc:
            raise RuntimeError("finance bridge returned invalid JSON") from exc
        if "error" in response:
            raise RuntimeError(str(response["error"]))
        return response.get("result")

    def tools(self):
        return self.call("__list_tools__")
'''


class FinanceBridgeServer:
    """Expose a read-only subset of finance tools over a Unix socket."""

    def __init__(
        self,
        socket_path: str,
        mcp_client: McpClientManager,
        user_paths: dict[str, str] | None,
        allowed_tools: frozenset[str],
        tool_timeout: float = 60.0,
    ) -> None:
        self._socket_path = os.fspath(socket_path)
        self._mcp_client = mcp_client
        self._user_paths = dict(user_paths or {})
        self._allowed_tools = frozenset(allowed_tools)
        self._tool_timeout = float(tool_timeout)
        self._server: asyncio.AbstractServer | None = None
        self._tool_catalog = build_tool_catalog(mcp_client, self._allowed_tools)

    async def start(self) -> None:
        """Start listening on the configured Unix socket path."""
        if self._server is not None:
            return
        parent_dir = os.path.dirname(self._socket_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        self._remove_socket_file()
        self._server = await asyncio.start_unix_server(
            self._handle_client,
            path=self._socket_path,
            limit=_REQUEST_LIMIT,
        )
        os.chmod(self._socket_path, 0o777)

    async def stop(self) -> None:
        """Stop the server and remove the socket file."""
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        self._remove_socket_file()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                response = await self._dispatch_request(line)
                writer.write(
                    json.dumps(response, separators=(",", ":"), default=str).encode("utf-8")
                    + b"\n"
                )
                await writer.drain()
        except asyncio.CancelledError:
            raise
        finally:
            writer.close()
            await writer.wait_closed()

    async def _dispatch_request(self, raw_request: bytes) -> dict[str, Any]:
        try:
            request = json.loads(raw_request.decode("utf-8"))
            if not isinstance(request, dict):
                raise ValueError("request must be a JSON object")
            tool_name = request.get("tool_name")
            if not isinstance(tool_name, str) or not tool_name.strip():
                raise ValueError("tool_name is required")
            params = request.get("params") or {}
            if not isinstance(params, dict):
                raise ValueError("params must be a JSON object")
            return {"result": await self._dispatch_tool(tool_name, params)}
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return {"error": str(exc)}

    async def _dispatch_tool(self, tool_name: str, params: dict[str, Any]) -> Any:
        if tool_name == _LIST_TOOLS:
            return list(self._tool_catalog)
        if tool_name not in self._allowed_tools:
            raise ValueError(f"Tool not allowed: {tool_name}")

        clean_input = {
            key: value
            for key, value in dict(params).items()
            if not str(key).startswith("_user_") and key not in _SERVER_RESERVED_ARG_KEYS
        }
        if self._user_paths:
            clean_input.update(self._user_paths)

        try:
            result, error = await asyncio.wait_for(
                self._mcp_client.call_tool(tool_name, clean_input),
                timeout=self._tool_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                f"Tool '{tool_name}' timed out after {self._tool_timeout:g}s"
            ) from exc

        if error:
            message = error.get("message") if isinstance(error, dict) else None
            raise RuntimeError(message or f"Tool call failed: {tool_name}")
        return result

    def _remove_socket_file(self) -> None:
        try:
            os.unlink(self._socket_path)
        except FileNotFoundError:
            pass
