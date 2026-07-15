from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import mcp.types as mt
import pytest
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools.tool import ToolResult

import finance_cli.mcp_server as mcp_server
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


@contextmanager
def _gateway_context(db_path: Path) -> Iterator[None]:
    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        yield
    finally:
        reset_user_context(token)


def _tool_context() -> MiddlewareContext[mt.CallToolRequestParams]:
    return MiddlewareContext(message=mt.CallToolRequestParams(name="demo", arguments={}))


def _json_tool_result() -> ToolResult:
    return ToolResult(
        content=[
            mt.TextContent(
                type="text",
                text=json.dumps(
                    {
                        "data": {"file": "/data/finance/users/abc/file.csv"},
                        "summary": {"message": "Saved /var/www/finance_web/app.py"},
                    }
                ),
            )
        ],
        structured_content={
            "data": {"path": "/data/finance/users/abc/report.json"},
            "summary": {"warnings": ["See /var/www/finance_web/app.py"]},
        },
    )


def test_is_path_field_recognizes_file_and_path() -> None:
    assert mcp_server._is_path_field("file") is True
    assert mcp_server._is_path_field("path") is True


def test_sanitize_cache_payload_strips_absolute_file() -> None:
    payload = {"file": "/data/finance/users/abc/import.csv"}

    sanitized = mcp_server._sanitize_cache_payload(payload)

    assert sanitized["file"] == "import.csv"


def test_sanitize_cache_payload_preserves_relative_path() -> None:
    payload = {"path": "backups/manifest.json"}

    sanitized = mcp_server._sanitize_cache_payload(payload)

    assert sanitized["path"] == "backups/manifest.json"


def test_sanitize_cache_payload_list_branch_strips_absolute_and_preserves_relative() -> None:
    payload = {"files": ["/data/finance/users/abc/a.csv", "relative/b.csv", 7]}

    sanitized = mcp_server._sanitize_cache_payload(payload)

    assert sanitized["files"] == ["a.csv", "relative/b.csv", 7]


def test_scrub_server_paths_replaces_data_paths_with_basenames() -> None:
    text = "failed to read /data/finance/users/abc/file.csv"

    assert mcp_server._scrub_server_paths(text) == "failed to read file.csv"


def test_scrub_server_paths_replaces_var_paths_with_basenames() -> None:
    text = "traceback in /var/www/finance_web/app.py"

    assert mcp_server._scrub_server_paths(text) == "traceback in app.py"


def test_scrub_server_paths_replaces_entire_absolute_path_token() -> None:
    text = (
        "Failed decrypting DB for user '2': "
        "finance_cli/finance-web/data/users/2/finance.db"
    )

    assert mcp_server._scrub_server_paths(text) == (
        "Failed decrypting DB for user '2': finance.db"
    )


def test_scrub_server_paths_does_not_match_url_paths() -> None:
    text = "GET https://example.test/data/users/abc/file.csv"

    assert mcp_server._scrub_server_paths(text) == text


def test_scrub_server_paths_does_not_match_api_routes() -> None:
    text = "GET /api/v1/sessions"

    assert mcp_server._scrub_server_paths(text) == text


def test_sanitize_tool_result_strips_paths_from_json_textcontent_and_structured_content() -> None:
    result = ToolResult(
        content=[
            mt.TextContent(
                type="text",
                text=json.dumps(
                    {
                        "data": {"file": "/data/finance/users/abc/file.csv"},
                        "summary": {"message": "Saved /var/www/finance_web/app.py"},
                    }
                ),
            ),
            mt.TextContent(type="text", text="plain /data/finance/users/abc/file.csv"),
        ],
        structured_content={
            "data": {"path": "/data/finance/users/abc/report.json"},
            "summary": {"warnings": ["See /var/www/finance_web/app.py"]},
        },
    )

    sanitized = mcp_server._sanitize_tool_result(result)
    payload = json.loads(sanitized.content[0].text)

    assert payload["data"]["file"] == "file.csv"
    assert payload["summary"]["message"] == "Saved app.py"
    assert sanitized.content[1].text == "plain /data/finance/users/abc/file.csv"
    assert sanitized.structured_content["data"]["path"] == "report.json"
    assert sanitized.structured_content["summary"]["warnings"] == ["See app.py"]


def test_path_sanitize_middleware_skips_sanitization_in_cli_mode() -> None:
    result = _json_tool_result()

    async def call_next(_context):
        return result

    sanitized = asyncio.run(
        mcp_server.PathSanitizeMiddleware().on_call_tool(_tool_context(), call_next)
    )
    payload = json.loads(sanitized.content[0].text)

    assert sanitized is result
    assert payload["data"]["file"] == "/data/finance/users/abc/file.csv"
    assert sanitized.structured_content["data"]["path"] == "/data/finance/users/abc/report.json"


def test_path_sanitize_middleware_sanitizes_in_gateway_mode(tmp_path: Path) -> None:
    result = _json_tool_result()

    async def call_next(_context):
        return result

    with _gateway_context(tmp_path / "finance.db"):
        sanitized = asyncio.run(
            mcp_server.PathSanitizeMiddleware().on_call_tool(_tool_context(), call_next)
        )

    payload = json.loads(sanitized.content[0].text)

    assert payload["data"]["file"] == "file.csv"
    assert payload["summary"]["message"] == "Saved app.py"
    assert sanitized.structured_content["data"]["path"] == "report.json"
    assert sanitized.structured_content["summary"]["warnings"] == ["See app.py"]


def test_path_sanitize_middleware_scrubs_exc_args_in_gateway_mode(tmp_path: Path) -> None:
    async def call_next(_context):
        raise ValueError(
            "missing /data/finance/users/abc/file.csv and /var/www/finance_web/app.py"
        )

    with _gateway_context(tmp_path / "finance.db"):
        with pytest.raises(ValueError) as exc_info:
            asyncio.run(
                mcp_server.PathSanitizeMiddleware().on_call_tool(_tool_context(), call_next)
            )

    assert exc_info.value.args[0] == "missing file.csv and app.py"


def test_path_sanitize_middleware_scrubs_absolute_path_token_in_gateway_mode(
    tmp_path: Path,
) -> None:
    async def call_next(_context):
        raise ValueError(
            "Failed decrypting DB for user '2': "
            "finance_cli/finance-web/data/users/2/finance.db"
        )

    with _gateway_context(tmp_path / "finance.db"):
        with pytest.raises(ValueError) as exc_info:
            asyncio.run(
                mcp_server.PathSanitizeMiddleware().on_call_tool(_tool_context(), call_next)
            )

    assert exc_info.value.args[0] == "Failed decrypting DB for user '2': finance.db"


def test_path_sanitize_middleware_scrubs_oserror_filenames_in_gateway_mode(
    tmp_path: Path,
) -> None:
    async def call_next(_context):
        exc = OSError(1, "boom", "/data/finance/users/abc/file.csv")
        exc.filename2 = "/var/www/finance_web/app.py"
        raise exc

    with _gateway_context(tmp_path / "finance.db"):
        with pytest.raises(OSError) as exc_info:
            asyncio.run(
                mcp_server.PathSanitizeMiddleware().on_call_tool(_tool_context(), call_next)
            )

    assert exc_info.value.filename == "file.csv"
    assert exc_info.value.filename2 == "app.py"


def test_path_sanitize_middleware_does_not_scrub_exceptions_in_cli_mode() -> None:
    async def call_next(_context):
        raise ValueError("missing /data/finance/users/abc/file.csv")

    with pytest.raises(ValueError) as exc_info:
        asyncio.run(
            mcp_server.PathSanitizeMiddleware().on_call_tool(_tool_context(), call_next)
        )

    assert exc_info.value.args[0] == "missing /data/finance/users/abc/file.csv"
