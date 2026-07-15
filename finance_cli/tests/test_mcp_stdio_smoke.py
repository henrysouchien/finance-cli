from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from scripts.smoke_mcp_stdio import McpSmokeError, smoke_mcp_stdio

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _write_fake_mcp_server(tmp_path: Path, *, tools: list[str]) -> Path:
    script = tmp_path / "fake_mcp_server.py"
    script.write_text(
        textwrap.dedent(
            f"""\
            import json
            import sys

            tools = {tools!r}

            for line in sys.stdin:
                request = json.loads(line)
                method = request.get("method")
                if method == "initialize":
                    response = {{
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": {{
                            "protocolVersion": request["params"]["protocolVersion"],
                            "capabilities": {{"tools": {{"listChanged": False}}}},
                            "serverInfo": {{"name": "fake", "version": "0"}},
                        }},
                    }}
                    print(json.dumps(response), flush=True)
                elif method == "notifications/initialized":
                    continue
                elif method == "tools/list":
                    response = {{
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": {{"tools": [{{"name": name}} for name in tools]}},
                    }}
                    print(json.dumps(response), flush=True)
                else:
                    response = {{
                        "jsonrpc": "2.0",
                        "id": request.get("id"),
                        "error": {{"code": -32601, "message": method}},
                    }}
                    print(json.dumps(response), flush=True)
            """
        ),
        encoding="utf-8",
    )
    return script


def test_smoke_mcp_stdio_initializes_and_lists_required_tools(tmp_path: Path) -> None:
    server = _write_fake_mcp_server(
        tmp_path,
        tools=["setup_status", "onboarding_detect", "financial_summary"],
    )

    result = smoke_mcp_stdio(
        [sys.executable, str(server)],
        home=tmp_path / "home",
        timeout=5,
        min_tools=3,
        required_tools=("setup_status", "onboarding_detect"),
    )

    assert result.tool_count == 3
    assert result.required_tools == ("setup_status", "onboarding_detect")


def test_smoke_mcp_stdio_fails_when_required_tool_is_missing(tmp_path: Path) -> None:
    server = _write_fake_mcp_server(tmp_path, tools=["setup_status"])

    with pytest.raises(McpSmokeError, match="missing required tools: onboarding_detect"):
        smoke_mcp_stdio(
            [sys.executable, str(server)],
            home=tmp_path / "home",
            timeout=5,
            min_tools=1,
            required_tools=("setup_status", "onboarding_detect"),
        )


def test_smoke_mcp_stdio_cli_supports_executable_args(tmp_path: Path) -> None:
    server = _write_fake_mcp_server(
        tmp_path,
        tools=["setup_status", "onboarding_detect"],
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/smoke_mcp_stdio.py",
            sys.executable,
            "--arg",
            str(server),
            "--arg",
            "serve",
            "--home",
            str(tmp_path / "home"),
            "--min-tools",
            "2",
            "--require-tool",
            "setup_status",
            "--require-tool",
            "onboarding_detect",
        ],
        capture_output=True,
        cwd=_PROJECT_ROOT,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "MCP stdio smoke passed: 2 tools discovered" in result.stdout
