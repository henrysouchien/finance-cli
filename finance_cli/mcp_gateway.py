"""Gateway stdio MCP entrypoint.

This entrypoint exists so server-side stdio consumers do not execute the shared
`mcp_server` module as their startup path directly.
"""

from __future__ import annotations

from finance_cli.config import load_dotenv
from finance_cli.mcp_server import REGISTERED_TOOL_NAMES, mcp
from finance_cli.tool_registry import validate_registry


def main() -> None:
    load_dotenv()
    validate_registry(REGISTERED_TOOL_NAMES, strict=True)
    mcp.run()


if __name__ == "__main__":
    main()
