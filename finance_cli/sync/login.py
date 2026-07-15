"""Browser login entry point for local CashNerd sync."""

from __future__ import annotations

import asyncio
import sys

from .auth import LocalAuth
from .config import load_config
from .exceptions import SyncAuthError


async def _run_login() -> None:
    config = load_config()
    auth = LocalAuth(config.server_url)
    await auth.run_browser_oauth()


def main() -> int:
    try:
        asyncio.run(_run_login())
    except KeyboardInterrupt:
        print("Authentication cancelled.", file=sys.stderr)
        return 130
    except SyncAuthError as exc:
        print(f"Authentication failed: {exc}", file=sys.stderr)
        return 1

    print("Authentication succeeded. Token stored in ~/.cashnerd/auth/token.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
