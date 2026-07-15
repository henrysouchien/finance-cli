"""Local stdio MCP entry point backed by the synced ~/.cashnerd data directory."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
from contextlib import asynccontextmanager, suppress

from finance_cli.config import CliSettings, load_dotenv, set_runtime_cli_settings
from finance_cli.db import _install_id_var, set_db_encryption_mode_override
from finance_cli.mcp_local_config import McpLocalSettings
from finance_cli.sync import (
    CASHNERD_DATA_DIR,
    CASHNERD_DB_PATH,
    CASHNERD_RULES_PATH,
    CASHNERD_UPLOADS_DIR,
    LocalAuth,
    SyncEngine,
    SyncMiddleware,
    ensure_dirs,
    load_config,
)

_LOCAL_CLI_SETTINGS = CliSettings.model_construct(
    data_dir=CASHNERD_DATA_DIR.expanduser().resolve(),
    db_path=None,
    user_id="default",
    web_data_root=None,
    db_encryption_mode="off",
    log_level="INFO",
)
set_runtime_cli_settings(_LOCAL_CLI_SETTINGS)
set_db_encryption_mode_override("off")

from finance_cli.mcp_server import (  # noqa: E402
    REGISTERED_TOOL_NAMES,
    mcp,
)
from finance_cli.tool_registry import validate_registry  # noqa: E402
from finance_cli.user_context import (  # noqa: E402
    UserContext,
    reset_user_context,
    set_user_context,
)

_CONFIGURED = False
_SYNC_MIDDLEWARE: SyncMiddleware | None = None
_SYNC_LIFESPAN_INSTALLED = False
_USER_CONTEXT_TOKEN = None
_SUBSCRIBER_LOCK_POLL_SECONDS = 10.0

logger = logging.getLogger(__name__)


def _reconcile_sessions_on_startup() -> None:
    target = CASHNERD_DATA_DIR / "sessions"
    backup = CASHNERD_DATA_DIR / "sessions.old"
    if not backup.exists():
        return
    if not target.exists():
        os.rename(backup, target)
    else:
        shutil.rmtree(backup, ignore_errors=True)


async def _poll_install_subscriber_lock(
    engine: SyncEngine,
    *,
    interval: float = _SUBSCRIBER_LOCK_POLL_SECONDS,
) -> None:
    while True:
        try:
            if engine.try_acquire_install_subscriber_lock():
                engine.start_subscriber()
        except Exception:
            logger.exception("local subscriber lock poll failed")
        await asyncio.sleep(interval)


def _install_sync_lifespan(engine: SyncEngine) -> None:
    global _SYNC_LIFESPAN_INSTALLED
    if _SYNC_LIFESPAN_INSTALLED:
        return
    original_lifespan = mcp._lifespan

    @asynccontextmanager
    async def _sync_lifespan(server):
        async with original_lifespan(server):
            engine.start_subscriber()
            poll_task = asyncio.create_task(
                _poll_install_subscriber_lock(
                    engine,
                    interval=_SUBSCRIBER_LOCK_POLL_SECONDS,
                ),
                name="cashnerd-subscriber-lock-poll",
            )
            try:
                yield
            finally:
                poll_task.cancel()
                with suppress(asyncio.CancelledError):
                    await poll_task
                await engine.stop_subscriber()

    mcp._lifespan = _sync_lifespan
    _SYNC_LIFESPAN_INSTALLED = True


def configure_local_mcp() -> SyncMiddleware:
    global _CONFIGURED, _SYNC_MIDDLEWARE, _USER_CONTEXT_TOKEN
    if _CONFIGURED and _SYNC_MIDDLEWARE is not None:
        return _SYNC_MIDDLEWARE

    load_dotenv()
    McpLocalSettings.from_env()
    set_runtime_cli_settings(_LOCAL_CLI_SETTINGS)
    set_db_encryption_mode_override("off")
    ensure_dirs()
    _reconcile_sessions_on_startup()
    config = load_config()
    if _USER_CONTEXT_TOKEN is not None:
        reset_user_context(_USER_CONTEXT_TOKEN)
    _USER_CONTEXT_TOKEN = set_user_context(
        UserContext.from_paths(
            db_path=CASHNERD_DB_PATH,
            rules_path=CASHNERD_RULES_PATH,
            uploads_dir=CASHNERD_UPLOADS_DIR,
            local_mode=True,
            expected_user_id=None,
        )
    )

    auth = LocalAuth(config.server_url)
    engine = SyncEngine(config, auth)
    install_id = engine.install_id
    if install_id:
        _install_id_var.set(install_id)
    engine.try_acquire_install_subscriber_lock()
    engine.start_subscriber()
    _install_sync_lifespan(engine)

    middleware = SyncMiddleware(engine)
    if not any(isinstance(existing, SyncMiddleware) for existing in mcp.middleware):
        mcp.middleware.insert(0, middleware)

    _SYNC_MIDDLEWARE = middleware
    _CONFIGURED = True
    return middleware


def main() -> None:
    configure_local_mcp()
    validate_registry(REGISTERED_TOOL_NAMES, strict=True)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
