"""Local MCP sync client package."""

from .auth import LocalAuth
from .config import (
    CASHNERD_AUTH_DIR,
    CASHNERD_AGENT_MEMORY_PATH,
    CASHNERD_CONFIG_PATH,
    CASHNERD_DATA_DIR,
    CASHNERD_DB_PATH,
    CASHNERD_DIR,
    CASHNERD_PENDING_CHANGESET_PATH,
    CASHNERD_RULES_PATH,
    CASHNERD_SKILL_STATE_PATH,
    CASHNERD_SYNC_DIR,
    CASHNERD_SYNC_LOG_PATH,
    CASHNERD_TOKEN_PATH,
    CASHNERD_UPLOADS_DIR,
    SyncConfig,
    ensure_dirs,
    load_config,
    save_config,
)
from .engine import SyncEngine
from .exceptions import (
    SyncAuthError,
    SyncCatchupFailedError,
    SyncConflictError,
    SyncDegradedError,
    SyncSchemaMismatchError,
    SyncServerUnreachableError,
)
from .middleware import SyncMiddleware
from .bootstrap_lock import InstallBootstrapLock
from .subscriber import ChangeFeedSubscriber
from .subscriber_lock import InstallSubscriberLock

_DERIVED_TOOL_CLASSIFICATIONS = frozenset(
    {
        "DB_WRITE_TOOLS",
        "NO_SYNC_TOOLS",
        "SERVER_PROXIED_TOOLS",
    }
)

__all__ = [
    "CASHNERD_AUTH_DIR",
    "CASHNERD_AGENT_MEMORY_PATH",
    "CASHNERD_CONFIG_PATH",
    "CASHNERD_DATA_DIR",
    "CASHNERD_DB_PATH",
    "CASHNERD_DIR",
    "CASHNERD_PENDING_CHANGESET_PATH",
    "CASHNERD_RULES_PATH",
    "CASHNERD_SKILL_STATE_PATH",
    "CASHNERD_SYNC_DIR",
    "CASHNERD_SYNC_LOG_PATH",
    "CASHNERD_TOKEN_PATH",
    "CASHNERD_UPLOADS_DIR",
    "DB_WRITE_TOOLS",
    "LocalAuth",
    "NO_SYNC_TOOLS",
    "SERVER_PROXIED_TOOLS",
    "ChangeFeedSubscriber",
    "InstallBootstrapLock",
    "InstallSubscriberLock",
    "SyncAuthError",
    "SyncCatchupFailedError",
    "SyncConfig",
    "SyncConflictError",
    "SyncDegradedError",
    "SyncEngine",
    "SyncMiddleware",
    "SyncSchemaMismatchError",
    "SyncServerUnreachableError",
    "ensure_dirs",
    "load_config",
    "save_config",
]


def __getattr__(name: str):
    if name in _DERIVED_TOOL_CLASSIFICATIONS:
        from . import tool_classification

        return getattr(tool_classification, name)
    raise AttributeError(name)
