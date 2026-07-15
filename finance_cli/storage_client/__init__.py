"""Client package for the Phase 4 storage server proxy.

The gRPC transport stack is intentionally loaded lazily. Local MCP startup and
plain SQLite CLI paths import this package for dispatch helpers, but they only
need the generated gRPC client when a remote storage call is actually made.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - type-checker only
    from .bootstrap import (
        RemoteBootstrapError,
        RemoteBootstrapResult,
        bootstrap_remote_empty_user,
    )
    from .connection import StorageConnection
    from .cursor import StorageCursor
    from .errors import (
        DenylistError,
        KmsUnavailable,
        MaintenanceModeError,
        PathInvalidError,
        SessionAborted,
        SessionExpired,
        SessionInvalid,
        StorageClientError,
        from_grpc_error,
    )
    from .sync_snapshot import RemoteSyncSnapshot, export_sync_snapshot

_LAZY_ATTRS = {
    "DenylistError": ("finance_cli.storage_client.errors", "DenylistError"),
    "KmsUnavailable": ("finance_cli.storage_client.errors", "KmsUnavailable"),
    "MaintenanceModeError": ("finance_cli.storage_client.errors", "MaintenanceModeError"),
    "PathInvalidError": ("finance_cli.storage_client.errors", "PathInvalidError"),
    "RemoteBootstrapError": ("finance_cli.storage_client.bootstrap", "RemoteBootstrapError"),
    "RemoteBootstrapResult": ("finance_cli.storage_client.bootstrap", "RemoteBootstrapResult"),
    "SessionAborted": ("finance_cli.storage_client.errors", "SessionAborted"),
    "SessionExpired": ("finance_cli.storage_client.errors", "SessionExpired"),
    "SessionInvalid": ("finance_cli.storage_client.errors", "SessionInvalid"),
    "StorageClientError": ("finance_cli.storage_client.errors", "StorageClientError"),
    "StorageConnection": ("finance_cli.storage_client.connection", "StorageConnection"),
    "StorageCursor": ("finance_cli.storage_client.cursor", "StorageCursor"),
    "RemoteSyncSnapshot": ("finance_cli.storage_client.sync_snapshot", "RemoteSyncSnapshot"),
    "export_sync_snapshot": ("finance_cli.storage_client.sync_snapshot", "export_sync_snapshot"),
    "from_grpc_error": ("finance_cli.storage_client.errors", "from_grpc_error"),
    "bootstrap_remote_empty_user": (
        "finance_cli.storage_client.bootstrap",
        "bootstrap_remote_empty_user",
    ),
}

__all__ = [
    "DenylistError",
    "KmsUnavailable",
    "MaintenanceModeError",
    "PathInvalidError",
    "RemoteBootstrapError",
    "RemoteBootstrapResult",
    "RemoteSyncSnapshot",
    "SessionAborted",
    "SessionExpired",
    "SessionInvalid",
    "StorageConnection",
    "StorageClientError",
    "StorageCursor",
    "connect",
    "errors",
    "export_sync_snapshot",
    "from_grpc_error",
    "bootstrap_remote_empty_user",
]


def __getattr__(name: str):
    if name == "errors":
        module = importlib.import_module("finance_cli.storage_client.errors")
        globals()[name] = module
        return module
    try:
        module_name, attr_name = _LAZY_ATTRS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    value = getattr(importlib.import_module(module_name), attr_name)
    globals()[name] = value
    return value


def connect(
    target: str,
    *,
    user_id: str,
    product: str = "finance_cli",
    scopes=None,
    **kwargs,
):
    from .connection import StorageConnection

    return StorageConnection(
        target,
        user_id=user_id,
        product=product,
        scopes=scopes,
        **kwargs,
    )
