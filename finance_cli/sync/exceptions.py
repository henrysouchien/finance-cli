"""Sync-specific local client exceptions."""

from __future__ import annotations

from typing import Any

from finance_cli.exceptions import ConflictError


class SyncConflictError(Exception):
    """Raised when the server rejects a push with a 409 conflict."""

    def __init__(self, details: dict[str, Any]):
        self.details = details
        super().__init__("Sync conflict detected")


class SyncServerUnreachableError(Exception):
    """Raised when the sync server cannot be reached."""


class SyncAuthError(Exception):
    """Raised when local auth or sync session validation fails."""


class SyncDegradedError(Exception):
    """Raised when incremental subscriber replay is no longer trustworthy."""


class SyncCatchupFailedError(Exception):
    """Raised when a causal push cannot wait for the subscriber to catch up."""


class SyncSchemaMismatchError(Exception):
    """Raised when the server schema version is newer than the local copy."""

    def __init__(self, server_schema_version: int | None, client_schema_version: int | None):
        self.server_schema_version = server_schema_version
        self.client_schema_version = client_schema_version
        super().__init__(
            f"Schema mismatch: server={server_schema_version!r}, client={client_schema_version!r}"
        )


class SubscriberActiveError(ConflictError):
    """Raised when restore cannot proceed because another local MCP owns the install lock."""
