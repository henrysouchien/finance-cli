from __future__ import annotations

from finance_cli.sync.exceptions import (
    SyncAuthError,
    SyncCatchupFailedError,
    SyncConflictError,
    SyncDegradedError,
    SyncSchemaMismatchError,
    SyncServerUnreachableError,
)


def test_sync_conflict_error_retains_details() -> None:
    exc = SyncConflictError({"status": "conflict", "conflicts": [{"table": "transactions"}]})

    assert exc.details["status"] == "conflict"
    assert "Sync conflict" in str(exc)


def test_other_sync_exceptions_are_instantiable() -> None:
    assert str(SyncServerUnreachableError("offline")) == "offline"
    assert str(SyncAuthError("expired")) == "expired"
    assert str(SyncDegradedError("degraded")) == "degraded"
    assert str(SyncCatchupFailedError("catchup")) == "catchup"

    exc = SyncSchemaMismatchError(server_schema_version=57, client_schema_version=55)
    assert exc.server_schema_version == 57
    assert exc.client_schema_version == 55
