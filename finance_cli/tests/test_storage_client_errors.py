from __future__ import annotations

import logging

import grpc
import pytest

from finance_cli.storage_client import errors


class FakeRpcError(grpc.RpcError):
    def __init__(
        self,
        metadata=None,
        details: str = "audit_reason",
        code=grpc.StatusCode.UNKNOWN,
    ) -> None:
        self._metadata = metadata
        self._details = details
        self._code = code

    def trailing_metadata(self):
        return self._metadata

    def details(self):
        return self._details

    def code(self):
        return self._code


@pytest.mark.parametrize(
    ("code", "error_cls"),
    [
        ("SESSION_EXPIRED", errors.SessionExpired),
        ("SESSION_ABORTED", errors.SessionAborted),
        ("SESSION_INVALID", errors.SessionInvalid),
        ("DENYLIST", errors.DenylistError),
        ("PATH_INVALID", errors.PathInvalidError),
        ("KMS_UNAVAILABLE", errors.KmsUnavailable),
        ("MAINTENANCE_MODE", errors.MaintenanceModeError),
    ],
)
def test_from_grpc_error_maps_storage_codes(code: str, error_cls: type[Exception]) -> None:
    rpc_error = FakeRpcError(
        metadata=[("storage-server-error-code", code)],
        details=f"reason for {code}",
    )

    mapped = errors.from_grpc_error(rpc_error)

    assert isinstance(mapped, error_cls)
    assert mapped.reason == f"reason for {code}"
    assert str(mapped) == f"reason for {code}"


def test_from_grpc_error_prefers_reason_metadata() -> None:
    rpc_error = FakeRpcError(
        metadata=[
            ("storage-server-error-code", "DENYLIST"),
            ("storage-server-error-reason", "denylisted pragma"),
        ],
        details="grpc details",
    )

    mapped = errors.from_grpc_error(rpc_error)

    assert isinstance(mapped, errors.DenylistError)
    assert mapped.reason == "denylisted pragma"


def test_unknown_storage_code_returns_generic_error() -> None:
    rpc_error = FakeRpcError(
        metadata=[("storage-server-error-code", "SOMETHING_NEW")],
        details="new server code",
    )

    mapped = errors.from_grpc_error(rpc_error)

    assert type(mapped) is errors.StorageClientError
    assert mapped.reason == "new server code"


def test_rpc_error_without_trailing_metadata_returns_generic() -> None:
    rpc_error = FakeRpcError(metadata=None, details="plain grpc failure")

    mapped = errors.from_grpc_error(rpc_error)

    assert type(mapped) is errors.StorageClientError
    assert mapped.reason == "plain grpc failure"


def test_from_grpc_error_logs_storage_client_counter(caplog) -> None:
    del caplog
    rpc_error = FakeRpcError(
        metadata=[("storage-server-error-code", "KMS_UNAVAILABLE")],
        details="kms timeout",
        code=grpc.StatusCode.UNAVAILABLE,
    )
    records: list[logging.LogRecord] = []

    class ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = ListHandler()
    errors._METRIC_LOGGER.addHandler(handler)
    try:
        mapped = errors.from_grpc_error(rpc_error, rpc="OpenSession")
    finally:
        errors._METRIC_LOGGER.removeHandler(handler)

    assert isinstance(mapped, errors.KmsUnavailable)
    records = [record for record in records if record.getMessage() == "storage_client_error"]
    assert len(records) == 1
    assert records[0].event == "storage_client_error"
    assert records[0].metric == "storage_client_error"
    assert records[0].rpc == "OpenSession"
    assert records[0].status == "UNAVAILABLE"
    assert records[0].storage_server_error_code == "KMS_UNAVAILABLE"
