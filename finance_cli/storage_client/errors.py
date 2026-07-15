"""Typed exceptions for storage server client failures."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import WatchedFileHandler
from pathlib import Path
from typing import Any

import grpc

ERROR_CODE_METADATA_KEY = "storage-server-error-code"
ERROR_REASON_METADATA_KEYS = (
    "storage-server-error-reason",
    "storage-server-audit-reason",
    "storage-server-reason",
)
STORAGE_METRICS_PATH_ENV = "FINANCE_CLI_STORAGE_METRICS_PATH"
DEFAULT_STORAGE_METRICS_PATH = "/var/log/finance-web/storage_client_metrics.jsonl"
_METRIC_FIELD_ALLOWLIST = frozenset(
    {
        "event",
        "metric",
        "value",
        "rpc",
        "status",
        "duration_us",
        "call_id",
        "cached_age_us",
        "count",
        "outcome",
        "pool_size",
        "reason",
        "session_id",
        "storage_server_error_code",
        "ts",
        "user_id",
    }
)
_METRIC_LOGGER = logging.getLogger("finance_web.storage_client")


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        }
        for key in sorted(_METRIC_FIELD_ALLOWLIST):
            if key in record.__dict__:
                payload[key] = record.__dict__[key]
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _configure_metric_logger() -> None:
    for old_handler in list(_METRIC_LOGGER.handlers):
        old_handler.close()
    _METRIC_LOGGER.handlers.clear()
    _METRIC_LOGGER.setLevel(logging.INFO)
    _METRIC_LOGGER.propagate = False

    handler = _metric_file_handler()
    if handler is None:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.INFO)
        handler.setFormatter(_JSONFormatter())
    _METRIC_LOGGER.addHandler(handler)


def _metric_file_handler() -> logging.Handler | None:
    path = _metric_log_path()
    if path is None:
        return None
    try:
        if os.getenv(STORAGE_METRICS_PATH_ENV):
            path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(mode=0o640, exist_ok=True)
        try:
            os.chmod(path, 0o640)
        except OSError:
            pass
        handler = WatchedFileHandler(path)
        handler.setLevel(logging.INFO)
        handler.setFormatter(_JSONFormatter())
        return handler
    except OSError:
        return None


def _metric_log_path() -> Path | None:
    env_path = os.getenv(STORAGE_METRICS_PATH_ENV)
    if env_path:
        return Path(env_path).expanduser()
    default = Path(DEFAULT_STORAGE_METRICS_PATH)
    if _path_writable(default):
        return default
    return None


def _path_writable(path: Path) -> bool:
    if path.exists():
        return os.access(path, os.W_OK)
    parent = path.parent
    return parent.exists() and os.access(parent, os.W_OK)


def _utc_ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class StorageClientError(Exception):
    """Base for all storage_client errors."""

    def __init__(self, reason: str | None = None) -> None:
        self.reason = reason or self.__class__.__name__
        super().__init__(self.reason)


class SessionExpired(StorageClientError):
    """Idle-reaped session; transparent retry safe (handled in Batch B)."""


class SessionAborted(StorageClientError):
    """Non-retryable: max-lifetime, proxy restart, force-closed by admin."""


class SessionInvalid(StorageClientError):
    """Non-retryable: auth/synthetic check failed."""


class DenylistError(StorageClientError):
    """SQL blocked by proxy denylist."""


class PathInvalidError(StorageClientError):
    """File RPC path validation failed."""


class KmsUnavailable(StorageClientError):
    """Transient KMS failure; caller may retry at higher layer."""


class MaintenanceModeError(StorageClientError):
    """User's storage_mode is 'migrating' or 'replaying'; surface 503."""


_ERROR_CODE_MAP: dict[str, type[StorageClientError]] = {
    "SESSION_EXPIRED": SessionExpired,
    "SESSION_ABORTED": SessionAborted,
    "SESSION_INVALID": SessionInvalid,
    "DENYLIST": DenylistError,
    "PATH_INVALID": PathInvalidError,
    "KMS_UNAVAILABLE": KmsUnavailable,
    "MAINTENANCE_MODE": MaintenanceModeError,
}


def from_grpc_error(rpc_error: grpc.RpcError, *, rpc: str | None = None) -> StorageClientError:
    """Map storage server trailing metadata to a typed client exception."""

    metadata = _metadata_to_dict(_safe_call(rpc_error, "trailing_metadata") or ())
    storage_code = metadata.get(ERROR_CODE_METADATA_KEY)
    reason = _reason_from_error(rpc_error, metadata)
    if rpc is not None:
        record_storage_client_error(
            rpc,
            _grpc_status_name(rpc_error),
            reason=reason,
            storage_server_error_code=storage_code,
        )
    error_cls = _ERROR_CODE_MAP.get(str(storage_code or "").strip().upper(), StorageClientError)
    return error_cls(reason)


def record_storage_client_error(
    rpc: str,
    status: str,
    *,
    reason: str | None = None,
    storage_server_error_code: str | None = None,
) -> None:
    """Emit a structured counter event consumed by finance-web log metrics."""

    extra = {
        "event": "storage_client_error",
        "metric": "storage_client_error",
        "value": 1,
        "rpc": str(rpc or "unknown"),
        "status": str(status or "UNKNOWN"),
        "ts": _utc_ts(),
    }
    if reason:
        extra["reason"] = str(reason)
    if storage_server_error_code:
        extra["storage_server_error_code"] = str(storage_server_error_code)
    _METRIC_LOGGER.info("storage_client_error", extra=extra)


def record_storage_client_rpc(
    rpc: str,
    status: str,
    duration_us: int,
    call_id: str | None = None,
    **extra: Any,
) -> None:
    """Emit one structured client-observed storage RPC latency event."""

    payload = {
        "event": "storage_client_rpc",
        "metric": "storage_client_rpc",
        "rpc": str(rpc or "unknown"),
        "status": str(status or "UNKNOWN"),
        "duration_us": max(int(duration_us), 0),
        "call_id": str(call_id) if call_id else None,
        "ts": _utc_ts(),
    }
    payload.update({str(key): value for key, value in extra.items()})
    _METRIC_LOGGER.info("storage_client_rpc", extra=payload)


def record_storage_session_pool_event(event_name: str, **fields: Any) -> None:
    """Emit one structured storage session-pool event."""

    payload = {
        "event": str(event_name),
        "ts": _utc_ts(),
    }
    payload.update({str(key): value for key, value in fields.items()})
    _METRIC_LOGGER.info(str(event_name), extra=payload)


def _metadata_to_dict(metadata: Any) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in metadata or ():
        try:
            key, value = item
        except (TypeError, ValueError):
            continue
        key_s = _to_text(key).lower()
        if key_s:
            result[key_s] = _to_text(value)
    return result


def _reason_from_error(rpc_error: grpc.RpcError, metadata: dict[str, str]) -> str:
    for key in ERROR_REASON_METADATA_KEYS:
        value = metadata.get(key)
        if value:
            return value
    details = _safe_call(rpc_error, "details")
    if details:
        return _to_text(details)
    code = metadata.get(ERROR_CODE_METADATA_KEY)
    if code:
        return code
    return "unknown storage server error"


def _safe_call(obj: object, method_name: str) -> Any:
    method = getattr(obj, method_name, None)
    if method is None:
        return None
    try:
        return method()
    except Exception:
        return None


def _grpc_status_name(rpc_error: grpc.RpcError) -> str:
    status = _safe_call(rpc_error, "code")
    if status is None:
        return "UNKNOWN"
    name = getattr(status, "name", None)
    if name:
        return str(name)
    text = str(status)
    if "." in text:
        return text.rsplit(".", 1)[-1]
    return text or "UNKNOWN"


def _to_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


_configure_metric_logger()
