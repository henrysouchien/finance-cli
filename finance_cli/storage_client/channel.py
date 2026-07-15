"""Process-wide gRPC channel pooling for the storage server client."""

from __future__ import annotations

import collections
import threading
import time
import uuid
from typing import Any

import grpc

from .errors import record_storage_client_rpc

_CHANNEL_OPTIONS = (
    ("grpc.max_send_message_length", 8 * 1024 * 1024),
    ("grpc.max_receive_message_length", 8 * 1024 * 1024),
)
_CALL_ID_METADATA_KEY = "x-storage-call-id"


class _ClientCallDetails(
    collections.namedtuple(
        "_ClientCallDetails",
        ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression"),
    ),
    grpc.ClientCallDetails,
):
    pass


class _RpcTiming:
    def __init__(self, *, rpc: str, call_id: str, t_start: float) -> None:
        self._rpc = rpc
        self._call_id = call_id
        self._t_start = t_start
        self._lock = threading.Lock()
        self._recorded = False

    def record(self, status: str) -> None:
        with self._lock:
            if self._recorded:
                return
            self._recorded = True
        duration_us = max(int((time.monotonic() - self._t_start) * 1_000_000), 1)
        try:
            record_storage_client_rpc(
                rpc=self._rpc,
                status=status or "UNKNOWN",
                duration_us=duration_us,
                call_id=self._call_id,
            )
        except Exception:
            pass


class _UnaryUnaryTimingInterceptor(grpc.UnaryUnaryClientInterceptor):
    def intercept_unary_unary(self, continuation, client_call_details, request):
        call_details, timing = _prepare_call(client_call_details)
        try:
            call = continuation(call_details, request)
        except grpc.RpcError as exc:
            timing.record(_status_from_rpc_error(exc))
            raise
        except Exception:
            timing.record("UNKNOWN")
            raise
        return _record_future_on_done(call, timing)


class _StreamUnaryTimingInterceptor(grpc.StreamUnaryClientInterceptor):
    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        call_details, timing = _prepare_call(client_call_details)
        try:
            call = continuation(call_details, request_iterator)
        except grpc.RpcError as exc:
            timing.record(_status_from_rpc_error(exc))
            raise
        except Exception:
            timing.record("UNKNOWN")
            raise
        return _record_future_on_done(call, timing)


class _UnaryStreamTimingInterceptor(grpc.UnaryStreamClientInterceptor):
    def intercept_unary_stream(self, continuation, client_call_details, request):
        call_details, timing = _prepare_call(client_call_details)
        try:
            call = continuation(call_details, request)
        except grpc.RpcError as exc:
            timing.record(_status_from_rpc_error(exc))
            raise
        except Exception:
            timing.record("UNKNOWN")
            raise
        return _UnaryStreamTimingCall(call, timing)


class _UnaryStreamTimingCall:
    def __init__(self, call: Any, timing: _RpcTiming) -> None:
        self._call = call
        self._timing = timing
        self._iterator = iter(call)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._iterator)
        except StopIteration:
            self._timing.record(_status_from_call(self._call))
            raise
        except grpc.RpcError as exc:
            self._timing.record(_status_from_rpc_error(exc))
            raise
        except Exception:
            self._timing.record("UNKNOWN")
            raise

    def next(self):
        return self.__next__()

    def cancel(self) -> bool:
        cancel = getattr(self._call, "cancel", None)
        if cancel is None:
            self._timing.record("CANCELLED")
            return False
        try:
            return bool(cancel())
        finally:
            self._timing.record("CANCELLED")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._call, name)


class ChannelPool:
    """Thread-safe cache of one insecure gRPC channel per target."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._channels: dict[str, grpc.Channel] = {}

    def get(self, target: str) -> grpc.Channel:
        with self._lock:
            channel = self._channels.get(target)
            if channel is None:
                channel = grpc.insecure_channel(target, options=_CHANNEL_OPTIONS)
                # Production telemetry covers the default ChannelPool path; tests or callers
                # that inject a custom channel_pool must return an already-intercepted channel.
                channel = grpc.intercept_channel(
                    channel,
                    _UnaryUnaryTimingInterceptor(),
                    _StreamUnaryTimingInterceptor(),
                    _UnaryStreamTimingInterceptor(),
                )
                self._channels[target] = channel
            return channel

    def close_all(self) -> None:
        with self._lock:
            channels = list(self._channels.values())
            self._channels.clear()
        for channel in channels:
            close = getattr(channel, "close", None)
            if close is not None:
                close()


_default_pool = ChannelPool()


def get_channel(target: str) -> grpc.Channel:
    return _default_pool.get(target)


def _prepare_call(client_call_details: grpc.ClientCallDetails) -> tuple[_ClientCallDetails, _RpcTiming]:
    call_id = uuid.uuid4().hex
    method = getattr(client_call_details, "method", "")
    metadata = tuple(getattr(client_call_details, "metadata", None) or ())
    call_details = _ClientCallDetails(
        method,
        getattr(client_call_details, "timeout", None),
        metadata + ((_CALL_ID_METADATA_KEY, call_id),),
        getattr(client_call_details, "credentials", None),
        getattr(client_call_details, "wait_for_ready", None),
        getattr(client_call_details, "compression", None),
    )
    return call_details, _RpcTiming(rpc=_rpc_name(method), call_id=call_id, t_start=time.monotonic())


def _record_future_on_done(call: Any, timing: _RpcTiming) -> Any:
    add_done_callback = getattr(call, "add_done_callback", None)
    if add_done_callback is None:
        timing.record(_status_from_call(call))
        return call
    try:
        add_done_callback(lambda completed_call: timing.record(_status_from_call(completed_call)))
    except Exception:
        timing.record(_status_from_call(call))
    return call


def _rpc_name(method: Any) -> str:
    text = method.decode("utf-8", "replace") if isinstance(method, bytes) else str(method or "")
    rpc = text.rsplit("/", 1)[-1]
    return rpc or "unknown"


def _status_from_call(call: Any) -> str:
    if _safe_call(call, "cancelled") is True:
        return "CANCELLED"
    code = _safe_call(call, "code")
    if code is not None:
        return _status_name(code)
    exception = _safe_call(call, "exception")
    if isinstance(exception, grpc.RpcError):
        return _status_from_rpc_error(exception)
    return "OK"


def _status_from_rpc_error(rpc_error: grpc.RpcError) -> str:
    code = _safe_call(rpc_error, "code")
    if code is None:
        return "UNKNOWN"
    return _status_name(code)


def _status_name(status: Any) -> str:
    name = getattr(status, "name", None)
    if name:
        return str(name)
    text = str(status)
    if "." in text:
        return text.rsplit(".", 1)[-1]
    return text or "UNKNOWN"


def _safe_call(obj: Any, method_name: str) -> Any:
    method = getattr(obj, method_name, None)
    if method is None:
        return None
    try:
        return method()
    except Exception:
        return None
