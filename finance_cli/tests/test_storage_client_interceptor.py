from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import grpc
import pytest

from finance_cli.storage_client import channel, errors


class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self):
        return self._code


class FakeFuture:
    def __init__(
        self,
        *,
        code: grpc.StatusCode = grpc.StatusCode.OK,
        done: bool = True,
        exception: Exception | None = None,
        response: Any = "response",
    ) -> None:
        self._code = code
        self._done = done
        self._exception = exception
        self._response = response
        self._callbacks: list[Callable[[FakeFuture], None]] = []

    def add_done_callback(self, callback: Callable[["FakeFuture"], None]) -> None:
        self._callbacks.append(callback)
        if self._done:
            callback(self)

    def trigger(self) -> None:
        self._done = True
        for callback in list(self._callbacks):
            callback(self)

    def code(self):
        return self._code

    def cancelled(self) -> bool:
        return self._code is grpc.StatusCode.CANCELLED

    def exception(self, timeout=None):
        del timeout
        return self._exception

    def result(self, timeout=None):
        del timeout
        if self._exception is not None:
            raise self._exception
        return self._response


class FakeStream:
    def __init__(
        self,
        items: list[Any],
        *,
        error: grpc.RpcError | None = None,
    ) -> None:
        self._items = list(items)
        self._index = 0
        self._error = error
        self._code = grpc.StatusCode.OK

    def __iter__(self):
        return self

    def __next__(self):
        if self._index < len(self._items):
            item = self._items[self._index]
            self._index += 1
            return item
        if self._error is not None:
            self._code = self._error.code()
            raise self._error
        raise StopIteration

    def code(self):
        return self._code

    def cancelled(self) -> bool:
        return self._code is grpc.StatusCode.CANCELLED

    def cancel(self) -> bool:
        self._code = grpc.StatusCode.CANCELLED
        return True


class RootListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def captured_rpc_records(monkeypatch):
    records: list[dict[str, Any]] = []

    def record_storage_client_rpc(**kwargs: Any) -> None:
        records.append(kwargs)

    monkeypatch.setattr(channel, "record_storage_client_rpc", record_storage_client_rpc)
    return records


@pytest.fixture
def metric_file(monkeypatch, tmp_path: Path):
    path = tmp_path / "storage_client_metrics.jsonl"
    monkeypatch.setenv(errors.STORAGE_METRICS_PATH_ENV, str(path))
    errors._configure_metric_logger()
    try:
        yield path
    finally:
        monkeypatch.delenv(errors.STORAGE_METRICS_PATH_ENV, raising=False)
        errors._configure_metric_logger()


def test_unary_unary_success_records_ok_duration_and_call_id(captured_rpc_records) -> None:
    fake_call = FakeFuture()

    def continuation(client_call_details, request):
        del request
        assert _call_id_values(client_call_details)
        return fake_call

    returned = channel._UnaryUnaryTimingInterceptor().intercept_unary_unary(
        continuation,
        _details("/storage_server.v1.SqliteProxy/Execute"),
        object(),
    )

    assert returned is fake_call
    assert len(captured_rpc_records) == 1
    record = captured_rpc_records[0]
    assert record["rpc"] == "Execute"
    assert record["status"] == "OK"
    assert record["duration_us"] > 0
    assert re.fullmatch(r"[0-9a-f]{32}", record["call_id"])


def test_unary_unary_error_records_grpc_status_and_duration(captured_rpc_records) -> None:
    rpc_error = FakeRpcError(grpc.StatusCode.ABORTED)

    def continuation(client_call_details, request):
        del client_call_details, request
        raise rpc_error

    with pytest.raises(FakeRpcError):
        channel._UnaryUnaryTimingInterceptor().intercept_unary_unary(
            continuation,
            _details("/storage_server.v1.SqliteProxy/OpenSession"),
            object(),
        )

    assert len(captured_rpc_records) == 1
    record = captured_rpc_records[0]
    assert record["rpc"] == "OpenSession"
    assert record["status"] == "ABORTED"
    assert record["duration_us"] > 0
    assert re.fullmatch(r"[0-9a-f]{32}", record["call_id"])


def test_stream_unary_success_records_once_at_terminal_future(captured_rpc_records) -> None:
    fake_call = FakeFuture(done=False)

    returned = channel._StreamUnaryTimingInterceptor().intercept_stream_unary(
        lambda details, iterator: fake_call,
        _details("/storage_server.v1.SqliteProxy/WriteFile"),
        iter(()),
    )

    assert returned is fake_call
    assert captured_rpc_records == []
    fake_call.trigger()
    fake_call.trigger()
    assert len(captured_rpc_records) == 1
    assert captured_rpc_records[0]["rpc"] == "WriteFile"
    assert captured_rpc_records[0]["status"] == "OK"
    assert captured_rpc_records[0]["duration_us"] > 0


def test_stream_unary_error_records_once(captured_rpc_records) -> None:
    rpc_error = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
    fake_call = FakeFuture(
        code=grpc.StatusCode.UNAVAILABLE,
        done=False,
        exception=rpc_error,
    )

    channel._StreamUnaryTimingInterceptor().intercept_stream_unary(
        lambda details, iterator: fake_call,
        _details("/storage_server.v1.SqliteProxy/RestoreUserBackup"),
        iter(()),
    )

    fake_call.trigger()
    fake_call.trigger()
    assert len(captured_rpc_records) == 1
    assert captured_rpc_records[0]["rpc"] == "RestoreUserBackup"
    assert captured_rpc_records[0]["status"] == "UNAVAILABLE"


def test_unary_stream_full_consumption_records_once_on_exhaustion(captured_rpc_records) -> None:
    wrapped = channel._UnaryStreamTimingInterceptor().intercept_unary_stream(
        lambda details, request: FakeStream(["chunk-1", "chunk-2"]),
        _details("/storage_server.v1.SqliteProxy/ExportSyncSnapshot"),
        object(),
    )

    assert list(wrapped) == ["chunk-1", "chunk-2"]
    assert len(captured_rpc_records) == 1
    assert captured_rpc_records[0]["rpc"] == "ExportSyncSnapshot"
    assert captured_rpc_records[0]["status"] == "OK"
    assert captured_rpc_records[0]["duration_us"] > 0


def test_unary_stream_early_cancel_records_cancelled(captured_rpc_records) -> None:
    wrapped = channel._UnaryStreamTimingInterceptor().intercept_unary_stream(
        lambda details, request: FakeStream(["chunk-1", "chunk-2"]),
        _details("/storage_server.v1.SqliteProxy/ExportUserBackup"),
        object(),
    )

    assert next(wrapped) == "chunk-1"
    assert wrapped.cancel() is True

    assert len(captured_rpc_records) == 1
    assert captured_rpc_records[0]["rpc"] == "ExportUserBackup"
    assert captured_rpc_records[0]["status"] == "CANCELLED"
    assert captured_rpc_records[0]["duration_us"] > 0


def test_call_id_metadata_is_present_for_every_interceptor_variant(captured_rpc_records) -> None:
    del captured_rpc_records
    captures: list[channel._ClientCallDetails] = []

    def capture(details):
        captures.append(details)

    channel._UnaryUnaryTimingInterceptor().intercept_unary_unary(
        lambda details, request: (capture(details), FakeFuture())[1],
        _details("/storage_server.v1.SqliteProxy/Execute"),
        object(),
    )
    channel._StreamUnaryTimingInterceptor().intercept_stream_unary(
        lambda details, iterator: (capture(details), FakeFuture())[1],
        _details("/storage_server.v1.SqliteProxy/WriteFile"),
        iter(()),
    )
    wrapped = channel._UnaryStreamTimingInterceptor().intercept_unary_stream(
        lambda details, request: (capture(details), FakeStream([]))[1],
        _details("/storage_server.v1.SqliteProxy/ExportUserBackup"),
        object(),
    )
    list(wrapped)

    assert len(captures) == 3
    for details in captures:
        assert ("authorization", "Bearer token") in details.metadata
        call_ids = _call_id_values(details)
        assert len(call_ids) == 1
        assert re.fullmatch(r"[0-9a-f]{32}", call_ids[0])


def test_record_storage_client_rpc_writes_one_json_line(metric_file: Path) -> None:
    errors.record_storage_client_rpc(
        rpc="Execute",
        status="OK",
        duration_us=1234,
        call_id="a" * 32,
    )
    _flush_metric_logger()

    lines = metric_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "storage_client_rpc"
    assert payload["metric"] == "storage_client_rpc"
    assert payload["rpc"] == "Execute"
    assert payload["status"] == "OK"
    assert payload["duration_us"] == 1234
    assert payload["call_id"] == "a" * 32
    assert payload["ts"]
    assert payload["timestamp"]


def test_storage_client_metric_does_not_duplicate_to_root_logger(metric_file: Path) -> None:
    del metric_file
    root = logging.getLogger()
    handler = RootListHandler()
    root.addHandler(handler)
    try:
        errors.record_storage_client_rpc(
            rpc="Execute",
            status="OK",
            duration_us=99,
            call_id="b" * 32,
        )
        _flush_metric_logger()
    finally:
        root.removeHandler(handler)

    assert [
        record
        for record in handler.records
        if record.name == "finance_web.storage_client" or record.getMessage() == "storage_client_rpc"
    ] == []


def _details(method: str) -> channel._ClientCallDetails:
    return channel._ClientCallDetails(
        method,
        None,
        (("authorization", "Bearer token"),),
        None,
        None,
        None,
    )


def _call_id_values(client_call_details: channel._ClientCallDetails) -> list[str]:
    return [
        value
        for key, value in client_call_details.metadata
        if key == channel._CALL_ID_METADATA_KEY
    ]


def _flush_metric_logger() -> None:
    for handler in errors._METRIC_LOGGER.handlers:
        handler.flush()
