"""SqlParam conversion helpers for the storage client."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ._generated import storage_server_pb2 as pb2


Params = Mapping[str, Any] | Sequence[Any] | None


def to_sql_param(value: Any) -> pb2.SqlParam:
    """Convert a sqlite-compatible Python value to the proxy SqlParam oneof."""

    if value is None:
        return pb2.SqlParam(null=pb2.NULL_VALUE_UNSPECIFIED)
    if isinstance(value, bool):
        return pb2.SqlParam(integer=int(value))
    if isinstance(value, int):
        return pb2.SqlParam(integer=value)
    if isinstance(value, float):
        return pb2.SqlParam(real=value)
    if isinstance(value, str):
        return pb2.SqlParam(text=value)
    if isinstance(value, bytes):
        return pb2.SqlParam(blob=value)
    if isinstance(value, bytearray):
        return pb2.SqlParam(blob=bytes(value))
    if isinstance(value, memoryview):
        return pb2.SqlParam(blob=value.tobytes())
    raise TypeError(f"unsupported SQL parameter type: {type(value).__name__}")


def from_proto_value(param: pb2.SqlParam) -> object:
    """Convert a proxy SqlParam oneof back to the sqlite-style Python value."""

    kind = param.WhichOneof("v")
    if kind in {None, "null"}:
        return None
    if kind == "integer":
        return int(param.integer)
    if kind == "real":
        return float(param.real)
    if kind == "text":
        return str(param.text)
    if kind == "blob":
        return bytes(param.blob)
    raise TypeError(f"unsupported SqlParam oneof: {kind}")


def split_params(params: Params) -> tuple[list[pb2.SqlParam], dict[str, pb2.SqlParam]]:
    """Return positional and named proto params for an execute-style call."""

    if params is None:
        return [], {}
    if isinstance(params, Mapping):
        return [], {str(key): to_sql_param(value) for key, value in params.items()}
    if isinstance(params, (str, bytes, bytearray, memoryview)):
        raise TypeError("SQL parameters must be a sequence or mapping, not a scalar")
    return [to_sql_param(value) for value in params], {}


def to_bindings(params: Params) -> pb2.SqlBindings:
    """Convert one executemany binding set."""

    positional, named = split_params(params)
    binding = pb2.SqlBindings()
    binding.positional.extend(positional)
    copy_named_params(binding.named, named)
    return binding


def copy_named_params(target, named: dict[str, pb2.SqlParam]) -> None:
    for key, value in named.items():
        target[str(key)].CopyFrom(value)
