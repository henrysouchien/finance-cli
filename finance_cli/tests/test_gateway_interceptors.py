from __future__ import annotations

import asyncio
import json

from agent_gateway.tool_dispatcher import InterceptContext

from finance_cli.gateway import interceptors as gateway_interceptors
from finance_cli.gateway.interceptors import (
    make_input_size_interceptor,
    make_rate_limit_interceptor,
)


def _ctx(*, session_id: str = "sess-1", tool_input: dict | None = None) -> InterceptContext:
    return InterceptContext(
        tool_call_id="tool-1",
        tool_name="goal_list",
        tool_input=tool_input or {},
        session_id=session_id,
    )


def test_rate_limit_allows_under_limit(monkeypatch) -> None:
    times = iter([1.0, 2.0])
    monkeypatch.setattr(gateway_interceptors, "_monotonic", lambda: next(times))
    interceptor = make_rate_limit_interceptor(2)

    first = asyncio.run(interceptor(_ctx()))
    second = asyncio.run(interceptor(_ctx()))

    assert first.action == "allow"
    assert second.action == "allow"


def test_rate_limit_denies_over_limit(monkeypatch) -> None:
    times = iter([1.0, 2.0, 3.0])
    monkeypatch.setattr(gateway_interceptors, "_monotonic", lambda: next(times))
    interceptor = make_rate_limit_interceptor(2)

    asyncio.run(interceptor(_ctx()))
    asyncio.run(interceptor(_ctx()))
    third = asyncio.run(interceptor(_ctx()))

    assert third.action == "deny"
    assert third.code == "rate_limit_exceeded"


def test_rate_limit_window_expiry_allows_again(monkeypatch) -> None:
    times = iter([1.0, 62.0])
    monkeypatch.setattr(gateway_interceptors, "_monotonic", lambda: next(times))
    interceptor = make_rate_limit_interceptor(1)

    first = asyncio.run(interceptor(_ctx()))
    second = asyncio.run(interceptor(_ctx()))

    assert first.action == "allow"
    assert second.action == "allow"


def test_rate_limit_isolates_sessions(monkeypatch) -> None:
    times = iter([1.0, 2.0, 3.0])
    monkeypatch.setattr(gateway_interceptors, "_monotonic", lambda: next(times))
    interceptor = make_rate_limit_interceptor(1)

    first = asyncio.run(interceptor(_ctx(session_id="sess-a")))
    second = asyncio.run(interceptor(_ctx(session_id="sess-b")))
    third = asyncio.run(interceptor(_ctx(session_id="sess-a")))

    assert first.action == "allow"
    assert second.action == "allow"
    assert third.action == "deny"


def test_input_size_allows_small_input() -> None:
    tool_input = {"symbol": "AAPL"}
    max_bytes = len(
        json.dumps(tool_input, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    )
    interceptor = make_input_size_interceptor(max_bytes)

    decision = asyncio.run(interceptor(_ctx(tool_input=tool_input)))

    assert decision.action == "allow"


def test_input_size_denies_large_input() -> None:
    tool_input = {"symbol": "AAPL"}
    max_bytes = (
        len(json.dumps(tool_input, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        - 1
    )
    interceptor = make_input_size_interceptor(max_bytes)

    decision = asyncio.run(interceptor(_ctx(tool_input=tool_input)))

    assert decision.action == "deny"
    assert decision.code == "tool_input_too_large"


def test_input_size_denies_unserializable_input() -> None:
    interceptor = make_input_size_interceptor(100)

    decision = asyncio.run(interceptor(_ctx(tool_input={"values": {1, 2, 3}})))

    assert decision.action == "deny"
    assert decision.code == "tool_input_not_serializable"
    assert decision.message == "tool input not serializable"


def test_input_size_measures_utf8_bytes_not_characters() -> None:
    tool_input = {"text": "漢"}
    serialized = json.dumps(tool_input, separators=(",", ":"), ensure_ascii=False)
    assert len(serialized.encode("utf-8")) > len(serialized)
    interceptor = make_input_size_interceptor(len(serialized))

    decision = asyncio.run(interceptor(_ctx(tool_input=tool_input)))

    assert decision.action == "deny"
    assert decision.code == "tool_input_too_large"
