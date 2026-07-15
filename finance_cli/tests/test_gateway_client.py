from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from finance_cli.telegram_bot.gateway_client import (
    BackendHTTPError,
    GatewayClient,
    SessionState,
    _parse_sse_event,
    parse_sse_events,
)


def test_parse_sse_event_reads_json_dict() -> None:
    assert _parse_sse_event('data: {"type":"text_delta","text":"hi"}') == {
        "type": "text_delta",
        "text": "hi",
    }


def test_parse_sse_event_ignores_non_data_lines() -> None:
    assert _parse_sse_event("event: message\nid: 1") is None


def test_parse_sse_event_joins_multiple_data_lines() -> None:
    assert _parse_sse_event('data: {"type":"text_delta",\ndata: "text":"hi"}') == {
        "type": "text_delta",
        "text": "hi",
    }


def test_parse_sse_event_skips_malformed_json() -> None:
    assert _parse_sse_event("data: {not-json}") is None


def test_parse_sse_events_returns_partial_buffer() -> None:
    events, remainder = parse_sse_events('data: {"type":"text_delta"}')

    assert events == []
    assert remainder == 'data: {"type":"text_delta"}'


def test_parse_sse_events_parses_multiple_events() -> None:
    events, remainder = parse_sse_events(
        'data: {"type":"one"}\n\n'
        'data: {"type":"two"}\n\n'
    )

    assert events == [{"type": "one"}, {"type": "two"}]
    assert remainder == ""


def test_parse_sse_events_normalizes_crlf() -> None:
    events, remainder = parse_sse_events('data: {"type":"one"}\r\n\r\ndata: {"type":"two"}\r\n\r\n')

    assert events == [{"type": "one"}, {"type": "two"}]
    assert remainder == ""


def test_ensure_session_caches_until_refresh_window() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(
            200,
            json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 2000},
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = GatewayClient("http://gateway", "key", http_client=http_client, time_fn=lambda: 1000)

    async def scenario() -> tuple[SessionState, SessionState]:
        first = await client.ensure_session()
        second = await client.ensure_session()
        return first, second

    first, second = asyncio.run(scenario())

    assert first == second
    assert calls == ["/api/chat/init"]


def test_ensure_session_refreshes_inside_300_second_window() -> None:
    responses = iter(
        [
            {"session_token": "tok-1", "session_id": "sess-1", "expires_at": 1200},
            {"session_token": "tok-2", "session_id": "sess-2", "expires_at": 2400},
        ]
    )
    now = {"value": 1000}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=next(responses))

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = GatewayClient("http://gateway", "key", http_client=http_client, time_fn=lambda: now["value"])

    async def scenario() -> tuple[SessionState, SessionState]:
        first = await client.ensure_session()
        now["value"] = 950
        second = await client.ensure_session(force_refresh=True)
        return first, second

    first, second = asyncio.run(scenario())

    assert first.token == "tok-1"
    assert second.token == "tok-2"


def test_ensure_session_force_refresh_bypasses_cache() -> None:
    counter = {"value": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        counter["value"] += 1
        return httpx.Response(
            200,
            json={
                "session_token": f"tok-{counter['value']}",
                "session_id": f"sess-{counter['value']}",
                "expires_at": 5000,
            },
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = GatewayClient("http://gateway", "key", http_client=http_client, time_fn=lambda: 1000)

    async def scenario() -> tuple[str, str]:
        first = await client.ensure_session()
        second = await client.ensure_session(force_refresh=True)
        return first.token, second.token

    first, second = asyncio.run(scenario())

    assert first == "tok-1"
    assert second == "tok-2"


def test_ensure_session_sends_top_level_user_id() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["json"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
        )

    client = GatewayClient(
        "http://gateway",
        "key",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    asyncio.run(client.ensure_session(user_id="telegram-user-123"))

    assert captured["json"] == {
        "api_key": "key",
        "context": {"channel": "telegram"},
        "user_id": "telegram-user-123",
    }


def test_ensure_session_resolves_user_bound_key() -> None:
    captured: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content.decode("utf-8")))
        return httpx.Response(
            200,
            json={
                "session_token": f"tok-{len(captured)}",
                "session_id": f"sess-{len(captured)}",
                "expires_at": 5000,
            },
        )

    client = GatewayClient(
        "http://gateway",
        lambda user_id: f"key-for-{user_id}",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        time_fn=lambda: 1000,
    )

    async def scenario() -> None:
        await client.ensure_session(user_id="1")
        await client.ensure_session(user_id="1")
        await client.ensure_session(user_id="2")

    asyncio.run(scenario())

    assert captured == [
        {"api_key": "key-for-1", "context": {"channel": "telegram"}, "user_id": "1"},
        {"api_key": "key-for-2", "context": {"channel": "telegram"}, "user_id": "2"},
    ]


def test_invalidate_session_clears_cached_session() -> None:
    client = GatewayClient("http://gateway", "key", http_client=httpx.AsyncClient())
    client._session = SessionState("tok", "sess", 2000)  # type: ignore[attr-defined]
    client._pinned_token = "tok"  # type: ignore[attr-defined]

    client.invalidate_session()

    assert client._session is None  # type: ignore[attr-defined]
    assert client._pinned_token is None  # type: ignore[attr-defined]


def test_seed_session_restores_cached_session_without_pinning_token() -> None:
    client = GatewayClient("http://gateway", "key", http_client=httpx.AsyncClient())
    client._pinned_token = "old-pinned"  # type: ignore[attr-defined]

    client.seed_session("tok-seeded", "sess-seeded", 4321)

    assert client._session == SessionState("tok-seeded", "sess-seeded", 4321)  # type: ignore[attr-defined]
    assert client._pinned_token is None  # type: ignore[attr-defined]


def test_ensure_session_raises_http_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"detail": "bad key"})

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = GatewayClient("http://gateway", "key", http_client=http_client)

    with pytest.raises(BackendHTTPError, match="401"):
        asyncio.run(client.ensure_session())


def test_stream_chat_yields_events_and_clears_pinned_token() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/init"):
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        body = b'data: {"type":"text_delta","text":"hi"}\n\n'
        return httpx.Response(200, content=body)

    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    client = GatewayClient("http://gateway", "key", http_client=http_client)

    async def scenario() -> tuple[list[dict[str, object]], str | None]:
        events = [event async for event in client.stream_chat([{"role": "user", "content": "hello"}])]
        return events, client._pinned_token  # type: ignore[attr-defined]

    events, pinned = asyncio.run(scenario())

    assert events == [{"type": "text_delta", "text": "hi"}]
    assert pinned is None


def test_stream_chat_includes_model_and_context() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/init"):
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        captured["json"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(200, content=b'data: {"type":"stream_complete"}\n\n')

    transport = httpx.MockTransport(handler)
    client = GatewayClient("http://gateway", "key", http_client=httpx.AsyncClient(transport=transport))

    async def scenario() -> None:
        async for _event in client.stream_chat(
            [{"role": "user", "content": "hello"}],
            context={"compaction": True},
            model="claude-opus-4-6",
        ):
            pass

    asyncio.run(scenario())

    assert captured["json"] == {
        "messages": [{"role": "user", "content": "hello"}],
        "context": {"compaction": True},
        "model": "claude-opus-4-6",
    }


def test_stream_chat_sends_top_level_user_id() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/init"):
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        captured["json"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(200, content=b'data: {"type":"stream_complete"}\n\n')

    client = GatewayClient(
        "http://gateway",
        "key",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    async def scenario() -> None:
        async for _event in client.stream_chat(
            [{"role": "user", "content": "hello"}],
            context={"channel": "telegram"},
            user_id="telegram-user-123",
        ):
            pass

    asyncio.run(scenario())

    assert captured["json"] == {
        "messages": [{"role": "user", "content": "hello"}],
        "context": {"channel": "telegram"},
        "user_id": "telegram-user-123",
    }


def test_stream_chat_raises_on_401() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/init"):
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        return httpx.Response(401, text="expired")

    client = GatewayClient(
        "http://gateway",
        "key",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    async def scenario() -> None:
        async for _event in client.stream_chat([{"role": "user", "content": "hello"}]):
            pass

    with pytest.raises(BackendHTTPError, match="401"):
        asyncio.run(scenario())


def test_stream_chat_raises_on_409() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/init"):
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        return httpx.Response(409, text="busy")

    client = GatewayClient(
        "http://gateway",
        "key",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    async def scenario() -> None:
        async for _event in client.stream_chat([{"role": "user", "content": "hello"}]):
            pass

    with pytest.raises(BackendHTTPError, match="409"):
        asyncio.run(scenario())


def test_submit_approval_uses_pinned_token() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers.get("Authorization")
        return httpx.Response(200, json={"status": "ok"})

    client = GatewayClient(
        "http://gateway",
        "key",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    client._pinned_token = "pinned-token"  # type: ignore[attr-defined]

    status_code, body = asyncio.run(client.submit_approval("tool-1", "nonce-1", True))

    assert status_code == 200
    assert body == {"status": "ok"}
    assert captured["authorization"] == "Bearer pinned-token"


@pytest.mark.parametrize("status_code", [404, 409, 410])
def test_submit_approval_returns_expected_status_codes(status_code: int) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"error": "expected"})

    client = GatewayClient(
        "http://gateway",
        "key",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    client._session = SessionState("tok-1", "sess-1", 5000)  # type: ignore[attr-defined]

    returned_status, body = asyncio.run(client.submit_approval("tool-1", "nonce-1", True))

    assert returned_status == status_code
    assert body == {"error": "expected"}


def test_submit_approval_raises_on_unexpected_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    client = GatewayClient(
        "http://gateway",
        "key",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    client._session = SessionState("tok-1", "sess-1", 5000)  # type: ignore[attr-defined]

    with pytest.raises(BackendHTTPError, match="500"):
        asyncio.run(client.submit_approval("tool-1", "nonce-1", True))


def test_close_only_closes_owned_client() -> None:
    http_client = httpx.AsyncClient()
    client = GatewayClient("http://gateway", "key", http_client=http_client)

    asyncio.run(client.close())

    assert http_client.is_closed is False
