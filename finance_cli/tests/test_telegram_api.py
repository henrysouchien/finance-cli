from __future__ import annotations

import asyncio
from typing import Any
from urllib import request as urllib_request

from finance_cli.telegram_bot.telegram_api import TelegramAPI


class _FakeResponse:
    def __init__(self, body: str | bytes) -> None:
        self._body = body.encode("utf-8") if isinstance(body, str) else body

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        return None

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            return self._body
        return self._body[:size]


def test_send_photo_posts_png_as_send_photo(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["data"] = req.data
        captured["timeout"] = timeout
        return _FakeResponse('{"ok": true, "result": {"message_id": 1}}')

    monkeypatch.setattr(urllib_request, "urlopen", fake_urlopen)

    api = TelegramAPI("bot-token")
    result = asyncio.run(
        api.send_photo(
            "12345",
            b"png-bytes",
            filename="chart.png",
            media_type="image/png",
            caption="Monthly chart",
        )
    )

    assert result == {"message_id": 1}
    assert captured["url"].endswith("/sendPhoto")
    assert b'name="photo"; filename="chart.png"' in captured["data"]
    assert b"Content-Type: image/png" in captured["data"]
    assert b"png-bytes" in captured["data"]
    assert b'name="caption"' in captured["data"]
    assert b"Monthly chart" in captured["data"]


def test_send_document_posts_svg_as_send_document(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["data"] = req.data
        captured["timeout"] = timeout
        return _FakeResponse('{"ok": true, "result": {"message_id": 2}}')

    monkeypatch.setattr(urllib_request, "urlopen", fake_urlopen)

    api = TelegramAPI("bot-token")
    result = asyncio.run(
        api.send_document(
            "12345",
            b"<svg></svg>",
            filename="chart.svg",
            media_type="image/svg+xml",
            caption="Vector chart",
        )
    )

    assert result == {"message_id": 2}
    assert captured["url"].endswith("/sendDocument")
    assert b'name="document"; filename="chart.svg"' in captured["data"]
    assert b"Content-Type: image/svg+xml" in captured["data"]
    assert b"<svg></svg>" in captured["data"]
    assert b"Vector chart" in captured["data"]


def test_send_photo_uses_send_document_for_svg(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_send_document(
        chat_id: str | int,
        document_bytes: bytes,
        *,
        filename: str = "document.bin",
        media_type: str = "application/octet-stream",
        caption: str | None = None,
    ) -> dict[str, Any]:
        captured.update(
            {
                "chat_id": chat_id,
                "document_bytes": document_bytes,
                "filename": filename,
                "media_type": media_type,
                "caption": caption,
            }
        )
        return {"message_id": 3}

    api = TelegramAPI("bot-token")
    monkeypatch.setattr(api, "send_document", fake_send_document)

    result = asyncio.run(
        api.send_photo(
            "12345",
            b"<svg></svg>",
            filename="chart.svg",
            media_type="image/svg+xml",
            caption="Vector chart",
        )
    )

    assert result == {"message_id": 3}
    assert captured == {
        "chat_id": "12345",
        "document_bytes": b"<svg></svg>",
        "filename": "chart.svg",
        "media_type": "image/svg+xml",
        "caption": "Vector chart",
    }


def test_set_webhook_posts_expected_payload(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_request(method: str, payload: dict[str, Any]) -> bool:
        captured["method"] = method
        captured["payload"] = payload
        return True

    api = TelegramAPI("bot-token")
    monkeypatch.setattr(api, "_request", fake_request)

    result = asyncio.run(
        api.set_webhook(
            "https://cashnerd.ai/api/telegram/webhook/user-1",
            secret_token="secret-123",
            allowed_updates=["message", "callback_query"],
            max_connections=1,
        )
    )

    assert result is True
    assert captured == {
        "method": "setWebhook",
        "payload": {
            "url": "https://cashnerd.ai/api/telegram/webhook/user-1",
            "secret_token": "secret-123",
            "allowed_updates": ["message", "callback_query"],
            "max_connections": 1,
        },
    }


def test_get_file_posts_file_id(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_request(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        captured["method"] = method
        captured["payload"] = payload
        return {"file_id": "doc-123", "file_path": "documents/statement.csv"}

    api = TelegramAPI("bot-token")
    monkeypatch.setattr(api, "_request", fake_request)

    result = asyncio.run(api.get_file("doc-123"))

    assert result == {"file_id": "doc-123", "file_path": "documents/statement.csv"}
    assert captured == {"method": "getFile", "payload": {"file_id": "doc-123"}}


def test_download_file_reads_from_file_endpoint_with_limit(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        return _FakeResponse(b"abcdef")

    monkeypatch.setattr(urllib_request, "urlopen", fake_urlopen)

    api = TelegramAPI("bot-token")
    result = asyncio.run(api.download_file("documents/my statement.csv", max_bytes=4))

    assert result == b"abcde"
    assert captured["url"].endswith("/file/botbot-token/documents/my%20statement.csv")


def test_delete_webhook_calls_api(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_request(method: str, payload: dict[str, Any]) -> bool:
        captured["method"] = method
        captured["payload"] = payload
        return True

    api = TelegramAPI("bot-token")
    monkeypatch.setattr(api, "_request", fake_request)

    result = asyncio.run(api.delete_webhook())

    assert result is True
    assert captured == {
        "method": "deleteWebhook",
        "payload": {},
    }


def test_get_webhook_info_calls_api(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_request(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        captured["method"] = method
        captured["payload"] = payload
        return {
            "url": "https://cashnerd.ai/api/telegram/webhook/user-1",
            "pending_update_count": 2,
            "last_error_message": "Connection timed out",
        }

    api = TelegramAPI("bot-token")
    monkeypatch.setattr(api, "_request", fake_request)

    result = asyncio.run(api.get_webhook_info())

    assert result == {
        "url": "https://cashnerd.ai/api/telegram/webhook/user-1",
        "pending_update_count": 2,
        "last_error_message": "Connection timed out",
    }
    assert captured == {
        "method": "getWebhookInfo",
        "payload": {},
    }
