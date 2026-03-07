"""Minimal async Telegram Bot API client built on urllib."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib import request


def split_message(text: str, max_len: int = 4096) -> list[str]:
    """Split a long message into Telegram-sized chunks."""
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= max_len:
            chunks.append(remaining)
            break

        split_at = remaining.rfind("\n", 0, max_len + 1)
        if split_at > 0:
            chunks.append(remaining[:split_at])
            remaining = remaining[split_at + 1 :]
        else:
            chunks.append(remaining[:max_len])
            remaining = remaining[max_len:]

    return chunks


class TelegramAPI:
    """Async wrapper around the Telegram Bot API."""

    def __init__(self, token: str, *, poll_timeout: int = 30) -> None:
        self._token = token
        self._http_timeout = poll_timeout + 5

    async def get_updates(self, offset: int | None, timeout: int) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "timeout": timeout,
            "allowed_updates": ["message"],
        }
        if offset is not None:
            payload["offset"] = offset
        result = await self._request("getUpdates", payload)
        return result if isinstance(result, list) else []

    async def send_message(self, chat_id: str | int, text: str) -> dict[str, Any]:
        result = await self._request(
            "sendMessage",
            {
                "chat_id": str(chat_id),
                "text": text,
            },
        )
        return result if isinstance(result, dict) else {}

    async def send_chat_action(self, chat_id: str | int, action: str) -> dict[str, Any]:
        result = await self._request(
            "sendChatAction",
            {
                "chat_id": str(chat_id),
                "action": action,
            },
        )
        return result if isinstance(result, dict) else {}

    async def edit_message_text(self, chat_id: str | int, message_id: int, text: str) -> dict[str, Any]:
        result = await self._request(
            "editMessageText",
            {
                "chat_id": str(chat_id),
                "message_id": message_id,
                "text": text,
            },
        )
        return result if isinstance(result, dict) else {}

    async def _request(self, method: str, payload: dict[str, Any]) -> Any:
        return await asyncio.to_thread(self._request_blocking, method, payload)

    def _request_blocking(self, method: str, payload: dict[str, Any]) -> Any:
        url = f"https://api.telegram.org/bot{self._token}/{method}"
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=self._http_timeout) as resp:
            raw = resp.read().decode("utf-8")

        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Unexpected Telegram response for {method}")
        if not parsed.get("ok"):
            description = parsed.get("description") or f"Telegram API error calling {method}"
            raise RuntimeError(str(description))
        return parsed.get("result")
