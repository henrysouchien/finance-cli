"""Minimal async Telegram Bot API client built on urllib."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib import parse, request


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
            "allowed_updates": ["message", "callback_query"],
        }
        if offset is not None:
            payload["offset"] = offset
        result = await self._request("getUpdates", payload)
        return result if isinstance(result, list) else []

    async def set_webhook(
        self,
        url: str,
        *,
        secret_token: str,
        allowed_updates: list[str] | None = None,
        max_connections: int | None = None,
    ) -> bool:
        payload: dict[str, Any] = {
            "url": url,
            "secret_token": secret_token,
        }
        if allowed_updates is not None:
            payload["allowed_updates"] = allowed_updates
        if max_connections is not None:
            payload["max_connections"] = int(max_connections)
        result = await self._request("setWebhook", payload)
        return bool(result)

    async def delete_webhook(self) -> bool:
        result = await self._request("deleteWebhook", {})
        return bool(result)

    async def get_webhook_info(self) -> dict[str, Any]:
        result = await self._request("getWebhookInfo", {})
        return result if isinstance(result, dict) else {}

    async def get_file(self, file_id: str) -> dict[str, Any]:
        result = await self._request("getFile", {"file_id": file_id})
        return result if isinstance(result, dict) else {}

    async def download_file(
        self,
        file_path: str,
        *,
        max_bytes: int | None = None,
    ) -> bytes:
        return await asyncio.to_thread(self._download_file_blocking, file_path, max_bytes)

    async def send_message(
        self,
        chat_id: str | int,
        text: str,
        *,
        parse_mode: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "chat_id": str(chat_id),
            "text": text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        result = await self._request("sendMessage", payload)
        return result if isinstance(result, dict) else {}

    async def send_message_with_keyboard(
        self,
        chat_id: str | int,
        text: str,
        inline_keyboard: list[list[dict[str, str]]],
    ) -> dict[str, Any]:
        result = await self._request(
            "sendMessage",
            {
                "chat_id": str(chat_id),
                "text": text,
                "reply_markup": {"inline_keyboard": inline_keyboard},
            },
        )
        return result if isinstance(result, dict) else {}

    async def send_photo(
        self,
        chat_id: str | int,
        photo_bytes: bytes,
        *,
        filename: str = "chart.png",
        media_type: str = "image/png",
        caption: str | None = None,
    ) -> dict[str, Any]:
        if media_type == "image/svg+xml" or filename.lower().endswith(".svg"):
            return await self.send_document(
                chat_id,
                photo_bytes,
                filename=filename,
                media_type=media_type,
                caption=caption,
            )
        result = await asyncio.to_thread(
            self._send_photo_blocking,
            str(chat_id),
            photo_bytes,
            filename,
            media_type,
            caption,
        )
        return result if isinstance(result, dict) else {}

    async def send_document(
        self,
        chat_id: str | int,
        document_bytes: bytes,
        *,
        filename: str = "document.bin",
        media_type: str = "application/octet-stream",
        caption: str | None = None,
    ) -> dict[str, Any]:
        result = await asyncio.to_thread(
            self._send_document_blocking,
            str(chat_id),
            document_bytes,
            filename,
            media_type,
            caption,
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

    async def answer_callback_query(
        self, callback_query_id: str, text: str = ""
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        result = await self._request("answerCallbackQuery", payload)
        return result if isinstance(result, dict) else {}

    async def edit_message_text(
        self,
        chat_id: str | int,
        message_id: int,
        text: str,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "chat_id": str(chat_id),
            "message_id": message_id,
            "text": text,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        if parse_mode:
            payload["parse_mode"] = parse_mode
        result = await self._request("editMessageText", payload)
        return result if isinstance(result, dict) else {}

    async def _request(self, method: str, payload: dict[str, Any]) -> Any:
        return await asyncio.to_thread(self._request_blocking, method, payload)

    def _send_photo_blocking(
        self,
        chat_id: str,
        photo_bytes: bytes,
        filename: str,
        media_type: str,
        caption: str | None,
    ) -> Any:
        return self._send_multipart_blocking(
            method="sendPhoto",
            field_name="photo",
            chat_id=chat_id,
            file_bytes=photo_bytes,
            filename=filename,
            media_type=media_type,
            caption=caption,
        )

    def _send_document_blocking(
        self,
        chat_id: str,
        document_bytes: bytes,
        filename: str,
        media_type: str,
        caption: str | None,
    ) -> Any:
        return self._send_multipart_blocking(
            method="sendDocument",
            field_name="document",
            chat_id=chat_id,
            file_bytes=document_bytes,
            filename=filename,
            media_type=media_type,
            caption=caption,
        )

    def _send_multipart_blocking(
        self,
        *,
        method: str,
        field_name: str,
        chat_id: str,
        file_bytes: bytes,
        filename: str,
        media_type: str,
        caption: str | None,
    ) -> Any:
        import uuid

        # Sanitize user-facing strings to prevent header injection in multipart body
        filename = filename.replace("\r", "").replace("\n", "").replace('"', "'")
        if caption:
            caption = caption.replace("\r", "").replace("\n", " ")
        boundary = "----FormBoundary" + uuid.uuid4().hex[:16]
        parts: list[bytes] = [
            (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="chat_id"\r\n\r\n'
                f"{chat_id}\r\n"
            ).encode("utf-8"),
            (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'
                f"Content-Type: {media_type}\r\n\r\n"
            ).encode("utf-8"),
            file_bytes,
            b"\r\n",
        ]
        if caption:
            parts.append(
                (
                    f"--{boundary}\r\n"
                    'Content-Disposition: form-data; name="caption"\r\n\r\n'
                    f"{caption}\r\n"
                ).encode("utf-8")
            )
        parts.append(f"--{boundary}--\r\n".encode("utf-8"))
        body = b"".join(parts)
        url = f"https://api.telegram.org/bot{self._token}/{method}"
        req = request.Request(
            url,
            data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
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

    def _download_file_blocking(self, file_path: str, max_bytes: int | None) -> bytes:
        clean_path = str(file_path).lstrip("/")
        quoted_path = parse.quote(clean_path, safe="/")
        url = f"https://api.telegram.org/file/bot{self._token}/{quoted_path}"
        req = request.Request(url, method="GET")
        with request.urlopen(req, timeout=self._http_timeout) as resp:
            if max_bytes is None:
                return resp.read()
            return resp.read(max_bytes + 1)

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
