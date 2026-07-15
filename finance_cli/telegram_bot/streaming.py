"""Streaming helpers for Telegram draft responses."""

from __future__ import annotations

import asyncio
import html
import logging
import re
import time
from typing import Any, Callable

from .telegram_api import TelegramAPI

log = logging.getLogger(__name__)

_TOOL_START = "\x00TS:"
_TOOL_END = "\x00"
_TOOL_RE = re.compile(r"\x00TS:(.*?)\x00", re.DOTALL)


def _markdown_to_telegram_html(text: str) -> str:
    """Convert markdown text (with optional tool-status markers) to Telegram HTML.

    Handles: fenced code blocks, inline code, bold, italic, and tool-status
    markers inserted by ``send_tool_status``.  Everything else is HTML-escaped
    so that raw ``<`` / ``>`` / ``&`` in the source don't break ``parse_mode=HTML``.
    """
    # 1. Extract tool-status markers → <i>…</i>
    tool_parts: list[str] = []

    def _save_tool(m: re.Match[str]) -> str:
        tool_parts.append(f"<i>{html.escape(m.group(1))}</i>")
        return f"\x01T{len(tool_parts) - 1}\x01"

    text = _TOOL_RE.sub(_save_tool, text)

    # 2. Extract fenced code blocks → <pre>…</pre>
    code_blocks: list[str] = []

    def _save_block(m: re.Match[str]) -> str:
        code_blocks.append(f"<pre>{html.escape(m.group(1))}</pre>")
        return f"\x01B{len(code_blocks) - 1}\x01"

    text = re.sub(r"```(?:\w*\n)?(.*?)```", _save_block, text, flags=re.DOTALL)

    # 3. Extract inline code → <code>…</code>
    inline_codes: list[str] = []

    def _save_inline(m: re.Match[str]) -> str:
        inline_codes.append(f"<code>{html.escape(m.group(1))}</code>")
        return f"\x01I{len(inline_codes) - 1}\x01"

    text = re.sub(r"`([^`]+)`", _save_inline, text)

    # 4. HTML-escape remaining text (preserves \x01 placeholders)
    text = html.escape(text)

    # 5. Markdown bold / italic
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", text)

    # 6. Restore placeholders (order doesn't matter)
    for i, part in enumerate(inline_codes):
        text = text.replace(f"\x01I{i}\x01", part)
    for i, part in enumerate(code_blocks):
        text = text.replace(f"\x01B{i}\x01", part)
    for i, part in enumerate(tool_parts):
        text = text.replace(f"\x01T{i}\x01", part)

    return text


class DraftStream:
    """Throttle Telegram sends/edits while text is still streaming."""

    def __init__(
        self,
        api: TelegramAPI,
        chat_id: str | int,
        *,
        throttle_seconds: float = 1.5,
        min_initial_chars: int = 100,
        max_chars: int = 4096,
        now_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self._api = api
        self._chat_id = chat_id
        self._throttle_seconds = throttle_seconds
        self._min_initial_chars = min_initial_chars
        self._max_chars = max_chars
        self._now_fn = now_fn
        self._sleep_fn = sleep_fn
        self._buffer = ""
        self._current_message_id: int | None = None
        self._current_start = 0
        self._last_send_time = 0.0

    async def append(self, text: str) -> None:
        if not text:
            return
        self._buffer += text  # raw markdown — formatted at send time
        await self._flush(force=False, bypass_initial=False)

    async def send_tool_status(self, text: str) -> None:
        if not text:
            return
        if self._buffer and not self._buffer.endswith("\n"):
            self._buffer += "\n"
        self._buffer += f"{_TOOL_START}{text}{_TOOL_END}"
        await self._flush(force=True, bypass_initial=True)

    async def finish(self) -> None:
        await self._flush(force=True, bypass_initial=True)

    def _current_chunk(self) -> str:
        return self._buffer[self._current_start :]

    async def _flush(self, *, force: bool, bypass_initial: bool) -> None:
        while len(self._current_chunk()) > self._max_chars:
            chunk = self._current_chunk()[: self._max_chars]
            await self._upsert(chunk, force=True)
            self._current_start += self._max_chars
            self._current_message_id = None

        chunk = self._current_chunk()
        if not chunk:
            return
        if (
            not force
            and not bypass_initial
            and self._current_message_id is None
            and len(chunk) < self._min_initial_chars
        ):
            return

        await self._upsert(chunk, force=force)

    async def _upsert(self, text: str, *, force: bool) -> None:
        formatted = _markdown_to_telegram_html(text)
        if self._current_message_id is None:
            result = await self._call_telegram(
                self._api.send_message,
                chat_id=self._chat_id,
                text=formatted,
                parse_mode="HTML",
            )
            message_id = self._message_id(result)
            if message_id is not None:
                self._current_message_id = message_id
            self._last_send_time = self._now_fn()
            return

        elapsed = self._now_fn() - self._last_send_time
        if not force and elapsed < self._throttle_seconds:
            return
        await self._call_telegram(
            self._api.edit_message_text,
            chat_id=self._chat_id,
            message_id=self._current_message_id,
            text=formatted,
            parse_mode="HTML",
        )
        self._last_send_time = self._now_fn()

    async def _call_telegram(self, fn: Any, **kwargs: Any) -> Any:
        while True:
            try:
                return await fn(**kwargs)
            except Exception as exc:
                retry_after = getattr(exc, "retry_after", None)
                if retry_after:
                    await self._sleep_fn(float(retry_after))
                    continue
                message = str(exc).lower()
                if "message is not modified" in message:
                    return None
                log.warning("Telegram streaming update failed: %s", exc)
                return None

    @staticmethod
    def _message_id(result: Any) -> int | None:
        if isinstance(result, dict):
            raw = result.get("message_id")
            if isinstance(raw, int):
                return raw
        return None


__all__ = ["DraftStream"]
