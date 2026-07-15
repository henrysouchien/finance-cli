from __future__ import annotations

import asyncio

from finance_cli.telegram_bot.streaming import DraftStream, _markdown_to_telegram_html


class FakeTelegramAPI:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, object]] = []
        self.edits: list[dict[str, object]] = []
        self._next_message_id = 100

    async def send_message(
        self,
        chat_id: str | int,
        text: str,
        *,
        parse_mode: str | None = None,
    ) -> dict[str, int]:
        message = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "message_id": self._next_message_id,
        }
        self._next_message_id += 1
        self.sent_messages.append(message)
        return {"message_id": int(message["message_id"])}

    async def edit_message_text(
        self,
        chat_id: str | int,
        message_id: int,
        text: str,
        *,
        reply_markup=None,
        parse_mode: str | None = None,
    ) -> dict[str, int]:
        self.edits.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "reply_markup": reply_markup,
                "parse_mode": parse_mode,
            }
        )
        return {"message_id": message_id}


def test_append_below_min_initial_chars_buffers_only() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123, min_initial_chars=10)

    asyncio.run(draft.append("short"))

    assert api.sent_messages == []
    assert api.edits == []


def test_append_above_min_initial_chars_sends_first_message() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123, min_initial_chars=5)

    asyncio.run(draft.append("hello world"))

    assert api.sent_messages == [
        {
            "chat_id": 123,
            "text": "hello world",
            "parse_mode": "HTML",
            "message_id": 100,
        }
    ]


def test_finish_flushes_buffer_even_below_minimum() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123, min_initial_chars=100)

    async def scenario() -> None:
        await draft.append("short")
        await draft.finish()

    asyncio.run(scenario())

    assert api.sent_messages[0]["text"] == "short"


def test_append_escapes_html() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123, min_initial_chars=1)

    asyncio.run(draft.append("<b>unsafe</b>"))

    assert api.sent_messages[0]["text"] == "&lt;b&gt;unsafe&lt;/b&gt;"


def test_second_append_edits_after_throttle_window() -> None:
    api = FakeTelegramAPI()
    now = {"value": 0.0}
    draft = DraftStream(api, 123, min_initial_chars=1, throttle_seconds=1.5, now_fn=lambda: now["value"])

    async def scenario() -> None:
        await draft.append("hello")
        now["value"] = 2.0
        await draft.append(" world")

    asyncio.run(scenario())

    assert api.edits[-1]["text"] == "hello world"


def test_second_append_inside_throttle_window_waits_for_finish() -> None:
    api = FakeTelegramAPI()
    now = {"value": 0.0}
    draft = DraftStream(api, 123, min_initial_chars=1, throttle_seconds=10.0, now_fn=lambda: now["value"])

    async def scenario() -> None:
        await draft.append("hello")
        now["value"] = 1.0
        await draft.append(" world")
        assert api.edits == []
        await draft.finish()

    asyncio.run(scenario())

    assert api.edits[-1]["text"] == "hello world"


def test_send_tool_status_force_flushes_and_italics_status() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123, min_initial_chars=100)

    async def scenario() -> None:
        await draft.append("hello")
        await draft.send_tool_status("Calling budget_status...")

    asyncio.run(scenario())

    assert api.sent_messages[0]["text"] == "hello\n<i>Calling budget_status...</i>"


def test_send_tool_status_edits_existing_message() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123, min_initial_chars=1)

    async def scenario() -> None:
        await draft.append("hello")
        await draft.send_tool_status("Done.")

    asyncio.run(scenario())

    assert api.edits[-1]["text"] == "hello\n<i>Done.</i>"


def test_auto_split_creates_new_message_when_chunk_exceeds_limit() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123, min_initial_chars=1, max_chars=5)

    async def scenario() -> None:
        await draft.append("abcdefghij")
        await draft.finish()

    asyncio.run(scenario())

    assert [message["text"] for message in api.sent_messages] == ["abcde", "fghij"]


def test_tool_status_after_split_updates_latest_chunk() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123, min_initial_chars=1, max_chars=20)

    async def scenario() -> None:
        await draft.append("x" * 25)
        await draft.send_tool_status("ok")

    asyncio.run(scenario())

    assert [message["text"] for message in api.sent_messages] == ["x" * 20, "x" * 5]
    assert api.edits[-1]["text"] == ("x" * 5) + "\n<i>ok</i>"


def test_finish_noops_when_empty() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123)

    asyncio.run(draft.finish())

    assert api.sent_messages == []
    assert api.edits == []


# --- _markdown_to_telegram_html unit tests ---


def test_markdown_bold() -> None:
    assert _markdown_to_telegram_html("**hello**") == "<b>hello</b>"


def test_markdown_italic() -> None:
    assert _markdown_to_telegram_html("*hello*") == "<i>hello</i>"


def test_markdown_bold_and_italic() -> None:
    assert (
        _markdown_to_telegram_html("**bold** and *italic*")
        == "<b>bold</b> and <i>italic</i>"
    )


def test_markdown_inline_code() -> None:
    assert _markdown_to_telegram_html("run `ls -la` now") == "run <code>ls -la</code> now"


def test_markdown_code_block() -> None:
    result = _markdown_to_telegram_html("before\n```\nprint('hi')\n```\nafter")
    assert "<pre>" in result
    assert "print(&#x27;hi&#x27;)" in result
    assert "after" in result


def test_markdown_code_block_with_language() -> None:
    result = _markdown_to_telegram_html("```python\nx = 1\n```")
    assert result == "<pre>x = 1\n</pre>"


def test_markdown_html_in_code_is_escaped() -> None:
    result = _markdown_to_telegram_html("`<script>alert(1)</script>`")
    assert result == "<code>&lt;script&gt;alert(1)&lt;/script&gt;</code>"


def test_markdown_bold_inside_code_not_converted() -> None:
    result = _markdown_to_telegram_html("`**not bold**`")
    assert result == "<code>**not bold**</code>"


def test_markdown_html_outside_code_escaped() -> None:
    assert _markdown_to_telegram_html("<div>hi</div>") == "&lt;div&gt;hi&lt;/div&gt;"


def test_markdown_plain_text_unchanged() -> None:
    assert _markdown_to_telegram_html("just some text") == "just some text"


def test_markdown_mixed_formatting() -> None:
    text = "Use **txn list** to see `--limit 10` results"
    result = _markdown_to_telegram_html(text)
    assert result == "Use <b>txn list</b> to see <code>--limit 10</code> results"


def test_draft_stream_formats_markdown_bold() -> None:
    api = FakeTelegramAPI()
    draft = DraftStream(api, 123, min_initial_chars=1)

    asyncio.run(draft.append("**hello world**"))

    assert api.sent_messages[0]["text"] == "<b>hello world</b>"
