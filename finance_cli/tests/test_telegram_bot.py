from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest

from finance_cli.db import connect
from finance_cli.skills import SKILL_FILES
from finance_cli.telegram_bot import bot as bot_module
from finance_cli.telegram_bot.bot import TelegramBot
from finance_cli.telegram_bot.compaction import KEEP_RECENT_MESSAGES
from finance_cli.telegram_bot.config import BotConfig, load_config
from finance_cli.telegram_bot.gateway_client import BackendHTTPError, SessionState
from finance_cli.telegram_bot.store import BotStore, RequestMetrics
from finance_cli.telegram_bot.store import build_tool_only_turn_message
from finance_cli.telegram_bot.telegram_api import split_message


def _make_config(**kwargs: Any) -> BotConfig:
    base = dict(
        TELEGRAM_BOT_TOKEN="bot-token",
        TELEGRAM_CHAT_ID="12345",
        TELEGRAM_GATEWAY_URL="http://127.0.0.1:8002",
        GATEWAY_USER_KEY="gateway-key",
        ANTHROPIC_AUTH_TOKEN="",
    )
    base.update(kwargs)
    return BotConfig(**base)


def _message_update(chat_id: int, text: str) -> dict[str, object]:
    return {
        "update_id": 1,
        "message": {
            "chat": {"id": chat_id},
            "text": text,
        },
    }


def _write_upload_file(path: Path, payload: bytes = b"upload", *, mtime: float | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


class FakeTelegramAPI:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, object]] = []
        self.keyboard_messages: list[dict[str, object]] = []
        self.chat_actions: list[dict[str, object]] = []
        self.callback_answers: list[dict[str, object]] = []
        self.edits: list[dict[str, object]] = []
        self.photos: list[dict[str, object]] = []
        self.documents: list[dict[str, object]] = []
        self.file_info: dict[str, dict[str, object]] = {}
        self.file_bytes: dict[str, bytes] = {}
        self.get_file_calls: list[str] = []
        self.download_file_calls: list[dict[str, object]] = []
        self._next_message_id = 100

    async def get_updates(self, offset: int | None = None, timeout: int | None = None) -> list[dict[str, object]]:
        del offset, timeout
        return []

    async def get_file(self, file_id: str) -> dict[str, object]:
        self.get_file_calls.append(file_id)
        return dict(self.file_info[file_id])

    async def download_file(
        self,
        file_path: str,
        *,
        max_bytes: int | None = None,
    ) -> bytes:
        self.download_file_calls.append({"file_path": file_path, "max_bytes": max_bytes})
        return self.file_bytes[file_path]

    async def send_message(
        self,
        chat_id: str | int | None,
        text: str,
        *,
        parse_mode: str | None = None,
    ) -> dict[str, object]:
        message = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "message_id": self._next_message_id,
        }
        self._next_message_id += 1
        self.sent_messages.append(message)
        return {"message_id": message["message_id"]}

    async def send_message_with_keyboard(
        self,
        chat_id: str | int | None,
        text: str,
        inline_keyboard: list[list[dict[str, str]]],
    ) -> dict[str, object]:
        message = {
            "chat_id": chat_id,
            "text": text,
            "inline_keyboard": inline_keyboard,
            "message_id": self._next_message_id,
        }
        self._next_message_id += 1
        self.keyboard_messages.append(message)
        return {"message_id": message["message_id"]}

    async def send_chat_action(self, chat_id: str | int | None, action: str) -> dict[str, object]:
        self.chat_actions.append({"chat_id": chat_id, "action": action})
        return {"ok": True}

    async def answer_callback_query(
        self,
        callback_query_id: str,
        text: str = "",
    ) -> dict[str, object]:
        self.callback_answers.append({"callback_query_id": callback_query_id, "text": text})
        return {"ok": True}

    async def edit_message_text(
        self,
        chat_id: str | int | None,
        message_id: int,
        text: str,
        *,
        reply_markup: dict[str, object] | None = None,
        parse_mode: str | None = None,
    ) -> dict[str, object]:
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

    async def send_photo(
        self,
        chat_id: str | int | None,
        photo_bytes: bytes,
        *,
        filename: str = "chart.png",
        media_type: str = "image/png",
        caption: str | None = None,
    ) -> dict[str, object]:
        message = {
            "chat_id": chat_id,
            "photo_bytes": photo_bytes,
            "filename": filename,
            "media_type": media_type,
            "caption": caption,
            "message_id": self._next_message_id,
        }
        self._next_message_id += 1
        self.photos.append(message)
        return {"message_id": message["message_id"]}

    async def send_document(
        self,
        chat_id: str | int | None,
        document_bytes: bytes,
        *,
        filename: str = "document.bin",
        media_type: str = "application/octet-stream",
        caption: str | None = None,
    ) -> dict[str, object]:
        message = {
            "chat_id": chat_id,
            "document_bytes": document_bytes,
            "filename": filename,
            "media_type": media_type,
            "caption": caption,
            "message_id": self._next_message_id,
        }
        self._next_message_id += 1
        self.documents.append(message)
        return {"message_id": message["message_id"]}


class FakeGatewayClient:
    def __init__(
        self,
        *,
        stream_plans: list[Any] | None = None,
        approval_statuses: list[int] | None = None,
        sessions: list[SessionState] | None = None,
    ) -> None:
        self.stream_plans = list(stream_plans or [])
        self.approval_statuses = list(approval_statuses or [])
        self.sessions = list(
            sessions
            or [SessionState(token="tok-1", session_id="sess-1", expires_at=10_000)]
        )
        self.session_index = 0
        self.stream_calls: list[dict[str, Any]] = []
        self.submit_calls: list[tuple[str, str, bool]] = []
        self.ensure_session_calls: list[dict[str, Any]] = []
        self.invalidate_calls = 0
        self.close_calls = 0

    async def ensure_session(
        self,
        *,
        user_id: str | None = None,
        force_refresh: bool = False,
    ) -> SessionState:
        self.ensure_session_calls.append({"force_refresh": force_refresh, "user_id": user_id})
        if force_refresh and self.session_index < len(self.sessions) - 1:
            self.session_index += 1
        return self.sessions[self.session_index]

    def invalidate_session(self) -> None:
        self.invalidate_calls += 1
        if self.session_index < len(self.sessions) - 1:
            self.session_index += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def stream_chat(
        self,
        messages: list[dict[str, Any]],
        *,
        context: dict[str, Any] | None = None,
        model: str | None = None,
        user_id: str | None = None,
    ):
        self.stream_calls.append(
            {
                "messages": [dict(message) for message in messages],
                "context": context,
                "model": model,
                "user_id": user_id,
            }
        )
        if not self.stream_plans:
            return
            yield  # pragma: no cover
        plan = self.stream_plans.pop(0)
        if isinstance(plan, Exception):
            raise plan
        for event in plan:
            await asyncio.sleep(0)
            yield event

    async def submit_approval(
        self,
        tool_call_id: str,
        nonce: str,
        approved: bool,
    ) -> tuple[int, dict[str, Any]]:
        self.submit_calls.append((tool_call_id, nonce, approved))
        status = self.approval_statuses.pop(0) if self.approval_statuses else 200
        return status, {"status": "ok"}


class BlockingGatewayClient(FakeGatewayClient):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream_chat(self, messages, *, context=None, model=None, user_id=None):
        self.stream_calls.append(
            {"messages": list(messages), "context": context, "model": model, "user_id": user_id}
        )
        self.started.set()
        await self.release.wait()
        yield {"type": "text_delta", "text": "done"}
        yield {"type": "stream_complete", "usage": {}}


def _make_store(tmp_path: Path) -> BotStore:
    store = BotStore(tmp_path / "telegram-bot.db")
    store.startup()
    return store


def _set_claude_monthly_cap(store: BotStore, *, limit_usd6: int, system_limit_usd6: int | None = None) -> None:
    with connect(store.db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = ?,
                   system_limit_usd6 = ?
             WHERE provider = 'claude'
               AND period = 'monthly'
            """,
            (limit_usd6, system_limit_usd6),
        )
        conn.commit()


def _save_successful_turn(store: BotStore, request_id: str, user_text: str, assistant_text: str) -> None:
    store.save_user_message(user_text, request_id)
    store.save_assistant_message(assistant_text, request_id)
    store.save_request(
        RequestMetrics(
            request_id=request_id,
            session_id=f"telegram-{request_id}",
            model="claude-sonnet-4-6",
        )
    )


def _save_failed_turn(store: BotStore, request_id: str, user_text: str) -> None:
    store.save_user_message(user_text, request_id)
    store.save_request(
        RequestMetrics(
            request_id=request_id,
            session_id=f"telegram-{request_id}",
            model="claude-sonnet-4-6",
            error="boom",
        )
    )


def test_config_loads_from_env(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")
    monkeypatch.setenv("GATEWAY_USER_KEY", "gateway-user-key")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "optional-test-token")
    monkeypatch.setenv("TELEGRAM_GATEWAY_URL", "http://localhost:9000")
    monkeypatch.setenv("TELEGRAM_BOT_MODEL", "claude-opus-4-6")
    monkeypatch.setenv("TELEGRAM_BOT_MAX_TURNS", "9")
    monkeypatch.setenv("TELEGRAM_BOT_MAX_TOKENS", "8000")
    monkeypatch.setenv("TELEGRAM_BOT_THINKING", "false")
    monkeypatch.setenv("TELEGRAM_BOT_HISTORY_MAX_TURNS", "7")
    monkeypatch.setenv("TELEGRAM_BOT_SESSION_IDLE_TIMEOUT", "45")
    monkeypatch.setenv("TELEGRAM_BOT_POLL_TIMEOUT", "12")
    monkeypatch.setenv("TELEGRAM_BOT_APPROVAL_TIMEOUT", "45")

    config = load_config()

    assert config.telegram_token == "bot-token"
    assert config.telegram_chat_id == "999"
    assert config.gateway_user_key == "gateway-user-key"
    assert config.gateway_url == "http://localhost:9000"
    assert config.anthropic_auth_token == "optional-test-token"
    assert config.model == "claude-opus-4-6"
    assert config.max_turns == 9
    assert config.max_tokens == 8000
    assert config.thinking is False
    assert config.history_max_turns == 7
    assert config.session_idle_timeout == 45
    assert config.poll_timeout == 12
    assert config.approval_timeout == 45


def test_config_missing_required(monkeypatch) -> None:
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")
    monkeypatch.setenv("GATEWAY_USER_KEY", "gateway-user-key")

    with pytest.raises(ValueError, match="TELEGRAM_BOT_TOKEN"):
        load_config()


def test_config_defaults(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")
    monkeypatch.setenv("GATEWAY_USER_KEY", "gateway-user-key")
    monkeypatch.delenv("ANTHROPIC_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_GATEWAY_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_MODEL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_MAX_TURNS", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_MAX_TOKENS", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_THINKING", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_HISTORY_MAX_TURNS", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_SESSION_IDLE_TIMEOUT", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_POLL_TIMEOUT", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_APPROVAL_TIMEOUT", raising=False)

    config = load_config()

    assert config.anthropic_auth_token == ""
    assert config.gateway_url == "http://127.0.0.1:8002"
    assert config.model == "claude-sonnet-4-6"
    assert config.max_turns == 15
    assert config.max_tokens == 16000
    assert config.thinking is True
    assert config.history_max_turns == 20
    assert config.session_idle_timeout == 1800
    assert config.poll_timeout == 30
    assert config.approval_timeout == 300


def test_split_message_short() -> None:
    assert split_message("short") == ["short"]


def test_split_message_long_newlines() -> None:
    text = "a" * 5 + "\n" + "b" * 5 + "\n" + "c" * 5

    chunks = split_message(text, max_len=10)

    assert chunks == ["aaaaa", "bbbbb", "ccccc"]


def test_split_message_no_newlines() -> None:
    text = "x" * 25

    chunks = split_message(text, max_len=10)

    assert chunks == ["x" * 10, "x" * 10, "x" * 5]


def test_auth_gate_rejects() -> None:
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), client=FakeGatewayClient())

    asyncio.run(bot._handle_update(_message_update(99999, "hello")))

    assert bot._history == []
    assert bot.api.sent_messages == []


def test_auth_gate_accepts() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient(stream_plans=[[{"type": "stream_complete", "usage": {}}]])
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_update(_message_update(12345, "hello")))

    assert bot._history == [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "No response generated."},
    ]


def test_command_reset_clears_history_model_skill_and_invalidates_session(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    try:
        api = FakeTelegramAPI()
        client = FakeGatewayClient()
        bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)
        bot._history = [{"role": "user", "content": "old"}]
        bot._model_override = "claude-opus-4-6"
        bot._active_skill = "normalizer_builder"

        asyncio.run(bot._handle_command(12345, "/reset"))

        assert bot._history == []
        assert bot._model_override is None
        assert bot._active_skill is None
        assert client.invalidate_calls == 1
        assert api.sent_messages[-1]["text"] == "History cleared."

        with connect(tmp_path / "telegram-bot.db") as conn:
            row = conn.execute(
                "SELECT role, content, request_id FROM bot_chat_messages ORDER BY id DESC LIMIT 1"
            ).fetchone()
            assert row is not None
            assert (row["role"], row["content"], row["request_id"]) == (
                "assistant",
                "[HISTORY_RESET]",
                None,
            )
    finally:
        store.shutdown()


def test_history_command_uses_bot_owned_history() -> None:
    api = FakeTelegramAPI()
    bot = TelegramBot(config=_make_config(), api=api, client=FakeGatewayClient())
    bot._history = [{"role": "user", "content": "one"}, {"role": "assistant", "content": "two"}]

    asyncio.run(bot._handle_command(12345, "/history"))

    assert api.sent_messages[-1]["text"] == "Conversation: 2 messages."


def test_model_command_sets_override() -> None:
    api = FakeTelegramAPI()
    bot = TelegramBot(config=_make_config(), api=api, client=FakeGatewayClient())

    asyncio.run(bot._handle_command(12345, "/model claude-opus-4-6"))

    assert bot._model_override == "claude-opus-4-6"
    assert api.sent_messages[-1]["text"] == "Model set to: claude-opus-4-6"


def test_start_command_help_includes_key_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CASHNERD_PUBLIC_BASE_URL", "https://cashnerd.test")
    api = FakeTelegramAPI()
    bot = TelegramBot(config=_make_config(), api=api, client=FakeGatewayClient())

    asyncio.run(bot._handle_command(12345, "/start"))

    text = str(api.sent_messages[-1]["text"])
    assert "/compact summarize older messages" in text
    assert "/reset clear conversation" in text
    assert "Privacy: https://cashnerd.test/privacy" in text


def test_onboarding_command_activates_onboarding_skill() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient()
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_command(12345, "/onboarding"))

    assert bot._active_skill == "onboarding"
    assert api.sent_messages[-1]["text"] == (
        "Onboarding activated — I'll guide you through setting up your finances (~20-45 min).\n\n"
        "Send any message to begin, or /onboarding off to exit."
    )


def test_onboarding_command_logs_wizard_started(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    try:
        api = FakeTelegramAPI()
        client = FakeGatewayClient()
        bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)

        asyncio.run(bot._handle_command(12345, "/onboarding"))

        with connect(tmp_path / "telegram-bot.db") as conn:
            row = conn.execute(
                """
                SELECT event, outcome, properties, source
                FROM analytics_events
                WHERE event = 'onboarding.wizard'
                """
            ).fetchone()

        assert row is not None
        assert row["event"] == "onboarding.wizard"
        assert row["outcome"] == "started"
        assert row["source"] == "telegram"
        assert json.loads(row["properties"]) == {
            "context": "telegram",
            "step": "command",
        }
    finally:
        store.shutdown()


def test_dev_command_alias_activates_normalizer_builder() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient()
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_command(12345, "/dev normalizer"))

    assert bot._active_skill == "normalizer_builder"
    assert client.invalidate_calls == 0
    assert api.sent_messages[-1]["text"] == (
        "Dev mode: normalizer_builder\n"
        "Skill playbook loaded into system prompt.\n"
        "/dev off to exit."
    )


def test_dev_command_full_skill_name_activates_skill() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient()
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_command(12345, "/dev normalizer_builder"))

    assert bot._active_skill == "normalizer_builder"
    assert client.invalidate_calls == 0


def test_dev_command_off_clears_active_skill() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient()
    bot = TelegramBot(config=_make_config(), api=api, client=client)
    bot._active_skill = "normalizer_builder"

    asyncio.run(bot._handle_command(12345, "/dev off"))

    assert bot._active_skill is None
    assert client.invalidate_calls == 0
    assert api.sent_messages[-1]["text"] == "Dev mode off (was: normalizer_builder)."


def test_dev_command_off_when_not_active_reports_state() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient()
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_command(12345, "/dev off"))

    assert bot._active_skill is None
    assert client.invalidate_calls == 0
    assert api.sent_messages[-1]["text"] == "Dev mode is not active."


def test_dev_command_without_args_shows_status() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient()
    bot = TelegramBot(config=_make_config(), api=api, client=client)
    bot._active_skill = "normalizer_builder"

    asyncio.run(bot._handle_command(12345, "/dev"))

    assert bot._active_skill == "normalizer_builder"
    assert "Active: normalizer_builder" in api.sent_messages[-1]["text"]
    assert "Usage:" in api.sent_messages[-1]["text"]


def test_dev_off_deactivates_skill() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient()
    bot = TelegramBot(config=_make_config(), api=api, client=client)
    bot._active_skill = "normalizer_builder"

    asyncio.run(bot._handle_command(12345, "/dev off"))

    assert bot._active_skill is None
    assert api.sent_messages[-1]["text"] == "Dev mode off (was: normalizer_builder)."


def test_dev_command_unknown_skill_is_rejected() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient()
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_command(12345, "/dev typo"))

    assert bot._active_skill is None
    assert client.invalidate_calls == 0
    available = ", ".join(sorted(SKILL_FILES.keys()))
    assert (
        api.sent_messages[-1]["text"]
        == f"Unknown skill: typo\nAvailable: {available}"
    )


def test_cleanup_telegram_uploads_deletes_old_files(tmp_path: Path) -> None:
    now = time.time()
    upload_dir = tmp_path / "uploads" / "telegram"
    old_file = _write_upload_file(
        upload_dir / "old.csv",
        mtime=now - bot_module._TELEGRAM_UPLOAD_RETENTION_SECONDS - 1,
    )
    fresh_file = _write_upload_file(upload_dir / "fresh.csv", mtime=now - 60)

    result = bot_module._cleanup_telegram_uploads(upload_dir, now=now)

    assert result["deleted"] == 1
    assert not old_file.exists()
    assert fresh_file.exists()


def test_cleanup_telegram_uploads_enforces_total_size_oldest_first(tmp_path: Path) -> None:
    now = time.time()
    upload_dir = tmp_path / "uploads" / "telegram"
    oldest = _write_upload_file(upload_dir / "oldest.csv", b"a" * 7, mtime=now - 30)
    middle = _write_upload_file(upload_dir / "middle.csv", b"b" * 6, mtime=now - 20)
    newest = _write_upload_file(upload_dir / "newest.csv", b"c" * 4, mtime=now - 10)

    result = bot_module._cleanup_telegram_uploads(
        upload_dir,
        now=now,
        max_age_seconds=9999,
        max_total_bytes=10,
    )

    assert result["deleted"] == 1
    assert not oldest.exists()
    assert middle.exists()
    assert newest.exists()
    assert result["remaining_bytes"] == 10


def test_cleanup_telegram_uploads_preserves_protected_current_file(tmp_path: Path) -> None:
    now = time.time()
    upload_dir = tmp_path / "uploads" / "telegram"
    old_file = _write_upload_file(upload_dir / "old.csv", b"a" * 7, mtime=now - 30)
    current_file = _write_upload_file(upload_dir / "current.csv", b"b" * 7, mtime=now - 10)

    result = bot_module._cleanup_telegram_uploads(
        upload_dir,
        now=now,
        max_age_seconds=9999,
        max_total_bytes=5,
        protected_paths=[current_file],
    )

    assert result["deleted"] == 1
    assert not old_file.exists()
    assert current_file.exists()
    assert result["remaining_bytes"] == 7


def test_document_update_stages_csv_upload_and_invokes_agent(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    try:
        old_upload = _write_upload_file(
            store.db_path.parent / "uploads" / "telegram" / "old.csv",
            mtime=time.time() - bot_module._TELEGRAM_UPLOAD_RETENTION_SECONDS - 1,
        )
        api = FakeTelegramAPI()
        api.file_info["doc-123"] = {
            "file_path": "documents/statement.csv",
            "file_size": 37,
        }
        api.file_bytes["documents/statement.csv"] = (
            b"Date,Description,Amount\n2026-01-01,Coffee,-5.00\n"
        )
        client = FakeGatewayClient(
            stream_plans=[
                [
                    {"type": "text_delta", "text": "Imported."},
                    {"type": "stream_complete", "usage": {}},
                ]
            ]
        )
        bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)

        asyncio.run(
            bot._handle_update(
                {
                    "update_id": 1,
                    "message": {
                        "chat": {"id": 12345},
                        "date": 1_700_000_000,
                        "caption": "January card statement",
                        "document": {
                            "file_id": "doc-123",
                            "file_name": "../../statement.csv",
                            "mime_type": "text/csv",
                        },
                    },
                }
            )
        )

        staged_files = sorted((store.db_path.parent / "uploads" / "telegram").glob("*.csv"))
        assert len(staged_files) == 1
        staged_path = staged_files[0]
        assert not old_upload.exists()
        assert staged_path.name.endswith("-statement.csv")
        assert staged_path.read_bytes() == api.file_bytes["documents/statement.csv"]
        assert staged_path.stat().st_mode & 0o777 == 0o600
        assert api.get_file_calls == ["doc-123"]
        assert api.download_file_calls == [
            {"file_path": "documents/statement.csv", "max_bytes": 20 * 1024 * 1024}
        ]
        assert {"chat_id": 12345, "action": "upload_document"} in api.chat_actions
        assert api.sent_messages[0]["text"] == (
            "Received the CSV upload. I saved it for import and will process it now."
        )
        prompt = client.stream_calls[0]["messages"][-1]["content"]
        assert str(staged_path) in prompt
        assert "ingest_csv" in prompt
        assert 'institution="auto"' in prompt
        assert "commit=True" in prompt
        assert "User caption: January card statement" in prompt
    finally:
        store.shutdown()


def test_document_update_rejects_unsupported_file_without_download() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient()
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(
        bot._handle_update(
            {
                "update_id": 1,
                "message": {
                    "chat": {"id": 12345},
                    "document": {
                        "file_id": "doc-123",
                        "file_name": "notes.txt",
                        "mime_type": "text/plain",
                    },
                },
            }
        )
    )

    assert api.sent_messages[-1]["text"] == (
        "I can import CSV or PDF statements from Telegram. Send a .csv or .pdf file."
    )
    assert api.get_file_calls == []
    assert api.download_file_calls == []
    assert client.stream_calls == []


def test_pid_file_for_token_uses_token_hash(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(bot_module, "_PID_DIR", tmp_path)

    expected_hash = hashlib.sha256(b"bot-token").hexdigest()[:8]

    assert bot_module._pid_file_for_token("bot-token") == tmp_path / f"telegram_bot_{expected_hash}.pid"


def test_pid_lock_writes_pid_and_can_be_reacquired(tmp_path) -> None:
    pid_file = tmp_path / "telegram.pid"
    bot_module._release_pid_lock()

    try:
        bot_module._acquire_pid_lock(pid_file)
        assert pid_file.read_text() == str(os.getpid())
        assert bot_module._lock_fd is not None
    finally:
        bot_module._release_pid_lock()

    assert bot_module._lock_fd is None

    try:
        bot_module._acquire_pid_lock(pid_file)
        assert pid_file.read_text() == str(os.getpid())
    finally:
        bot_module._release_pid_lock()


def test_pid_lock_raises_with_existing_pid(monkeypatch, tmp_path) -> None:
    import fcntl

    pid_file = tmp_path / "telegram.pid"
    pid_file.write_text("4242")

    def fake_flock(fd: int, flags: int) -> None:
        del fd, flags
        raise BlockingIOError("busy")

    monkeypatch.setattr(fcntl, "flock", fake_flock)

    with pytest.raises(
        SystemExit,
        match=r"Another Telegram bot instance is running \(PID 4242\)\. Kill it first: kill 4242",
    ):
        bot_module._acquire_pid_lock(pid_file)

    assert bot_module._lock_fd is None


def test_handle_agent_message_success_persists_history_and_metrics(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    try:
        api = FakeTelegramAPI()
        client = FakeGatewayClient(
            stream_plans=[
                [
                    {"type": "text_delta", "text": "Balance summary"},
                    {
                        "type": "tool_call_start",
                        "tool_call_id": "tool-1",
                        "tool_name": "balance_show",
                        "tool_input": {"view": "personal"},
                    },
                    {
                        "type": "tool_call_complete",
                        "tool_call_id": "tool-1",
                        "tool_name": "balance_show",
                        "result": {"summary": {"net_worth": "$50,234"}},
                        "error": None,
                        "duration_ms": 12,
                        "server": "finance-cli",
                    },
                    {
                        "type": "stream_complete",
                        "usage": {
                            "input_tokens": 12,
                            "output_tokens": 34,
                            "cache_creation_input_tokens": 5,
                            "cache_read_input_tokens": 6,
                            "estimated_cost": 0.0123,
                        },
                    },
                ]
            ]
        )
        bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)

        asyncio.run(bot._handle_agent_message(12345, "show balances"))

        assert bot._history == [
            {"role": "user", "content": "show balances"},
            {
                "role": "assistant",
                "content": 'Balance summary\n\n[Tools: balance_show(view="personal") → {"net_worth": "$50,234"}]',
            },
        ]
        assert api.edits[-1]["text"] == (
            "Balance summary\n<i>\u23f3 Checking balances...</i>\n<i>\u2705 Checking balances</i>"
        )

        with connect(tmp_path / "telegram-bot.db") as conn:
            request = conn.execute("SELECT * FROM bot_requests").fetchone()
            cost_row = conn.execute(
                """
                SELECT cost_usd6,
                       is_byok,
                       allowance_debit_usd6,
                       credits_debit_usd6,
                       overflow_unattributed_usd6
                  FROM cost_ledger
                 ORDER BY created_at DESC
                 LIMIT 1
                """
            ).fetchone()
            assert request is not None
            assert request["session_id"] == "sess-1"
            assert request["input_tokens"] == 12
            assert request["output_tokens"] == 34
            assert request["cache_creation_tokens"] == 5
            assert request["cache_read_tokens"] == 6
            assert request["estimated_cost"] == pytest.approx(0.0123)
            assert request["tool_call_count"] == 1
            assert request["error"] is None
            assert cost_row["cost_usd6"] == 12300
            assert cost_row["is_byok"] == 0
            assert (
                cost_row["allowance_debit_usd6"]
                + cost_row["credits_debit_usd6"]
                + cost_row["overflow_unattributed_usd6"]
                == cost_row["cost_usd6"]
            )
    finally:
        store.shutdown()


def test_handle_agent_message_standard_cap_downgrades_to_haiku(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    try:
        _set_claude_monthly_cap(store, limit_usd6=0, system_limit_usd6=0)
        api = FakeTelegramAPI()
        client = FakeGatewayClient(
            stream_plans=[
                [
                    {"type": "text_delta", "text": "Balance summary"},
                    {"type": "stream_complete", "usage": {}},
                ]
            ]
        )
        bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)

        asyncio.run(bot._handle_agent_message(12345, "show balances"))

        assert client.stream_calls[0]["model"] == "claude-haiku-4-5"
    finally:
        store.shutdown()


def test_handle_agent_message_lite_cap_block_sends_credit_cta(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = _make_store(tmp_path)
    try:
        _set_claude_monthly_cap(store, limit_usd6=0, system_limit_usd6=0)
        api = FakeTelegramAPI()
        client = FakeGatewayClient()
        monkeypatch.setenv("CASHNERD_PUBLIC_BASE_URL", "https://test.cashnerd.local")
        monkeypatch.setenv("STRIPE_PRICE_LITE", "price_lite")
        monkeypatch.setattr(
            bot_module,
            "_local_user_billing_snapshot",
            lambda user_id: {
                "id": str(user_id),
                "tier": "paid",
                "stripe_price_id": "price_lite",
            },
        )
        bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)

        asyncio.run(bot._handle_agent_message(12345, "show balances"))

        assert client.stream_calls == []
        texts = [str(message["text"]) for message in [*api.sent_messages, *api.edits]]
        assert any(
            "Buy credits in Billing settings: https://test.cashnerd.local/settings/billing" in text
            for text in texts
        )
    finally:
        store.shutdown()


def test_handle_agent_message_byok_records_byok_cost_row(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = _make_store(tmp_path)
    try:
        _set_claude_monthly_cap(store, limit_usd6=0, system_limit_usd6=0)
        api = FakeTelegramAPI()
        client = FakeGatewayClient(
            stream_plans=[
                [
                    {"type": "text_delta", "text": "Balance summary"},
                    {"type": "stream_complete", "usage": {"estimated_cost": 0.0123}},
                ]
            ]
        )
        monkeypatch.setattr(
            bot_module,
            "_local_user_billing_snapshot",
            lambda user_id: {
                "id": str(user_id),
                "tier": "paid",
                "anthropic_api_key_secret_ref": "secret-ref",
            },
        )
        bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)

        asyncio.run(bot._handle_agent_message(12345, "show balances"))

        with connect(store.db_path) as conn:
            row = conn.execute(
                """
                SELECT is_byok,
                       allowance_debit_usd6,
                       credits_debit_usd6,
                       overflow_unattributed_usd6
                  FROM cost_ledger
                 ORDER BY created_at DESC
                 LIMIT 1
                """
            ).fetchone()

        assert row["is_byok"] == 1
        assert row["allowance_debit_usd6"] == 0
        assert row["credits_debit_usd6"] == 0
        assert row["overflow_unattributed_usd6"] == 0
    finally:
        store.shutdown()


def test_onboarding_tool_events_emit_analytics(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    try:
        api = FakeTelegramAPI()
        client = FakeGatewayClient(
            stream_plans=[
                [
                    {
                        "type": "tool_call_start",
                        "tool_call_id": "tool-plaid",
                        "tool_name": "plaid_link",
                        "tool_input": {"wait": False},
                    },
                    {
                        "type": "tool_call_complete",
                        "tool_call_id": "tool-plaid",
                        "tool_name": "plaid_link",
                        "result": {
                            "data": {"session": {"hosted_link_url": "https://example.com/plaid"}}
                        },
                        "error": None,
                        "duration_ms": 12,
                        "server": "finance-cli",
                    },
                    {
                        "type": "tool_call_start",
                        "tool_call_id": "tool-import",
                        "tool_name": "ingest_statement",
                        "tool_input": {"file": "/tmp/statement.pdf"},
                    },
                    {
                        "type": "tool_call_complete",
                        "tool_call_id": "tool-import",
                        "tool_name": "ingest_statement",
                        "result": {"summary": {"total_transactions": 0}},
                        "error": {"message": "parse failed"},
                        "duration_ms": 8,
                        "server": "finance-cli",
                    },
                    {
                        "type": "tool_call_start",
                        "tool_call_id": "tool-cat",
                        "tool_name": "cat_auto_categorize",
                        "tool_input": {"dry_run": False},
                    },
                    {
                        "type": "tool_call_complete",
                        "tool_call_id": "tool-cat",
                        "tool_name": "cat_auto_categorize",
                        "result": {"summary": {"updated": 4}},
                        "error": None,
                        "duration_ms": 18,
                        "server": "finance-cli",
                    },
                    {
                        "type": "tool_call_complete",
                        "tool_call_id": "tool-state",
                        "tool_name": "skill_state_set",
                        "result": {"data": {"name": "onboarding", "state": {"complete": True}}},
                        "error": None,
                        "duration_ms": 4,
                        "server": "finance-cli",
                    },
                    {
                        "type": "tool_call_complete",
                        "tool_call_id": "tool-exchange",
                        "tool_name": "plaid_exchange",
                        "result": {"plaid_item_id": "item_live_123"},
                        "error": None,
                        "duration_ms": 22,
                        "server": "finance-cli",
                    },
                    {"type": "stream_complete", "usage": {}},
                ]
            ]
        )
        bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)
        bot._active_skill = "onboarding"

        asyncio.run(bot._handle_agent_message(12345, "Start onboarding"))

        with connect(tmp_path / "telegram-bot.db") as conn:
            rows = conn.execute(
                """
                SELECT event, outcome, properties, source
                FROM analytics_events
                ORDER BY id
                """
            ).fetchall()

        observed = [
            (
                row["event"],
                row["outcome"],
                json.loads(row["properties"]) if row["properties"] else None,
                row["source"],
            )
            for row in rows
        ]

        assert observed == [
            ("onboarding.plaid_link", "started", None, "telegram"),
            ("onboarding.csv_import", "started", {"file_type": "pdf"}, "telegram"),
            ("onboarding.csv_import", "failed", {"file_type": "pdf"}, "telegram"),
            ("onboarding.first_categorization", "started", None, "telegram"),
            ("onboarding.first_categorization", "succeeded", None, "telegram"),
            ("onboarding.complete", "succeeded", None, "telegram"),
            ("onboarding.plaid_link", "succeeded", None, "telegram"),
        ]
    finally:
        store.shutdown()


def test_onboarding_ignores_legacy_profile_assessment_setup_events() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient(
        stream_plans=[
            [
                {
                    "type": "tool_call_complete",
                    "tool_call_id": "tool-state",
                    "tool_name": "skill_state_set",
                    "result": {
                        "data": {
                            "name": "onboarding",
                            "state": {
                                "profile_complete": True,
                                "assessment_shown": True,
                                "setup_complete": True,
                            },
                        }
                    },
                    "error": None,
                    "duration_ms": 4,
                    "server": "finance-cli",
                },
                {"type": "stream_complete", "usage": {}},
            ]
        ]
    )
    bot = TelegramBot(config=_make_config(), api=api, client=client)
    bot._active_skill = "onboarding"
    events: list[tuple[str, str]] = []

    def record_event(
        event: str,
        *,
        outcome: str,
        properties: dict[str, Any] | None = None,
        metrics: RequestMetrics | None = None,
    ) -> None:
        del properties, metrics
        events.append((event, outcome))

    bot._emit_onboarding_event = record_event  # type: ignore[method-assign]

    asyncio.run(bot._handle_agent_message(12345, "Continue onboarding"))

    assert events == []


def test_tool_only_turn_normalizes_empty_text_and_appends_tool_summary() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient(
        stream_plans=[
            [
                {
                    "type": "tool_call_start",
                    "tool_call_id": "tool-1",
                    "tool_name": "balance_show",
                    "tool_input": {"view": "personal"},
                },
                {
                    "type": "tool_call_complete",
                    "tool_call_id": "tool-1",
                    "tool_name": "balance_show",
                    "result": {"summary": {"net_worth": "$50,234"}},
                    "error": None,
                    "duration_ms": 12,
                    "server": "finance-cli",
                },
                {"type": "stream_complete", "usage": {}},
            ]
        ]
    )
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_agent_message(12345, "show balances"))

    assert bot._history[-1]["content"] == (
        'No response generated.\n\n[Tools: balance_show(view="personal") → {"net_worth": "$50,234"}]'
    )
    assert api.edits[-1]["text"].endswith("\nNo response generated.")


def test_code_execute_images_are_sent_and_stripped_from_history() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient(
        stream_plans=[
            [
                {
                    "type": "tool_call_start",
                    "tool_call_id": "tool-1",
                    "tool_name": "code_execute",
                    "tool_input": {"code": "print('chart')"},
                },
                {
                    "type": "tool_call_complete",
                    "tool_call_id": "tool-1",
                    "tool_name": "code_execute",
                    "result": {
                        "stdout": "",
                        "images": [
                            {
                                "filename": "chart.png",
                                "media_type": "image/png",
                                "data_base64": "ZmFrZS1pbWFnZQ==",
                            }
                        ],
                    },
                    "error": None,
                    "duration_ms": 12,
                    "server": None,
                },
                {"type": "stream_complete", "usage": {}},
            ]
        ]
    )
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_agent_message(12345, "show me a chart"))

    assert len(api.photos) == 1
    assert api.photos[0]["filename"] == "chart.png"
    assert api.photos[0]["caption"] == "chart.png"
    assert bot._history[-1]["content"] == (
        'No response generated.\n\n[Tools: code_execute(code="print(\'chart\')") → {"images": [{"data_base64": "[sent to user]", "filename": "chart.png", "media_type": "image/png"}], "stdout": ""}]'
    )


def test_failed_stream_turn_removes_user_message_from_history_and_store(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    try:
        api = FakeTelegramAPI()
        client = FakeGatewayClient(
            stream_plans=[
                [
                    {"type": "text_delta", "text": "Partial"},
                    {"type": "error", "error": "gateway failed"},
                ]
            ]
        )
        bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)

        asyncio.run(bot._handle_agent_message(12345, "status"))

        assert bot._history == []
        assert store.load_recent_messages(limit=10) == []
        assert any("Error: gateway failed" in str(message["text"]) for message in api.sent_messages)

        with connect(tmp_path / "telegram-bot.db") as conn:
            request = conn.execute("SELECT * FROM bot_requests").fetchone()
            assert request is not None
            assert request["error"] == "gateway failed"
    finally:
        store.shutdown()


def test_handle_agent_message_retries_once_after_401() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient(
        stream_plans=[
            BackendHTTPError(401, "expired"),
            [
                {"type": "text_delta", "text": "Recovered"},
                {"type": "stream_complete", "usage": {}},
            ],
        ],
        sessions=[
            SessionState(token="tok-1", session_id="sess-1", expires_at=10_000),
            SessionState(token="tok-2", session_id="sess-2", expires_at=20_000),
        ],
    )
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_agent_message(12345, "status"))

    assert client.invalidate_calls == 1
    assert len(client.stream_calls) == 2
    assert client.ensure_session_calls == [
        {"force_refresh": False, "user_id": "12345"},
        {"force_refresh": False, "user_id": "12345"},
    ]
    assert bot._history[-1] == {"role": "assistant", "content": "Recovered"}


def test_handle_agent_message_409_conflict_is_failed_turn() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient(stream_plans=[BackendHTTPError(409, "busy")])
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(bot._handle_agent_message(12345, "status"))

    assert bot._history == []
    assert api.sent_messages[-1]["text"] == "<i>Another request in progress.</i>"


def test_consume_stream_passes_skill_context_when_active() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient(stream_plans=[[{"type": "stream_complete", "usage": {}}]])
    bot = TelegramBot(config=_make_config(), api=api, client=client)
    bot._active_skill = "normalizer_builder"

    asyncio.run(
        bot._consume_stream(
            messages=[{"role": "user", "content": "status"}],
            draft=bot_module.DraftStream(api, 12345),
            assistant_parts=[],
            tool_inputs={},
            metrics=RequestMetrics(
                request_id="req-1",
                session_id="",
                model=bot._config.model,
            ),
        )
    )

    assert client.stream_calls[0]["context"] == {"skill": "normalizer_builder"}
    assert client.stream_calls[0]["user_id"] == "12345"


def test_consume_stream_omits_skill_context_when_inactive() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient(stream_plans=[[{"type": "stream_complete", "usage": {}}]])
    bot = TelegramBot(config=_make_config(), api=api, client=client)

    asyncio.run(
        bot._consume_stream(
            messages=[{"role": "user", "content": "status"}],
            draft=bot_module.DraftStream(api, 12345),
            assistant_parts=[],
            tool_inputs={},
            metrics=RequestMetrics(
                request_id="req-2",
                session_id="",
                model=bot._config.model,
            ),
        )
    )

    assert client.stream_calls[0]["context"] is None
    assert client.stream_calls[0]["user_id"] == "12345"


def test_history_trimming_after_success() -> None:
    api = FakeTelegramAPI()
    client = FakeGatewayClient(
        stream_plans=[[{"type": "text_delta", "text": "ok"}, {"type": "stream_complete", "usage": {}}]]
    )
    bot = TelegramBot(
        config=_make_config(TELEGRAM_BOT_HISTORY_MAX_TURNS=2),
        api=api,
        client=client,
    )
    bot._history = [
        {"role": "user", "content": "m1"},
        {"role": "assistant", "content": "m2"},
        {"role": "user", "content": "m3"},
        {"role": "assistant", "content": "m4"},
    ]

    asyncio.run(bot._handle_agent_message(12345, "latest"))

    assert len(bot._history) == 4
    assert bot._history[0]["content"] == "m3"
    assert bot._history[-2] == {"role": "user", "content": "latest"}
    assert bot._history[-1] == {"role": "assistant", "content": "ok"}


def test_stream_silent_auto_approves_allowed_tools_and_denies_unexpected() -> None:
    client = FakeGatewayClient(
        stream_plans=[
            [
                {
                    "type": "tool_approval_request",
                    "tool_call_id": "tool-1",
                    "nonce": "nonce-1",
                    "tool_name": "agent_session_write",
                },
                {
                    "type": "tool_approval_request",
                    "tool_call_id": "tool-2",
                    "nonce": "nonce-2",
                    "tool_name": "txn_list",
                },
                {"type": "text_delta", "text": "summary"},
            ]
        ]
    )
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), client=client)

    result = asyncio.run(bot._stream_silent([{"role": "user", "content": "compact this"}]))

    assert result == "summary"
    assert client.submit_calls == [
        ("tool-1", "nonce-1", True),
        ("tool-2", "nonce-2", False),
    ]


def test_run_compaction_rewrites_history_and_invalidates_between_phases() -> None:
    summary = (
        "The user and assistant had been reviewing budgets and balances, preserving specific "
        "amounts and follow-ups for future reference.\n\nThey also captured the active work "
        "thread so the remaining live history could stay short and focused."
    )
    client = FakeGatewayClient(
        stream_plans=[
            [{"type": "text_delta", "text": "Saved a session note."}],
            [{"type": "text_delta", "text": summary}],
        ]
    )
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), client=client)
    bot._history = [
        {"role": "user" if index % 2 == 0 else "assistant", "content": f"message-{index}"}
        for index in range(12)
    ]

    asyncio.run(bot._run_compaction())

    assert client.invalidate_calls == 1
    assert client.stream_calls[0]["context"] == {"compaction": True}
    assert client.stream_calls[1]["context"] == {"compaction": True}
    assert client.stream_calls[0]["user_id"] == "12345"
    assert client.stream_calls[1]["user_id"] == "12345"
    assert bot._history[0]["content"].startswith("[Previous conversation summary]\n")
    assert bot._history[1]["content"] == "Understood. I have the context from our previous conversation."
    assert len(bot._history) == KEEP_RECENT_MESSAGES + 2


def test_compaction_runs_before_user_message_append(monkeypatch) -> None:
    summary = (
        "The user and assistant had been reviewing budgets and balances, preserving specific "
        "amounts and follow-ups for future reference.\n\nThey also captured the active work "
        "thread so the remaining live history could stay short and focused."
    )
    client = FakeGatewayClient(
        stream_plans=[
            [{"type": "text_delta", "text": "Saved a session note."}],
            [{"type": "text_delta", "text": summary}],
            [{"type": "text_delta", "text": "ok"}, {"type": "stream_complete", "usage": {}}],
        ]
    )
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), client=client)
    bot._history = [
        {"role": "user" if index % 2 == 0 else "assistant", "content": f"message-{index}"}
        for index in range(12)
    ]
    monkeypatch.setattr(bot_module, "needs_compaction", lambda messages: True)

    asyncio.run(bot._handle_agent_message(12345, "latest"))

    third_call_messages = client.stream_calls[2]["messages"]
    assert third_call_messages[0]["content"].startswith("[Previous conversation summary]\n")
    assert third_call_messages[-1] == {"role": "user", "content": "latest"}


def test_stop_cancels_current_task_and_clears_pending_approvals() -> None:
    async def scenario() -> tuple[FakeTelegramAPI, TelegramBot]:
        api = FakeTelegramAPI()
        client = BlockingGatewayClient()
        bot = TelegramBot(config=_make_config(), api=api, client=client)
        task = asyncio.create_task(bot._run_as_current_task(bot._handle_agent_message(12345, "status")))
        await client.started.wait()
        bot._pending_approvals["nonce-1"] = {"tool_call_id": "tool-1"}
        await bot._handle_stop_command(12345)
        with suppress(asyncio.CancelledError):
            await task
        return api, bot

    api, bot = asyncio.run(scenario())

    assert bot._pending_approvals == {}
    assert api.sent_messages[-1]["text"] == "Stopped."


def test_get_updates_includes_callback_query(monkeypatch) -> None:
    from finance_cli.telegram_bot.telegram_api import TelegramAPI

    api = TelegramAPI("token", poll_timeout=5)
    captured: dict[str, Any] = {}

    async def fake_request(method: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        captured["method"] = method
        captured["payload"] = payload
        return []

    monkeypatch.setattr(api, "_request", fake_request)

    asyncio.run(api.get_updates(offset=10, timeout=30))

    assert captured["method"] == "getUpdates"
    assert captured["payload"]["offset"] == 10
    assert captured["payload"]["allowed_updates"] == ["message", "callback_query"]


def test_store_loads_only_successful_messages_after_reset(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    try:
        store.save_user_message("before", "req-before")
        store.save_assistant_message("before reply", "req-before")
        store.save_request(
            RequestMetrics(
                request_id="req-before",
                session_id="telegram-req-before",
                model="claude-sonnet-4-6",
            )
        )

        store.mark_history_reset()

        store.save_user_message("failed", "req-failed")
        failed = RequestMetrics(
            request_id="req-failed",
            session_id="telegram-req-failed",
            model="claude-sonnet-4-6",
            error="boom",
        )
        store.save_request(failed)

        store.save_user_message("latest", "req-latest")
        store.save_assistant_message("latest reply", "req-latest")
        store.save_request(
            RequestMetrics(
                request_id="req-latest",
                session_id="telegram-req-latest",
                model="claude-sonnet-4-6",
            )
        )

        assert store.load_recent_messages(limit=10) == [
            {"role": "user", "content": "latest"},
            {"role": "assistant", "content": "latest reply"},
        ]
    finally:
        store.shutdown()


def test_store_loads_successful_tool_only_turn_marker(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    tool_calls = [
        {
            "tool_name": "balance_show",
            "server": "finance-cli",
            "duration_ms": 12,
            "is_error": False,
            "result_bytes": 48,
        }
    ]

    try:
        store.save_user_message("show my balance", "req-tool-only")
        store.save_assistant_message(build_tool_only_turn_message(tool_calls), "req-tool-only")
        store.save_request(
            RequestMetrics(
                request_id="req-tool-only",
                session_id="telegram-req-tool-only",
                model="claude-sonnet-4-6",
                tool_calls=tool_calls,
                tool_call_count=1,
            )
        )

        store.save_user_message("failed", "req-failed")
        store.save_request(
            RequestMetrics(
                request_id="req-failed",
                session_id="telegram-req-failed",
                model="claude-sonnet-4-6",
                error="boom",
            )
        )

        assert store.load_recent_messages(limit=10) == [
            {"role": "user", "content": "show my balance"},
            {
                "role": "assistant",
                "content": (
                    "[Tool-only turn]\n"
                    "The assistant completed this turn by calling tools but did not emit a final text response.\n"
                    "Tools used: balance_show (ok)."
                ),
            },
        ]
    finally:
        store.shutdown()


def test_store_save_compaction_marks_old_messages(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    try:
        _save_successful_turn(store, "req-1", "u1", "a1")
        _save_successful_turn(store, "req-2", "u2", "a2")
        _save_successful_turn(store, "req-3", "u3", "a3")
        _save_successful_turn(store, "req-4", "u4", "a4")

        store.save_compaction("Summary with enough detail to persist across restarts.", keep_recent=6)

        with connect(tmp_path / "telegram-bot.db") as conn:
            rows = conn.execute(
                "SELECT role, content, request_id, compacted_at FROM bot_chat_messages ORDER BY id"
            ).fetchall()

        assert rows[0]["compacted_at"] is not None
        assert rows[1]["compacted_at"] is not None
        assert [rows[idx]["compacted_at"] for idx in range(2, 8)] == [None] * 6
        assert rows[8]["request_id"] == "[COMPACTION]"
        assert rows[8]["content"].startswith("[Previous conversation summary]\n")
        assert rows[8]["compacted_at"] is None
        assert rows[9]["request_id"] == "[COMPACTION]"
        assert rows[9]["content"] == "Understood. I have the context from our previous conversation."
        assert rows[9]["compacted_at"] is None
    finally:
        store.shutdown()


def test_store_save_compaction_does_not_count_failed_request_orphan_rows(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    try:
        _save_successful_turn(store, "req-1", "u1", "a1")
        _save_failed_turn(store, "req-failed", "failed only")
        _save_successful_turn(store, "req-2", "u2", "a2")
        _save_successful_turn(store, "req-3", "u3", "a3")

        store.save_compaction("Summary with enough detail to persist across restarts.", keep_recent=4)

        with connect(tmp_path / "telegram-bot.db") as conn:
            rows = conn.execute(
                "SELECT content, request_id, compacted_at FROM bot_chat_messages ORDER BY id"
            ).fetchall()

        assert rows[0]["compacted_at"] is not None
        assert rows[1]["compacted_at"] is not None
        assert rows[2]["request_id"] == "req-failed"
        assert rows[2]["compacted_at"] is None
    finally:
        store.shutdown()


def test_store_load_recent_messages_includes_summary_pair_after_restart(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    try:
        _save_successful_turn(store, "req-1", "u1", "a1")
        _save_successful_turn(store, "req-2", "u2", "a2")
        _save_successful_turn(store, "req-3", "u3", "a3")
        _save_successful_turn(store, "req-4", "u4", "a4")
        store.save_compaction("Summary with enough detail to persist across restarts.", keep_recent=6)
    finally:
        store.shutdown()

    reloaded = _make_store(tmp_path)
    try:
        assert reloaded.load_recent_messages(limit=20) == [
            {"role": "user", "content": "u2"},
            {"role": "assistant", "content": "a2"},
            {"role": "user", "content": "u3"},
            {"role": "assistant", "content": "a3"},
            {"role": "user", "content": "u4"},
            {"role": "assistant", "content": "a4"},
            {
                "role": "user",
                "content": "[Previous conversation summary]\nSummary with enough detail to persist across restarts.",
            },
            {
                "role": "assistant",
                "content": "Understood. I have the context from our previous conversation.",
            },
        ]
    finally:
        reloaded.shutdown()


def test_run_bot_restores_history_and_closes_store(monkeypatch, tmp_path: Path) -> None:
    config = _make_config()
    events: list[str] = []
    pid_file = tmp_path / "telegram.pid"
    logger = logging.getLogger("finance_cli")
    original_level = logger.level

    class FakeStore:
        def __init__(self) -> None:
            self.history = [
                {"role": "user", "content": "restored user"},
                {"role": "assistant", "content": "restored assistant"},
            ]

        def startup(self) -> None:
            events.append("store.startup")

        def load_recent_messages(self, limit: int = 40) -> list[dict[str, str]]:
            events.append(f"store.load:{limit}")
            return list(self.history)

        def close_all_open_sessions(self, reason: str = "restart") -> int:
            events.append(f"store.close_all:{reason}")
            return 0

        def shutdown(self) -> None:
            events.append("store.shutdown")

    class FakeClient:
        def __init__(self, gateway_url: str, gateway_user_key: str) -> None:
            assert gateway_url == config.gateway_url
            assert gateway_user_key == config.gateway_user_key
            events.append("client.init")

        async def ensure_session(self, *, user_id: str | None = None, force_refresh: bool = False):
            events.append(f"client.ensure:{force_refresh}:{user_id}")
            return SessionState("tok", "sess", 9999)

        async def close(self) -> None:
            events.append("client.close")

    class FakeTelegramBot:
        def __init__(self, passed_config: BotConfig, *, client, store, api=None) -> None:
            assert passed_config == config
            assert api is None
            events.append("bot.init")
            self.client = client
            self._history: list[dict[str, str]] = []

        def stop(self) -> None:
            events.append("bot.stop")

        def _close_session(self, reason: str, message_time: float = 0.0) -> None:
            del message_time
            events.append(f"bot.close_session:{reason}")

        async def start(self) -> None:
            events.append("bot.start")
            assert self._history == [
                {"role": "user", "content": "restored user"},
                {"role": "assistant", "content": "restored assistant"},
            ]

    monkeypatch.setattr(bot_module, "load_dotenv", lambda: events.append("load_dotenv"))
    monkeypatch.setattr(bot_module, "setup_logging", lambda: events.append("setup_logging"))
    monkeypatch.setattr(bot_module, "load_config", lambda: config)
    monkeypatch.setattr(bot_module, "BotStore", FakeStore)
    monkeypatch.setattr(bot_module, "GatewayClient", FakeClient)
    monkeypatch.setattr(bot_module, "TelegramBot", FakeTelegramBot)
    monkeypatch.setattr(
        bot_module,
        "_pid_file_for_token",
        lambda token: events.append(f"pid.path:{token}") or pid_file,
    )
    monkeypatch.setattr(
        bot_module,
        "_acquire_pid_lock",
        lambda path: events.append(f"pid.acquire:{path}"),
    )
    monkeypatch.setattr(bot_module, "_release_pid_lock", lambda: events.append("pid.release"))
    monkeypatch.delenv("FINANCE_CLI_LOG_LEVEL", raising=False)

    try:
        asyncio.run(bot_module.run_bot())

        assert events == [
            "load_dotenv",
            "setup_logging",
            "pid.path:bot-token",
            f"pid.acquire:{pid_file}",
            "store.startup",
            "client.init",
            "client.ensure:False:12345",
            "bot.init",
            "store.load:40",
            "store.close_all:restart",
            "bot.start",
            "bot.close_session:restart",
            "pid.release",
            "client.close",
            "store.shutdown",
        ]
        assert logger.level == logging.INFO
    finally:
        logger.setLevel(original_level)


def test_run_bot_respects_debug_log_level(monkeypatch, tmp_path: Path) -> None:
    logger = logging.getLogger("finance_cli")
    original_level = logger.level
    pid_file = tmp_path / "telegram.pid"

    class FakeStore:
        def startup(self) -> None:
            return None

        def load_recent_messages(self, limit: int = 40) -> list[dict[str, str]]:
            return []

        def close_all_open_sessions(self, reason: str = "restart") -> int:
            del reason
            return 0

        def shutdown(self) -> None:
            return None

    class FakeClient:
        def __init__(self, gateway_url: str, gateway_user_key: str) -> None:
            del gateway_url, gateway_user_key

        async def ensure_session(self, *, user_id: str | None = None, force_refresh: bool = False):
            del user_id, force_refresh
            return SessionState("tok", "sess", 9999)

        async def close(self) -> None:
            return None

    class FakeTelegramBot:
        def __init__(self, passed_config: BotConfig, *, client, store, api=None) -> None:
            del passed_config, client, store, api
            self._history: list[dict[str, str]] = []

        def stop(self) -> None:
            return None

        def _close_session(self, reason: str, message_time: float = 0.0) -> None:
            del reason, message_time
            return None

        async def start(self) -> None:
            return None

    monkeypatch.setattr(bot_module, "load_dotenv", lambda: None)
    monkeypatch.setattr(bot_module, "setup_logging", lambda: None)
    monkeypatch.setattr(bot_module, "load_config", _make_config)
    monkeypatch.setattr(bot_module, "BotStore", FakeStore)
    monkeypatch.setattr(bot_module, "GatewayClient", FakeClient)
    monkeypatch.setattr(bot_module, "TelegramBot", FakeTelegramBot)
    monkeypatch.setattr(bot_module, "_pid_file_for_token", lambda token: pid_file)
    monkeypatch.setattr(bot_module, "_acquire_pid_lock", lambda path: None)
    monkeypatch.setattr(bot_module, "_release_pid_lock", lambda: None)
    monkeypatch.setenv("FINANCE_CLI_LOG_LEVEL", "debug")

    try:
        asyncio.run(bot_module.run_bot())
        assert logger.level == logging.DEBUG
    finally:
        logger.setLevel(original_level)
