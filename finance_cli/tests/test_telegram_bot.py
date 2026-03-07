from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from finance_cli.telegram_bot import agent as agent_module
from finance_cli.telegram_bot.agent import FinanceAgent
from finance_cli.telegram_bot import bot as bot_module
from finance_cli.telegram_bot.bot import TelegramBot
from finance_cli.telegram_bot.config import BotConfig, load_config
from finance_cli.telegram_bot.store import BotStore
from finance_cli.telegram_bot.telegram_api import split_message
from finance_cli.db import connect


def _make_config() -> BotConfig:
    return BotConfig(
        telegram_token="bot-token",
        telegram_chat_id="12345",
        anthropic_api_key="test-key",
    )


def _message_update(chat_id: int, text: str) -> dict[str, object]:
    return {
        "update_id": 1,
        "message": {
            "chat": {"id": chat_id},
            "text": text,
        },
    }


class FakeTelegramAPI:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, object]] = []
        self.chat_actions: list[dict[str, object]] = []
        self.edits: list[dict[str, object]] = []
        self._next_message_id = 100

    async def get_updates(self, offset: int | None = None, timeout: int | None = None) -> list[dict[str, object]]:
        return []

    async def send_message(self, chat_id: str | int | None, text: str) -> dict[str, object]:
        message = {
            "chat_id": chat_id,
            "text": text,
            "message_id": self._next_message_id,
        }
        self._next_message_id += 1
        self.sent_messages.append(message)
        return {"message_id": message["message_id"]}

    async def send_chat_action(self, chat_id: str | int | None, action: str) -> dict[str, object]:
        self.chat_actions.append({"chat_id": chat_id, "action": action})
        return {"ok": True}

    async def edit_message_text(
        self,
        chat_id: str | int | None,
        message_id: int,
        text: str,
    ) -> dict[str, object]:
        self.edits.append({"chat_id": chat_id, "message_id": message_id, "text": text})
        return {"message_id": message_id}


class FakeAgent:
    def __init__(self, *, response: str = "All set.", error: Exception | None = None) -> None:
        self.response = response
        self.error = error
        self.run_calls: list[str] = []
        self.history: list[dict[str, str]] = []
        self.model_override: str | None = None
        self.reset_count = 0

    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    async def run(self, text: str) -> str:
        self.run_calls.append(text)
        if self.error is not None:
            raise self.error
        return self.response

    def reset_history(self) -> None:
        self.reset_count += 1
        self.history.clear()
        self.model_override = None


def _make_store(tmp_path: Path) -> BotStore:
    store = BotStore(tmp_path / "telegram-bot.db")
    store.startup()
    return store


def test_config_loads_from_env(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("TELEGRAM_BOT_MODEL", "claude-opus-4-6")
    monkeypatch.setenv("TELEGRAM_BOT_MAX_TURNS", "9")
    monkeypatch.setenv("TELEGRAM_BOT_MAX_TOKENS", "8000")
    monkeypatch.setenv("TELEGRAM_BOT_THINKING", "false")
    monkeypatch.setenv("TELEGRAM_BOT_HISTORY_MAX_TURNS", "7")
    monkeypatch.setenv("TELEGRAM_BOT_POLL_TIMEOUT", "12")

    config = load_config()

    assert config.telegram_token == "bot-token"
    assert config.telegram_chat_id == "999"
    assert config.anthropic_api_key == "sk-test"
    assert config.model == "claude-opus-4-6"
    assert config.max_turns == 9
    assert config.max_tokens == 8000
    assert config.thinking is False
    assert config.history_max_turns == 7
    assert config.poll_timeout == 12


def test_config_missing_required(monkeypatch) -> None:
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    with pytest.raises(ValueError, match="TELEGRAM_BOT_TOKEN"):
        load_config()


def test_config_defaults(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.delenv("TELEGRAM_BOT_MODEL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_MAX_TURNS", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_MAX_TOKENS", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_THINKING", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_HISTORY_MAX_TURNS", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_POLL_TIMEOUT", raising=False)

    config = load_config()

    assert config.model == "claude-sonnet-4-6"
    assert config.max_turns == 15
    assert config.max_tokens == 16000
    assert config.thinking is True
    assert config.history_max_turns == 20
    assert config.poll_timeout == 30


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
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), agent=FakeAgent())
    bot._handle_agent_message = AsyncMock()  # type: ignore[method-assign]

    asyncio.run(bot._handle_update(_message_update(99999, "hello")))

    bot._handle_agent_message.assert_not_awaited()
    assert bot.api.sent_messages == []


def test_auth_gate_accepts() -> None:
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), agent=FakeAgent())
    bot._handle_agent_message = AsyncMock()  # type: ignore[method-assign]

    asyncio.run(bot._handle_update(_message_update(12345, "hello")))

    bot._handle_agent_message.assert_awaited_once_with(12345, "hello")


def test_command_reset() -> None:
    agent = FakeAgent()
    agent.history = [{"role": "user", "content": "old"}]
    agent.model_override = "claude-opus-4-6"
    api = FakeTelegramAPI()
    bot = TelegramBot(config=_make_config(), api=api, agent=agent)

    asyncio.run(bot._handle_command(12345, "/reset"))

    assert agent.reset_count == 1
    assert agent.history == []
    assert agent.model_override is None
    assert api.sent_messages[-1]["text"] == "History cleared."


def test_handle_agent_success() -> None:
    api = FakeTelegramAPI()
    agent = FakeAgent(response="Net worth is $10.")
    bot = TelegramBot(config=_make_config(), api=api, agent=agent)

    asyncio.run(bot._handle_agent_message(12345, "status"))

    assert agent.run_calls == ["status"]
    assert api.sent_messages[0]["text"] == "Working on it..."
    assert api.sent_messages[-1]["text"] == "Net worth is $10."
    assert api.edits[-1]["text"] == "Done."


def test_handle_agent_error() -> None:
    api = FakeTelegramAPI()
    agent = FakeAgent(error=RuntimeError("boom"))
    bot = TelegramBot(config=_make_config(), api=api, agent=agent)

    asyncio.run(bot._handle_agent_message(12345, "status"))

    assert agent.run_calls == ["status"]
    assert api.sent_messages[-1]["text"] == "Error: boom"
    assert api.edits[-1]["text"] == "Done."


def test_history_trimming(monkeypatch) -> None:
    class FakeRunner:
        def __init__(self, **kwargs) -> None:
            self.event_log = kwargs["event_log"]

        async def run(self, messages, system_prompt, max_turns) -> None:
            self.event_log.append({"type": "text_delta", "text": "ok"})
            self.event_log.append({"type": "stream_complete", "usage": {}})

    monkeypatch.setattr(agent_module, "AgentRunner", FakeRunner)

    agent = FinanceAgent(_make_config())
    agent._mcp = object()
    agent.history = [
        {
            "role": "user" if index % 2 == 0 else "assistant",
            "content": f"msg-{index}",
        }
        for index in range(40)
    ]

    response = asyncio.run(agent.run("latest"))

    assert response == "ok"
    assert len(agent.history) == 40
    assert agent.history[0]["content"] == "msg-2"
    assert agent.history[-2] == {"role": "user", "content": "latest"}
    assert agent.history[-1] == {"role": "assistant", "content": "ok"}


def test_store_loads_only_successful_messages_after_reset(tmp_path) -> None:
    store = _make_store(tmp_path)

    try:
        store.save_user_message("before", "req-before")
        store.save_assistant_message("before reply", "req-before")
        store.save_request(
            agent_module.RequestMetrics(
                request_id="req-before",
                session_id="telegram-req-before",
                model="claude-sonnet-4-6",
            )
        )

        store.mark_history_reset()

        store.save_user_message("failed", "req-failed")
        failed = agent_module.RequestMetrics(
            request_id="req-failed",
            session_id="telegram-req-failed",
            model="claude-sonnet-4-6",
            error="boom",
        )
        store.save_request(failed)

        store.save_user_message("latest", "req-latest")
        store.save_assistant_message("latest reply", "req-latest")
        store.save_request(
            agent_module.RequestMetrics(
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


def test_agent_persists_metrics_and_tool_calls(tmp_path, monkeypatch) -> None:
    class FakeRunner:
        def __init__(self, **kwargs) -> None:
            self.event_log = kwargs["event_log"]
            self.session_id = kwargs["session_id"]

        async def run(self, messages, system_prompt, max_turns) -> None:
            assert self.session_id.startswith("telegram-")
            self.event_log.append({"type": "text_delta", "text": "Balance summary"})
            self.event_log.append(
                {
                    "type": "tool_call_complete",
                    "tool_call_id": "tool-1",
                    "tool_name": "balance_show",
                    "result": {"cash": 1234},
                    "error": None,
                    "duration_ms": 321,
                    "server": "finance-cli",
                }
            )
            self.event_log.append(
                {
                    "type": "stream_complete",
                    "usage": {
                        "input_tokens": 12,
                        "output_tokens": 34,
                        "cache_creation_input_tokens": 5,
                        "cache_read_input_tokens": 6,
                        "estimated_cost": 0.0123,
                    },
                }
            )

    monkeypatch.setattr(agent_module, "AgentRunner", FakeRunner)
    store = _make_store(tmp_path)

    try:
        agent = FinanceAgent(_make_config(), store=store)
        agent._mcp = object()

        response = asyncio.run(agent.run("show balances"))

        assert response == "Balance summary"
        assert store.load_recent_messages(limit=10) == [
            {"role": "user", "content": "show balances"},
            {"role": "assistant", "content": "Balance summary"},
        ]

        with connect(tmp_path / "telegram-bot.db") as conn:
            request = conn.execute("SELECT * FROM bot_requests").fetchone()
            assert request is not None
            assert request["model"] == "claude-sonnet-4-6"
            assert request["session_id"] == f"telegram-{request['request_id']}"
            assert request["input_tokens"] == 12
            assert request["output_tokens"] == 34
            assert request["cache_creation_tokens"] == 5
            assert request["cache_read_tokens"] == 6
            assert request["estimated_cost"] == pytest.approx(0.0123)
            assert request["tool_call_count"] == 1
            assert request["error"] is None

            tool_call = conn.execute("SELECT * FROM bot_tool_calls").fetchone()
            assert tool_call is not None
            assert tool_call["tool_name"] == "balance_show"
            assert tool_call["server"] == "finance-cli"
            assert tool_call["duration_ms"] == 321
            assert tool_call["is_error"] == 0
            assert tool_call["result_bytes"] == len(json.dumps({"cash": 1234}, default=str))
    finally:
        store.shutdown()


def test_agent_persists_error_request_but_not_history(tmp_path, monkeypatch) -> None:
    class FakeRunner:
        def __init__(self, **kwargs) -> None:
            self.event_log = kwargs["event_log"]

        async def run(self, messages, system_prompt, max_turns) -> None:
            self.event_log.append({"type": "text_delta", "text": "Partial"})
            self.event_log.append({"type": "error", "error": "gateway failed"})

    monkeypatch.setattr(agent_module, "AgentRunner", FakeRunner)
    store = _make_store(tmp_path)

    try:
        agent = FinanceAgent(_make_config(), store=store)
        agent._mcp = object()

        response = asyncio.run(agent.run("status"))

        assert response == "Partial\n\nWarning: response may be incomplete (agent error: gateway failed)"
        assert agent.history == []
        assert store.load_recent_messages(limit=10) == []

        with connect(tmp_path / "telegram-bot.db") as conn:
            request = conn.execute("SELECT * FROM bot_requests").fetchone()
            assert request is not None
            assert request["error"] == "gateway failed"

            messages = conn.execute(
                "SELECT role, content FROM bot_chat_messages ORDER BY id"
            ).fetchall()
            assert [(row["role"], row["content"]) for row in messages] == [("user", "status")]
    finally:
        store.shutdown()


def test_agent_reset_marks_store(tmp_path) -> None:
    store = _make_store(tmp_path)

    try:
        agent = FinanceAgent(_make_config(), store=store)
        agent.history = [{"role": "user", "content": "old"}]
        agent.model_override = "claude-opus-4-6"

        agent.reset_history()

        assert agent.history == []
        assert agent.model_override is None

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


def test_run_bot_restores_history_and_closes_store(monkeypatch) -> None:
    config = _make_config()
    events: list[str] = []

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

        def shutdown(self) -> None:
            events.append("store.shutdown")

    class FakeFinanceAgent:
        def __init__(self, passed_config: BotConfig, store) -> None:
            assert passed_config == config
            assert isinstance(store, FakeStore)
            events.append("agent.init")
            self.history: list[dict[str, str]] = []

    class FakeTelegramBot:
        def __init__(self, passed_config: BotConfig, *, agent, api=None) -> None:
            assert passed_config == config
            events.append("bot.init")
            self.agent = agent

        def stop(self) -> None:
            events.append("bot.stop")

        async def start(self) -> None:
            events.append("bot.start")
            assert self.agent.history == [
                {"role": "user", "content": "restored user"},
                {"role": "assistant", "content": "restored assistant"},
            ]

    monkeypatch.setattr(bot_module, "load_dotenv", lambda: events.append("load_dotenv"))
    monkeypatch.setattr(bot_module, "setup_logging", lambda: events.append("setup_logging"))
    monkeypatch.setattr(bot_module, "load_config", lambda: config)
    monkeypatch.setattr(bot_module, "BotStore", FakeStore)
    monkeypatch.setattr(bot_module, "FinanceAgent", FakeFinanceAgent)
    monkeypatch.setattr(bot_module, "TelegramBot", FakeTelegramBot)

    asyncio.run(bot_module.run_bot())

    assert events == [
        "load_dotenv",
        "setup_logging",
        "store.startup",
        "agent.init",
        "store.load:40",
        "bot.init",
        "bot.start",
        "store.shutdown",
    ]
