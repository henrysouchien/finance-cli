from __future__ import annotations

import asyncio
import importlib
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from finance_cli.db import _apply_bot_sessions, connect, initialize_database
from finance_cli.telegram_bot import bot as bot_module
from finance_cli.telegram_bot.bot import TelegramBot
from finance_cli.telegram_bot.config import BotConfig
from finance_cli.telegram_bot.gateway_client import SessionState
from finance_cli.telegram_bot.store import BotStore, RequestMetrics


def _ts(year: int, month: int, day: int, hour: int, minute: int, second: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc).timestamp())


def _sqlite_time(unix_ts: int) -> str:
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _make_config(**overrides: Any) -> BotConfig:
    base = dict(
        TELEGRAM_BOT_TOKEN="bot-token",
        TELEGRAM_CHAT_ID="12345",
        TELEGRAM_GATEWAY_URL="http://127.0.0.1:8002",
        GATEWAY_USER_KEY="gateway-key",
        ANTHROPIC_AUTH_TOKEN="",
        TELEGRAM_BOT_SESSION_IDLE_TIMEOUT=1800,
    )
    base.update(overrides)
    return BotConfig(**base)


def _message_update(chat_id: int, text: str, *, date: int) -> dict[str, object]:
    return {
        "update_id": 1,
        "message": {
            "chat": {"id": chat_id},
            "text": text,
            "date": date,
        },
    }


class FakeTelegramAPI:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, object]] = []
        self.edits: list[dict[str, object]] = []
        self.chat_actions: list[dict[str, object]] = []
        self._next_message_id = 100

    async def get_updates(self, offset: int | None = None, timeout: int | None = None) -> list[dict[str, object]]:
        del offset, timeout
        return []

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
        del inline_keyboard
        return await self.send_message(chat_id, text)

    async def send_chat_action(self, chat_id: str | int | None, action: str) -> dict[str, object]:
        self.chat_actions.append({"chat_id": chat_id, "action": action})
        return {"ok": True}

    async def answer_callback_query(self, callback_query_id: str, text: str = "") -> dict[str, object]:
        del callback_query_id, text
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


class FakeGatewayClient:
    def __init__(self, *, stream_plans: list[Any] | None = None) -> None:
        self.stream_plans = list(stream_plans or [])
        self.stream_calls: list[dict[str, Any]] = []
        self.ensure_session_calls: list[dict[str, Any]] = []
        self.invalidate_calls = 0
        self.submit_calls: list[tuple[str, str, bool]] = []
        self.close_calls = 0

    async def ensure_session(
        self,
        *,
        user_id: str | None = None,
        force_refresh: bool = False,
    ) -> SessionState:
        self.ensure_session_calls.append({"force_refresh": force_refresh, "user_id": user_id})
        return SessionState(token="tok-1", session_id="gateway-sess", expires_at=999999)

    def invalidate_session(self) -> None:
        self.invalidate_calls += 1

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
        return 200, {"status": "ok"}


def _make_store(tmp_path: Path) -> tuple[BotStore, Path]:
    db_path = tmp_path / "telegram-bot.db"
    store = BotStore(db_path)
    store.startup()
    return store, db_path


def _save_successful_turn(
    store: BotStore,
    request_id: str,
    *,
    session_id: str,
    user_text: str,
    assistant_text: str,
    estimated_cost: float = 0.0,
    tool_calls: list[dict[str, Any]] | None = None,
) -> None:
    store.save_user_message(user_text, request_id, bot_session_id=session_id)
    store.save_assistant_message(assistant_text, request_id, bot_session_id=session_id)
    store.save_request(
        RequestMetrics(
            request_id=request_id,
            session_id=f"gateway-{request_id}",
            model="claude-sonnet-4-6",
            bot_session_id=session_id,
            estimated_cost=estimated_cost,
            tool_calls=list(tool_calls or []),
            tool_call_count=len(tool_calls or []),
        )
    )


def _save_failed_turn(
    store: BotStore,
    request_id: str,
    *,
    session_id: str,
    user_text: str,
    tool_calls: list[dict[str, Any]] | None = None,
) -> None:
    store.save_user_message(user_text, request_id, bot_session_id=session_id)
    store.save_request(
        RequestMetrics(
            request_id=request_id,
            session_id=f"gateway-{request_id}",
            model="claude-sonnet-4-6",
            bot_session_id=session_id,
            error="boom",
            tool_calls=list(tool_calls or []),
            tool_call_count=len(tool_calls or []),
        )
    )


def _import_mcp_server(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, db_path: Path):
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    sys.modules.pop("finance_cli.mcp_server", None)
    return importlib.import_module("finance_cli.mcp_server")


def test_handle_agent_message_passes_chat_id_as_gateway_user_id(tmp_path: Path) -> None:
    store, _db_path = _make_store(tmp_path)
    api = FakeTelegramAPI()
    client = FakeGatewayClient(stream_plans=[[{"type": "stream_complete", "usage": {}}]])
    bot = TelegramBot(config=_make_config(), api=api, client=client, store=store)

    try:
        asyncio.run(bot._handle_agent_message(12345, "status"))
    finally:
        store.shutdown()

    assert client.ensure_session_calls == [{"force_refresh": False, "user_id": "12345"}]
    assert client.stream_calls[0]["user_id"] == "12345"


def test_migration_034_creates_bot_sessions_columns_and_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        tables = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        session_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(bot_sessions)").fetchall()
        }
        message_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(bot_chat_messages)").fetchall()
        }
        request_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(bot_requests)").fetchall()
        }
        session_indexes = {
            str(row["name"]) for row in conn.execute("PRAGMA index_list(bot_sessions)").fetchall()
        }

    assert "bot_sessions" in tables
    assert {
        "session_id",
        "started_at",
        "ended_at",
        "end_reason",
        "last_activity_at",
        "message_count",
        "request_count",
        "total_cost",
    }.issubset(session_columns)
    assert "bot_session_id" in message_columns
    assert "bot_session_id" in request_columns
    assert {"idx_bot_sessions_started", "idx_bot_sessions_ended"}.issubset(session_indexes)


def test_apply_bot_sessions_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _apply_bot_sessions(conn)
        _apply_bot_sessions(conn)
        message_columns = [
            str(row["name"]) for row in conn.execute("PRAGMA table_info(bot_chat_messages)").fetchall()
        ]
        request_columns = [
            str(row["name"]) for row in conn.execute("PRAGMA table_info(bot_requests)").fetchall()
        ]

    assert message_columns.count("bot_session_id") == 1
    assert request_columns.count("bot_session_id") == 1


def test_store_session_crud_aggregates_and_tags_rows(tmp_path: Path) -> None:
    store, db_path = _make_store(tmp_path)
    started_at = _ts(2026, 3, 10, 14, 0)
    active_at = _ts(2026, 3, 10, 14, 15)

    try:
        store.start_session("sess-1", message_time=started_at)
        _save_successful_turn(
            store,
            "req-1",
            session_id="sess-1",
            user_text="hello",
            assistant_text="hi there",
            estimated_cost=1.25,
        )
        store.update_session_activity("sess-1", message_time=active_at)
        store.end_session("sess-1", "idle")

        with connect(db_path) as conn:
            session = conn.execute(
                """
                SELECT started_at, last_activity_at, ended_at, end_reason,
                       message_count, request_count, total_cost
                FROM bot_sessions
                WHERE session_id = 'sess-1'
                """
            ).fetchone()
            message_rows = conn.execute(
                "SELECT role, bot_session_id FROM bot_chat_messages ORDER BY id"
            ).fetchall()
            request_row = conn.execute(
                "SELECT bot_session_id FROM bot_requests WHERE request_id = 'req-1'"
            ).fetchone()

        assert session is not None
        assert session["started_at"] == _sqlite_time(started_at)
        assert session["last_activity_at"] == _sqlite_time(active_at)
        assert session["ended_at"] == _sqlite_time(active_at)
        assert session["end_reason"] == "idle"
        assert session["message_count"] == 2
        assert session["request_count"] == 1
        assert session["total_cost"] == pytest.approx(1.25)
        assert [row["bot_session_id"] for row in message_rows] == ["sess-1", "sess-1"]
        assert request_row is not None and request_row["bot_session_id"] == "sess-1"
    finally:
        store.shutdown()


def test_store_end_session_uses_explicit_reset_time(tmp_path: Path) -> None:
    store, db_path = _make_store(tmp_path)
    started_at = _ts(2026, 3, 10, 15, 0)
    active_at = _ts(2026, 3, 10, 15, 20)
    reset_at = _ts(2026, 3, 10, 15, 45)

    try:
        store.start_session("sess-reset", message_time=started_at)
        store.update_session_activity("sess-reset", message_time=active_at)
        store.end_session("sess-reset", "reset", ended_at_time=reset_at)

        with connect(db_path) as conn:
            row = conn.execute(
                "SELECT last_activity_at, ended_at, end_reason FROM bot_sessions WHERE session_id = ?",
                ("sess-reset",),
            ).fetchone()

        assert row is not None
        assert row["last_activity_at"] == _sqlite_time(active_at)
        assert row["ended_at"] == _sqlite_time(reset_at)
        assert row["end_reason"] == "reset"
    finally:
        store.shutdown()


def test_store_close_all_open_sessions_and_get_last_session(tmp_path: Path) -> None:
    store, db_path = _make_store(tmp_path)
    first_at = _ts(2026, 3, 10, 10, 0)
    second_at = _ts(2026, 3, 10, 11, 0)
    third_at = _ts(2026, 3, 10, 12, 0)

    try:
        store.start_session("sess-empty", message_time=first_at)
        store.end_session("sess-empty", "restart")

        store.start_session("sess-1", message_time=second_at)
        _save_successful_turn(
            store,
            "req-1",
            session_id="sess-1",
            user_text="first",
            assistant_text="reply one",
            estimated_cost=0.4,
        )
        store.update_session_activity("sess-1", message_time=second_at + 60)

        store.start_session("sess-2", message_time=third_at)
        _save_successful_turn(
            store,
            "req-2",
            session_id="sess-2",
            user_text="second",
            assistant_text="reply two",
            estimated_cost=0.8,
        )
        store.update_session_activity("sess-2", message_time=third_at + 60)

        closed = store.close_all_open_sessions("restart")

        with connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT session_id, ended_at, end_reason, message_count, request_count, total_cost
                FROM bot_sessions
                ORDER BY started_at
                """
            ).fetchall()

        last = store.get_last_session()

        assert closed == 2
        assert [row["session_id"] for row in rows] == ["sess-empty", "sess-1", "sess-2"]
        assert rows[1]["message_count"] == 2
        assert rows[1]["request_count"] == 1
        assert rows[1]["total_cost"] == pytest.approx(0.4)
        assert rows[2]["message_count"] == 2
        assert rows[2]["request_count"] == 1
        assert rows[2]["total_cost"] == pytest.approx(0.8)
        assert last is not None
        assert last["session_id"] == "sess-2"
    finally:
        store.shutdown()


def test_ensure_session_uses_message_time_and_closes_idle(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    store, db_path = _make_store(tmp_path)
    bot = TelegramBot(
        config=_make_config(TELEGRAM_BOT_SESSION_IDLE_TIMEOUT=10),
        api=FakeTelegramAPI(),
        client=FakeGatewayClient(),
        store=store,
    )

    try:
        monkeypatch.setattr(bot_module.time, "time", lambda: 9_999_999.0)

        first = bot._ensure_session(_ts(2026, 3, 10, 9, 0))
        second = bot._ensure_session(_ts(2026, 3, 10, 9, 0, 5))
        third = bot._ensure_session(_ts(2026, 3, 10, 9, 0, 20))

        with connect(db_path) as conn:
            rows = conn.execute(
                "SELECT session_id, end_reason, ended_at FROM bot_sessions ORDER BY started_at"
            ).fetchall()

        assert first == second
        assert third != first
        assert rows[0]["session_id"] == first
        assert rows[0]["end_reason"] == "idle"
        assert rows[0]["ended_at"] == _sqlite_time(_ts(2026, 3, 10, 9, 0, 5))
        assert rows[1]["session_id"] == third
        assert rows[1]["ended_at"] is None
    finally:
        store.shutdown()


def test_status_creates_session_but_history_command_does_not(tmp_path: Path) -> None:
    store, db_path = _make_store(tmp_path)
    client = FakeGatewayClient(
        stream_plans=[[{"type": "text_delta", "text": "dashboard"}, {"type": "stream_complete", "usage": {}}]]
    )
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), client=client, store=store)

    try:
        asyncio.run(bot._handle_command(12345, "/history", message_time=_ts(2026, 3, 10, 8, 0)))
        asyncio.run(bot._handle_command(12345, "/status", message_time=_ts(2026, 3, 10, 8, 5)))

        with connect(db_path) as conn:
            count = conn.execute("SELECT COUNT(*) AS cnt FROM bot_sessions").fetchone()["cnt"]

        assert count == 1
    finally:
        store.shutdown()


def test_reset_closes_session_and_tags_reset_sentinel(tmp_path: Path) -> None:
    store, db_path = _make_store(tmp_path)
    client = FakeGatewayClient(
        stream_plans=[[{"type": "text_delta", "text": "ok"}, {"type": "stream_complete", "usage": {}}]]
    )
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), client=client, store=store)
    started_at = _ts(2026, 3, 10, 16, 0)
    reset_at = _ts(2026, 3, 10, 16, 30)

    try:
        asyncio.run(bot._handle_agent_message(12345, "hello", message_time=started_at))
        session_id = bot._current_session_id

        asyncio.run(bot._handle_command(12345, "/reset", message_time=reset_at))

        with connect(db_path) as conn:
            session = conn.execute(
                """
                SELECT ended_at, end_reason, message_count, request_count
                FROM bot_sessions
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
            reset_row = conn.execute(
                """
                SELECT content, request_id, bot_session_id
                FROM bot_chat_messages
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()

        assert bot._current_session_id is None
        assert session is not None
        assert session["ended_at"] == _sqlite_time(reset_at)
        assert session["end_reason"] == "reset"
        assert session["message_count"] == 2
        assert session["request_count"] == 1
        assert reset_row is not None
        assert reset_row["content"] == "[HISTORY_RESET]"
        assert reset_row["request_id"] is None
        assert reset_row["bot_session_id"] == session_id
    finally:
        store.shutdown()


def test_manual_compact_saves_sessionless_summary_even_with_active_session(tmp_path: Path) -> None:
    summary = (
        "The user and assistant reviewed budgets and balances in detail, preserving the"
        " relevant amounts and next steps so future turns could stay short."
    )
    store, db_path = _make_store(tmp_path)
    client = FakeGatewayClient(
        stream_plans=[
            [{"type": "text_delta", "text": "Saved a session note."}],
            [{"type": "text_delta", "text": summary}],
        ]
    )
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), client=client, store=store)
    bot._history = [
        {"role": "user" if index % 2 == 0 else "assistant", "content": f"message-{index}"}
        for index in range(12)
    ]

    try:
        active_session = bot._ensure_session(_ts(2026, 3, 10, 17, 0))
        asyncio.run(bot._handle_command(12345, "/compact", message_time=_ts(2026, 3, 10, 17, 30)))

        with connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT request_id, bot_session_id
                FROM bot_chat_messages
                WHERE request_id = '[COMPACTION]'
                ORDER BY id
                """
            ).fetchall()

        assert bot._current_session_id == active_session
        assert [row["bot_session_id"] for row in rows] == [None, None]
    finally:
        store.shutdown()


def test_auto_compaction_tags_summary_pair_with_current_session(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    summary = (
        "The user and assistant reviewed budgets and balances in detail, preserving the"
        " relevant amounts and next steps so future turns could stay short."
    )
    store, db_path = _make_store(tmp_path)
    client = FakeGatewayClient(
        stream_plans=[
            [{"type": "text_delta", "text": "Saved a session note."}],
            [{"type": "text_delta", "text": summary}],
            [{"type": "text_delta", "text": "final"}, {"type": "stream_complete", "usage": {}}],
        ]
    )
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), client=client, store=store)
    bot._history = [
        {"role": "user" if index % 2 == 0 else "assistant", "content": f"message-{index}"}
        for index in range(12)
    ]
    monkeypatch.setattr(bot_module, "needs_compaction", lambda messages: True)

    try:
        asyncio.run(bot._handle_agent_message(12345, "latest", message_time=_ts(2026, 3, 10, 18, 0)))

        with connect(db_path) as conn:
            compaction_sessions = {
                row["bot_session_id"]
                for row in conn.execute(
                    """
                    SELECT bot_session_id
                    FROM bot_chat_messages
                    WHERE request_id = '[COMPACTION]'
                    """
                ).fetchall()
            }
            request_row = conn.execute(
                """
                SELECT bot_session_id
                FROM bot_requests
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchone()

        assert request_row is not None
        assert compaction_sessions == {request_row["bot_session_id"]}
    finally:
        store.shutdown()


def test_handle_update_forwards_date_to_session_start_and_reset_end(tmp_path: Path) -> None:
    store, db_path = _make_store(tmp_path)
    client = FakeGatewayClient(
        stream_plans=[[{"type": "text_delta", "text": "hello"}, {"type": "stream_complete", "usage": {}}]]
    )
    bot = TelegramBot(config=_make_config(), api=FakeTelegramAPI(), client=client, store=store)
    started_at = _ts(2026, 3, 10, 19, 0)
    reset_at = _ts(2026, 3, 10, 19, 5)

    try:
        asyncio.run(bot._handle_update(_message_update(12345, "hi", date=started_at)))
        session_id = bot._current_session_id
        asyncio.run(bot._handle_update(_message_update(12345, "/reset", date=reset_at)))

        with connect(db_path) as conn:
            row = conn.execute(
                "SELECT started_at, ended_at FROM bot_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()

        assert row is not None
        assert row["started_at"] == _sqlite_time(started_at)
        assert row["ended_at"] == _sqlite_time(reset_at)
    finally:
        store.shutdown()


def test_session_recap_returns_last_closed_and_filters_failed_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "finance.db"
    store = BotStore(db_path)
    store.startup()
    session_id = "sess-recap"
    started_at = _ts(2026, 3, 10, 20, 0)
    tool_calls = [
        {
            "tool_name": "balance_show",
            "server": "finance-cli",
            "duration_ms": 12,
            "is_error": False,
            "result_bytes": 40,
        }
    ]
    failed_tool_calls = [
        {
            "tool_name": "txn_edit",
            "server": "finance-cli",
            "duration_ms": 9,
            "is_error": True,
            "result_bytes": 20,
        }
    ]

    try:
        store.start_session(session_id, message_time=started_at)
        _save_successful_turn(
            store,
            "req-ok",
            session_id=session_id,
            user_text="What happened last time?",
            assistant_text="We reviewed the accounts.",
            estimated_cost=0.5,
            tool_calls=tool_calls,
        )
        _save_failed_turn(
            store,
            "req-failed",
            session_id=session_id,
            user_text="failed only",
            tool_calls=failed_tool_calls,
        )
        store.save_compaction(
            "Summary with enough detail to survive compaction and remain useful later.",
            keep_recent=0,
            bot_session_id=session_id,
        )
        store.end_session(session_id, "idle")
        store.mark_history_reset(bot_session_id=session_id)

        mcp_server = _import_mcp_server(tmp_path, monkeypatch, db_path)
        result = mcp_server.session_recap()

        messages = result["data"]["messages"]
        tools = result["data"]["tools"]
        contents = [message["content"] for message in messages]

        assert result["data"]["session"]["session_id"] == session_id
        assert "failed only" not in contents
        assert "[HISTORY_RESET]" not in "".join(contents)
        assert any(message["compacted"] for message in messages)
        assert contents[0] == "What happened last time?"
        assert contents[1] == "We reviewed the accounts."
        assert tools == [{"tool": "balance_show", "count": 1}]
    finally:
        store.shutdown()


def test_session_list_includes_open_sessions_with_live_stats(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "finance.db"
    store = BotStore(db_path)
    store.startup()

    try:
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        closed_time = int((now - timedelta(days=2, hours=1)).timestamp())
        open_time = int((now - timedelta(days=2)).timestamp())

        store.start_session("sess-closed", message_time=closed_time)
        _save_successful_turn(
            store,
            "req-closed",
            session_id="sess-closed",
            user_text="closed user",
            assistant_text="closed assistant",
            estimated_cost=0.2,
        )
        store.end_session("sess-closed", "idle")

        store.start_session("sess-open", message_time=open_time)
        _save_successful_turn(
            store,
            "req-open",
            session_id="sess-open",
            user_text="open user",
            assistant_text="open assistant",
            estimated_cost=0.7,
        )

        mcp_server = _import_mcp_server(tmp_path, monkeypatch, db_path)
        result = mcp_server.session_list(days=30, limit=10)
        sessions = {
            session["session_id"]: session
            for session in result["data"]["sessions"]
        }

        assert {"sess-closed", "sess-open"}.issubset(sessions)
        assert sessions["sess-closed"]["message_count"] == 2
        assert sessions["sess-closed"]["request_count"] == 1
        assert sessions["sess-open"]["ended_at"] is None
        assert sessions["sess-open"]["message_count"] == 2
        assert sessions["sess-open"]["request_count"] == 1
        assert sessions["sess-open"]["total_cost"] == pytest.approx(0.7)
    finally:
        store.shutdown()
