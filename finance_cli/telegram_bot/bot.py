"""Telegram polling bot for finance_cli."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections import deque
from contextlib import suppress
from typing import Any

from finance_cli.config import load_dotenv
from finance_cli.logging_config import setup_logging

from .agent import FinanceAgent
from .config import BotConfig, load_config
from .store import BotStore
from .telegram_api import TelegramAPI, split_message

log = logging.getLogger(__name__)


class TelegramBot:
    """Single-user Telegram bot backed by the finance MCP agent."""

    def __init__(
        self,
        config: BotConfig,
        *,
        api: TelegramAPI | None = None,
        agent: FinanceAgent | None = None,
    ) -> None:
        self._config = config
        self._api = api or TelegramAPI(config.telegram_token, poll_timeout=config.poll_timeout)
        self._agent = agent or FinanceAgent(config)
        self._offset: int | None = None
        self._running = False
        self._current_task: asyncio.Task[str] | None = None
        self._update_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[list[dict[str, Any]]] | None = None
        self._pending_updates: deque[dict[str, Any]] = deque()
        self._stop_requested = False

    @property
    def agent(self) -> FinanceAgent:
        return self._agent

    @property
    def api(self) -> TelegramAPI:
        return self._api

    async def start(self) -> None:
        await self._agent.startup()
        self._running = True
        try:
            await self._poll_loop()
        finally:
            await self._shutdown_tasks()
            await self._agent.shutdown()

    def stop(self) -> None:
        self._running = False
        for task in (self._poll_task, self._update_task, self._current_task):
            if task is not None and not task.done():
                task.cancel()

    async def _poll_loop(self) -> None:
        while self._running:
            if self._update_task is None and self._pending_updates:
                update = self._pending_updates.popleft()
                self._update_task = asyncio.create_task(self._handle_update(update))

            if self._poll_task is None and self._running:
                self._poll_task = asyncio.create_task(
                    self._api.get_updates(self._offset, self._config.poll_timeout)
                )

            wait_for = [task for task in (self._poll_task, self._update_task) if task is not None]
            if not wait_for:
                await asyncio.sleep(0.1)
                continue

            done, _pending = await asyncio.wait(wait_for, return_when=asyncio.FIRST_COMPLETED)

            if self._poll_task in done:
                try:
                    updates = await self._poll_task
                except asyncio.CancelledError:
                    updates = []
                except Exception as exc:
                    log.exception("Telegram getUpdates failed: %s", exc)
                    await asyncio.sleep(5)
                    updates = []
                self._poll_task = None

                for update in updates:
                    update_id = update.get("update_id")
                    if isinstance(update_id, int):
                        self._offset = update_id + 1

                    if self._is_stop_command(update):
                        await self._handle_stop_update(update)
                    else:
                        self._pending_updates.append(update)

            if self._update_task in done:
                try:
                    await self._update_task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    log.exception("Update handler failed: %s", exc)
                self._update_task = None

    async def _handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message")
        if not isinstance(message, dict):
            return

        chat_id = self._extract_chat_id(message)
        text = message.get("text")
        if chat_id is None or not isinstance(text, str) or not text.strip():
            return

        if not self._is_authorized(chat_id):
            log.warning("Ignoring unauthorized Telegram chat_id=%s", chat_id)
            return

        if text.startswith("/"):
            await self._handle_command(chat_id, text.strip())
        else:
            await self._handle_agent_message(chat_id, text.strip())

    async def _handle_command(self, chat_id: int | str, text: str) -> None:
        command, _, args = text.partition(" ")
        command = command.strip().lower()
        args = args.strip()

        if command == "/start":
            await self._api.send_message(
                chat_id,
                (
                    "Finance bot ready.\n"
                    "/status quick dashboard\n"
                    "/reset clear history\n"
                    "/history show history length\n"
                    "/model <name> switch model\n"
                    "/stop cancel active request"
                ),
            )
            return

        if command == "/stop":
            await self._handle_stop_command(chat_id)
            return

        if command == "/reset":
            self._agent.reset_history()
            await self._api.send_message(chat_id, "History cleared.")
            return

        if command == "/status":
            await self._handle_agent_message(
                chat_id,
                "Give me a quick financial dashboard with net worth, liquidity, debt, budgets, and anything urgent.",
            )
            return

        if command == "/model":
            if not args:
                await self._api.send_message(chat_id, "Usage: /model <anthropic-model-name>")
                return
            self._agent.model_override = args
            await self._api.send_message(chat_id, f"Model set to: {args}")
            return

        if command == "/history":
            await self._api.send_message(chat_id, f"History length: {len(self._agent.history)} messages.")
            return

        await self._api.send_message(chat_id, f"Unknown command: {command}")

    async def _handle_agent_message(self, chat_id: int | str, text: str) -> None:
        status_message = await self._api.send_message(chat_id, "Working on it...")
        status_message_id = self._message_id_from_result(status_message)
        typing_task = asyncio.create_task(self._typing_loop(chat_id))
        response = ""
        stopped_by_command = False

        try:
            self._current_task = asyncio.create_task(self._agent.run(text))
            response = await self._current_task
        except asyncio.CancelledError:
            stopped_by_command = self._stop_requested
            self._stop_requested = False
            response = "Stopped."
        except Exception as exc:
            log.exception("Agent request failed: %s", exc)
            await self._finalize_status(chat_id, status_message_id, "Done.")
            await self._safe_send_message(chat_id, f"Error: {exc}")
            return
        finally:
            self._current_task = None
            typing_task.cancel()
            with suppress(asyncio.CancelledError):
                await typing_task

        final_status = "Stopped." if stopped_by_command else "Done."
        await self._finalize_status(chat_id, status_message_id, final_status)

        if stopped_by_command:
            return

        for chunk in split_message(response):
            await self._safe_send_message(chat_id, chunk)

    async def _handle_stop_update(self, update: dict[str, Any]) -> None:
        message = update.get("message")
        if not isinstance(message, dict):
            return
        chat_id = self._extract_chat_id(message)
        if chat_id is None or not self._is_authorized(chat_id):
            if chat_id is not None:
                log.warning("Ignoring unauthorized Telegram chat_id=%s", chat_id)
            return
        await self._handle_stop_command(chat_id)

    async def _handle_stop_command(self, chat_id: int | str) -> None:
        if self._current_task is not None and not self._current_task.done():
            self._stop_requested = True
            self._current_task.cancel()
            await self._safe_send_message(chat_id, "Stopped.")
        else:
            await self._safe_send_message(chat_id, "Nothing running.")

    async def _typing_loop(self, chat_id: int | str) -> None:
        try:
            while True:
                await self._api.send_chat_action(chat_id, "typing")
                await asyncio.sleep(4)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Failed to send typing indicator: %s", exc)

    async def _finalize_status(self, chat_id: int | str, message_id: int | None, text: str) -> None:
        if message_id is None:
            return
        try:
            await self._api.edit_message_text(chat_id, message_id, text)
        except Exception as exc:
            log.warning("Failed to edit Telegram status message: %s", exc)

    async def _safe_send_message(self, chat_id: int | str, text: str) -> None:
        try:
            await self._api.send_message(chat_id, text)
        except Exception as exc:
            log.warning("Failed to send Telegram message: %s", exc)

    async def _shutdown_tasks(self) -> None:
        for task in (self._poll_task, self._update_task, self._current_task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._poll_task, self._update_task, self._current_task):
            if task is not None:
                with suppress(asyncio.CancelledError):
                    await task
        self._poll_task = None
        self._update_task = None
        self._current_task = None

    def _is_authorized(self, chat_id: int | str) -> bool:
        return str(chat_id) == self._config.telegram_chat_id

    def _is_stop_command(self, update: dict[str, Any]) -> bool:
        message = update.get("message")
        if not isinstance(message, dict):
            return False
        text = message.get("text")
        return isinstance(text, str) and text.strip().startswith("/stop")

    @staticmethod
    def _extract_chat_id(message: dict[str, Any]) -> int | str | None:
        chat = message.get("chat")
        if isinstance(chat, dict) and "id" in chat:
            return chat["id"]
        return None

    @staticmethod
    def _message_id_from_result(result: dict[str, Any]) -> int | None:
        message_id = result.get("message_id")
        return message_id if isinstance(message_id, int) else None


async def run_bot() -> None:
    load_dotenv()
    setup_logging()
    config = load_config()
    store = BotStore()
    store.startup()
    try:
        agent = FinanceAgent(config, store=store)
        agent.history = store.load_recent_messages(limit=config.history_max_turns * 2)
        bot = TelegramBot(config, agent=agent)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, bot.stop)
            except NotImplementedError:
                pass

        log.info("Starting Telegram bot with %s restored messages", len(agent.history))
        await bot.start()
    finally:
        log.info("Shutting down Telegram bot store")
        store.shutdown()
