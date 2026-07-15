"""Legacy single-user polling bot. See finance-web/server/telegram_webhook.py for the
multi-user webhook implementation."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import signal
import stat
import time
import uuid
from collections import deque
from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any, Awaitable

from finance_cli.analytics import log_event
from finance_cli.billing import RequestResolution, resolve_request
from finance_cli.config import auto_migrate_data, get_db_path, load_dotenv
from finance_cli.cost_tracking import dollars_to_usd6, record_and_settle_cost
from finance_cli.error_capture import capture_error
from finance_cli.logging_config import setup_logging
from finance_cli.skills import SKILL_FILES

from .approval import build_approval_keyboard, format_approval_message, parse_callback_data
from .compaction import (
    KEEP_RECENT_MESSAGES,
    apply_compaction,
    build_flush_messages,
    build_summary_messages,
    estimate_tokens,
    needs_compaction,
)
from .config import BotConfig, load_config
from .gateway_client import BackendHTTPError, GatewayClient
from .store import BotStore, RequestMetrics
from .streaming import DraftStream
from .telegram_api import TelegramAPI

log = logging.getLogger(__name__)
_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_PID_DIR = Path.home() / ".finance_cli"
_MIN_SUMMARY_LENGTH = 50
_MAX_TELEGRAM_IMPORT_BYTES = 20 * 1024 * 1024
_TELEGRAM_UPLOAD_RETENTION_SECONDS = 24 * 60 * 60
_TELEGRAM_UPLOAD_MAX_TOTAL_BYTES = 100 * 1024 * 1024
_TELEGRAM_UPLOAD_CLEANUP_INTERVAL_SECONDS = 60 * 60
_TELEGRAM_IMPORT_SUFFIXES = frozenset({".csv", ".pdf"})
_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
APPROVAL_EXPIRED_DETAIL = "Approval expired. Send the request again to retry."
APPROVAL_EXPIRED_TEXT = f"⏰ {APPROVAL_EXPIRED_DETAIL}"
_COMPACTION_ALLOWED_TOOLS = frozenset(
    {
        "agent_session_write",
        "agent_session_search",
        "agent_session_read",
        "agent_memory_read",
    }
)
_lock_fd: int | None = None


def _credit_purchase_url() -> str:
    base = (
        os.getenv("CASHNERD_PUBLIC_BASE_URL", "").strip()
        or os.getenv("FRONTEND_ORIGIN", "").strip()
    ).rstrip("/")
    return f"{base}/settings/billing" if base else "/settings/billing"


def _privacy_url() -> str:
    base = (
        os.getenv("CASHNERD_PUBLIC_BASE_URL", "").strip()
        or os.getenv("FRONTEND_ORIGIN", "").strip()
    ).rstrip("/")
    return f"{base}/privacy" if base else "/privacy"


def _credit_cta_message(prefix: str = "AI usage limit reached.") -> str:
    return f"{prefix} Buy credits in Billing settings: {_credit_purchase_url()}"


def _billing_settings() -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(stripe_price_lite=os.getenv("STRIPE_PRICE_LITE", ""))


def _local_user_billing_snapshot(user_id: str | int) -> dict[str, Any]:
    return {"id": str(user_id), "user_id": str(user_id), "tier": "paid"}


def _friendly_tool_name(tool_name: str) -> str:
    """Map internal tool names to user-friendly status labels."""
    _FRIENDLY = {
        "txn_list": "Looking up transactions",
        "txn_search": "Searching transactions",
        "txn_show": "Loading transaction details",
        "txn_categorize": "Categorizing transaction",
        "txn_review": "Marking as reviewed",
        "budget_status": "Checking budgets",
        "budget_list": "Loading budgets",
        "balance_show": "Checking balances",
        "balance_net_worth": "Calculating net worth",
        "spending_trends": "Analyzing spending",
        "debt_dashboard": "Loading debt overview",
        "debt_simulate": "Running payoff simulation",
        "loan_list": "Loading loans",
        "loan_show": "Loading loan details",
        "loan_schedule": "Calculating repayment schedule",
        "cat_auto_categorize": "Categorizing transactions",
        "subs_list": "Loading subscriptions",
        "subs_detect": "Detecting subscriptions",
        "financial_summary": "Building summary",
        "daily_summary": "Loading daily summary",
        "weekly_summary": "Loading weekly summary",
        "goal_status": "Checking goals",
        "account_list": "Loading accounts",
    }
    friendly = _FRIENDLY.get(tool_name)
    if friendly:
        return friendly
    return tool_name.replace("_", " ").title()


def _pid_file_for_token(token: str) -> Path:
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()[:8]
    return _PID_DIR / f"telegram_bot_{token_hash}.pid"


def _acquire_pid_lock(pid_file: Path) -> None:
    import fcntl

    global _lock_fd

    pid_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(pid_file), os.O_CREAT | os.O_RDWR)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            try:
                os.lseek(fd, 0, os.SEEK_SET)
                old_pid = os.read(fd, 32).decode().strip() or "unknown"
            except Exception:
                old_pid = "unknown"
            raise SystemExit(
                f"Another Telegram bot instance is running (PID {old_pid}). Kill it first: kill {old_pid}"
            )

        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, str(os.getpid()).encode())
    except BaseException:
        os.close(fd)
        raise

    _lock_fd = fd


def _release_pid_lock() -> None:
    global _lock_fd

    if _lock_fd is None:
        return

    os.close(_lock_fd)
    _lock_fd = None


def _summarize_result(result: Any, max_len: int = 150) -> str:
    if result is None:
        return ""
    if isinstance(result, dict):
        if "summary" in result and result["summary"]:
            summary = json.dumps(result["summary"], default=str, sort_keys=True)
            if summary not in ("null", "{}", "\"\""):
                return summary[:max_len] + "..." if len(summary) > max_len else summary
    text = json.dumps(result, default=str, sort_keys=True)
    return text[:max_len] + "..." if len(text) > max_len else text


def _tool_error_message(error: Any) -> str | None:
    if isinstance(error, dict):
        return str(error.get("message", "unknown"))
    if error is None:
        return None
    return str(error)


def _tool_csv_import_properties(tool_name: str) -> dict[str, str] | None:
    if tool_name == "ingest_csv":
        return {"file_type": "csv"}
    if tool_name == "ingest_statement":
        return {"file_type": "pdf"}
    return None


def _document_size_bytes(document: dict[str, Any]) -> int | None:
    value = document.get("file_size")
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and value >= 0:
        return int(value)
    return None


def _document_import_suffix(document: dict[str, Any]) -> str | None:
    raw_name = document.get("file_name")
    if isinstance(raw_name, str):
        suffix = Path(raw_name).suffix.lower()
        if suffix in _TELEGRAM_IMPORT_SUFFIXES:
            return suffix

    mime_type = str(document.get("mime_type") or "").strip().lower()
    if mime_type in {"text/csv", "application/csv", "application/vnd.ms-excel"}:
        return ".csv"
    if mime_type == "application/pdf":
        return ".pdf"
    return None


def _safe_upload_filename(document: dict[str, Any], suffix: str) -> str:
    raw_name = document.get("file_name")
    name = Path(raw_name).name if isinstance(raw_name, str) and raw_name.strip() else ""
    if not name:
        name = f"telegram-upload{suffix}"
    name = _SAFE_FILENAME_RE.sub("_", name).strip("._") or f"telegram-upload{suffix}"
    if not name.lower().endswith(suffix):
        name = f"{Path(name).stem or 'telegram-upload'}{suffix}"
    if len(name) > 96:
        stem = Path(name).stem[: 96 - len(suffix) - 1].rstrip("._") or "telegram-upload"
        name = f"{stem}{suffix}"
    return name


def _write_private_bytes(path: Path, payload: bytes) -> None:
    fd: int | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            fd = None
            handle.write(payload)
    finally:
        if fd is not None:
            os.close(fd)


def _cleanup_telegram_uploads(
    upload_dir: Path,
    *,
    now: float | None = None,
    max_age_seconds: int = _TELEGRAM_UPLOAD_RETENTION_SECONDS,
    max_total_bytes: int = _TELEGRAM_UPLOAD_MAX_TOTAL_BYTES,
    protected_paths: Iterable[Path] = (),
) -> dict[str, int]:
    if not upload_dir.is_dir():
        return {"deleted": 0, "bytes_deleted": 0, "remaining_bytes": 0}

    current_time = time.time() if now is None else now
    cutoff = current_time - max_age_seconds
    protected = {Path(path).resolve(strict=False) for path in protected_paths}
    deleted = 0
    bytes_deleted = 0

    def iter_files() -> list[tuple[Path, os.stat_result]]:
        files: list[tuple[Path, os.stat_result]] = []
        for candidate in upload_dir.rglob("*"):
            try:
                file_stat = candidate.lstat()
            except OSError:
                continue
            if stat.S_ISREG(file_stat.st_mode) or stat.S_ISLNK(file_stat.st_mode):
                files.append((candidate, file_stat))
        return files

    def is_protected(path: Path) -> bool:
        return path.resolve(strict=False) in protected

    def remove(path: Path, file_stat: os.stat_result) -> None:
        nonlocal deleted, bytes_deleted
        if is_protected(path):
            return
        try:
            path.unlink()
        except OSError:
            return
        deleted += 1
        bytes_deleted += file_stat.st_size

    for path, file_stat in iter_files():
        if file_stat.st_mtime < cutoff:
            remove(path, file_stat)

    files = iter_files()
    total_bytes = sum(file_stat.st_size for _path, file_stat in files)
    if max_total_bytes >= 0 and total_bytes > max_total_bytes:
        for path, file_stat in sorted(files, key=lambda item: (item[1].st_mtime, str(item[0]))):
            if total_bytes <= max_total_bytes:
                break
            if is_protected(path):
                continue
            try:
                path.unlink()
            except OSError:
                continue
            deleted += 1
            bytes_deleted += file_stat.st_size
            total_bytes -= file_stat.st_size

    return {
        "deleted": deleted,
        "bytes_deleted": bytes_deleted,
        "remaining_bytes": max(total_bytes, 0),
    }


def _is_onboarding_complete_result(result: Any) -> bool:
    if not isinstance(result, dict):
        return False

    data = result.get("data")
    if isinstance(data, dict):
        if data.get("name") == "onboarding":
            state = data.get("state")
            if isinstance(state, dict) and state.get("complete") is True:
                return True
        if data.get("complete") is True:
            return True

    state = result.get("state")
    if isinstance(state, dict) and state.get("complete") is True:
        return True

    return result.get("complete") is True


_TOOL_SUMMARY_MAX = 500


def _format_args(tool_input: dict[str, Any], max_val_len: int = 30) -> str:
    """Format tool args compactly for inclusion in chat history."""
    if not tool_input:
        return ""

    parts: list[str] = []
    for key, value in tool_input.items():
        if value is None or value == "":
            continue
        if value is True:
            parts.append(key)
        elif value is False:
            parts.append(f"{key}=false")
        else:
            text = json.dumps(value, default=str)
            if len(text) > max_val_len:
                text = text[:max_val_len] + "..."
            parts.append(f"{key}={text}")
    return ", ".join(parts)


def _build_tool_summary(tool_calls: list[dict[str, Any]]) -> str:
    """Build a compact [Tools: ...] line from tool call records."""
    if not tool_calls:
        return ""

    parts: list[str] = []
    for tool_call in tool_calls:
        name = str(tool_call.get("tool_name", "?"))
        args = _format_args(tool_call.get("tool_input", {}))
        if tool_call.get("is_error"):
            error = str(tool_call.get("error_message") or "unknown error")
            parts.append(f"{name}({args}) → ERROR: {error}")
        else:
            summary = str(tool_call.get("result_summary", ""))
            parts.append(f"{name}({args}) → {summary}" if summary else f"{name}({args})")

    text = "[Tools: " + " | ".join(parts) + "]"
    if len(text) > _TOOL_SUMMARY_MAX:
        text = text[: _TOOL_SUMMARY_MAX - 4] + "...]"
    return text


class TelegramBot:
    """Single-user Telegram bot backed by the finance gateway."""

    def __init__(
        self,
        config: BotConfig,
        *,
        api: TelegramAPI | None = None,
        client: GatewayClient | None = None,
        store: BotStore | None = None,
    ) -> None:
        self._config = config
        self._api = api or TelegramAPI(config.telegram_token, poll_timeout=config.poll_timeout)
        self._client = client or GatewayClient(config.gateway_url, config.gateway_user_key)
        self._store = store
        self._history: list[dict[str, str]] = []
        self._model_override: str | None = None
        self._active_skill: str | None = None
        self._first_categorization_emitted: bool = False
        self._pending_approvals: dict[str, dict[str, Any]] = {}
        self._approval_generation = 0
        self._approval_generation_label = APPROVAL_EXPIRED_TEXT
        self._offset: int | None = None
        self._running = False
        self._current_task: asyncio.Task[None] | None = None
        self._update_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[list[dict[str, Any]]] | None = None
        self._pending_updates: deque[dict[str, Any]] = deque()
        self._stop_requested = False
        self._current_session_id: str | None = None
        self._last_message_time: float = 0.0
        self._last_upload_cleanup_time: float = 0.0

    @property
    def api(self) -> TelegramAPI:
        return self._api

    def _emit_onboarding_event(
        self,
        event: str,
        *,
        outcome: str,
        properties: dict[str, Any] | None = None,
        metrics: RequestMetrics | None = None,
    ) -> None:
        if self._active_skill != "onboarding":
            return
        db_path = self._store.db_path if self._store is not None else None
        if db_path is None:
            return

        log_event(
            db_path,
            event,
            outcome=outcome,
            properties=properties,
            source="telegram",
            request_id=metrics.request_id if metrics is not None else None,
            session_id=metrics.session_id if metrics is not None else None,
            conversation_id=metrics.bot_session_id if metrics is not None else self._current_session_id,
        )

    def _telegram_upload_dir(self) -> Path:
        db_path = self._store.db_path if self._store is not None else get_db_path()
        return Path(db_path).expanduser().resolve().parent / "uploads" / "telegram"

    def _cleanup_telegram_uploads_if_due(
        self,
        *,
        force: bool = False,
        protected_paths: Iterable[Path] = (),
    ) -> None:
        now = time.time()
        if (
            not force
            and now - self._last_upload_cleanup_time
            < _TELEGRAM_UPLOAD_CLEANUP_INTERVAL_SECONDS
        ):
            return

        result = _cleanup_telegram_uploads(
            self._telegram_upload_dir(),
            now=now,
            protected_paths=protected_paths,
        )
        self._last_upload_cleanup_time = now
        if result["deleted"]:
            log.info(
                "Cleaned up %d Telegram upload(s), %d bytes removed",
                result["deleted"],
                result["bytes_deleted"],
            )

    async def _stage_telegram_document(self, document: dict[str, Any]) -> tuple[Path, str]:
        file_id = document.get("file_id")
        if not isinstance(file_id, str) or not file_id.strip():
            raise ValueError("Telegram did not include a file id for that document.")

        suffix = _document_import_suffix(document)
        if suffix is None:
            raise ValueError("I can import CSV or PDF statements from Telegram. Send a .csv or .pdf file.")

        declared_size = _document_size_bytes(document)
        if declared_size is not None and declared_size > _MAX_TELEGRAM_IMPORT_BYTES:
            raise ValueError("That file is too large for Telegram import. Use the web upload for files over 20 MB.")

        file_info = await self._api.get_file(file_id)
        file_path = file_info.get("file_path")
        if not isinstance(file_path, str) or not file_path.strip():
            raise RuntimeError("Telegram did not return a downloadable file path.")

        remote_size = _document_size_bytes(file_info)
        if remote_size is not None and remote_size > _MAX_TELEGRAM_IMPORT_BYTES:
            raise ValueError("That file is too large for Telegram import. Use the web upload for files over 20 MB.")

        payload = await self._api.download_file(
            file_path,
            max_bytes=_MAX_TELEGRAM_IMPORT_BYTES,
        )
        if len(payload) > _MAX_TELEGRAM_IMPORT_BYTES:
            raise ValueError("That file is too large for Telegram import. Use the web upload for files over 20 MB.")
        if not payload:
            raise ValueError("Telegram returned an empty file.")

        self._cleanup_telegram_uploads_if_due(force=True)
        filename = f"{uuid.uuid4().hex}-{_safe_upload_filename(document, suffix)}"
        staged_path = self._telegram_upload_dir() / filename
        _write_private_bytes(staged_path, payload)
        self._cleanup_telegram_uploads_if_due(force=True, protected_paths=[staged_path])
        return staged_path, suffix

    async def _handle_document_message(
        self,
        chat_id: int | str,
        document: dict[str, Any],
        *,
        message_time: float,
    ) -> None:
        try:
            await self._api.send_chat_action(chat_id, "upload_document")
            staged_path, suffix = await self._stage_telegram_document(document)
        except ValueError as exc:
            await self._api.send_message(chat_id, str(exc))
            return
        except Exception as exc:
            capture_error(
                exc,
                source="telegram",
                endpoint="document_upload",
                db_path=self._store.db_path if self._store is not None else None,
            )
            log.exception("Telegram document download failed: %s", exc)
            await self._api.send_message(
                chat_id,
                "I could not download that Telegram file. Use the web app upload and continue there.",
            )
            return

        if suffix == ".csv":
            import_instruction = (
                f"Use ingest_csv(file={json.dumps(str(staged_path))}, "
                'institution="auto", commit=True).'
            )
            file_kind = "CSV"
        else:
            import_instruction = (
                f"Use ingest_statement(file={json.dumps(str(staged_path))}, commit=True)."
            )
            file_kind = "PDF"

        await self._api.send_message(
            chat_id,
            f"Received the {file_kind} upload. I saved it for import and will process it now.",
        )
        prompt = (
            f"The user uploaded a {file_kind} statement through Telegram.\n"
            f"upload_path: {staged_path}\n"
            f"{import_instruction}\n"
            "After the import, summarize inserted/skipped rows and the next cleanup step."
        )
        caption = document.get("caption")
        if isinstance(caption, str) and caption.strip():
            prompt += f"\nUser caption: {caption.strip()}"
        await self._run_as_current_task(
            self._handle_agent_message(chat_id, prompt, message_time=message_time)
        )

    async def start(self) -> None:
        await self._client.ensure_session(user_id=self._config.telegram_chat_id)
        self._cleanup_telegram_uploads_if_due(force=True)
        self._running = True
        try:
            await self._poll_loop()
        finally:
            await self._shutdown_tasks()
            await self._client.close()

    def stop(self) -> None:
        self._running = False
        self._stop_requested = True
        self._cancel_pending_approval_timers()
        self._pending_approvals.clear()
        for task in (self._poll_task, self._update_task, self._current_task):
            if task is not None and not task.done():
                task.cancel()

    async def _poll_loop(self) -> None:
        while self._running:
            self._cleanup_telegram_uploads_if_due()
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

                    callback_query = update.get("callback_query")
                    if isinstance(callback_query, dict):
                        asyncio.create_task(self._handle_callback_query(callback_query))
                        continue

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
        raw_message_time = message.get("date", 0)
        message_time = (
            float(raw_message_time)
            if isinstance(raw_message_time, (int, float))
            else 0.0
        )
        if chat_id is None:
            return

        if not self._is_authorized(chat_id):
            log.warning("Ignoring unauthorized Telegram chat_id=%s", chat_id)
            return

        document = message.get("document")
        if isinstance(document, dict):
            document_payload = dict(document)
            caption = message.get("caption")
            if isinstance(caption, str):
                document_payload["caption"] = caption
            await self._handle_document_message(
                chat_id,
                document_payload,
                message_time=message_time,
            )
            return

        text = message.get("text")
        if not isinstance(text, str) or not text.strip():
            return

        if text.startswith("/"):
            await self._handle_command(chat_id, text.strip(), message_time=message_time)
        else:
            await self._run_as_current_task(
                self._handle_agent_message(chat_id, text.strip(), message_time=message_time)
            )

    async def _handle_command(
        self,
        chat_id: int | str,
        text: str,
        message_time: float = 0.0,
    ) -> None:
        command, _, args = text.partition(" ")
        command = command.strip().lower()
        args = args.strip()

        if command == "/start":
            await self._api.send_message(
                chat_id,
                (
                    "Finance bot ready.\n"
                    "/onboarding start guided setup\n"
                    "/status quick dashboard\n"
                    "/reset clear conversation\n"
                    "/history show conversation length\n"
                    "/compact summarize older messages\n"
                    "/model <name> switch model\n"
                    "/stop cancel active request\n"
                    f"Privacy: {_privacy_url()}"
                ),
            )
            return

        if command == "/stop":
            await self._handle_stop_command(chat_id)
            return

        if command == "/onboarding":
            if args and args.lower() == "off":
                if self._active_skill == "onboarding":
                    self._active_skill = None
                    await self._api.send_message(chat_id, "Onboarding mode off.")
                else:
                    await self._api.send_message(chat_id, "Onboarding mode is not active.")
                return

            self._active_skill = "onboarding"
            self._first_categorization_emitted = False
            self._emit_onboarding_event(
                "onboarding.wizard",
                outcome="started",
                properties={"step": "command", "context": "telegram"},
            )
            await self._api.send_message(
                chat_id,
                (
                    "Onboarding activated — I'll guide you through setting up your finances (~20-45 min).\n\n"
                    "Send any message to begin, or /onboarding off to exit."
                ),
            )
            return

        if command == "/reset":
            closing_session_id = self._current_session_id
            self._close_session("reset", message_time=message_time)
            self._history.clear()
            self._model_override = None
            self._active_skill = None
            await self._clear_pending_approvals(reason="cancelled")
            self._client.invalidate_session()
            if self._store is not None:
                self._store.mark_history_reset(bot_session_id=closing_session_id)
            await self._api.send_message(chat_id, "History cleared.")
            return

        if command == "/status":
            await self._run_as_current_task(
                self._handle_agent_message(
                    chat_id,
                    "Give me a quick financial dashboard with net worth, liquidity, debt, budgets, and anything urgent.",
                    message_time=message_time,
                )
            )
            return

        if command == "/model":
            if not args:
                await self._api.send_message(chat_id, "Usage: /model <model-name> (e.g. claude-sonnet-4-20250514)")
                return
            self._model_override = args
            await self._api.send_message(chat_id, f"Model set to: {args}")
            return

        if command == "/history":
            await self._api.send_message(chat_id, f"Conversation: {len(self._history)} messages.")
            return

        if command == "/compact":
            await self._run_as_current_task(self._handle_compact(chat_id))
            return

        if command == "/dev":
            if args and args.lower() == "off":
                if self._active_skill:
                    old = self._active_skill
                    self._active_skill = None
                    await self._api.send_message(chat_id, f"Dev mode off (was: {old}).")
                else:
                    await self._api.send_message(chat_id, "Dev mode is not active.")
                return

            if not args:
                available = ", ".join(sorted(SKILL_FILES.keys()))
                status = f"Active: {self._active_skill}" if self._active_skill else "Not active"
                await self._api.send_message(
                    chat_id,
                    f"Dev mode: {status}\n\nUsage: /dev <skill> | /dev off\nAvailable: {available}",
                )
                return

            skill_aliases = {"normalizer": "normalizer_builder"}
            skill_name = skill_aliases.get(args.lower(), args.lower())
            if skill_name not in SKILL_FILES:
                available = ", ".join(sorted(SKILL_FILES.keys()))
                await self._api.send_message(
                    chat_id,
                    f"Unknown skill: {skill_name}\nAvailable: {available}",
                )
                return

            self._active_skill = skill_name
            await self._api.send_message(
                chat_id,
                (
                    f"Dev mode: {skill_name}\n"
                    "Skill playbook loaded into system prompt.\n"
                    "/dev off to exit."
                ),
            )
            return

        await self._api.send_message(chat_id, f"Unknown command: {command}")

    def _ensure_session(self, message_time: float = 0.0) -> str:
        """Ensure an active bot session exists, closing idle sessions first."""
        ts = message_time or time.time()
        if (
            self._current_session_id is not None
            and self._last_message_time > 0
            and ts - self._last_message_time > self._config.session_idle_timeout
        ):
            self._close_session("idle")

        if self._current_session_id is None:
            self._current_session_id = uuid.uuid4().hex
            if self._store is not None:
                self._store.start_session(self._current_session_id, message_time=ts)
            log.info("Started session %s", self._current_session_id)

        self._last_message_time = ts
        if self._store is not None:
            self._store.update_session_activity(self._current_session_id, message_time=ts)
        return self._current_session_id

    def _close_session(self, reason: str, message_time: float = 0.0) -> None:
        """Finalize the active bot session."""
        if self._current_session_id is None:
            return
        if self._store is not None:
            self._store.end_session(self._current_session_id, reason, ended_at_time=message_time)
        log.info("Closed session %s (reason=%s)", self._current_session_id, reason)
        self._current_session_id = None

    async def _handle_agent_message(
        self,
        chat_id: int | str,
        text: str,
        message_time: float = 0.0,
    ) -> None:
        request_id = uuid.uuid4().hex
        bot_session_id = self._ensure_session(message_time)
        metrics = RequestMetrics(
            request_id=request_id,
            session_id="",
            model=self._model_override or self._config.model,
            bot_session_id=bot_session_id,
            start_time=time.time(),
        )
        user_entry = {"role": "user", "content": text}
        assistant_parts: list[str] = []
        tool_inputs: dict[str, dict[str, Any]] = {}
        typing_task = asyncio.create_task(self._typing_loop(chat_id))
        draft: DraftStream | None = None
        blocked_response: str | None = None
        resolution: RequestResolution | None = None

        log.info("request=%s starting", request_id)
        log.debug("request=%s user: %s", request_id, text[:200].replace("\n", " "))

        try:
            if needs_compaction(self._history):
                await self._run_compaction(bot_session_id=bot_session_id)

            self._history.append(user_entry)
            if self._store is not None:
                self._store.save_user_message(text, request_id, bot_session_id=bot_session_id)

            draft = DraftStream(self._api, chat_id)
            db_path = self._store.db_path if self._store is not None else None
            if db_path is not None:
                try:
                    resolution = resolve_request(
                        _local_user_billing_snapshot(self._config.telegram_chat_id),
                        db_path,
                        _billing_settings(),
                        explicit_model=self._model_override or self._config.model,
                    )
                except Exception as exc:
                    capture_error(
                        exc,
                        source="telegram",
                        endpoint="chat",
                        db_path=db_path,
                        context={
                            "request_id": request_id,
                            "model": metrics.model,
                        },
                    )
                    log.warning("Telegram cost resolution failed: %s", exc)
                    metrics.error = "cost_resolution_failed"
                    blocked_response = "AI is temporarily unavailable. Please try again shortly."
                    assistant_parts[:] = [blocked_response]
                    await draft.append(blocked_response)

                if resolution is not None and resolution.action == "block":
                    blocked_response = _credit_cta_message()
                    log.warning(
                        "Telegram request blocked by plan cap request=%s credits_available=%s",
                        request_id,
                        resolution.credits_available,
                    )
                    assistant_parts[:] = [blocked_response]
                    await draft.append(blocked_response)

            if blocked_response is None:
                await self._consume_stream(
                    messages=list(self._history),
                    draft=draft,
                    assistant_parts=assistant_parts,
                    tool_inputs=tool_inputs,
                    metrics=metrics,
                    effective_model=resolution.effective_model if resolution is not None else None,
                )

            assistant_text = "".join(assistant_parts).strip()
            if not assistant_text and metrics.error is None:
                assistant_text = "No response generated."
                assistant_parts[:] = [assistant_text]
                normalized_text = assistant_text
                if metrics.tool_calls:
                    normalized_text = f"\n{assistant_text}"
                await draft.append(normalized_text)
        except asyncio.CancelledError:
            metrics.error = "cancelled"
            raise
        except Exception as exc:
            if metrics.error is None:
                metrics.error = str(exc)
            capture_error(
                exc,
                source="telegram",
                endpoint="chat",
                context={
                    "request_id": request_id,
                    "model": metrics.model,
                },
                db_path=self._store.db_path if self._store is not None else None,
            )
            log.exception("Gateway request failed: %s", exc)
            if draft is not None:
                await draft.send_tool_status(f"Error: {exc}")
        finally:
            reason = "cancelled" if self._stop_requested else "expired"
            await self._clear_pending_approvals(reason=reason)
            if draft is not None:
                await draft.finish()
            typing_task.cancel()
            with suppress(asyncio.CancelledError):
                await typing_task

        assistant_text = "".join(assistant_parts).strip()
        if metrics.error is None:
            history_text = assistant_text
            tool_summary = _build_tool_summary(metrics.tool_calls)
            if tool_summary:
                history_text = f"{assistant_text}\n\n{tool_summary}"
            self._history.append({"role": "assistant", "content": history_text})
            self._trim_history()
            if self._store is not None:
                self._store.save_assistant_message(
                    history_text,
                    request_id,
                    bot_session_id=bot_session_id,
                )
        else:
            self._remove_message(user_entry)

        if self._store is not None:
            self._store.save_request(metrics)
            if metrics.estimated_cost > 0:
                record_and_settle_cost(
                    self._store.db_path,
                    "claude",
                    "chat",
                    dollars_to_usd6(metrics.estimated_cost),
                    idempotency_key=f"bot_{metrics.request_id}",
                    is_byok=resolution.mode == "byok" if resolution is not None else False,
                    input_tokens=metrics.input_tokens,
                    output_tokens=metrics.output_tokens,
                    cache_creation_tokens=metrics.cache_creation_tokens,
                    cache_read_tokens=metrics.cache_read_tokens,
                    model=metrics.model,
                    request_id=metrics.request_id,
                )
        self._log_request(metrics, assistant_text)

    async def _consume_stream(
        self,
        *,
        messages: list[dict[str, str]],
        draft: DraftStream,
        assistant_parts: list[str],
        tool_inputs: dict[str, dict[str, Any]],
        metrics: RequestMetrics,
        effective_model: str | None = None,
    ) -> None:
        attempt = 0
        while attempt < 2:
            try:
                session = await self._client.ensure_session(
                    user_id=self._config.telegram_chat_id
                )
                metrics.session_id = session.session_id
                metrics.model = effective_model or self._model_override or self._config.model
                context: dict[str, Any] | None = None
                if self._active_skill:
                    context = {"skill": self._active_skill}
                async for event in self._client.stream_chat(
                    messages,
                    context=context,
                    model=effective_model or self._model_override,
                    user_id=self._config.telegram_chat_id,
                ):
                    await self._handle_event(
                        draft=draft,
                        event=event,
                        assistant_parts=assistant_parts,
                        tool_inputs=tool_inputs,
                        metrics=metrics,
                    )
                return
            except BackendHTTPError as exc:
                if exc.status_code == 401 and attempt == 0:
                    self._client.invalidate_session()
                    attempt += 1
                    continue
                if exc.status_code == 409:
                    await draft.send_tool_status("Another request in progress.")
                    metrics.error = "409 conflict"
                    return
                raise

    async def _handle_event(
        self,
        *,
        draft: DraftStream,
        event: dict[str, Any],
        assistant_parts: list[str],
        tool_inputs: dict[str, dict[str, Any]],
        metrics: RequestMetrics,
    ) -> None:
        event_type = str(event.get("type", ""))

        if event_type == "text_delta":
            delta = str(event.get("text", "") or "")
            if delta:
                assistant_parts.append(delta)
                await draft.append(delta)
            return

        if event_type == "tool_call_start":
            tool_name = str(event.get("tool_name", "tool"))
            tool_call_id = str(event.get("tool_call_id", "") or "")
            raw_input = event.get("tool_input")
            tool_inputs[tool_call_id] = raw_input if isinstance(raw_input, dict) else {}
            if tool_name == "plaid_link":
                self._emit_onboarding_event(
                    "onboarding.plaid_link",
                    outcome="started",
                    metrics=metrics,
                )
            elif tool_name in {"ingest_csv", "ingest_statement"}:
                self._emit_onboarding_event(
                    "onboarding.csv_import",
                    outcome="started",
                    properties=_tool_csv_import_properties(tool_name),
                    metrics=metrics,
                )
            elif tool_name == "cat_auto_categorize" and not self._first_categorization_emitted:
                self._emit_onboarding_event(
                    "onboarding.first_categorization",
                    outcome="started",
                    metrics=metrics,
                )
            await draft.send_tool_status(f"\u23f3 {_friendly_tool_name(tool_name)}...")
            return

        if event_type == "tool_call_complete":
            tool_name = str(event.get("tool_name", "tool"))
            error = event.get("error")
            tool_call_id = str(event.get("tool_call_id", "") or "")
            result = event.get("result")
            if tool_name == "code_execute":
                result = await self._send_code_execute_images(result)
                event["result"] = result
            error_message = _tool_error_message(error)
            outcome = "failed" if error is not None else "succeeded"

            if tool_name == "plaid_link" and error is not None:
                self._emit_onboarding_event(
                    "onboarding.plaid_link",
                    outcome="failed",
                    metrics=metrics,
                )
            elif tool_name == "plaid_exchange":
                self._emit_onboarding_event(
                    "onboarding.plaid_link",
                    outcome=outcome,
                    metrics=metrics,
                )
            elif tool_name in {"ingest_csv", "ingest_statement"}:
                self._emit_onboarding_event(
                    "onboarding.csv_import",
                    outcome=outcome,
                    properties=_tool_csv_import_properties(tool_name),
                    metrics=metrics,
                )
            elif tool_name == "cat_auto_categorize" and not self._first_categorization_emitted:
                self._first_categorization_emitted = True
                self._emit_onboarding_event(
                    "onboarding.first_categorization",
                    outcome=outcome,
                    metrics=metrics,
                )
            elif tool_name == "skill_state_set" and error is None:
                if _is_onboarding_complete_result(result):
                    self._emit_onboarding_event(
                        "onboarding.complete",
                        outcome="succeeded",
                        metrics=metrics,
                    )

            metrics.tool_calls.append(
                {
                    "tool_name": tool_name,
                    "server": event.get("server"),
                    "duration_ms": int(event.get("duration_ms", 0) or 0),
                    "is_error": error is not None,
                    "result_bytes": len(json.dumps(result, default=str)) if result is not None else 0,
                    "tool_input": tool_inputs.get(tool_call_id, {}),
                    "result_summary": _summarize_result(result),
                    "error_message": error_message,
                }
            )
            metrics.tool_call_count = len(metrics.tool_calls)
            if error is not None:
                await draft.send_tool_status(f"\u274c {_friendly_tool_name(tool_name)}")
            else:
                await draft.send_tool_status(f"\u2705 {_friendly_tool_name(tool_name)}")
            return

        if event_type == "tool_approval_request":
            await self._send_approval_prompt(event)
            return

        if event_type == "stream_complete":
            usage = event.get("usage")
            if isinstance(usage, dict):
                metrics.input_tokens = int(usage.get("input_tokens", 0) or 0)
                metrics.output_tokens = int(usage.get("output_tokens", 0) or 0)
                metrics.cache_creation_tokens = int(
                    usage.get("cache_creation_input_tokens", 0) or 0
                )
                metrics.cache_read_tokens = int(usage.get("cache_read_input_tokens", 0) or 0)
                metrics.estimated_cost = float(usage.get("estimated_cost", 0.0) or 0.0)
            return

        if event_type in {"error", "stream_error"}:
            error_text = str(event.get("error", "unknown"))
            metrics.error = error_text
            await draft.send_tool_status(f"Error: {error_text}")
            return

        if event_type in {"thinking_delta", "heartbeat"}:
            return

    async def _send_code_execute_images(self, result: Any) -> Any:
        if not isinstance(result, dict):
            return result

        images = result.get("images")
        if not isinstance(images, list) or not images:
            return result

        sanitized_result = dict(result)
        sanitized_images: list[Any] = []
        for index, image in enumerate(images, start=1):
            if not isinstance(image, dict):
                sanitized_images.append(image)
                continue

            next_image = dict(image)
            data_base64 = next_image.get("data_base64")
            if isinstance(data_base64, str) and data_base64:
                filename = str(next_image.get("filename") or f"chart-{index}.png")
                media_type = str(next_image.get("media_type") or "image/png")
                try:
                    await self._api.send_photo(
                        self._config.telegram_chat_id,
                        base64.b64decode(data_base64),
                        filename=filename,
                        media_type=media_type,
                        caption=filename,
                    )
                except Exception as exc:
                    log.warning("Failed to send code execution image %s: %s", filename, exc)
                next_image["data_base64"] = "[sent to user]"
            sanitized_images.append(next_image)

        sanitized_result["images"] = sanitized_images
        return sanitized_result

    async def _send_approval_prompt(self, event: dict[str, Any]) -> None:
        nonce = event.get("nonce")
        if not isinstance(nonce, str) or not nonce:
            return
        tool_name = str(event.get("tool_name", "unknown"))
        keyboard = build_approval_keyboard(nonce, tool_name)
        try:
            result = await self._api.send_message_with_keyboard(
                self._config.telegram_chat_id,
                format_approval_message(event),
                keyboard,
            )
        except Exception as exc:
            log.warning("Failed to send approval prompt: %s", exc)
            return

        entry = {
            "tool_call_id": str(event.get("tool_call_id", "") or ""),
            "tool_name": tool_name,
            "message_id": self._message_id_from_result(result),
            "chat_id": self._config.telegram_chat_id,
            "generation": self._approval_generation,
        }
        self._pending_approvals[nonce] = entry
        entry["timeout_task"] = asyncio.create_task(
            self._approval_timeout(nonce, self._config.approval_timeout)
        )

    async def _approval_timeout(self, nonce: str, timeout: int) -> None:
        await asyncio.sleep(timeout)
        pending = self._pending_approvals.pop(nonce, None)
        if pending is None:
            return

        tool_call_id = str(pending.get("tool_call_id", "") or "")
        chat_id = pending.get("chat_id")
        message_id = pending.get("message_id")
        try:
            await self._client.submit_approval(tool_call_id, nonce, False)
        except Exception as exc:
            log.warning("Failed to submit timeout denial for nonce=%s: %s", nonce, exc)

        if isinstance(message_id, int) and chat_id is not None:
            await self._safe_edit_keyboard(chat_id, message_id, APPROVAL_EXPIRED_TEXT)

    async def _clear_pending_approvals(self, *, reason: str = "expired") -> None:
        label = APPROVAL_EXPIRED_TEXT if reason == "expired" else "🚫 Cancelled"
        self._approval_generation += 1
        self._approval_generation_label = label
        if not self._pending_approvals:
            return

        entries = list(self._pending_approvals.values())
        self._pending_approvals.clear()
        for entry in entries:
            timeout_task = entry.get("timeout_task")
            if isinstance(timeout_task, asyncio.Task) and not timeout_task.done():
                timeout_task.cancel()

        for entry in entries:
            chat_id = entry.get("chat_id")
            message_id = entry.get("message_id")
            if isinstance(message_id, int) and chat_id is not None:
                await self._safe_edit_keyboard(chat_id, message_id, label)

    def _cancel_pending_approval_timers(self) -> None:
        self._approval_generation += 1
        self._approval_generation_label = "🚫 Cancelled"
        for entry in self._pending_approvals.values():
            timeout_task = entry.get("timeout_task")
            if isinstance(timeout_task, asyncio.Task) and not timeout_task.done():
                timeout_task.cancel()

    async def _handle_compact(self, chat_id: int | str) -> None:
        msg_count = len(self._history)
        if msg_count < 9:
            await self._api.send_message(chat_id, f"Not enough history to compact ({msg_count} messages).")
            return
        await self._api.send_message(
            chat_id,
            f"Summarizing {msg_count} older messages...",
        )
        try:
            await self._run_compaction(bot_session_id=None)
            new_count = len(self._history)
            await self._api.send_message(
                chat_id,
                f"Done -- conversation trimmed from {msg_count} to {new_count} messages.",
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("Manual compaction failed: %s", exc)
            await self._api.send_message(chat_id, f"Compaction failed: {exc}")

    async def _run_compaction(self, bot_session_id: str | None = None) -> None:
        log.info(
            "Compaction triggered (%d messages, ~%d tokens)",
            len(self._history),
            estimate_tokens(self._history),
        )

        flush_msgs = build_flush_messages(self._history)
        try:
            await asyncio.wait_for(self._stream_silent(flush_msgs), timeout=60)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Compaction flush failed, proceeding to summary: %s", exc)

        self._client.invalidate_session()

        summary_msgs = build_summary_messages(self._history)
        try:
            summary = await asyncio.wait_for(self._stream_silent(summary_msgs), timeout=120)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Compaction summary failed: %s", exc)
            return

        if not summary or len(summary) < _MIN_SUMMARY_LENGTH:
            log.warning("Compaction summary too short (%d chars), skipping", len(summary))
            return

        old_len = len(self._history)
        self._history[:] = apply_compaction(self._history, summary)
        log.info("Compaction complete: %d -> %d messages", old_len, len(self._history))
        if self._store is not None:
            self._store.save_compaction(
                summary,
                KEEP_RECENT_MESSAGES,
                bot_session_id=bot_session_id,
            )

    async def _stream_silent(self, messages: list[dict[str, str]]) -> str:
        parts: list[str] = []
        async for event in self._client.stream_chat(
            messages,
            context={"compaction": True},
            user_id=self._config.telegram_chat_id,
        ):
            event_type = str(event.get("type", ""))
            if event_type == "text_delta":
                parts.append(str(event.get("text", "")))
            elif event_type == "tool_approval_request":
                tool_name = str(event.get("tool_name", ""))
                approved = tool_name in _COMPACTION_ALLOWED_TOOLS
                if not approved:
                    log.warning("Compaction requested unexpected tool %s, denying", tool_name)
                await self._client.submit_approval(
                    str(event.get("tool_call_id", "")),
                    str(event.get("nonce", "")),
                    approved,
                )
            elif event_type in {"error", "stream_error"}:
                raise RuntimeError(f"Compaction error: {event.get('error', 'unknown')}")
        return "".join(parts).strip()

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
            await self._clear_pending_approvals(reason="cancelled")
            self._current_task.cancel()
            await self._safe_send_message(chat_id, "Stopped.")
        else:
            await self._clear_pending_approvals(reason="cancelled")
            await self._safe_send_message(chat_id, "Nothing running.")

    async def _handle_callback_query(self, callback_query: dict[str, Any]) -> None:
        cq_id = callback_query.get("id")
        data = callback_query.get("data")
        message = callback_query.get("message")
        nonce = "unknown"
        pending: dict[str, Any] | None = None

        try:
            chat_id = self._extract_chat_id(message) if isinstance(message, dict) else None
            if chat_id is not None and not self._is_authorized(chat_id):
                if isinstance(cq_id, str):
                    await self._api.answer_callback_query(cq_id)
                return

            if not isinstance(cq_id, str) or not isinstance(data, str):
                if isinstance(cq_id, str):
                    await self._api.answer_callback_query(cq_id)
                return

            parsed = parse_callback_data(data)
            if parsed is None:
                await self._api.answer_callback_query(cq_id)
                return

            nonce, action = parsed
            pending = self._pending_approvals.pop(nonce, None)
            if pending is None:
                await self._api.answer_callback_query(cq_id, "Expired")
                return

            tool_call_id = str(pending.get("tool_call_id", "") or "")
            approved = action == "y"
            status_code, _body = await self._client.submit_approval(tool_call_id, nonce, approved)
            timeout_task = pending.get("timeout_task")
            if isinstance(timeout_task, asyncio.Task) and not timeout_task.done():
                timeout_task.cancel()

            if status_code == 200:
                label = "Approved" if approved else "Denied"
                toast = label
            elif status_code == 404:
                toast = "Not found (timed out)"
                label = "Approval not found (may have timed out)."
            elif status_code == 409:
                toast = "Already submitted"
                label = "Approval already submitted."
            elif status_code == 410:
                toast = "Expired"
                label = APPROVAL_EXPIRED_DETAIL
            else:
                toast = f"Failed ({status_code})"
                label = f"Approval failed ({status_code})."

            await self._api.answer_callback_query(cq_id, toast)

            pending_chat_id = pending.get("chat_id", chat_id)
            pending_message_id = pending.get("message_id")
            if not isinstance(pending_message_id, int) or pending_chat_id is None:
                return

            display = label
            if status_code == 200:
                display = f"✅ {label}" if approved else f"❌ {label}"
            await self._api.edit_message_text(
                pending_chat_id,
                pending_message_id,
                display,
                reply_markup={"inline_keyboard": []},
            )
        except Exception:
            log.exception("Approval callback failed for nonce=%s", nonce)
            stale = False
            if isinstance(pending, dict):
                if pending.get("generation") != self._approval_generation:
                    stale = True
                    log.warning(
                        "Approval callback failed for nonce=%s (generation mismatch, not reinserting)",
                        nonce,
                    )
                    self._edit_approval_keyboard(pending, self._approval_generation_label)
                else:
                    timeout_task = pending.get("timeout_task")
                    if isinstance(timeout_task, asyncio.Task) and not timeout_task.done():
                        timeout_task.cancel()
                    pending["timeout_task"] = asyncio.create_task(self._approval_timeout(nonce, 30))
                    self._pending_approvals[nonce] = pending
            if isinstance(cq_id, str):
                try:
                    toast = "Error" if stale else "Error — try again"
                    await self._api.answer_callback_query(cq_id, toast)
                except Exception:
                    pass

    async def _typing_loop(self, chat_id: int | str) -> None:
        try:
            while True:
                await self._api.send_chat_action(chat_id, "typing")
                await asyncio.sleep(4)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Failed to send typing indicator: %s", exc)

    async def _safe_send_message(self, chat_id: int | str, text: str) -> None:
        try:
            await self._api.send_message(chat_id, text)
        except Exception as exc:
            log.warning("Failed to send Telegram message: %s", exc)

    def _edit_approval_keyboard(self, entry: dict[str, Any], label: str) -> None:
        chat_id = entry.get("chat_id")
        message_id = entry.get("message_id")
        if isinstance(message_id, int) and chat_id is not None:
            asyncio.create_task(self._safe_edit_keyboard(chat_id, message_id, label))

    async def _safe_edit_keyboard(self, chat_id: Any, message_id: int, label: str) -> None:
        try:
            await self._api.edit_message_text(
                chat_id,
                message_id,
                label,
                reply_markup={"inline_keyboard": []},
            )
        except Exception as exc:
            log.warning("Failed to edit approval keyboard: %s", exc)

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

    async def _run_as_current_task(self, work: Awaitable[None]) -> None:
        task = asyncio.create_task(work)
        self._current_task = task
        try:
            await task
        finally:
            if self._current_task is task:
                self._current_task = None
            self._stop_requested = False

    def _trim_history(self) -> None:
        max_messages = max(1, self._config.history_max_turns * 2)
        while len(self._history) > max_messages:
            self._history.pop(0)

    def _remove_message(self, message: dict[str, str]) -> None:
        for index in range(len(self._history) - 1, -1, -1):
            if self._history[index] == message:
                self._history.pop(index)
                return

    def _log_request(self, metrics: RequestMetrics, assistant_text: str) -> None:
        if assistant_text:
            log.debug(
                "request=%s response: %s",
                metrics.request_id,
                assistant_text[:200].replace("\n", " "),
            )

        summary = "request=%s model=%s tokens=%s/%s cost=$%.4f tools=%s latency=%sms"
        if metrics.error is None:
            log.info(
                summary,
                metrics.request_id,
                metrics.model,
                metrics.input_tokens,
                metrics.output_tokens,
                metrics.estimated_cost,
                metrics.tool_call_count,
                metrics.latency_ms,
            )
            return

        log.warning(
            summary + " error=%s",
            metrics.request_id,
            metrics.model,
            metrics.input_tokens,
            metrics.output_tokens,
            metrics.estimated_cost,
            metrics.tool_call_count,
            metrics.latency_ms,
            metrics.error,
        )

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
    auto_migrate_data()
    setup_logging()
    raw_level_name = str(os.getenv("FINANCE_CLI_LOG_LEVEL", "INFO"))
    level_name = raw_level_name.strip().upper()
    if level_name not in _VALID_LOG_LEVELS:
        level_name = "INFO"
    logging.getLogger("finance_cli").setLevel(getattr(logging, level_name))
    config = load_config()
    pid_file = _pid_file_for_token(config.telegram_token)
    _acquire_pid_lock(pid_file)
    store: BotStore | None = None
    client: GatewayClient | None = None
    bot: TelegramBot | None = None
    try:
        store = BotStore()
        store.startup()
        client = GatewayClient(config.gateway_url, config.gateway_user_key)
        await client.ensure_session(user_id=config.telegram_chat_id)
        bot = TelegramBot(config, client=client, store=store)
        bot._history = store.load_recent_messages(limit=config.history_max_turns * 2)
        store.close_all_open_sessions("restart")

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, bot.stop)
            except NotImplementedError:
                pass

        log.info("Starting Telegram bot with %s restored messages", len(bot._history))
        await bot.start()
    finally:
        if bot is not None:
            bot._close_session("restart")
        _release_pid_lock()
        if client is not None:
            await client.close()
        if store is not None:
            log.info("Shutting down Telegram bot store")
            store.shutdown()
