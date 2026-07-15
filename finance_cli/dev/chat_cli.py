"""Gateway-direct dev chat CLI for local agent debugging."""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Callable, TextIO
from urllib.parse import urlparse

import httpx

from finance_cli.telegram_bot.gateway_client import parse_sse_events

DEFAULT_BASE_URL = "http://127.0.0.1:8002"
CONFIG_SUBPATH = Path("cashnerd") / "cli_config.json"
SESSIONS_SUBPATH = Path("cashnerd") / "sessions"
_DEFAULT_SESSION = "default"
_SESSION_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
APPROVAL_SAFETY_MARGIN_SECONDS = 5
APPROVAL_MAX_COUNTDOWN_SECONDS = 90
APPROVAL_SUBMIT_TIMEOUT_SECONDS = 30.0
DIVIDER = "-" * 60
_TOOL_SUMMARY_MAX = 500
_PROD_SMOKE_NO_TOOL_EXPECTED = "LIVE_AGENT_OK"
_PROD_SMOKE_TOOL_EXPECTED = "LIVE_AGENT_TOOL_OK"
_PROD_SMOKE_TOOL_NAME = "provider_status"
_PROD_SMOKE_NO_TOOL_MESSAGE = (
    "Production smoke test. Reply with exactly LIVE_AGENT_OK. Do not call any tools."
)
_PROD_SMOKE_TOOL_MESSAGE = (
    "Production user-scoped tool smoke test. You must call the read-only "
    "provider_status tool exactly once to verify MCP tool connectivity. After it "
    "completes, reply exactly LIVE_AGENT_TOOL_OK. Do not include any provider, "
    "account, or financial details."
)
_CLI_INIT_CONTEXT = {"channel": "cli"}


class CLIError(RuntimeError):
    """Raised for expected CLI usage and transport errors."""


@dataclass(slots=True)
class CLIConfig:
    gateway_user_key: str
    user_id: str
    base_url: str


@dataclass(slots=True)
class GatewaySession:
    token: str
    session_id: str
    expires_at: int


@dataclass(slots=True)
class SessionState:
    name: str
    path: Path
    created_at: int
    messages: list[dict[str, str]]


@dataclass(slots=True)
class SmokeResult:
    text: str
    event_types: list[str]
    tool_starts: list[str]
    tool_completes: list[str]
    errors: list[str]


class JsonlEventCapture:
    """Append parsed gateway SSE events to a local JSONL transcript."""

    def __init__(
        self,
        path: Path,
        *,
        session_name: str,
        time_fn: Callable[[], float],
    ) -> None:
        self.path = path
        self._session_name = session_name
        self._time_fn = time_fn
        self._handle: TextIO | None = None
        self._event_index = 0

    def open(self) -> None:
        fd: int | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
            flags |= getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(self.path, flags, 0o600)
            os.fchmod(fd, 0o600)
            self._handle = os.fdopen(fd, "a", encoding="utf-8")
            fd = None
        except OSError as exc:
            raise CLIError(f"Failed to open capture JSONL at {self.path}: {exc}") from exc
        finally:
            if fd is not None:
                os.close(fd)

    def close(self) -> None:
        if self._handle is None:
            return
        self._handle.close()
        self._handle = None

    def write_events(
        self,
        events: list[dict[str, Any]],
        *,
        session: GatewaySession,
        turn_index: int,
        attempt: int,
        source: str = "gateway_sse",
    ) -> None:
        if self._handle is None:
            raise CLIError("Capture JSONL writer is not open.")
        for event in events:
            self._event_index += 1
            row = dict(event)
            row["capture"] = {
                "source": source,
                "session_name": self._session_name,
                "session_id": session.session_id,
                "turn_index": turn_index,
                "attempt": attempt,
                "event_index": self._event_index,
                "captured_at": int(self._time_fn()),
            }
            self._handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
        self._handle.flush()

    def write_event(
        self,
        event: dict[str, Any],
        *,
        session: GatewaySession,
        turn_index: int,
        attempt: int,
        source: str,
    ) -> None:
        self.write_events(
            [event],
            session=session,
            turn_index=turn_index,
            attempt=attempt,
            source=source,
        )


def _cache_root() -> Path:
    override = os.getenv("XDG_CACHE_HOME", "").strip()
    if override:
        return Path(override).expanduser()
    return Path.home() / ".cache"


def _config_path() -> Path:
    return _cache_root() / CONFIG_SUBPATH


def _validate_session_name(name: str) -> str:
    if not _SESSION_NAME_RE.fullmatch(name):
        raise CLIError(
            "Invalid session name. Use 1-64 letters, numbers, underscores, or hyphens."
        )
    return name


def _parse_tool_name_allowlist(values: list[str] | None) -> frozenset[str]:
    names: set[str] = set()
    for value in values or []:
        for part in value.split(","):
            name = part.strip()
            if not name:
                continue
            if not _TOOL_NAME_RE.fullmatch(name):
                raise CLIError(
                    "Invalid auto-approval tool name. Use exact tool names or "
                    "tool:qualifier approval keys with "
                    "letters, numbers, underscores, dots, colons, or hyphens."
                )
            names.add(name)
    return frozenset(names)


def _approval_key(tool_name: str, resolved_qualifier: str | None) -> str:
    qualifier = (resolved_qualifier or "").strip()
    return f"{tool_name}:{qualifier}" if qualifier else tool_name


def _display_event(event: dict[str, Any]) -> dict[str, Any]:
    nested = event.get("event")
    if isinstance(nested, dict):
        return nested
    return event


def _session_path(name: str) -> Path:
    return _cache_root() / SESSIONS_SUBPATH / f"{name}.json"


def _ensure_not_production() -> None:
    if os.getenv("APP_ENV", "").strip().lower() == "production":
        raise CLIError("Refusing to run dev chat CLI with APP_ENV=production.")


def _require_production_smoke_confirmation(allow_production: bool) -> None:
    if not allow_production:
        raise CLIError(
            "smoke-prod requires --allow-production. This command can spend live "
            "agent budget and may touch production read-only tools."
        )


def _validate_base_url(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    parsed = urlparse(normalized)
    if parsed.scheme != "http":
        raise CLIError("Base URL must use http://127.0.0.1.")
    if parsed.hostname != "127.0.0.1":
        raise CLIError("Base URL must use http://127.0.0.1.")
    if parsed.path not in {"", "/"} or parsed.params or parsed.query or parsed.fragment:
        raise CLIError("Base URL must not include a path, query, or fragment.")
    return normalized


def _pick_value(*values: str | None) -> str | None:
    for value in values:
        if value is None:
            continue
        stripped = value.strip()
        if stripped:
            return stripped
    return None


def _load_saved_config(path: Path) -> CLIConfig | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise CLIError(f"Failed to read CLI config: {path}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CLIError(f"Invalid CLI config file: {path}") from exc

    if not isinstance(payload, dict):
        raise CLIError(f"Invalid CLI config file: {path}")

    try:
        gateway_user_key = str(
            payload.get("gateway_user_key") or payload["gateway_api_key"]
        ).strip()
        user_id = str(payload["user_id"]).strip()
        base_url = _validate_base_url(str(payload["base_url"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise CLIError(f"Invalid CLI config file: {path}") from exc

    return CLIConfig(
        gateway_user_key=gateway_user_key,
        user_id=user_id,
        base_url=base_url,
    )


def _save_config(path: Path, config: CLIConfig) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(config), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)


def _session_corrupted_error(path: Path) -> CLIError:
    return CLIError(f"session file corrupted at {path}; run again with --new to reset")


def _coerce_session_created_at(value: Any, *, path: Path, time_fn: Callable[[], float]) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    replacement = int(time_fn())
    sys.stderr.write(
        f"[session_warn] invalid created_at in {path}; using {replacement}\n"
    )
    sys.stderr.flush()
    return replacement


def _load_session(path: Path, *, name: str, time_fn: Callable[[], float]) -> SessionState:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return SessionState(
            name=name,
            path=path,
            created_at=int(time_fn()),
            messages=[],
        )
    except OSError as exc:
        raise CLIError(f"Failed to read session file at {path}; run again with --new to reset") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise _session_corrupted_error(path) from exc

    if not isinstance(payload, dict):
        raise _session_corrupted_error(path)

    messages = payload.get("messages")
    if not isinstance(messages, list):
        raise _session_corrupted_error(path)

    validated_messages: list[dict[str, str]] = []
    for message in messages:
        if not isinstance(message, dict):
            raise _session_corrupted_error(path)
        role = message.get("role")
        content = message.get("content")
        if not isinstance(role, str) or not isinstance(content, str):
            raise _session_corrupted_error(path)
        if role not in {"user", "assistant"}:
            raise _session_corrupted_error(path)
        validated_messages.append({"role": role, "content": content})

    return SessionState(
        name=name,
        path=path,
        created_at=_coerce_session_created_at(
            payload.get("created_at"),
            path=path,
            time_fn=time_fn,
        ),
        messages=validated_messages,
    )


def _load_session_created_at_best_effort(
    path: Path,
    *,
    time_fn: Callable[[], float],
) -> int:
    now = int(time_fn())
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return now
    except OSError:
        return now

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return now

    if not isinstance(payload, dict):
        return now

    created_at = payload.get("created_at")
    if isinstance(created_at, int) and not isinstance(created_at, bool):
        return created_at
    return now


def _save_session(state: SessionState, *, time_fn: Callable[[], float]) -> None:
    state.path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": state.name,
        "created_at": state.created_at,
        "updated_at": int(time_fn()),
        "messages": state.messages,
    }

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            dir=state.path.parent,
            delete=False,
            prefix=f".{state.name}.",
            suffix=".tmp",
            encoding="utf-8",
        ) as temp_file:
            temp_path = Path(temp_file.name)
            json.dump(payload, temp_file, indent=2, sort_keys=True)
            temp_file.write("\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.chmod(temp_path, 0o600)
        os.replace(temp_path, state.path)
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
        raise


def _resolve_config(
    *,
    config_path: Path,
    base_url: str | None,
    user_key: str | None,
    user_id: str | None,
    require_complete: bool = True,
) -> CLIConfig:
    saved = _load_saved_config(config_path)
    resolved_base_url = _validate_base_url(
        _pick_value(
            base_url,
            os.getenv("GATEWAY_BASE_URL"),
            saved.base_url if saved is not None else None,
            DEFAULT_BASE_URL,
        )
        or DEFAULT_BASE_URL
    )
    resolved_user_key = _pick_value(
        user_key,
        os.getenv("GATEWAY_USER_KEY"),
        saved.gateway_user_key if saved is not None else None,
    )
    resolved_user_id = _pick_value(
        user_id,
        os.getenv("CASHNERD_USER_ID"),
        saved.user_id if saved is not None else None,
    )

    if require_complete and (not resolved_user_key or not resolved_user_id):
        raise CLIError(
            "Missing gateway credentials. Run `python -m finance_cli.dev.chat_cli login` "
            "or set GATEWAY_USER_KEY and CASHNERD_USER_ID."
        )

    return CLIConfig(
        gateway_user_key=resolved_user_key or "",
        user_id=resolved_user_id or "",
        base_url=resolved_base_url,
    )


def _extract_error_detail(response: httpx.Response) -> str:
    try:
        raw = response.read()
    except httpx.HTTPError:
        raw = b""

    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return f"HTTP {response.status_code}"

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text

    if isinstance(payload, dict):
        detail = payload.get("detail") or payload.get("error") or payload.get("message")
        if detail:
            if isinstance(detail, str):
                return detail
            return json.dumps(detail, sort_keys=True, default=str)
        return json.dumps(payload, sort_keys=True, default=str)
    return str(payload)


def _derive_approval_countdown_seconds(expires_at: Any, now_fn: Callable[[], float]) -> int | None:
    if not isinstance(expires_at, (int, float)) or expires_at <= 0:
        return None
    remaining = int(expires_at) - int(now_fn()) - APPROVAL_SAFETY_MARGIN_SECONDS
    if remaining <= 0:
        return 0
    return min(remaining, APPROVAL_MAX_COUNTDOWN_SECONDS)


def _format_countdown(seconds: int | None) -> str:
    if seconds is None:
        return "unknown"
    minutes, remainder = divmod(max(seconds, 0), 60)
    return f"{minutes}:{remainder:02d} remaining"


def _json_compact(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _json_pretty(value: Any) -> str:
    return json.dumps(value, sort_keys=True, indent=2, default=str)


def _event_error_message(raw_error: Any) -> str:
    if isinstance(raw_error, str) and raw_error.strip():
        return raw_error
    if isinstance(raw_error, dict):
        message = raw_error.get("message") or raw_error.get("error")
        if isinstance(message, str) and message.strip():
            return message
    try:
        return json.dumps(raw_error, sort_keys=True, default=str)
    except TypeError:
        return str(raw_error)


def _summarize_result(result: Any, max_len: int = 150) -> str:
    if result is None:
        return ""
    if isinstance(result, dict):
        summary = result.get("summary")
        if summary:
            rendered = json.dumps(summary, default=str, sort_keys=True)
            if rendered not in {"null", "{}", "\"\""}:
                return rendered[:max_len] + "..." if len(rendered) > max_len else rendered
    rendered = json.dumps(result, default=str, sort_keys=True)
    return rendered[:max_len] + "..." if len(rendered) > max_len else rendered


def _tool_error_message(error: Any) -> str | None:
    if isinstance(error, dict):
        return str(error.get("message", "unknown"))
    if error is None:
        return None
    return str(error)


def _format_args(tool_input: dict[str, Any], max_val_len: int = 30) -> str:
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
    if not tool_calls:
        return ""

    parts: list[str] = []
    for tool_call in tool_calls:
        name = str(tool_call.get("tool_name", "?"))
        args = _format_args(tool_call.get("tool_input", {}))
        if tool_call.get("is_error"):
            error = str(tool_call.get("error_message") or "unknown error")
            parts.append(f"{name}({args}) -> ERROR: {error}")
            continue
        summary = str(tool_call.get("result_summary", ""))
        parts.append(f"{name}({args}) -> {summary}" if summary else f"{name}({args})")

    text = "[Tools: " + " | ".join(parts) + "]"
    if len(text) > _TOOL_SUMMARY_MAX:
        text = text[: _TOOL_SUMMARY_MAX - 4] + "...]"
    return text


class DevChatCLI:
    """Small sync CLI for direct gateway chat debugging."""

    def __init__(
        self,
        *,
        stdout: TextIO,
        stderr: TextIO,
        input_fn: Callable[[], str],
        time_fn: Callable[[], float],
        transport: httpx.BaseTransport | None = None,
        client_limits: httpx.Limits | None = None,
    ) -> None:
        self._stdout = stdout
        self._stderr = stderr
        self._input_fn = input_fn
        self._time_fn = time_fn
        self._transport = transport
        self._client_limits = client_limits
        self._colors = self._supports_color(stdout)
        self._text_open = False

    def _client_kwargs(self, *, timeout: float | None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "timeout": timeout,
            "follow_redirects": True,
            "trust_env": False,
        }
        if self._transport is not None:
            kwargs["transport"] = self._transport
        if self._client_limits is not None:
            kwargs["limits"] = self._client_limits
        return kwargs

    def _supports_color(self, stream: TextIO) -> bool:
        return bool(getattr(stream, "isatty", lambda: False)()) and not os.getenv("NO_COLOR")

    def _style(self, text: str, code: str) -> str:
        if not self._colors:
            return text
        return f"\033[{code}m{text}\033[0m"

    def _write(self, text: str = "") -> None:
        self._stdout.write(text)
        self._stdout.flush()

    def _write_line(self, text: str = "") -> None:
        self._write(f"{text}\n")

    def _close_text_line(self) -> None:
        if self._text_open:
            self._write("\n")
            self._text_open = False

    def _prompt(self, prompt: str) -> str:
        self._write(prompt)
        try:
            return self._input_fn()
        except EOFError:
            return ""

    def _prompt_value(self, label: str, current: str | None = None) -> str:
        suffix = f" [{current}]" if current else ""
        entered = self._prompt(f"{label}{suffix}: ").strip()
        if entered:
            return entered
        return current or ""

    def _init_session(self, *, client: httpx.Client, config: CLIConfig) -> GatewaySession:
        try:
            response = client.post(
                f"{config.base_url}/api/chat/init",
                json={
                    "api_key": config.gateway_user_key,
                    "user_id": config.user_id,
                    "context": _CLI_INIT_CONTEXT,
                },
            )
        except httpx.ConnectError as exc:
            raise CLIError(
                f"Gateway not reachable at {config.base_url}. Start it locally and retry."
            ) from exc
        except httpx.HTTPError as exc:
            raise CLIError(f"Gateway init failed: {exc}") from exc

        if response.status_code >= 400:
            raise CLIError(
                f"Gateway init failed ({response.status_code}): {_extract_error_detail(response)}"
            )

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise CLIError("Gateway init returned invalid JSON.") from exc

        if not isinstance(payload, dict):
            raise CLIError("Gateway init returned an invalid session payload.")

        token = str(payload.get("session_token", "") or "").strip()
        session_id = str(payload.get("session_id", "") or "").strip()
        expires_at = int(payload.get("expires_at", 0) or 0)
        if not token or not session_id or expires_at <= 0:
            raise CLIError("Gateway init returned an invalid session payload.")

        return GatewaySession(token=token, session_id=session_id, expires_at=expires_at)

    def login(
        self,
        *,
        base_url: str | None,
        user_key: str | None,
        user_id: str | None,
        config_path: Path,
    ) -> int:
        _ensure_not_production()
        saved = _load_saved_config(config_path)
        resolved_base_url = _validate_base_url(
            _pick_value(
                base_url,
                os.getenv("GATEWAY_BASE_URL"),
                saved.base_url if saved is not None else None,
                DEFAULT_BASE_URL,
            )
            or DEFAULT_BASE_URL
        )
        resolved_user_key = _pick_value(
            user_key,
            os.getenv("GATEWAY_USER_KEY"),
            saved.gateway_user_key if saved is not None else None,
        )
        if not resolved_user_key:
            resolved_user_key = self._prompt_value(
                "Gateway user key",
                saved.gateway_user_key if saved is not None else None,
            )
        resolved_user_id = _pick_value(
            user_id,
            os.getenv("CASHNERD_USER_ID"),
            saved.user_id if saved is not None else None,
        )
        if not resolved_user_id:
            resolved_user_id = self._prompt_value(
                "CashNerd user_id (str(users.id))",
                saved.user_id if saved is not None else None,
            )
        if not resolved_user_key or not resolved_user_id:
            raise CLIError("Both gateway_user_key and user_id are required.")

        config = CLIConfig(
            gateway_user_key=resolved_user_key,
            user_id=resolved_user_id,
            base_url=resolved_base_url,
        )

        with httpx.Client(**self._client_kwargs(timeout=30.0)) as client:
            session = self._init_session(client=client, config=config)

        _save_config(config_path, config)
        self._write_line(f"[login] verified init session {session.session_id}")
        self._write_line(f"[login] config saved to {config_path}")
        self._write_line(f"[login] base_url={config.base_url}")
        self._write_line(f"[login] user_id={config.user_id}")
        return 0

    def chat(
        self,
        *,
        message: str | None,
        skill: str | None,
        raw: bool,
        new_history: bool,
        session_name: str,
        capture_jsonl: str | None,
        auto_approve_tools: frozenset[str],
        base_url: str | None,
        user_key: str | None,
        user_id: str | None,
        config_path: Path,
    ) -> int:
        session_name = _validate_session_name(session_name)
        _ensure_not_production()
        if auto_approve_tools and capture_jsonl is None:
            raise CLIError("--auto-approve-tool requires --capture-jsonl for audit.")
        path = _session_path(session_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        if new_history:
            state = SessionState(
                name=session_name,
                path=path,
                created_at=_load_session_created_at_best_effort(
                    path,
                    time_fn=self._time_fn,
                ),
                messages=[],
            )
            try:
                _save_session(state, time_fn=self._time_fn)
            except Exception as exc:
                self._stderr.write(f"[session_warn] failed to persist: {exc}\n")
                self._stderr.flush()
        else:
            state = _load_session(path, name=session_name, time_fn=self._time_fn)

        config = _resolve_config(
            config_path=config_path,
            base_url=base_url,
            user_key=user_key,
            user_id=user_id,
            require_complete=True,
        )

        prompt_message = (message or "").strip()
        history: list[dict[str, str]] = [dict(item) for item in state.messages]
        capture = (
            JsonlEventCapture(
                Path(capture_jsonl).expanduser(),
                session_name=session_name,
                time_fn=self._time_fn,
            )
            if capture_jsonl
            else None
        )

        if capture is not None:
            capture.open()
            self._write_line(f"[capture] writing JSONL to {capture.path}")

        all_turns_ok = True
        try:
            with httpx.Client(**self._client_kwargs(timeout=None)) as client:
                session = self._init_session(client=client, config=config)
                turn_index = 0

                while True:
                    if not prompt_message:
                        prompt_message = self._prompt("You> ").strip()
                        if not prompt_message:
                            break

                    turn_index += 1
                    session, turn_ok = self._run_chat_turn(
                        client=client,
                        config=config,
                        session=session,
                        history=history,
                        message=prompt_message,
                        skill=skill,
                        raw=raw,
                        capture=capture,
                        auto_approve_tools=auto_approve_tools,
                        turn_index=turn_index,
                    )
                    all_turns_ok = all_turns_ok and turn_ok
                    state.messages = [dict(item) for item in history]
                    try:
                        _save_session(state, time_fn=self._time_fn)
                    except Exception as exc:
                        self._stderr.write(f"[session_warn] failed to persist: {exc}\n")
                        self._stderr.flush()
                    prompt_message = ""
                    if message is not None:
                        break
        finally:
            if capture is not None:
                capture.close()

        return 0 if all_turns_ok else 1

    def smoke_prod(
        self,
        *,
        base_url: str | None,
        user_key: str | None,
        user_id: str | None,
        config_path: Path,
        allow_production: bool,
        skip_tool: bool,
    ) -> int:
        _require_production_smoke_confirmation(allow_production)
        config = _resolve_config(
            config_path=config_path,
            base_url=base_url,
            user_key=user_key,
            user_id=user_id,
            require_complete=True,
        )

        all_ok = True
        with httpx.Client(**self._client_kwargs(timeout=None)) as client:
            session = self._init_session(client=client, config=config)
            self._write_line(f"[smoke] verified init session {session.session_id}")
            self._write_line(f"[smoke] base_url={config.base_url}")
            self._write_line(f"[smoke] user_id={config.user_id}")

            session, no_tool_result = self._run_smoke_turn(
                client=client,
                config=config,
                session=session,
                label="no-tool",
                message=_PROD_SMOKE_NO_TOOL_MESSAGE,
                context={"channel": "cli", "purpose": "prod_smoke_no_tool"},
            )
            no_tool_ok = self._smoke_result_ok(
                "no-tool",
                no_tool_result,
                expected_text=_PROD_SMOKE_NO_TOOL_EXPECTED,
                expected_tool=None,
            )
            self._render_smoke_result("no-tool", no_tool_result, ok=no_tool_ok)
            all_ok = all_ok and no_tool_ok

            if not skip_tool:
                session, tool_result = self._run_smoke_turn(
                    client=client,
                    config=config,
                    session=session,
                    label="tool",
                    message=_PROD_SMOKE_TOOL_MESSAGE,
                    context={"channel": "cli", "purpose": "prod_smoke_tool"},
                )
                tool_ok = self._smoke_result_ok(
                    "tool",
                    tool_result,
                    expected_text=_PROD_SMOKE_TOOL_EXPECTED,
                    expected_tool=_PROD_SMOKE_TOOL_NAME,
                )
                self._render_smoke_result("tool", tool_result, ok=tool_ok)
                all_ok = all_ok and tool_ok

        self._write_line("[smoke] PASS" if all_ok else "[smoke] FAIL")
        return 0 if all_ok else 1

    def _run_smoke_turn(
        self,
        *,
        client: httpx.Client,
        config: CLIConfig,
        session: GatewaySession,
        label: str,
        message: str,
        context: dict[str, Any],
    ) -> tuple[GatewaySession, SmokeResult]:
        payload = {
            "messages": [{"role": "user", "content": message}],
            "context": context,
            "user_id": config.user_id,
        }
        attempt = 0
        while attempt < 2:
            text_parts: list[str] = []
            event_types: list[str] = []
            tool_starts: list[str] = []
            tool_completes: list[str] = []
            errors: list[str] = []
            buffer = ""
            try:
                with client.stream(
                    "POST",
                    f"{config.base_url}/api/chat",
                    headers={
                        "Accept": "text/event-stream",
                        "Authorization": f"Bearer {session.token}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                ) as response:
                    if response.status_code == 401 and attempt == 0:
                        session = self._init_session(client=client, config=config)
                        attempt += 1
                        continue
                    if response.status_code >= 400:
                        raise CLIError(
                            f"Smoke {label} failed ({response.status_code}): "
                            f"{_extract_error_detail(response)}"
                        )

                    for chunk in response.iter_text():
                        if not chunk:
                            continue
                        buffer += chunk
                        events, buffer = parse_sse_events(buffer)
                        self._collect_smoke_events(
                            events,
                            text_parts=text_parts,
                            event_types=event_types,
                            tool_starts=tool_starts,
                            tool_completes=tool_completes,
                            errors=errors,
                        )

                    if buffer:
                        events, _remainder = parse_sse_events(f"{buffer}\n\n")
                        self._collect_smoke_events(
                            events,
                            text_parts=text_parts,
                            event_types=event_types,
                            tool_starts=tool_starts,
                            tool_completes=tool_completes,
                            errors=errors,
                        )
                return session, SmokeResult(
                    text="".join(text_parts).strip(),
                    event_types=event_types,
                    tool_starts=tool_starts,
                    tool_completes=tool_completes,
                    errors=errors,
                )
            except httpx.ConnectError as exc:
                raise CLIError(
                    f"Gateway not reachable at {config.base_url}. Start it locally, "
                    "open an SSH tunnel, or run smoke-prod on the server."
                ) from exc
            except httpx.HTTPError as exc:
                raise CLIError(f"Smoke {label} failed: {exc}") from exc

        raise CLIError(f"Smoke {label} failed after session refresh.")

    def _collect_smoke_events(
        self,
        events: list[dict[str, Any]],
        *,
        text_parts: list[str],
        event_types: list[str],
        tool_starts: list[str],
        tool_completes: list[str],
        errors: list[str],
    ) -> None:
        for event in events:
            display_event = _display_event(event)
            event_type = str(display_event.get("type", "") or "")
            event_types.append(event_type)
            if event_type in {"text", "text_delta"}:
                text_parts.append(str(display_event.get("text", "") or ""))
            elif event_type == "tool_call_start":
                tool_starts.append(str(display_event.get("tool_name", "tool")))
            elif event_type == "tool_call_complete":
                tool_completes.append(str(display_event.get("tool_name", "tool")))
                if display_event.get("error") is not None:
                    errors.append(_event_error_message(display_event.get("error")))
            elif event_type == "tool_approval_request":
                errors.append(
                    "unexpected approval request for "
                    f"{display_event.get('tool_name', 'tool')}"
                )
            elif event_type in {"error", "stream_error"}:
                errors.append(_event_error_message(display_event.get("error")))

    def _smoke_result_ok(
        self,
        label: str,
        result: SmokeResult,
        *,
        expected_text: str,
        expected_tool: str | None,
    ) -> bool:
        del label
        if result.text != expected_text or result.errors:
            return False
        if "stream_complete" not in result.event_types:
            return False
        if expected_tool is None:
            return not result.tool_starts and not result.tool_completes
        return (
            result.tool_starts == [expected_tool]
            and result.tool_completes == [expected_tool]
        )

    def _render_smoke_result(self, label: str, result: SmokeResult, *, ok: bool) -> None:
        status = "PASS" if ok else "FAIL"
        self._write_line(f"[smoke:{label}] {status}")
        self._write_line(f"  text={result.text!r}")
        self._write_line(
            "  tools="
            + (
                "none"
                if not result.tool_starts and not result.tool_completes
                else f"start:{','.join(result.tool_starts) or 'none'} "
                f"complete:{','.join(result.tool_completes) or 'none'}"
            )
        )
        self._write_line(
            f"  events={','.join(result.event_types) if result.event_types else 'none'}"
        )
        if result.errors:
            self._write_line(f"  errors={'; '.join(result.errors)}")

    def _run_chat_turn(
        self,
        *,
        client: httpx.Client,
        config: CLIConfig,
        session: GatewaySession,
        history: list[dict[str, str]],
        message: str,
        skill: str | None,
        raw: bool,
        capture: JsonlEventCapture | None,
        auto_approve_tools: frozenset[str],
        turn_index: int,
    ) -> tuple[GatewaySession, bool]:
        request_messages = [*history, {"role": "user", "content": message}]
        context: dict[str, Any] = {"channel": "cli"}
        if skill:
            context["skill"] = skill

        payload = {
            "messages": request_messages,
            "context": context,
            "user_id": config.user_id,
        }
        started_at = self._time_fn()

        self._write_line(self._style(f"You> {message}", "2"))
        self._write_line(DIVIDER)

        attempt = 0
        assistant_parts: list[str] = []
        tool_inputs: dict[str, dict[str, Any]] = {}
        tool_calls: list[dict[str, Any]] = []
        saw_error = False

        while attempt < 2:
            assistant_parts = []
            tool_inputs = {}
            tool_calls = []
            saw_error = False
            buffer = ""

            try:
                with client.stream(
                    "POST",
                    f"{config.base_url}/api/chat",
                    headers={
                        "Accept": "text/event-stream",
                        "Authorization": f"Bearer {session.token}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                ) as response:
                    if response.status_code == 401:
                        if attempt == 0:
                            session = self._init_session(client=client, config=config)
                            attempt += 1
                            continue
                        raise CLIError(
                            f"Chat failed after session refresh (401): {_extract_error_detail(response)}"
                        )
                    if response.status_code >= 400:
                        raise CLIError(
                            f"Chat failed ({response.status_code}): {_extract_error_detail(response)}"
                        )

                    for chunk in response.iter_text():
                        if not chunk:
                            continue
                        buffer += chunk
                        events, buffer = parse_sse_events(buffer)
                        if capture is not None:
                            capture.write_events(
                                events,
                                session=session,
                                turn_index=turn_index,
                                attempt=attempt,
                            )
                        for event in events:
                            if self._handle_event(
                                client=client,
                                base_url=config.base_url,
                                session=session,
                                event=event,
                                raw=raw,
                                assistant_parts=assistant_parts,
                                tool_inputs=tool_inputs,
                                tool_calls=tool_calls,
                                capture=capture,
                                auto_approve_tools=auto_approve_tools,
                                turn_index=turn_index,
                                attempt=attempt,
                            ):
                                saw_error = True

                    if buffer:
                        events, _remainder = parse_sse_events(f"{buffer}\n\n")
                        if capture is not None:
                            capture.write_events(
                                events,
                                session=session,
                                turn_index=turn_index,
                                attempt=attempt,
                            )
                        for event in events:
                            if self._handle_event(
                                client=client,
                                base_url=config.base_url,
                                session=session,
                                event=event,
                                raw=raw,
                                assistant_parts=assistant_parts,
                                tool_inputs=tool_inputs,
                                tool_calls=tool_calls,
                                capture=capture,
                                auto_approve_tools=auto_approve_tools,
                                turn_index=turn_index,
                                attempt=attempt,
                            ):
                                saw_error = True
                break
            except httpx.ConnectError as exc:
                raise CLIError(
                    f"Gateway not reachable at {config.base_url}. Start it locally and retry."
                ) from exc
            except httpx.HTTPError as exc:
                raise CLIError(f"Chat failed: {exc}") from exc

        self._close_text_line()
        self._write_line(DIVIDER)
        self._write_line(f"[done] {self._time_fn() - started_at:.1f}s")

        if saw_error:
            return session, False

        assistant_text = "".join(assistant_parts).strip()
        if not assistant_text:
            assistant_text = "No response generated."

        history.append({"role": "user", "content": message})
        history_text = assistant_text
        tool_summary = _build_tool_summary(tool_calls)
        if tool_summary:
            history_text = f"{assistant_text}\n\n{tool_summary}"
        history.append({"role": "assistant", "content": history_text})
        return session, True

    def _handle_event(
        self,
        *,
        client: httpx.Client,
        base_url: str,
        session: GatewaySession,
        event: dict[str, Any],
        raw: bool,
        assistant_parts: list[str],
        tool_inputs: dict[str, dict[str, Any]],
        tool_calls: list[dict[str, Any]],
        capture: JsonlEventCapture | None,
        auto_approve_tools: frozenset[str],
        turn_index: int,
        attempt: int,
    ) -> bool:
        event = _display_event(event)
        if raw:
            self._close_text_line()
            self._write_line(f"[raw] {_json_compact(event)}")

        event_type = str(event.get("type", "") or "")
        if event_type in {"text", "text_delta"}:
            text = str(event.get("text", "") or "")
            if text:
                assistant_parts.append(text)
                if not self._text_open:
                    self._write("[text] ")
                    self._text_open = True
                self._write(text)
            return False

        self._close_text_line()

        if event_type == "tool_call_start":
            tool_call_id = str(event.get("tool_call_id", "") or "")
            raw_input = event.get("tool_input")
            tool_inputs[tool_call_id] = raw_input if isinstance(raw_input, dict) else {}
            suffix = ""
            if tool_call_id and tool_inputs[tool_call_id]:
                suffix = f" input={_json_compact(tool_inputs[tool_call_id])}"
            self._write_line(
                self._style(
                    f"[tool_call_start] {event.get('tool_name', 'tool')}  "
                    f"call_id={tool_call_id}{suffix}",
                    "36",
                )
            )
            return False

        if event_type == "tool_approval_request":
            self._render_approval_request(
                client=client,
                base_url=base_url,
                session=session,
                event=event,
                capture=capture,
                auto_approve_tools=auto_approve_tools,
                turn_index=turn_index,
                attempt=attempt,
            )
            return False

        if event_type == "tool_call_complete":
            tool_name = str(event.get("tool_name", "tool"))
            tool_call_id = str(event.get("tool_call_id", "") or "")
            result = event.get("result")
            error = event.get("error")
            tool_calls.append(
                {
                    "tool_name": tool_name,
                    "tool_input": tool_inputs.get(tool_call_id, {}),
                    "is_error": error is not None,
                    "result_summary": _summarize_result(result),
                    "error_message": _tool_error_message(error),
                }
            )
            try:
                result_bytes = (
                    len(json.dumps(result, default=str).encode("utf-8"))
                    if result is not None
                    else 0
                )
            except TypeError:
                result_bytes = len(str(result).encode("utf-8")) if result is not None else 0
            line = (
                f"[tool_call_complete] {tool_name}  "
                f"call_id={tool_call_id}  result_bytes={result_bytes}"
            )
            if error is not None:
                line = f"{line}  error={_event_error_message(error)}"
            self._write_line(self._style(line, "36"))
            return False

        if event_type in {"error", "stream_error"}:
            self._write_line(
                self._style(
                    f"[{event_type}] {_event_error_message(event.get('error'))}",
                    "31",
                )
            )
            return True

        if event_type == "stream_complete":
            self._write_line("[stream_complete]")
            return False

        return False

    def _render_approval_request(
        self,
        *,
        client: httpx.Client,
        base_url: str,
        session: GatewaySession,
        event: dict[str, Any],
        capture: JsonlEventCapture | None,
        auto_approve_tools: frozenset[str],
        turn_index: int,
        attempt: int,
    ) -> None:
        tool_name = str(event.get("tool_name", "tool"))
        resolved_qualifier_value = event.get("resolved_qualifier")
        resolved_qualifier = (
            str(resolved_qualifier_value).strip()
            if resolved_qualifier_value not in (None, "")
            else None
        )
        approval_key = _approval_key(tool_name, resolved_qualifier)
        tool_call_id = str(event.get("tool_call_id", "") or "")
        nonce = str(event.get("nonce", "") or "")
        tool_input = event.get("tool_input") if isinstance(event.get("tool_input"), dict) else {}
        countdown = _format_countdown(
            _derive_approval_countdown_seconds(event.get("expires_at"), self._time_fn)
        )

        qualifier_fragment = (
            f"  qualifier={resolved_qualifier}" if resolved_qualifier is not None else ""
        )
        self._write_line(
            self._style(
                f"[tool_approval_request] {tool_name}{qualifier_fragment}  "
                f"call_id={tool_call_id}  "
                f"nonce={nonce}  expires_in={countdown}",
                "1;33",
            )
        )
        self._write_line(_json_pretty(tool_input))
        auto_approved = approval_key in auto_approve_tools
        if auto_approved:
            approved = True
            self._write_line(
                f"[auto_approval] approving {approval_key} via --auto-approve-tool"
            )
        else:
            approved = self._prompt("  -> Allow? [y/N]: ").strip().lower() == "y"
        submitted = self._submit_approval(
            client=client,
            base_url=base_url,
            session=session,
            tool_call_id=tool_call_id,
            nonce=nonce,
            approved=approved,
        )
        if auto_approved and capture is not None:
            capture.write_event(
                {
                    "type": "dev_chat_cli_approval_decision",
                    "tool_name": tool_name,
                    "resolved_qualifier": resolved_qualifier,
                    "approval_key": approval_key,
                    "tool_call_id": tool_call_id,
                    "nonce": nonce,
                    "approved": approved,
                    "outcome": "approved" if approved else "denied",
                    "submitted": submitted,
                    "decision_source": "auto_approve_tool",
                },
                session=session,
                turn_index=turn_index,
                attempt=attempt,
                source="dev_chat_cli_auto_approval",
            )

    def _submit_approval(
        self,
        *,
        client: httpx.Client,
        base_url: str,
        session: GatewaySession,
        tool_call_id: str,
        nonce: str,
        approved: bool,
    ) -> bool:
        if self._transport is None:
            with httpx.Client(
                timeout=APPROVAL_SUBMIT_TIMEOUT_SECONDS,
                follow_redirects=True,
                trust_env=False,
            ) as approval_client:
                return self._post_approval(
                    client=approval_client,
                    base_url=base_url,
                    session=session,
                    tool_call_id=tool_call_id,
                    nonce=nonce,
                    approved=approved,
                )
        return self._post_approval(
            client=client,
            base_url=base_url,
            session=session,
            tool_call_id=tool_call_id,
            nonce=nonce,
            approved=approved,
        )

    def _post_approval(
        self,
        *,
        client: httpx.Client,
        base_url: str,
        session: GatewaySession,
        tool_call_id: str,
        nonce: str,
        approved: bool,
    ) -> bool:
        try:
            response = client.post(
                f"{base_url}/api/chat/tool-approval",
                headers={
                    "Authorization": f"Bearer {session.token}",
                    "Content-Type": "application/json",
                },
                json={
                    "tool_call_id": tool_call_id,
                    "nonce": nonce,
                    "approved": approved,
                },
                timeout=APPROVAL_SUBMIT_TIMEOUT_SECONDS,
            )
        except httpx.ConnectError:
            self._write_line(self._style("[approval_error] gateway unreachable", "31"))
            return False
        except httpx.HTTPError as exc:
            self._write_line(self._style(f"[approval_error] {exc}", "31"))
            return False

        if response.status_code >= 400:
            self._write_line(
                self._style(
                    f"[approval_error] status={response.status_code} "
                    f"detail={_extract_error_detail(response)}",
                    "31",
                )
            )
            return False

        decision = "approved" if approved else "denied"
        self._write_line(f"[approval_submitted] {decision}")
        return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m finance_cli.dev.chat_cli",
        description="Gateway-direct dev chat CLI for the local finance gateway.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_connection_args(target: argparse.ArgumentParser) -> None:
        target.add_argument(
            "--base-url",
            default=None,
            help=f"Gateway base URL. Defaults to saved config, env, or {DEFAULT_BASE_URL}.",
        )
        target.add_argument(
            "--user-key",
            default=None,
            help="Gateway user key override. Otherwise uses env or saved config.",
        )
        target.add_argument(
            "--user-id",
            default=None,
            help="CashNerd user_id override. Must be str(users.id) from PostgreSQL.",
        )

    login_parser = subparsers.add_parser("login", help="Verify /api/chat/init and save CLI config.")
    add_connection_args(login_parser)

    chat_parser = subparsers.add_parser(
        "chat",
        help="Stream chat directly from the gateway using init -> bearer -> chat.",
    )
    add_connection_args(chat_parser)
    chat_parser.add_argument(
        "message",
        nargs="*",
        help="Message to send. If omitted, starts an interactive prompt.",
    )
    chat_parser.add_argument("--skill", help="Attach context.skill to every chat request.")
    chat_parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw parsed SSE events alongside rendered output.",
    )
    chat_parser.add_argument(
        "--session",
        default=_DEFAULT_SESSION,
        help=(
            "Named session for persisted history. Default 'default'. Concurrent invocations "
            "on the same session race — last writer wins."
        ),
    )
    chat_parser.add_argument(
        "--new",
        action="store_true",
        help=(
            "Truncate this session's history before sending. Equivalent to deleting the "
            "session file and starting fresh."
        ),
    )
    chat_parser.add_argument(
        "--capture-jsonl",
        default=None,
        help=(
            "Append parsed gateway SSE events to this JSONL file for transcript grading. "
            "Captures can contain tool inputs/results; keep them local."
        ),
    )
    chat_parser.add_argument(
        "--auto-approve-tool",
        action="append",
        default=[],
        metavar="TOOL",
        help=(
            "Automatically approve approval requests for an exact tool name or "
            "tool:qualifier key. Requires --capture-jsonl; repeat or comma-separate."
        ),
    )

    smoke_parser = subparsers.add_parser(
        "smoke-prod",
        help="Run guarded live gateway smoke checks against a loopback production gateway.",
    )
    add_connection_args(smoke_parser)
    smoke_parser.add_argument(
        "--allow-production",
        action="store_true",
        help="Required acknowledgement for live production smoke checks.",
    )
    smoke_parser.add_argument(
        "--skip-tool",
        action="store_true",
        help="Only run the no-tool stream smoke; skip the read-only provider_status tool check.",
    )
    return parser


def main(
    argv: list[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    input_fn: Callable[[], str] | None = None,
    time_fn: Callable[[], float] = time.time,
    transport: httpx.BaseTransport | None = None,
) -> int:
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    input_fn = input_fn or (lambda: input())
    parser = build_parser()
    args = parser.parse_args(argv)

    cli = DevChatCLI(
        stdout=stdout,
        stderr=stderr,
        input_fn=input_fn,
        time_fn=time_fn,
        transport=transport,
    )
    config_path = _config_path()

    try:
        if args.command == "login":
            return cli.login(
                base_url=args.base_url,
                user_key=args.user_key,
                user_id=args.user_id,
                config_path=config_path,
            )

        if args.command == "chat":
            message = " ".join(args.message).strip() if args.message else None
            return cli.chat(
                message=message,
                skill=args.skill,
                raw=bool(args.raw),
                new_history=bool(args.new),
                session_name=args.session,
                capture_jsonl=args.capture_jsonl,
                auto_approve_tools=_parse_tool_name_allowlist(args.auto_approve_tool),
                base_url=args.base_url,
                user_key=args.user_key,
                user_id=args.user_id,
                config_path=config_path,
            )

        if args.command == "smoke-prod":
            return cli.smoke_prod(
                base_url=args.base_url,
                user_key=args.user_key,
                user_id=args.user_id,
                config_path=config_path,
                allow_production=bool(args.allow_production),
                skip_tool=bool(args.skip_tool),
            )
    except CLIError as exc:
        stderr.write(f"{exc}\n")
        stderr.flush()
        return 1

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
