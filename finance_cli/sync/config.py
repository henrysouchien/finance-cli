"""Configuration and filesystem layout for the local synced MCP."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, field_validator

CASHNERD_DIR = Path.home() / ".cashnerd"
CASHNERD_CONFIG_PATH = CASHNERD_DIR / "config.json"

CASHNERD_AUTH_DIR = CASHNERD_DIR / "auth"
CASHNERD_TOKEN_PATH = CASHNERD_AUTH_DIR / "token.json"

CASHNERD_DATA_DIR = CASHNERD_DIR / "data"
CASHNERD_DB_PATH = CASHNERD_DATA_DIR / "finance.db"
CASHNERD_RULES_PATH = CASHNERD_DATA_DIR / "rules.yaml"
CASHNERD_UPLOADS_DIR = CASHNERD_DATA_DIR / "uploads"
CASHNERD_SKILL_STATE_PATH = CASHNERD_DATA_DIR / "skill_state.json"
CASHNERD_AGENT_MEMORY_PATH = CASHNERD_DATA_DIR / "agent_memory.md"

CASHNERD_SYNC_DIR = CASHNERD_DIR / "sync"
CASHNERD_PENDING_CHANGESET_PATH = CASHNERD_SYNC_DIR / "pending_changeset.json"
CASHNERD_SYNC_LOG_PATH = CASHNERD_SYNC_DIR / "sync_log.json"


class SyncConfig(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=False)

    user_id: str | None = None
    server_url: str = "https://cashnerd.ai"
    last_sync_ts: str | None = None
    schema_version: int | None = None
    install_id: str | None = None

    @field_validator("server_url", mode="before")
    @classmethod
    def _strip_server_url(cls, value):
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("server_url")
    @classmethod
    def _require_https_or_loopback(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme == "https":
            return value
        if parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}:
            return value
        raise ValueError(
            f"CASHNERD_SERVER_URL must use https:// (got {parsed.scheme}://). "
            f"http:// only permitted for localhost, 127.0.0.1, or ::1."
        )


def _server_url_default() -> str:
    value = str(os.environ.get("CASHNERD_SERVER_URL") or "").strip()
    return value or "https://cashnerd.ai"


def _chmod_if_exists(path: Path, mode: int) -> None:
    try:
        path.chmod(mode)
    except FileNotFoundError:
        return


def ensure_dirs() -> None:
    """Create the local CashNerd directories with user-only permissions."""
    for path in (
        CASHNERD_DIR,
        CASHNERD_AUTH_DIR,
        CASHNERD_DATA_DIR,
        CASHNERD_UPLOADS_DIR,
        CASHNERD_SYNC_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
        _chmod_if_exists(path, 0o700)


def _config_from_payload(payload: dict[str, Any]) -> SyncConfig:
    return SyncConfig(
        user_id=str(payload["user_id"]) if payload.get("user_id") is not None else None,
        server_url=str(payload.get("server_url") or _server_url_default()).strip() or _server_url_default(),
        last_sync_ts=(
            str(payload["last_sync_ts"])
            if payload.get("last_sync_ts") is not None
            else None
        ),
        schema_version=(
            int(payload["schema_version"])
            if payload.get("schema_version") is not None
            else None
        ),
        install_id=(
            str(payload["install_id"])
            if payload.get("install_id") is not None
            else None
        ),
    )


def load_config() -> SyncConfig:
    ensure_dirs()
    if not CASHNERD_CONFIG_PATH.exists():
        return SyncConfig(server_url=_server_url_default())

    payload = json.loads(CASHNERD_CONFIG_PATH.read_text(encoding="utf-8"))
    return _config_from_payload(payload)


def save_config(config: SyncConfig) -> None:
    ensure_dirs()
    payload = config.model_dump(mode="json")
    payload["server_url"] = str(payload.get("server_url") or _server_url_default()).strip() or _server_url_default()
    CASHNERD_CONFIG_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _chmod_if_exists(CASHNERD_CONFIG_PATH, 0o600)
