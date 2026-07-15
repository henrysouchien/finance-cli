"""Configuration for the remote MCP process."""

from __future__ import annotations

import logging
from pathlib import Path

from pydantic import Field, ValidationError, field_validator
from finance_cli.settings_base import FinanceBaseSettings

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_DATA_ROOT = _REPO_ROOT / "finance-web" / "data" / "users"
_DEFAULT_BASE_URL = "https://cashnerd.ai"
_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 8003


class McpRemoteSettings(FinanceBaseSettings):
    google_client_id: str = Field(
        ...,
        min_length=1,
        validation_alias="GOOGLE_CLIENT_ID",
    )
    google_client_secret: str = Field(
        ...,
        min_length=1,
        validation_alias="GOOGLE_CLIENT_SECRET",
    )
    base_url: str = Field(_DEFAULT_BASE_URL, validation_alias="MCP_REMOTE_BASE_URL")
    data_root: Path = Field(
        default_factory=lambda: _DEFAULT_DATA_ROOT,
        validation_alias="FINANCE_GATEWAY_DATA_ROOT",
    )
    database_url: str = Field("", validation_alias="DATABASE_URL")
    host: str = Field(_DEFAULT_HOST, validation_alias="MCP_REMOTE_HOST")
    port: int = Field(
        _DEFAULT_PORT,
        ge=1,
        le=65535,
        validation_alias="MCP_REMOTE_PORT",
    )

    @field_validator(
        "google_client_id",
        "google_client_secret",
        "base_url",
        "database_url",
        "host",
        mode="before",
    )
    @classmethod
    def _strip_string_fields(cls, value):
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("base_url", mode="before")
    @classmethod
    def _default_base_url_when_empty(cls, value):
        if isinstance(value, str):
            value = value.strip()
            return value or _DEFAULT_BASE_URL
        return _DEFAULT_BASE_URL if value in {None, ""} else value

    @field_validator("host", mode="before")
    @classmethod
    def _default_host_when_empty(cls, value):
        if isinstance(value, str):
            value = value.strip()
            return value or _DEFAULT_HOST
        return _DEFAULT_HOST if value in {None, ""} else value

    @field_validator("data_root", mode="after")
    @classmethod
    def _normalize_data_root(cls, value: Path) -> Path:
        return value.expanduser().resolve()

    @classmethod
    def from_env(cls) -> "McpRemoteSettings":
        try:
            return cls()
        except ValidationError as exc:
            logger.error("McpRemoteSettings validation failed: %s", exc)
            fields = cls._summarize_validation_errors(exc)
            raise ValueError(f"Missing or invalid config: {', '.join(fields)}") from exc

    @classmethod
    def _summarize_validation_errors(cls, exc: ValidationError) -> list[str]:
        fields: list[str] = []
        for error in exc.errors():
            loc = error.get("loc", ())
            if not loc:
                continue
            label = str(loc[0])
            if label not in fields:
                fields.append(label)
        return fields or ["unknown"]
