"""Configuration for the Telegram bot."""

from __future__ import annotations

import logging
import os

from pydantic import Field, ValidationError, field_validator, model_validator
from finance_cli.settings_base import FinanceBaseSettings

logger = logging.getLogger(__name__)

_GATEWAY_USER_KEY_ENV = "GATEWAY_USER_KEY"
_UNSUPPORTED_GATEWAY_API_KEY_ENV = "GATEWAY_API_KEY"
_UNSUPPORTED_FINANCE_GATEWAY_API_KEY_ENV = "FINANCE_GATEWAY_API_KEY"
_DEFAULT_GATEWAY_URL = "http://127.0.0.1:8002"
_DEFAULT_MODEL = "claude-sonnet-4-6"


class TelegramBotSettings(FinanceBaseSettings):
    telegram_token: str = Field(
        ...,
        min_length=1,
        validation_alias="TELEGRAM_BOT_TOKEN",
    )
    telegram_chat_id: str = Field(
        ...,
        min_length=1,
        validation_alias="TELEGRAM_CHAT_ID",
    )
    anthropic_auth_token: str = Field("", validation_alias="ANTHROPIC_AUTH_TOKEN")
    gateway_url: str = Field(_DEFAULT_GATEWAY_URL, validation_alias="TELEGRAM_GATEWAY_URL")
    gateway_user_key: str = Field("", validation_alias=_GATEWAY_USER_KEY_ENV)
    unsupported_gateway_api_key: str = Field("", validation_alias=_UNSUPPORTED_GATEWAY_API_KEY_ENV)
    unsupported_finance_gateway_api_key: str = Field(
        "",
        validation_alias=_UNSUPPORTED_FINANCE_GATEWAY_API_KEY_ENV,
    )
    model: str = Field(_DEFAULT_MODEL, validation_alias="TELEGRAM_BOT_MODEL")
    max_turns: int = Field(15, gt=0, validation_alias="TELEGRAM_BOT_MAX_TURNS")
    max_tokens: int = Field(16000, gt=0, validation_alias="TELEGRAM_BOT_MAX_TOKENS")
    thinking: bool = Field(True, validation_alias="TELEGRAM_BOT_THINKING")
    history_max_turns: int = Field(
        20,
        gt=0,
        validation_alias="TELEGRAM_BOT_HISTORY_MAX_TURNS",
    )
    session_idle_timeout: int = Field(
        1800,
        gt=0,
        validation_alias="TELEGRAM_BOT_SESSION_IDLE_TIMEOUT",
    )
    poll_timeout: int = Field(30, gt=0, validation_alias="TELEGRAM_BOT_POLL_TIMEOUT")
    approval_timeout: int = Field(300, gt=0, validation_alias="TELEGRAM_BOT_APPROVAL_TIMEOUT")

    @field_validator(
        "telegram_token",
        "telegram_chat_id",
        "anthropic_auth_token",
        "gateway_url",
        "gateway_user_key",
        "unsupported_gateway_api_key",
        "unsupported_finance_gateway_api_key",
        "model",
        mode="before",
    )
    @classmethod
    def _strip_string_fields(cls, value):
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("gateway_url", mode="before")
    @classmethod
    def _default_gateway_url_when_empty(cls, value):
        if isinstance(value, str):
            value = value.strip()
            return value or _DEFAULT_GATEWAY_URL
        return _DEFAULT_GATEWAY_URL if value in {None, ""} else value

    @field_validator("model", mode="before")
    @classmethod
    def _default_model_when_empty(cls, value):
        if isinstance(value, str):
            value = value.strip()
            return value or _DEFAULT_MODEL
        return _DEFAULT_MODEL if value in {None, ""} else value

    @field_validator("thinking", mode="before")
    @classmethod
    def _parse_thinking(cls, value):
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no"}
        return value

    @model_validator(mode="after")
    def _require_gateway_user_key(self) -> "TelegramBotSettings":
        if self.unsupported_gateway_api_key or self.unsupported_finance_gateway_api_key:
            raise ValueError(
                "GATEWAY_API_KEY and FINANCE_GATEWAY_API_KEY are no longer supported; "
                f"use {_GATEWAY_USER_KEY_ENV}"
            )
        canonical = self.gateway_user_key.strip()
        if not canonical:
            raise ValueError(f"{_GATEWAY_USER_KEY_ENV} is required")
        return self

    @classmethod
    def from_env(cls) -> "TelegramBotSettings":
        if (
            os.environ.get(_UNSUPPORTED_GATEWAY_API_KEY_ENV, "").strip()
            or os.environ.get(_UNSUPPORTED_FINANCE_GATEWAY_API_KEY_ENV, "").strip()
        ):
            raise ValueError("Missing or invalid config: GATEWAY_API_KEY")
        try:
            return cls()
        except ValidationError as exc:
            logger.error("TelegramBotSettings validation failed: %s", exc)
            fields = cls._summarize_validation_errors(exc)
            raise ValueError(f"Missing or invalid config: {', '.join(fields)}") from exc

    @classmethod
    def _summarize_validation_errors(cls, exc: ValidationError) -> list[str]:
        fields: list[str] = []
        for error in exc.errors():
            loc = error.get("loc", ())
            label = str(loc[0]) if loc else cls._model_error_label(error.get("msg", ""))
            if label not in fields:
                fields.append(label)
        return fields or ["unknown"]

    @staticmethod
    def _model_error_label(message: str) -> str:
        if "GATEWAY_API_KEY and FINANCE_GATEWAY_API_KEY are no longer supported" in message:
            return "GATEWAY_API_KEY"
        if "GATEWAY_USER_KEY" in message:
            return "GATEWAY_USER_KEY"
        return "unknown"


BotConfig = TelegramBotSettings


def load_config() -> TelegramBotSettings:
    return TelegramBotSettings.from_env()
