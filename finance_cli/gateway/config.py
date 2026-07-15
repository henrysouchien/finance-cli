"""Configuration for the finance gateway service.

All secrets are loaded from environment variables at startup.
See ``docs/operations/SECRET_ROTATION.md`` for the rotation runbook.
"""

from __future__ import annotations

import logging
import os
import secrets
from pathlib import Path
from typing import Annotated, ClassVar, Literal

from pydantic import Field, ValidationError, field_validator, model_validator
from pydantic_settings import NoDecode
from finance_cli.config import PACKAGE_TEMPLATE_DIR
from finance_cli.gateway.user_keys import GATEWAY_USER_KEYS_ENV, load_gateway_user_key_set
from finance_cli.settings_base import (
    FinanceBaseSettings,
    parse_string_list,
    validate_credentialed_cors_origins,
)

logger = logging.getLogger(__name__)

_UNSUPPORTED_GATEWAY_API_KEY_ENV = "GATEWAY_API_KEY"
_UNSUPPORTED_FINANCE_GATEWAY_API_KEY_ENV = "FINANCE_GATEWAY_API_KEY"
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_DATA_ROOT = _REPO_ROOT / "finance-web" / "data" / "users"
_DEFAULT_TEMPLATE_RULES_PATH = PACKAGE_TEMPLATE_DIR / "rules_template.yaml"
_DEFAULT_MODEL = "claude-sonnet-4-6"
_DEFAULT_CODE_EXEC_DOCKER_IMAGE = "finance-cli-code-exec:latest"
_POSITIVE_FIELD_LABELS = {
    "resolver_timeout_seconds": "resolver timeout",
    "client_timeout": "client timeout",
    "interceptor_rate_limit_rpm": "rate limit RPM",
    "interceptor_max_input_bytes": "max input bytes",
    "web_max_budget_usd": "web max budget",
    "web_compaction_trigger": "web compaction trigger",
    "telegram_per_turn_timeout": "Telegram per-turn timeout",
}


class GatewaySettings(FinanceBaseSettings):
    config_file_env_vars: ClassVar[tuple[str, ...]] = (
        "FINANCE_CONFIG_FILE",
        "FINANCE_GATEWAY_CONFIG_FILE",
    )
    config_file_section: ClassVar[str | None] = "gateway"

    gateway_user_keys: str = Field("", validation_alias=GATEWAY_USER_KEYS_ENV)
    unsupported_gateway_api_key: str = Field("", validation_alias=_UNSUPPORTED_GATEWAY_API_KEY_ENV)
    unsupported_finance_gateway_api_key: str = Field(
        "",
        validation_alias=_UNSUPPORTED_FINANCE_GATEWAY_API_KEY_ENV,
    )
    anthropic_auth_token: str = Field("", validation_alias="ANTHROPIC_AUTH_TOKEN")
    host: str = Field("127.0.0.1", validation_alias="FINANCE_GATEWAY_HOST")
    port: int = Field(8002, ge=1, le=65535, validation_alias="FINANCE_GATEWAY_PORT")
    jwt_secret: str = Field("", validation_alias="FINANCE_GATEWAY_JWT_SECRET")
    env: Literal["development", "test", "staging", "production"] = Field(
        "development",
        validation_alias="FINANCE_GATEWAY_ENV",
    )
    model: str = Field(_DEFAULT_MODEL, validation_alias="FINANCE_GATEWAY_MODEL")
    data_root: Path = Field(_DEFAULT_DATA_ROOT, validation_alias="FINANCE_GATEWAY_DATA_ROOT")
    template_rules_path: Path = Field(
        _DEFAULT_TEMPLATE_RULES_PATH,
        validation_alias="FINANCE_GATEWAY_RULES_TEMPLATE",
    )
    cors_origins: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["http://localhost:3000", "http://localhost:5173"],
        validation_alias="FINANCE_GATEWAY_CORS_ORIGINS",
    )
    per_turn_timeout: int = Field(120, gt=0, validation_alias="FINANCE_GATEWAY_PER_TURN_TIMEOUT")
    telegram_per_turn_timeout: int = Field(
        360,
        gt=0,
        validation_alias="FINANCE_GATEWAY_TELEGRAM_PER_TURN_TIMEOUT",
    )
    session_ttl: int = Field(3600, gt=0, validation_alias="FINANCE_GATEWAY_SESSION_TTL")
    max_turns: int = Field(15, gt=0, validation_alias="FINANCE_GATEWAY_MAX_TURNS")
    max_tokens: int = Field(16000, gt=0, validation_alias="FINANCE_GATEWAY_MAX_TOKENS")
    thinking: bool = Field(True, validation_alias="FINANCE_GATEWAY_THINKING")
    allowed_models: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        validation_alias="FINANCE_GATEWAY_ALLOWED_MODELS",
    )
    code_execution_enabled: bool = Field(True, validation_alias="FINANCE_GATEWAY_CODE_EXECUTION")
    code_exec_docker_image: str = Field(
        _DEFAULT_CODE_EXEC_DOCKER_IMAGE,
        validation_alias="CODE_EXECUTE_DOCKER_IMAGE",
    )
    database_url: str = Field("", validation_alias="DATABASE_URL")
    session_secret: str = Field("", validation_alias="SESSION_SECRET")
    resolver_timeout_seconds: float = Field(
        5.0,
        gt=0,
        validation_alias="RESOLVER_TIMEOUT_SECONDS",
    )
    mcp_config_path: str = Field("", validation_alias="FINANCE_GATEWAY_MCP_CONFIG")
    client_timeout: float = Field(
        300.0,
        gt=0,
        validation_alias="FINANCE_GATEWAY_CLIENT_TIMEOUT",
    )
    interceptor_rate_limit_rpm: int = Field(
        120,
        gt=0,
        validation_alias="FINANCE_GATEWAY_RATE_LIMIT_RPM",
    )
    interceptor_max_input_bytes: int = Field(
        100_000,
        gt=0,
        validation_alias="FINANCE_GATEWAY_MAX_INPUT_BYTES",
    )
    web_max_budget_usd: float = Field(
        8.0,
        gt=0,
        validation_alias="FINANCE_GATEWAY_WEB_MAX_BUDGET_USD",
    )
    web_compaction_trigger: int = Field(
        150_000,
        gt=0,
        validation_alias="FINANCE_GATEWAY_WEB_COMPACTION_TRIGGER",
    )

    @field_validator(
        "gateway_user_keys",
        "unsupported_gateway_api_key",
        "unsupported_finance_gateway_api_key",
        "anthropic_auth_token",
        "host",
        "jwt_secret",
        "model",
        "code_exec_docker_image",
        "database_url",
        "session_secret",
        "mcp_config_path",
        mode="before",
    )
    @classmethod
    def _strip_string_fields(cls, value):
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("cors_origins", "allowed_models", mode="before")
    @classmethod
    def _parse_string_lists(cls, value):
        return parse_string_list(value)

    @field_validator("cors_origins")
    @classmethod
    def _validate_cors_origins(cls, value: list[str]) -> list[str]:
        return validate_credentialed_cors_origins(
            value,
            setting_name="FINANCE_GATEWAY_CORS_ORIGINS",
        )

    @field_validator("thinking", "code_execution_enabled", mode="before")
    @classmethod
    def _strip_bool_strings(cls, value):
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("env", mode="before")
    @classmethod
    def _normalize_env(cls, value):
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator(*_POSITIVE_FIELD_LABELS, mode="before")
    @classmethod
    def _check_positive_hardening_values(cls, value, info):
        if value is None:
            return value
        candidate = value.strip() if isinstance(value, str) else value
        try:
            numeric = float(candidate)
        except (TypeError, ValueError):
            return candidate
        if numeric <= 0:
            label = _POSITIVE_FIELD_LABELS[info.field_name]
            raise ValueError(f"{label} must be positive, got {candidate}")
        return candidate

    @field_validator("model")
    @classmethod
    def _default_model_when_empty(cls, value: str) -> str:
        return value or _DEFAULT_MODEL

    @field_validator("code_exec_docker_image")
    @classmethod
    def _default_docker_image_when_empty(cls, value: str) -> str:
        return value or _DEFAULT_CODE_EXEC_DOCKER_IMAGE

    @field_validator("data_root", "template_rules_path", mode="after")
    @classmethod
    def _normalize_paths(cls, value: Path) -> Path:
        return value.expanduser().resolve()

    @model_validator(mode="after")
    def _require_gateway_user_keys(self) -> "GatewaySettings":
        if self.unsupported_gateway_api_key or self.unsupported_finance_gateway_api_key:
            raise ValueError(
                "GATEWAY_API_KEY and FINANCE_GATEWAY_API_KEY are no longer supported; "
                f"use {GATEWAY_USER_KEYS_ENV}"
            )
        load_gateway_user_key_set(self.gateway_user_keys)
        return self

    @model_validator(mode="after")
    def _ensure_jwt_secret(self) -> "GatewaySettings":
        if self.jwt_secret:
            return self
        if self.env == "production":
            raise ValueError(
                "FINANCE_GATEWAY_JWT_SECRET is required in production. "
                "Generate one with: openssl rand -hex 32"
            )
        object.__setattr__(self, "jwt_secret", secrets.token_hex(32))
        logger.warning(
            "FINANCE_GATEWAY_JWT_SECRET not set — auto-generated ephemeral secret "
            "(sessions will not survive restart)"
        )
        return self

    @model_validator(mode="after")
    def _check_auth_sources(self) -> "GatewaySettings":
        if self.database_url and not self.session_secret:
            raise ValueError("SESSION_SECRET is required when DATABASE_URL is set")
        if not self.anthropic_auth_token and not self.database_url:
            raise ValueError(
                "At least one credential source is required: ANTHROPIC_AUTH_TOKEN or DATABASE_URL"
            )
        return self

    @classmethod
    def from_env(cls) -> "GatewaySettings":
        if (
            os.environ.get(_UNSUPPORTED_GATEWAY_API_KEY_ENV, "").strip()
            or os.environ.get(_UNSUPPORTED_FINANCE_GATEWAY_API_KEY_ENV, "").strip()
        ):
            raise ValueError("Missing or invalid config: GATEWAY_API_KEY")
        try:
            return cls()
        except ValidationError as exc:
            logger.error("GatewaySettings validation failed: %s", exc)
            fields = cls._summarize_validation_errors(exc)
            raise ValueError(f"Missing or invalid config: {', '.join(fields)}") from exc

    @classmethod
    def _summarize_validation_errors(cls, exc: ValidationError) -> list[str]:
        fields: list[str] = []

        def add(label: str | None) -> None:
            if label and label not in fields:
                fields.append(label)

        for error in exc.errors():
            loc = error.get("loc", ())
            if loc:
                add(str(loc[0]))
                continue
            add(cls._model_error_label(error.get("msg", "")))

        for label in cls._env_validation_labels():
            add(label)

        return fields or ["unknown"]

    @staticmethod
    def _model_error_label(message: str) -> str | None:
        if "GATEWAY_API_KEY and FINANCE_GATEWAY_API_KEY are no longer supported" in message:
            return "GATEWAY_API_KEY"
        if "GATEWAY_USER_KEYS" in message:
            return "GATEWAY_USER_KEYS"
        if "FINANCE_GATEWAY_CORS_ORIGINS" in message:
            return "FINANCE_GATEWAY_CORS_ORIGINS"
        if "ANTHROPIC_AUTH_TOKEN or DATABASE_URL" in message:
            return "ANTHROPIC_AUTH_TOKEN or DATABASE_URL"
        if "SESSION_SECRET is required when DATABASE_URL is set" in message:
            return "SESSION_SECRET"
        if "FINANCE_GATEWAY_JWT_SECRET is required in production" in message:
            return "FINANCE_GATEWAY_JWT_SECRET is required in production"
        return None

    @staticmethod
    def _env_validation_labels() -> list[str]:
        labels: list[str] = []
        gateway_user_keys = os.environ.get(GATEWAY_USER_KEYS_ENV, "").strip()
        unsupported_gateway_api_key = os.environ.get(_UNSUPPORTED_GATEWAY_API_KEY_ENV, "").strip()
        unsupported_finance_gateway_api_key = os.environ.get(
            _UNSUPPORTED_FINANCE_GATEWAY_API_KEY_ENV,
            "",
        ).strip()
        auth_token = os.environ.get("ANTHROPIC_AUTH_TOKEN", "").strip()
        database_url = os.environ.get("DATABASE_URL", "").strip()
        session_secret = os.environ.get("SESSION_SECRET", "").strip()
        env = os.environ.get("FINANCE_GATEWAY_ENV", "development").strip().lower()
        jwt_secret = os.environ.get("FINANCE_GATEWAY_JWT_SECRET", "").strip()

        if unsupported_gateway_api_key or unsupported_finance_gateway_api_key:
            labels.append("GATEWAY_API_KEY")
        elif not gateway_user_keys:
            labels.append("GATEWAY_USER_KEYS")
        if not auth_token and not database_url:
            labels.append("ANTHROPIC_AUTH_TOKEN or DATABASE_URL")
        if database_url and not session_secret:
            labels.append("SESSION_SECRET")
        if env == "production" and not jwt_secret:
            labels.append("FINANCE_GATEWAY_JWT_SECRET is required in production")

        return labels


def load_settings() -> GatewaySettings:
    return GatewaySettings.from_env()
