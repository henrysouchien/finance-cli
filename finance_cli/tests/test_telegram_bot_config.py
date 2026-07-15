from __future__ import annotations

import pytest
from pydantic import ValidationError

from finance_cli.telegram_bot.config import TelegramBotSettings, load_config

_ENV_KEYS = [
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "ANTHROPIC_AUTH_TOKEN",
    "TELEGRAM_GATEWAY_URL",
    "GATEWAY_USER_KEY",
    "GATEWAY_API_KEY",
    "FINANCE_GATEWAY_API_KEY",
    "TELEGRAM_BOT_MODEL",
    "TELEGRAM_BOT_MAX_TURNS",
    "TELEGRAM_BOT_MAX_TOKENS",
    "TELEGRAM_BOT_THINKING",
    "TELEGRAM_BOT_HISTORY_MAX_TURNS",
    "TELEGRAM_BOT_SESSION_IDLE_TIMEOUT",
    "TELEGRAM_BOT_POLL_TIMEOUT",
    "TELEGRAM_BOT_APPROVAL_TIMEOUT",
]


def _clear_telegram_bot_env(monkeypatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_load_config_reports_missing_telegram_token(monkeypatch) -> None:
    _clear_telegram_bot_env(monkeypatch)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setenv("GATEWAY_USER_KEY", "gateway-user-key")

    with pytest.raises(ValueError, match=r"Missing or invalid config: TELEGRAM_BOT_TOKEN$"):
        load_config()


def test_load_config_reports_missing_telegram_chat_id(monkeypatch) -> None:
    _clear_telegram_bot_env(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("GATEWAY_USER_KEY", "gateway-user-key")

    with pytest.raises(ValueError, match=r"Missing or invalid config: TELEGRAM_CHAT_ID$"):
        load_config()


def test_load_config_reports_missing_gateway_user_key(monkeypatch) -> None:
    _clear_telegram_bot_env(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")

    with pytest.raises(ValueError, match=r"Missing or invalid config: GATEWAY_USER_KEY$"):
        load_config()


def test_load_config_rejects_unsupported_finance_gateway_api_key(monkeypatch) -> None:
    _clear_telegram_bot_env(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setenv("FINANCE_GATEWAY_API_KEY", "gateway-key")

    with pytest.raises(ValueError, match=r"Missing or invalid config: GATEWAY_API_KEY$"):
        load_config()


def test_load_config_rejects_unsupported_gateway_api_key_even_with_user_key(monkeypatch) -> None:
    _clear_telegram_bot_env(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setenv("GATEWAY_USER_KEY", "gateway-user-key")
    monkeypatch.setenv("GATEWAY_API_KEY", "legacy-key")

    with pytest.raises(ValueError, match=r"Missing or invalid config: GATEWAY_API_KEY$"):
        load_config()


def test_load_config_reports_negative_max_turns(monkeypatch) -> None:
    _clear_telegram_bot_env(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setenv("GATEWAY_USER_KEY", "gateway-user-key")
    monkeypatch.setenv("TELEGRAM_BOT_MAX_TURNS", "-5")

    with pytest.raises(ValueError, match=r"Missing or invalid config: TELEGRAM_BOT_MAX_TURNS$"):
        load_config()


def test_load_config_reports_non_numeric_poll_timeout(monkeypatch) -> None:
    _clear_telegram_bot_env(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setenv("GATEWAY_USER_KEY", "gateway-user-key")
    monkeypatch.setenv("TELEGRAM_BOT_POLL_TIMEOUT", "not-a-number")

    with pytest.raises(
        ValueError,
        match=r"Missing or invalid config: TELEGRAM_BOT_POLL_TIMEOUT$",
    ):
        load_config()


def test_load_config_summarizes_multiple_missing_fields_on_one_line(monkeypatch) -> None:
    _clear_telegram_bot_env(monkeypatch)
    monkeypatch.setenv("GATEWAY_USER_KEY", "gateway-user-key")

    with pytest.raises(ValueError) as exc_info:
        load_config()

    assert (
        str(exc_info.value)
        == "Missing or invalid config: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID"
    )
    assert "\n" not in str(exc_info.value)


def test_telegram_bot_settings_is_frozen() -> None:
    settings = TelegramBotSettings(
        **{
            "TELEGRAM_BOT_TOKEN": "bot-token",
            "TELEGRAM_CHAT_ID": "12345",
            "GATEWAY_USER_KEY": "gateway-user-key",
        }
    )

    with pytest.raises(ValidationError, match="frozen"):
        settings.max_turns = 10
