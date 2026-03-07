"""Configuration for the Telegram bot."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BotConfig:
    telegram_token: str
    telegram_chat_id: str
    anthropic_api_key: str
    model: str = "claude-sonnet-4-6"
    max_turns: int = 15
    max_tokens: int = 16000
    thinking: bool = True
    history_max_turns: int = 20
    poll_timeout: int = 30


def load_config() -> BotConfig:
    """Load Telegram bot configuration from environment variables."""
    env = os.environ
    required = {
        "TELEGRAM_BOT_TOKEN": env.get("TELEGRAM_BOT_TOKEN", "").strip(),
        "TELEGRAM_CHAT_ID": env.get("TELEGRAM_CHAT_ID", "").strip(),
        "ANTHROPIC_API_KEY": env.get("ANTHROPIC_API_KEY", "").strip(),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return BotConfig(
        telegram_token=required["TELEGRAM_BOT_TOKEN"],
        telegram_chat_id=required["TELEGRAM_CHAT_ID"],
        anthropic_api_key=required["ANTHROPIC_API_KEY"],
        model=env.get("TELEGRAM_BOT_MODEL", "claude-sonnet-4-6").strip() or "claude-sonnet-4-6",
        max_turns=int(env.get("TELEGRAM_BOT_MAX_TURNS", "15")),
        max_tokens=int(env.get("TELEGRAM_BOT_MAX_TOKENS", "16000")),
        thinking=env.get("TELEGRAM_BOT_THINKING", "true").strip().lower() not in {"0", "false", "no"},
        history_max_turns=int(env.get("TELEGRAM_BOT_HISTORY_MAX_TURNS", "20")),
        poll_timeout=int(env.get("TELEGRAM_BOT_POLL_TIMEOUT", "30")),
    )
