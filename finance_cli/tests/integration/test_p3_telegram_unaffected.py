from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from server.routers import telegram_router


def test_telegram_token_uses_provider_vault_ref(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tokens: dict[str, str] = {}

    def store_bot_token(user_id: str, token: str, *, data_root=None) -> str:
        del data_root
        tokens[str(user_id)] = token
        return f"vault://{user_id}/telegram/bot_token"

    def resolve_bot_token(user_id: str, ref: str, *, data_root=None, missing_ok: bool = False) -> str | None:
        del ref, data_root
        token = tokens.get(str(user_id))
        if token is None and not missing_ok:
            raise KeyError(user_id)
        return token

    monkeypatch.setattr(telegram_router.telegram_secrets, "store_bot_token", store_bot_token)
    monkeypatch.setattr(telegram_router.telegram_secrets, "resolve_bot_token", resolve_bot_token)
    data_root = tmp_path / "users"
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))
    user_dir = data_root / "1"
    user_dir.mkdir(parents=True)
    payload = {
        "bot_token": "bot-token",
        "user_id": "1",
        "bot_username": "cashnerd_bot",
        "webhook_secret": "webhook-secret",
    }

    ref = telegram_router._store_token(user_dir, payload)
    assert ref == "vault://1/telegram/bot_token"
    assert not (user_dir / "telegram_token.json").exists()
    assert tokens["1"] == "bot-token"

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE telegram_config (id INTEGER PRIMARY KEY, bot_token_ref TEXT, bot_username TEXT, webhook_secret TEXT)"
    )
    conn.execute("INSERT INTO telegram_config (id, bot_token_ref) VALUES (1, ?)", (ref,))
    assert telegram_router._load_token(user_dir, conn=conn)["bot_token"] == "bot-token"
    assert conn.execute("SELECT bot_token_ref FROM telegram_config WHERE id = 1").fetchone()[0] == ref
