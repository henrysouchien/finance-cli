from __future__ import annotations

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from agent_gateway import MissingUserIdError, NoCredentialError, ResolverResult

from finance_cli.crypto import encrypt_api_key
from finance_cli.gateway.server import _make_credentials_resolver


class FakeCursor:
    def __init__(self, row):
        self._row = row
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.executed.append((query, params))

    def fetchone(self):
        return self._row


class FakeConnection:
    def __init__(self, row) -> None:
        self.row = row
        self.cursor_factory = None
        self.cursor_obj = FakeCursor(row)
        self.committed = False

    def cursor(self, *, cursor_factory=None):
        self.cursor_factory = cursor_factory
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True


def _make_get_session(row):
    connection = FakeConnection(row)

    @contextmanager
    def _get_session():
        yield connection

    return _get_session, connection


def _init_request(user_id: object = "123", context: dict[str, Any] | None = None) -> SimpleNamespace:
    return SimpleNamespace(user_id=user_id, context={} if context is None else context)


def _make_resolver(
    row,
    monkeypatch: pytest.MonkeyPatch,
    *,
    user_key: str | None = None,
    fallback_auth_token: str = "sk-ant-oat01-fallback",
):
    get_session, connection = _make_get_session(row)
    lookup_calls: list[tuple[str, str, str | None]] = []

    def fake_get_user_api_key(
        user_id: str,
        provider: str,
        *,
        secret_ref: str | None = None,
    ) -> str | None:
        lookup_calls.append((user_id, provider, secret_ref))
        return user_key

    def unexpected_migration(*args, **kwargs) -> str:
        raise AssertionError("vault migration should not run in this test")

    monkeypatch.setattr("finance_cli.secrets_store.get_user_api_key", fake_get_user_api_key)
    monkeypatch.setattr("finance_cli.secrets_store.migrate_provider_ref_to_vault", unexpected_migration)
    resolver = _make_credentials_resolver(
        get_session_fn=get_session,
        session_secret="session-secret",
        fallback_auth_token=fallback_auth_token,
        model="claude-sonnet-4-6",
        max_tokens=16000,
        thinking=True,
    )
    return resolver, connection, lookup_calls


def _resolve(resolver, *, user_id: object = "123", context: dict[str, Any] | None = None) -> ResolverResult:
    return asyncio.run(resolver("gateway-api-key", _init_request(user_id, context)))


def _assert_result_identity(
    result: ResolverResult,
    *,
    user_id: str = "123",
    channel: str = "web",
    risk_user_id: int = 123,
) -> None:
    assert isinstance(result, ResolverResult)
    assert result.user_id == user_id
    assert result.channel == channel
    assert result.risk_user_id == risk_user_id
    assert result.role == "owner"
    assert result.user_email is None


def test_credentials_resolver_returns_byok_api_key_auth_config(monkeypatch: pytest.MonkeyPatch) -> None:
    row = {
        "anthropic_api_key_secret_ref": "vault://users/123/anthropic/api_key",
        "anthropic_api_key_enc": None,
    }
    resolver, connection, lookup_calls = _make_resolver(
        row,
        monkeypatch,
        user_key="sk-ant-api03-user-key",
    )

    result = _resolve(resolver, context={"channel": "cli"})

    _assert_result_identity(result, channel="cli")
    auth_config = result.auth_config.to_dict()
    assert auth_config["billing_mode"] == "byok"
    assert auth_config["auth_mode"] == "api"
    assert auth_config["api_key"] == "sk-ant-api03-user-key"
    assert "auth_token" not in auth_config
    assert lookup_calls == [("123", "anthropic", "vault://users/123/anthropic/api_key")]
    query, params = connection.cursor_obj.executed[0]
    assert "SELECT anthropic_api_key_secret_ref, anthropic_api_key_enc" in query
    assert params == ("123",)
    assert connection.cursor_factory is not None


def test_credentials_resolver_uses_auth_token_for_oauth_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = {
        "anthropic_api_key_secret_ref": "vault://users/123/anthropic/api_key",
        "anthropic_api_key_enc": None,
    }
    resolver, _connection, _lookup_calls = _make_resolver(
        row,
        monkeypatch,
        user_key="sk-ant-oat01-user-token",
    )

    result = _resolve(resolver, context={"channel": "telegram"})

    _assert_result_identity(result, channel="telegram")
    auth_config = result.auth_config.to_dict()
    assert auth_config["billing_mode"] == "byok"
    assert auth_config["auth_mode"] == "oauth"
    assert auth_config["auth_token"] == "sk-ant-oat01-user-token"
    assert "api_key" not in auth_config


def test_credentials_resolver_raises_on_decrypt_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    row = {"anthropic_api_key_secret_ref": None, "anthropic_api_key_enc": "not-a-valid-ciphertext"}
    resolver, _connection, _lookup_calls = _make_resolver(row, monkeypatch)

    with pytest.raises(NoCredentialError, match="could not be decrypted"):
        _resolve(resolver)


def test_credentials_resolver_falls_back_to_metered_token(monkeypatch: pytest.MonkeyPatch) -> None:
    row = {"anthropic_api_key_secret_ref": None, "anthropic_api_key_enc": None}
    resolver, _connection, _lookup_calls = _make_resolver(row, monkeypatch)

    result = _resolve(resolver)

    _assert_result_identity(result)
    auth_config = result.auth_config.to_dict()
    assert auth_config["billing_mode"] == "metered"
    assert auth_config["auth_mode"] == "oauth"
    assert auth_config["auth_token"] == "sk-ant-oat01-fallback"


def test_credentials_resolver_raises_when_no_credentials_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    row = {"anthropic_api_key_secret_ref": None, "anthropic_api_key_enc": None}
    resolver, _connection, _lookup_calls = _make_resolver(
        row,
        monkeypatch,
        fallback_auth_token="",
    )

    with pytest.raises(NoCredentialError, match="No credential available for user 123"):
        _resolve(resolver)


def test_credentials_resolver_user_not_found_falls_back_to_metered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver, _connection, _lookup_calls = _make_resolver(None, monkeypatch)

    result = _resolve(resolver, user_id="999")

    _assert_result_identity(result, user_id="999", risk_user_id=999)
    auth_config = result.auth_config.to_dict()
    assert auth_config["billing_mode"] == "metered"
    assert auth_config["auth_mode"] == "oauth"
    assert auth_config["auth_token"] == "sk-ant-oat01-fallback"


@pytest.mark.parametrize("user_id", [None, "", "  "])
def test_credentials_resolver_raises_when_user_id_missing(
    monkeypatch: pytest.MonkeyPatch,
    user_id: object,
) -> None:
    resolver, connection, _lookup_calls = _make_resolver(None, monkeypatch)

    with pytest.raises(MissingUserIdError, match="user_id is required"):
        _resolve(resolver, user_id=user_id)

    assert connection.cursor_obj.executed == []


def test_credentials_resolver_raises_when_user_id_non_numeric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver, connection, _lookup_calls = _make_resolver(None, monkeypatch)

    with pytest.raises(MissingUserIdError, match="positive integer"):
        _resolve(resolver, user_id="abc")

    assert connection.cursor_obj.executed == []


@pytest.mark.parametrize("user_id", ["0", "-1"])
def test_credentials_resolver_raises_when_user_id_non_positive(
    monkeypatch: pytest.MonkeyPatch,
    user_id: str,
) -> None:
    resolver, connection, _lookup_calls = _make_resolver(None, monkeypatch)

    with pytest.raises(MissingUserIdError, match="must be positive"):
        _resolve(resolver, user_id=user_id)

    assert connection.cursor_obj.executed == []


def test_credentials_resolver_raises_when_channel_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver, connection, _lookup_calls = _make_resolver(None, monkeypatch)

    with pytest.raises(MissingUserIdError, match="channel must be one of"):
        _resolve(resolver, context={"channel": "excel"})

    assert connection.cursor_obj.executed == []


def test_credentials_resolver_defaults_channel_to_web_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver, _connection, _lookup_calls = _make_resolver(None, monkeypatch)

    result = _resolve(resolver)

    _assert_result_identity(result, channel="web")


@pytest.mark.parametrize("channel", ["cli", "telegram"])
def test_credentials_resolver_uses_claimed_cli_and_telegram_channels(
    monkeypatch: pytest.MonkeyPatch,
    channel: str,
) -> None:
    encrypted_key = encrypt_api_key("sk-ant-api03-user-key", "session-secret")
    row = {"anthropic_api_key_secret_ref": None, "anthropic_api_key_enc": encrypted_key}
    resolver, _connection, _lookup_calls = _make_resolver(row, monkeypatch)

    result = _resolve(resolver, context={"channel": channel})

    _assert_result_identity(result, channel=channel)
