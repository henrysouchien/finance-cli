from __future__ import annotations

import logging

import pytest

from scripts import migrate_api_keys_to_secrets_manager as legacy_migration
from scripts import migrate_api_keys_to_vault as migration


def test_legacy_script_delegates_to_vault_migration() -> None:
    assert legacy_migration.main is migration.main


def test_expected_refs_are_vault_uris() -> None:
    assert migration._expected_vault_ref("u1", "anthropic") == "vault://u1/anthropic/api_key"
    assert migration._expected_vault_ref("u1", "stripe") == "vault://u1/stripe/secret_key"


def test_main_refuses_secrets_manager_fallback(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setenv("FINANCE_CLI_PROVIDER_SM_FALLBACK", "1")

    def _fail_from_env():
        pytest.fail("Settings.from_env should not run when provider SM fallback is enabled")

    monkeypatch.setattr(migration.Settings, "from_env", staticmethod(_fail_from_env))

    with caplog.at_level(logging.ERROR):
        assert migration.main(["--dry-run"]) == 2

    assert "FINANCE_CLI_PROVIDER_SM_FALLBACK" in caplog.text


def test_store_user_api_key_in_vault_writes_expected_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, str]] = []

    def _set_provider_secret(user_id: str, ref: str, key: str) -> str:
        calls.append((user_id, ref, key))
        return ref

    monkeypatch.setattr(migration.crypto_envelope, "set_provider_secret", _set_provider_secret)

    ref = migration._store_user_api_key_in_vault("u1", "anthropic", "sk-ant-test")

    assert ref == "vault://u1/anthropic/api_key"
    assert calls == [("u1", "vault://u1/anthropic/api_key", "sk-ant-test")]


def test_store_user_api_key_in_vault_rejects_non_vault_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        migration.crypto_envelope,
        "set_provider_secret",
        lambda _user_id, _ref, _key: "finance-cli/users/u1/anthropic-api-key",
    )

    with pytest.raises(RuntimeError, match="non-vault ref"):
        migration._store_user_api_key_in_vault("u1", "anthropic", "sk-ant-test")
