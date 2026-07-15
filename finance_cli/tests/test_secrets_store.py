from __future__ import annotations

import pytest
import boto3
from moto import mock_aws

from finance_cli import crypto_envelope, secrets_backend, secrets_store
from finance_cli.exceptions import InvalidCiphertextError


@pytest.fixture(autouse=True)
def _reset_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    secrets_backend._client = None
    crypto_envelope.evict_caches()
    yield
    secrets_backend._client = None
    crypto_envelope.evict_caches()


def _configure_provider_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="providers")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    monkeypatch.setenv("FINANCE_CLI_PROVIDERS_ENABLED", "1")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path))


@mock_aws
def test_store_get_delete_round_trip(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_provider_env(monkeypatch, tmp_path)
    secret_ref = secrets_store.store_user_api_key("u1", "anthropic", "sk-ant-1234")

    assert secret_ref == "vault://u1/anthropic/api_key"
    assert secrets_store.get_user_api_key("u1", "anthropic", secret_ref=secret_ref) == "sk-ant-1234"

    secrets_store.delete_user_api_key("u1", "anthropic", secret_ref=secret_ref)

    with pytest.raises(Exception):
        secrets_store.resolve_secret_ref(secret_ref, "u1")


@mock_aws
def test_get_user_api_key_returns_none_for_legacy_ref() -> None:
    assert secrets_store.get_user_api_key("u1", "stripe", secret_ref="STRIPE_API_KEY") is None
    assert secrets_store.get_user_api_key("u1", "stripe", secret_ref=None) is None


@mock_aws
def test_resolve_secret_ref_dispatches_vault_name_and_arn(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_provider_env(monkeypatch, tmp_path)
    vault_ref = crypto_envelope.format_vault_ref("u1", "stripe", "secret_key")
    crypto_envelope.set_provider_secret("u1", vault_ref, "sk-vault")
    secrets_backend.put_secret("finance-cli-test/users/u1/stripe-api-key", "sk-name")
    arn = secrets_backend._get_client().describe_secret(
        SecretId="finance-cli-test/users/u1/stripe-api-key"
    )["ARN"]

    assert secrets_store.resolve_secret_ref(vault_ref, "u1") == "sk-vault"
    assert secrets_store.resolve_secret_ref("finance-cli-test/users/u1/stripe-api-key", "u1") == "sk-name"
    assert secrets_store.resolve_secret_ref(arn, "u1") == "sk-name"

    with pytest.raises(ValueError):
        secrets_store.resolve_secret_ref("STRIPE_API_KEY", "u1")


@mock_aws
def test_migrate_provider_ref_to_vault_round_trip_and_idempotent(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_provider_env(monkeypatch, tmp_path)
    old_ref = "finance-cli-test/users/u1/stripe-api-key"
    secrets_backend.put_secret(old_ref, "sk-legacy")

    new_ref = secrets_store.migrate_provider_ref_to_vault(
        "u1",
        provider="stripe",
        path=("secret_key",),
        plaintext="sk-legacy",
        old_sm_ref=old_ref,
    )
    assert new_ref == "vault://u1/stripe/secret_key"
    assert secrets_store.resolve_secret_ref(new_ref, "u1") == "sk-legacy"
    assert secrets_backend._get_client().describe_secret(SecretId=old_ref)["DeletedDate"] is not None

    again = secrets_store.migrate_provider_ref_to_vault(
        "u1",
        provider="stripe",
        path=("secret_key",),
        plaintext="sk-legacy",
        old_sm_ref=old_ref,
    )
    assert again == new_ref


@mock_aws
def test_migrate_provider_ref_to_vault_mismatch_raises(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_provider_env(monkeypatch, tmp_path)
    old_ref = "finance-cli-test/users/u1/stripe-api-key"
    secrets_backend.put_secret(old_ref, "sk-legacy")
    crypto_envelope.set_provider_secret("u1", "vault://u1/stripe/secret_key", "sk-existing")

    with pytest.raises(InvalidCiphertextError):
        secrets_store.migrate_provider_ref_to_vault(
            "u1",
            provider="stripe",
            path=("secret_key",),
            plaintext="sk-different",
            old_sm_ref=old_ref,
        )
