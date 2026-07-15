from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import crypto_envelope, secrets_backend
from finance_cli.plaid_client import delete_secret, get_secret_payload, store_plaid_token
from finance_cli.secrets_store import store_user_api_key
from finance_cli.stripe_client import _resolve_stripe_api_key


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_PROVIDERS_ENABLED", "1")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path))
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="p3a")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    secrets_backend._client = None


def _stripe_conn(ref: str, *, user_id: str = "1") -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE tenant_marker (singleton INTEGER PRIMARY KEY, user_id TEXT)")
    conn.execute("INSERT INTO tenant_marker (singleton, user_id) VALUES (1, ?)", (user_id,))
    conn.execute(
        """
        CREATE TABLE stripe_connections (
            id TEXT PRIMARY KEY,
            api_key_ref TEXT,
            status TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO stripe_connections (id, api_key_ref, status) VALUES ('default', ?, 'active')",
        (ref,),
    )
    conn.commit()
    return conn


@mock_aws
def test_plaid_reader_writer_and_delete_are_dual_format(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, tmp_path)
    legacy_ref = "plaid/access_token/1/item_legacy"
    secrets_backend.put_secret(
        legacy_ref,
        json.dumps({"access_token": "access-legacy", "item_id": "item_legacy"}),
    )
    vault_ref = store_plaid_token(
        user_id="1",
        institution="Bank",
        access_token="access-vault",
        item_id="item_vault",
    )

    assert vault_ref == "vault://1/plaid/items/item_vault/access_token"
    assert get_secret_payload(legacy_ref, user_id="1")["access_token"] == "access-legacy"
    assert get_secret_payload(vault_ref, user_id="1") == {
        "access_token": "access-vault",
        "item_id": "item_vault",
    }

    delete_secret(vault_ref)
    with pytest.raises(Exception):
        crypto_envelope.get_provider_secret("1", vault_ref)


@mock_aws
def test_stripe_reader_accepts_legacy_name_arn_and_vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, tmp_path)
    name_ref = "finance-cli-test/users/1/stripe-api-key"
    secrets_backend.put_secret(name_ref, "sk-name")
    conn = _stripe_conn(name_ref)

    api_key, migrated_ref = _resolve_stripe_api_key(conn, user_id="1")
    assert api_key == "sk-name"
    assert migrated_ref == "vault://1/stripe/secret_key"
    assert conn.execute("SELECT api_key_ref FROM stripe_connections").fetchone()[0] == migrated_ref

    vault_conn = _stripe_conn(migrated_ref)
    api_key, ref = _resolve_stripe_api_key(vault_conn, user_id="1")
    assert (api_key, ref) == ("sk-name", migrated_ref)

    arn_name_ref = "finance-cli-test/users/2/stripe-api-key"
    secrets_backend.put_secret(arn_name_ref, "sk-arn")
    arn_ref = secrets_backend._get_client().describe_secret(SecretId=arn_name_ref)["ARN"]
    arn_conn = _stripe_conn(arn_ref, user_id="2")
    api_key, migrated_arn_ref = _resolve_stripe_api_key(arn_conn, user_id="2")
    assert api_key == "sk-arn"
    assert migrated_arn_ref == "vault://2/stripe/secret_key"


@mock_aws
def test_anthropic_store_returns_vault_ref_and_reads_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, tmp_path)

    ref = store_user_api_key("1", "anthropic", "sk-ant-p3a")

    assert ref == "vault://1/anthropic/api_key"
    assert crypto_envelope.get_provider_secret("1", ref) == "sk-ant-p3a"
