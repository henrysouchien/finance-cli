from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import crypto_envelope, secrets_backend
from finance_cli.plaid_client import _get_access_token_for_item


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_PROVIDERS_ENABLED", "1")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path))
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="p3b")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    secrets_backend._client = None


def _conn_with_plaid_item(ref: str) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE tenant_marker (singleton INTEGER PRIMARY KEY, user_id TEXT)")
    conn.execute("INSERT INTO tenant_marker (singleton, user_id) VALUES (1, '1')")
    conn.execute(
        """
        CREATE TABLE plaid_items (
            id TEXT PRIMARY KEY,
            plaid_item_id TEXT,
            institution_name TEXT,
            access_token_ref TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref)
        VALUES ('row1', 'item_legacy', 'Bank', ?)
        """,
        (ref,),
    )
    conn.commit()
    return conn


@mock_aws
def test_lazy_plaid_secret_migration_updates_db_and_soft_deletes_sm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, tmp_path)
    legacy_ref = "plaid/access_token/1/item_legacy"
    secrets_backend.put_secret(
        legacy_ref,
        json.dumps({"access_token": "access-legacy", "item_id": "item_legacy"}),
    )
    conn = _conn_with_plaid_item(legacy_ref)
    row = conn.execute("SELECT * FROM plaid_items WHERE plaid_item_id = 'item_legacy'").fetchone()

    assert _get_access_token_for_item(row, conn=conn, user_id="1") == "access-legacy"

    new_ref = conn.execute("SELECT access_token_ref FROM plaid_items").fetchone()[0]
    assert new_ref == "vault://1/plaid/items/item_legacy/access_token"
    assert crypto_envelope.get_provider_secret("1", new_ref) == "access-legacy"
    assert secrets_backend._get_client().describe_secret(SecretId=legacy_ref)["DeletedDate"] is not None

    def fail_sm(*_args, **_kwargs):
        raise AssertionError("subsequent reads must not hit Secrets Manager")

    monkeypatch.setattr(secrets_backend, "get_secret", fail_sm)
    row = conn.execute("SELECT * FROM plaid_items WHERE plaid_item_id = 'item_legacy'").fetchone()
    assert _get_access_token_for_item(row, conn=conn, user_id="1") == "access-legacy"
