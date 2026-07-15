from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import crypto_envelope, secrets_backend
from scripts import migrate_revocation_queue_refs


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_PROVIDERS_ENABLED", "1")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path))
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="p3 queue")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    secrets_backend._client = None


def _queue_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE plaid_revocation_queue (
            id INTEGER PRIMARY KEY,
            user_id TEXT,
            plaid_item_id TEXT,
            secret_refs TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE webhook_migration_queue (
            id INTEGER PRIMARY KEY,
            user_id TEXT,
            source TEXT,
            payload TEXT
        )
        """
    )
    return conn


@mock_aws
def test_revocation_queue_refs_rewrite_to_vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, tmp_path)
    conn = _queue_conn()
    queue_ref = "plaid/access_token/1/item_queue"
    webhook_ref = "plaid/access_token/1/item_webhook"
    secrets_backend.put_secret(
        queue_ref,
        json.dumps({"access_token": "access-queue", "item_id": "item_queue"}),
    )
    secrets_backend.put_secret(
        webhook_ref,
        json.dumps({"access_token": "access-webhook", "item_id": "item_webhook"}),
    )
    conn.execute(
        """
        INSERT INTO plaid_revocation_queue (id, user_id, plaid_item_id, secret_refs)
        VALUES (1, '1', 'item_queue', ?)
        """,
        (json.dumps([queue_ref]),),
    )
    conn.execute(
        """
        INSERT INTO webhook_migration_queue (id, user_id, source, payload)
        VALUES (1, '1', 'plaid', ?)
        """,
        (json.dumps({"plaid_item_id": "item_webhook", "secret_refs": [webhook_ref]}),),
    )
    conn.commit()

    summary = migrate_revocation_queue_refs.migrate_connection(conn)

    assert summary["plaid_revocation_queue"]["refs_rewritten"] == 1
    assert summary["webhook_migration_queue"]["refs_rewritten"] == 1
    queue_refs = json.loads(conn.execute("SELECT secret_refs FROM plaid_revocation_queue").fetchone()[0])
    payload = json.loads(conn.execute("SELECT payload FROM webhook_migration_queue").fetchone()[0])
    assert queue_refs == ["vault://1/plaid/items/item_queue/access_token"]
    assert payload["secret_refs"] == ["vault://1/plaid/items/item_webhook/access_token"]
    assert crypto_envelope.get_provider_secret("1", queue_refs[0]) == "access-queue"
    assert crypto_envelope.get_provider_secret("1", payload["secret_refs"][0]) == "access-webhook"
    assert secrets_backend._get_client().describe_secret(SecretId=queue_ref)["DeletedDate"] is not None
    assert secrets_backend._get_client().describe_secret(SecretId=webhook_ref)["DeletedDate"] is not None
