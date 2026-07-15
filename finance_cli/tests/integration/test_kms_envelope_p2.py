from __future__ import annotations

import importlib
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import crypto_envelope, db_keys, secrets_backend
import finance_cli.user_provisioning as user_provisioning
from finance_cli.db import connect


def _configure(monkeypatch: pytest.MonkeyPatch, data_root: Path) -> None:
    importlib.reload(db_keys)
    importlib.reload(user_provisioning)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "require")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_PORTABLE_BUNDLE_FOR_DELETE", "true")
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="p2")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    secrets_backend._client = None
    crypto_envelope.evict_caches()


def _template_rules(tmp_path: Path) -> Path:
    path = tmp_path / "rules-template.yaml"
    path.write_text("keyword_rules: []\n", encoding="utf-8")
    return path


@mock_aws
def test_full_p2_migration_provisions_vault_and_reads_from_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "users"
    _configure(monkeypatch, data_root)

    user_provisioning.provision_user(data_root=data_root, user_id="u-p2", template_rules_path=_template_rules(tmp_path))

    blob = crypto_envelope.FileStorageBackend(data_root).get("u-p2", "db-dek")
    assert blob is not None
    ref = "finance-cli-test/users/u-p2/db-key"
    described = secrets_backend._get_client().describe_secret(SecretId=ref)
    assert described["DeletedDate"] is not None

    crypto_envelope.evict_caches()

    def fail_sm(*_args, **_kwargs):
        raise AssertionError("SM fallback should not be used after vault provisioning")

    monkeypatch.setattr(secrets_backend, "get_secret", fail_sm)
    assert db_keys.get_user_db_key("u-p2") == crypto_envelope.get_db_dek("u-p2", data_dir=data_root)
    with connect(user_provisioning.user_db_path(data_root, "u-p2"), expected_user_id="u-p2") as conn:
        assert conn.execute("SELECT 1").fetchone()[0] == 1


@mock_aws
def test_web_default_defers_legacy_sm_cleanup_until_server_gate_passes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "users"
    _configure(monkeypatch, data_root)
    monkeypatch.delenv("FINANCE_CLI_REQUIRE_PORTABLE_BUNDLE_FOR_DELETE", raising=False)
    monkeypatch.delenv("FINANCE_CLI_AWS_BACKUP_VAULT_NAME", raising=False)
    monkeypatch.delenv("FINANCE_CLI_AWS_BACKUP_RESOURCE_ARN", raising=False)

    user_provisioning.provision_user(
        data_root=data_root,
        user_id="u-web-defer",
        template_rules_path=_template_rules(tmp_path),
    )

    blob = crypto_envelope.FileStorageBackend(data_root).get("u-web-defer", "db-dek")
    assert blob is not None
    described = secrets_backend._get_client().describe_secret(
        SecretId="finance-cli-test/users/u-web-defer/db-key"
    )
    assert described.get("DeletedDate") is None


@mock_aws
def test_p2_migration_crash_before_db_dek_write_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "users"
    _configure(monkeypatch, data_root)
    real_provision = crypto_envelope.provision_db_dek
    crashes = {"remaining": 1}

    def crash_once(*args, **kwargs):
        if crashes["remaining"]:
            crashes["remaining"] -= 1
            raise RuntimeError("crash before step 5")
        return real_provision(*args, **kwargs)

    monkeypatch.setattr(crypto_envelope, "provision_db_dek", crash_once)
    with pytest.raises(RuntimeError, match="crash before step 5"):
        user_provisioning.provision_user(
            data_root=data_root,
            user_id="u-crash-before",
            template_rules_path=_template_rules(tmp_path),
        )

    assert crypto_envelope.FileStorageBackend(data_root).get("u-crash-before", "db-dek") is None
    assert secrets_backend.get_secret("finance-cli-test/users/u-crash-before/db-key", missing_ok=False)

    user_provisioning.provision_user(
        data_root=data_root,
        user_id="u-crash-before",
        template_rules_path=_template_rules(tmp_path),
    )
    assert crypto_envelope.FileStorageBackend(data_root).get("u-crash-before", "db-dek") is not None


@mock_aws
def test_p2_migration_crash_after_db_dek_write_before_soft_delete_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "users"
    _configure(monkeypatch, data_root)
    real_delete = db_keys.delete_user_db_key
    crashes = {"remaining": 1}

    def crash_once(user_id: str) -> None:
        if crashes["remaining"]:
            crashes["remaining"] -= 1
            raise RuntimeError("crash before step 8")
        real_delete(user_id)

    monkeypatch.setattr(db_keys, "delete_user_db_key", crash_once)
    with pytest.raises(RuntimeError, match="crash before step 8"):
        user_provisioning.provision_user(
            data_root=data_root,
            user_id="u-crash-after",
            template_rules_path=_template_rules(tmp_path),
        )

    assert crypto_envelope.FileStorageBackend(data_root).get("u-crash-after", "db-dek") is not None
    assert secrets_backend.get_secret("finance-cli-test/users/u-crash-after/db-key", missing_ok=False)

    user_provisioning.provision_user(
        data_root=data_root,
        user_id="u-crash-after",
        template_rules_path=_template_rules(tmp_path),
    )
    described = secrets_backend._get_client().describe_secret(
        SecretId="finance-cli-test/users/u-crash-after/db-key"
    )
    assert described["DeletedDate"] is not None
