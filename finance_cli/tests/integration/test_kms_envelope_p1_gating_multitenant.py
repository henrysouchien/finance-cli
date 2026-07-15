from __future__ import annotations

import tarfile
from io import BytesIO
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import backup_crypto, crypto_envelope, secrets_backend
from finance_cli.backup import create_backup
from finance_cli.db import connect, initialize_database


def _configure_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    user_id: str = "1",
) -> tuple[Path, Path, Path]:
    data_root = tmp_path / "users"
    user_dir = data_root / user_id
    db_path = user_dir / "finance.db"
    user_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(user_dir))
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_USER_ID", user_id)
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "off")
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="p1-multitenant")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    secrets_backend._client = None
    crypto_envelope.evict_caches()

    initialize_database(db_path)
    (user_dir / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    with connect(db_path, user_id=user_id) as conn:
        conn.execute("INSERT OR REPLACE INTO tenant_marker (singleton, user_id) VALUES (1, ?)", (user_id,))
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-mt', '2026-04-30', 'Multitenant transaction', -500, 'manual', 1)
            """
        )
        conn.commit()
    return data_root, user_dir, db_path


@mock_aws
def test_create_backup_writes_v3_when_db_dek_enc_exists_multitenant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = "1"
    data_root, user_dir, db_path = _configure_workspace(tmp_path, monkeypatch, user_id=user_id)
    crypto_envelope.provision_db_dek(user_id, dek=b"\x31" * 32, data_dir=data_root)

    with connect(db_path, user_id=user_id) as conn:
        backup = create_backup(conn, data_dir=user_dir, rules_path=user_dir / "rules.yaml", user_id=user_id, compact=True)

    bundle_bytes = backup.bundle_path.read_bytes()
    assert backup.bundle_path.parent == user_dir / "backups"
    assert bundle_bytes[: len(backup_crypto.MAGIC_V3)] == backup_crypto.MAGIC_V3
    header = backup_crypto.parse_bundle_header_v3(bundle_bytes)
    assert header["mode"] == "compact"
    assert header["recovery_db_dek_present"] is False


@mock_aws
def test_create_backup_writes_v2_when_db_dek_enc_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = "1"
    _data_root, user_dir, db_path = _configure_workspace(tmp_path, monkeypatch, user_id=user_id)

    with connect(db_path, user_id=user_id) as conn:
        backup = create_backup(conn, data_dir=user_dir, rules_path=user_dir / "rules.yaml", user_id=user_id, portable=True)

    bundle_bytes = backup.bundle_path.read_bytes()
    assert backup.bundle_path.parent == user_dir / "backups"
    assert bundle_bytes[: len(backup_crypto.MAGIC)] == backup_crypto.MAGIC


@mock_aws
def test_create_backup_portable_embeds_recovery_payload_multitenant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = "1"
    db_dek = b"\x42" * 32
    data_root, user_dir, db_path = _configure_workspace(tmp_path, monkeypatch, user_id=user_id)
    crypto_envelope.provision_db_dek(user_id, dek=db_dek, data_dir=data_root)

    with connect(db_path, user_id=user_id) as conn:
        backup = create_backup(conn, data_dir=user_dir, rules_path=user_dir / "rules.yaml", user_id=user_id, portable=True)

    bundle_bytes = backup.bundle_path.read_bytes()
    assert bundle_bytes[: len(backup_crypto.MAGIC_V3)] == backup_crypto.MAGIC_V3
    tar_bytes, header = backup_crypto.decrypt_bundle_v3(bundle_bytes, user_id)
    assert header["mode"] == "portable"
    assert header["recovery_db_dek_present"] is True

    with tarfile.open(fileobj=BytesIO(tar_bytes), mode="r:gz") as tar:
        recovery_file = tar.extractfile("recovery/db-dek.enc")
        assert recovery_file is not None
        recovery_blob = recovery_file.read()

    assert recovery_blob == crypto_envelope.FileStorageBackend(data_root).get(user_id, "db-dek")

    install_root = tmp_path / "install" / "users"
    crypto_envelope.install_db_dek_blob(user_id, recovery_blob, data_dir=install_root)
    assert (install_root / user_id / "db-dek.enc").exists()
    assert crypto_envelope.get_db_dek(user_id, data_dir=install_root) == db_dek
