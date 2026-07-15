from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import backup_crypto, crypto_envelope, secrets_backend
from finance_cli.backup import create_backup, restore_backup
from finance_cli.db import connect, initialize_database


def _create_portable_source_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    user_id: str,
) -> tuple[Path, bytes]:
    source_root = tmp_path / "source" / "users"
    source_dir = source_root / user_id
    db_path = source_dir / "finance.db"
    source_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(source_root))
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(source_dir))
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_USER_ID", user_id)
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "off")
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="p1-restore-mt")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    secrets_backend._client = None
    crypto_envelope.evict_caches()

    db_dek = b"\x52" * 32
    initialize_database(db_path)
    (source_dir / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    crypto_envelope.provision_db_dek(user_id, dek=db_dek, data_dir=source_root)
    with connect(db_path, user_id=user_id) as conn:
        conn.execute("INSERT OR REPLACE INTO tenant_marker (singleton, user_id) VALUES (1, ?)", (user_id,))
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-restore-mt', '2026-04-30', 'Restore multitenant', -900, 'manual', 1)
            """
        )
        conn.commit()
        backup = create_backup(conn, data_dir=source_dir, rules_path=source_dir / "rules.yaml", user_id=user_id, portable=True)

    assert backup.bundle_path.read_bytes()[: len(backup_crypto.MAGIC_V3)] == backup_crypto.MAGIC_V3
    return backup.bundle_path, db_dek


@mock_aws
def test_restore_installs_db_dek_to_correct_path_with_explicit_data_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = "2"
    bundle_path, db_dek = _create_portable_source_bundle(tmp_path, monkeypatch, user_id=user_id)
    target_root = tmp_path / "target" / "users"
    target_dir = target_root / user_id
    wrong_home = tmp_path / "home"

    monkeypatch.delenv("FINANCE_WEB_DATA_ROOT", raising=False)
    monkeypatch.delenv("FINANCE_CLI_DATA_DIR", raising=False)
    monkeypatch.delenv("FINANCE_CLI_DB", raising=False)
    monkeypatch.setenv("HOME", str(wrong_home))

    result = restore_backup(
        bundle_path,
        dry_run=False,
        target_db_path=target_dir / "finance.db",
        data_dir=target_dir,
        rules_path=target_dir / "rules.yaml",
        expected_user_id=user_id,
        user_id=user_id,
    )

    assert result.restored is True
    assert (target_dir / "db-dek.enc").exists()
    assert not (target_dir / user_id / "db-dek.enc").exists()
    assert not (wrong_home / ".local" / "share" / "finance_cli" / user_id / "db-dek.enc").exists()
    assert crypto_envelope.get_db_dek(user_id, data_dir=target_root) == db_dek
