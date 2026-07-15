from __future__ import annotations

from contextlib import suppress
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import backup_crypto, crypto_envelope
from finance_cli.backup import create_backup, restore_backup
from finance_cli.db import connect, initialize_database


def _setup_workspace(root: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    db_path = root / "finance.db"
    data_dir = root
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(data_dir))
    monkeypatch.setenv("FINANCE_CLI_USER_ID", "default")
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    initialize_database(db_path)
    (data_dir / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    return db_path, data_dir


@mock_aws
def test_create_restore_v3_bundle_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="test")["KeyMetadata"]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)

    db_path, data_dir = _setup_workspace(tmp_path, monkeypatch)
    crypto_envelope.provision_db_dek("default", dek=b"\x66" * 32, data_dir=data_dir)

    with connect(db_path) as conn:
        conn.execute("INSERT OR REPLACE INTO tenant_marker (singleton, user_id) VALUES (1, 'default')")
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-original', '2026-03-10', 'Original transaction', -500, 'manual', 1)
            """
        )
        conn.commit()
        backup = create_backup(conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml", portable=True)

    bundle_bytes = backup.bundle_path.read_bytes()
    assert bundle_bytes[: len(backup_crypto.MAGIC_V3)] == backup_crypto.MAGIC_V3
    assert backup_crypto.parse_bundle_header_v3(bundle_bytes)["recovery_db_dek_present"] is True

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-mutated', '2026-03-11', 'Mutated transaction', -700, 'manual', 1)
            """
        )
        conn.commit()

    conn = connect(db_path)
    try:
        result = restore_backup(
            backup.bundle_path,
            conn=conn,
            dry_run=False,
            data_dir=data_dir,
            rules_path=data_dir / "rules.yaml",
            expected_user_id="default",
            user_id="default",
        )
    finally:
        with suppress(Exception):
            conn.close()

    with connect(db_path) as restored_conn:
        rows = restored_conn.execute("SELECT description FROM transactions ORDER BY description").fetchall()

    assert result.restored is True
    assert [row["description"] for row in rows] == ["Original transaction"]
    assert not any("Phase 1" in warning for warning in result.warnings)
