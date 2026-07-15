from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

import finance_cli.backup as backup_module
from finance_cli import backup_crypto, crypto_envelope
from finance_cli.backup import can_hard_delete_db_dek_sm
from finance_cli.db import connect, initialize_database


def _configure(monkeypatch: pytest.MonkeyPatch, data_dir: Path) -> Path:
    db_path = data_dir / "finance.db"
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(data_dir))
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "off")
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="hard-delete")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    initialize_database(db_path)
    crypto_envelope.provision_db_dek("gate-user", dek=b"\x55" * 32, data_dir=data_dir)
    return db_path


def _insert_bundle_row(
    conn,
    bundle_path: Path,
    *,
    format_version: int,
    mode: str | None,
    recovery_db_dek_present: bool | None,
    user_id: str = "gate-user",
) -> None:
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    if not bundle_path.exists():
        bundle_path.write_bytes(bundle_path.name.encode("utf-8"))
    conn.execute(
        """
        INSERT INTO backup_log (
            backup_type, status, bundle_path, bundle_sha256, bundle_size,
            db_sha256, migration_ver, duration_ms, bundle_format_version,
            bundle_id, user_id, mode, recovery_db_dek_present, created_at
        ) VALUES (
            'local', 'completed', ?, 'sha', ?, 'dbsha', 60, 1, ?,
            ?, ?, ?, ?, ?
        )
        """,
        (
            str(bundle_path),
            bundle_path.stat().st_size,
            format_version,
            str(uuid.uuid4()),
            user_id,
            mode,
            None if recovery_db_dek_present is None else int(bool(recovery_db_dek_present)),
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        ),
    )
    conn.commit()


@mock_aws
def test_local_default_gate_scenarios(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FINANCE_CLI_REQUIRE_PORTABLE_BUNDLE_FOR_DELETE", raising=False)
    db_path = _configure(monkeypatch, tmp_path)

    with connect(db_path) as conn:
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is True

        _insert_bundle_row(conn, tmp_path / "backups" / "only-v2.bundle", format_version=2, mode=None, recovery_db_dek_present=None)
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is False

        conn.execute("DELETE FROM backup_log")
        _insert_bundle_row(conn, tmp_path / "backups" / "compact-v3.bundle", format_version=3, mode="compact", recovery_db_dek_present=False)
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is False

        _insert_bundle_row(conn, tmp_path / "backups" / "portable-v3.bundle", format_version=3, mode="portable", recovery_db_dek_present=True)
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is True


@mock_aws
def test_hard_delete_gate_finds_db_dek_in_multitenant_layout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = "1"
    data_root = tmp_path / "users"
    user_dir = data_root / user_id
    db_path = user_dir / "finance.db"
    user_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(user_dir))
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_USER_ID", user_id)
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "off")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_PORTABLE_BUNDLE_FOR_DELETE", "true")
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="hard-delete-mt")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)

    initialize_database(db_path)
    crypto_envelope.provision_db_dek(user_id, dek=b"\x55" * 32, data_dir=data_root)

    with connect(db_path, user_id=user_id) as conn:
        _insert_bundle_row(
            conn,
            user_dir / "backups" / "portable-v3.bundle",
            format_version=3,
            mode="portable",
            recovery_db_dek_present=True,
            user_id=user_id,
        )

        assert can_hard_delete_db_dek_sm(user_id, conn=conn, data_dir=user_dir) is True


@mock_aws
def test_hard_delete_gate_translates_cli_data_dir_when_data_dir_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = "1"
    data_root = tmp_path / "users"
    user_dir = data_root / user_id
    db_path = user_dir / "finance.db"
    user_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.delenv("FINANCE_WEB_DATA_ROOT", raising=False)
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(user_dir))
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_USER_ID", user_id)
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "off")
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="hard-delete-cli-dir")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)

    initialize_database(db_path)
    crypto_envelope.provision_db_dek(user_id, dek=b"\x55" * 32, data_dir=data_root)

    with connect(db_path, user_id=user_id) as conn:
        assert can_hard_delete_db_dek_sm(user_id, conn=conn) is True


class _FakeBackupClient:
    def __init__(self, created_at: datetime | None) -> None:
        self.created_at = created_at

    def list_recovery_points_by_backup_vault(self, **_kwargs):
        if self.created_at is None:
            return {"RecoveryPoints": []}
        return {"RecoveryPoints": [{"CreationDate": self.created_at}]}


@mock_aws
def test_web_data_root_defaults_to_server_backup_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = "1"
    data_root = tmp_path / "users"
    user_dir = data_root / user_id
    db_path = user_dir / "finance.db"
    user_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(user_dir))
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_USER_ID", user_id)
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "off")
    monkeypatch.delenv("FINANCE_CLI_REQUIRE_PORTABLE_BUNDLE_FOR_DELETE", raising=False)
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(
        Description="server-default-hard-delete"
    )["KeyMetadata"]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)

    initialize_database(db_path)
    crypto_envelope.provision_db_dek(user_id, dek=b"\x55" * 32, data_dir=data_root)

    with connect(db_path, user_id=user_id) as conn:
        assert can_hard_delete_db_dek_sm(user_id, conn=conn, data_dir=user_dir) is False

    monkeypatch.setenv("FINANCE_CLI_AWS_BACKUP_VAULT_NAME", "cashnerd-prod")
    monkeypatch.setenv("FINANCE_CLI_AWS_BACKUP_RESOURCE_ARN", "arn:aws:ec2:us-east-1:123:volume/vol-1")
    monkeypatch.setattr(
        backup_module.boto3,
        "client",
        lambda *_args, **_kwargs: _FakeBackupClient(datetime.now(timezone.utc) - timedelta(hours=1)),
    )
    with connect(db_path, user_id=user_id) as conn:
        assert can_hard_delete_db_dek_sm(user_id, conn=conn, data_dir=user_dir) is True


@mock_aws
def test_server_gate_requires_recent_backup_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_PORTABLE_BUNDLE_FOR_DELETE", "false")
    monkeypatch.setenv("FINANCE_CLI_AWS_BACKUP_VAULT_NAME", "cashnerd-prod")
    monkeypatch.setenv("FINANCE_CLI_AWS_BACKUP_RESOURCE_ARN", "arn:aws:ec2:us-east-1:123:volume/vol-1")

    monkeypatch.setattr(
        backup_module.boto3,
        "client",
        lambda *_args, **_kwargs: _FakeBackupClient(datetime.now(timezone.utc) - timedelta(hours=1)),
    )
    with connect(db_path) as conn:
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is True

    monkeypatch.delenv("FINANCE_CLI_AWS_BACKUP_VAULT_NAME", raising=False)
    with connect(db_path) as conn:
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is False

    monkeypatch.setenv("FINANCE_CLI_AWS_BACKUP_VAULT_NAME", "cashnerd-prod")
    monkeypatch.setattr(
        backup_module.boto3,
        "client",
        lambda *_args, **_kwargs: _FakeBackupClient(datetime.now(timezone.utc) - timedelta(hours=49)),
    )
    with connect(db_path) as conn:
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is False

    with connect(db_path) as conn:
        assert can_hard_delete_db_dek_sm("missing-user", conn=conn, data_dir=tmp_path) is False


@mock_aws
def test_gate_falls_back_to_header_for_null_mode_and_ignores_orphans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _configure(monkeypatch, tmp_path)
    monkeypatch.delenv("FINANCE_CLI_REQUIRE_PORTABLE_BUNDLE_FOR_DELETE", raising=False)
    portable_path = tmp_path / "backups" / "null-mode.bundle"
    portable_path.write_bytes(
        backup_crypto.encrypt_bundle_v3(
            b"tar bytes",
            "gate-user",
            str(uuid.uuid4()),
            mode="portable",
            recovery_db_dek_present=True,
        )
    )

    with connect(db_path) as conn:
        _insert_bundle_row(conn, portable_path, format_version=3, mode=None, recovery_db_dek_present=None)
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is True

        conn.execute("DELETE FROM backup_log")
        _insert_bundle_row(conn, tmp_path / "backups" / "weird.bundle", format_version=3, mode="weird", recovery_db_dek_present=True)
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is False

        orphan_path = tmp_path / "backups" / "orphan-portable.bundle"
        orphan_path.write_bytes(portable_path.read_bytes())
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is False

        conn.execute("DELETE FROM backup_log")
        missing_header = tmp_path / "backups" / "missing-header.bundle"
        _insert_bundle_row(conn, missing_header, format_version=3, mode=None, recovery_db_dek_present=None)
        missing_header.unlink()
        assert can_hard_delete_db_dek_sm("gate-user", conn=conn, data_dir=tmp_path) is False
