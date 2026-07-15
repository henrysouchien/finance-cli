from __future__ import annotations

import json
import tarfile
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import crypto_envelope
from finance_cli.db import connect
from finance_cli.sync import engine as sync_engine
from finance_cli.user_provisioning import provision_user, user_db_path
from server.sync_service import create_plaintext_snapshot


def _configure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "off")
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="p2 sync")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    crypto_envelope.evict_caches()


def _template_rules(tmp_path: Path) -> Path:
    path = tmp_path / "rules-template.yaml"
    path.write_text("keyword_rules: []\n", encoding="utf-8")
    return path


def _server_snapshot_with_db_dek(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str, bytes]:
    data_root = tmp_path / "server-users"
    user_id = "sync-p2"
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))
    provision_user(data_root=data_root, user_id=user_id, template_rules_path=_template_rules(tmp_path))
    with connect(user_db_path(data_root, user_id), expected_user_id=user_id) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-sync-p2', '2026-04-30', 'P2 sync', -100, 'manual', 1)
            """
        )
        conn.commit()
    dek = b"\x44" * 32
    crypto_envelope.provision_db_dek(user_id, dek=dek, data_dir=data_root)

    snapshot_path, _op_id = create_plaintext_snapshot(user_id, data_root)
    with tarfile.open(snapshot_path, "r:gz") as tar:
        names = set(tar.getnames())
        assert "db-dek.enc" in names
    return snapshot_path, user_id, dek


def _commit_snapshot_to_local(
    snapshot_path: Path,
    *,
    user_id: str,
    local_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_data = local_root / "data"
    local_data.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(sync_engine, "CASHNERD_DIR", local_root)
    monkeypatch.setattr(sync_engine, "CASHNERD_DATA_DIR", local_data)
    monkeypatch.setattr(sync_engine, "CASHNERD_DB_PATH", local_data / "finance.db")
    staging = local_root / "staging"
    with tarfile.open(snapshot_path, "r:gz") as tar:
        tar.extractall(staging)
    engine = object.__new__(sync_engine.SyncEngine)
    engine._commit_staged_files_sync(staging, user_id=user_id)
    assert not staging.exists()


@mock_aws
def test_snapshot_db_dek_installs_into_file_backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    snapshot_path, user_id, dek = _server_snapshot_with_db_dek(tmp_path, monkeypatch)
    local_root = tmp_path / "local-file"
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")

    _commit_snapshot_to_local(snapshot_path, user_id=user_id, local_root=local_root, monkeypatch=monkeypatch)

    local_data = local_root / "data"
    assert (local_data / user_id / "db-dek.enc").exists()
    provenance = json.loads((local_data / user_id / ".envelope_provenance.json").read_text(encoding="utf-8"))
    assert provenance["db-dek"] == "file"
    assert crypto_envelope.get_db_dek(user_id, data_dir=local_data) == dek
    with connect(local_data / "finance.db", expected_user_id=user_id) as conn:
        assert conn.execute("SELECT description FROM transactions WHERE id = 'txn-sync-p2'").fetchone()[0] == "P2 sync"


@mock_aws
def test_snapshot_db_dek_installs_into_keychain_backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch)
    snapshot_path, user_id, dek = _server_snapshot_with_db_dek(tmp_path, monkeypatch)
    local_root = tmp_path / "local-keychain"
    fake_keyring_store: dict[tuple[str, str], str] = {}

    class FakeKeyring:
        def get_password(self, service: str, account: str) -> str | None:
            return fake_keyring_store.get((service, account))

        def set_password(self, service: str, account: str, value: str) -> None:
            fake_keyring_store[(service, account)] = value

        def delete_password(self, service: str, account: str) -> None:
            fake_keyring_store.pop((service, account), None)

    def fake_select(_user_id: str, data_dir: Path):
        return crypto_envelope.MacOSKeychainBackend(data_dir, keyring_module=FakeKeyring())

    monkeypatch.setattr(crypto_envelope, "select_backend", fake_select)

    _commit_snapshot_to_local(snapshot_path, user_id=user_id, local_root=local_root, monkeypatch=monkeypatch)

    local_data = local_root / "data"
    assert not (local_data / user_id / "db-dek.enc").exists()
    assert fake_keyring_store
    provenance = json.loads((local_data / user_id / ".envelope_provenance.json").read_text(encoding="utf-8"))
    assert provenance["db-dek"] == "keychain:macos"
    assert crypto_envelope.get_db_dek(user_id, data_dir=local_data) == dek
