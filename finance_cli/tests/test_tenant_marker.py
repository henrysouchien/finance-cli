from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tarfile
import tempfile
from contextlib import suppress
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import __version__, secrets_backend
from finance_cli.backup import restore_backup
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import TenantMismatchError
from finance_cli.user_provisioning import provision_user


@pytest.fixture(autouse=True)
def _mock_restore_backup_secrets(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    secrets_backend._client = None
    with mock_aws():
        key_arn = boto3.client("kms", region_name="us-east-1").create_key(
            Description="tenant marker restore test"
        )["KeyMetadata"]["Arn"]
        monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
        yield
    secrets_backend._client = None


def _template_rules_path(tmp_path: Path) -> Path:
    path = tmp_path / "rules-template.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("keyword_rules: []\n", encoding="utf-8")
    return path


def _provisioned_db(tmp_path: Path, user_id: str) -> Path:
    data_root = tmp_path / "users"
    provision_user(
        data_root=data_root,
        user_id=user_id,
        template_rules_path=_template_rules_path(tmp_path),
    )
    return data_root / user_id / "finance.db"


def _marker_row(db_path: Path) -> sqlite3.Row | None:
    with connect(db_path) as conn:
        return conn.execute(
            "SELECT user_id, stamped_at FROM tenant_marker WHERE singleton = 1"
        ).fetchone()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _build_bundle(bundle_path: Path, db_path: Path) -> Path:
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    with tempfile.TemporaryDirectory(prefix="tenant_bundle_", dir=str(bundle_path.parent)) as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        shutil.copy2(db_path, temp_dir / "finance.db")
        (temp_dir / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")

        files = []
        for file_path in sorted(path for path in temp_dir.rglob("*") if path.is_file()):
            if file_path.name == "manifest.json":
                continue
            files.append(
                {
                    "path": file_path.relative_to(temp_dir).as_posix(),
                    "sha256": _sha256(file_path),
                    "size_bytes": int(file_path.stat().st_size),
                }
            )
        db_file = next(entry for entry in files if entry["path"] == "finance.db")
        manifest = {
            "version": 1,
            "created_at": "2026-04-15T00:00:00Z",
            "finance_cli_version": __version__,
            "db_sha256": db_file["sha256"],
            "db_size_bytes": db_file["size_bytes"],
            "files": files,
            "db_integrity_check": "ok",
            "migration_version": _migration_version(db_path),
        }
        (temp_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with tarfile.open(bundle_path, "w:gz") as tar:
            for file_path in sorted(path for path in temp_dir.rglob("*") if path.is_file()):
                tar.add(file_path, arcname=file_path.relative_to(temp_dir).as_posix())
    return bundle_path


def _migration_version(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        try:
            row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        except sqlite3.OperationalError:
            return 0
    return int((row or [0])[0] or 0)


def _blank_sqlite_db(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(path)):
        pass
    return path


def _restore_target(tmp_path: Path, user_id: str = "alice") -> Path:
    live_db = _provisioned_db(tmp_path, user_id)
    with connect(live_db) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    return live_db


def test_marker_table_exists_after_migration(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tenant_marker'"
        ).fetchone()

    assert row is not None


def test_marker_absent_on_fresh_db(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM tenant_marker").fetchone()[0]

    assert count == 0


def test_provision_user_stamps_marker(tmp_path: Path) -> None:
    db_path = _provisioned_db(tmp_path, "alice")

    row = _marker_row(db_path)

    assert row is not None
    assert row["user_id"] == "alice"
    assert row["stamped_at"]


def test_provision_user_idempotent_same_user(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    template_rules_path = _template_rules_path(tmp_path)

    provision_user(data_root=data_root, user_id="alice", template_rules_path=template_rules_path)
    provision_user(data_root=data_root, user_id="alice", template_rules_path=template_rules_path)

    with connect(data_root / "alice" / "finance.db") as conn:
        count = conn.execute("SELECT COUNT(*) FROM tenant_marker").fetchone()[0]
        user_id = conn.execute(
            "SELECT user_id FROM tenant_marker WHERE singleton = 1"
        ).fetchone()[0]

    assert count == 1
    assert user_id == "alice"


def test_provision_user_raises_on_existing_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_root = tmp_path / "users"
    template_rules_path = _template_rules_path(tmp_path)
    provision_user(data_root=data_root, user_id="alice", template_rules_path=template_rules_path)
    alice_db = data_root / "alice" / "finance.db"

    monkeypatch.setattr("finance_cli.user_provisioning.user_db_path", lambda *_args, **_kwargs: alice_db)

    with pytest.raises(TenantMismatchError) as excinfo:
        provision_user(data_root=data_root, user_id="bob", template_rules_path=template_rules_path)

    assert excinfo.value.reason == "mismatch"
    assert excinfo.value.expected_user_id == "bob"
    assert excinfo.value.actual_user_id == "alice"
    assert _marker_row(alice_db)["user_id"] == "alice"


def test_second_row_insert_fails_check_constraint(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO tenant_marker (singleton, user_id) VALUES (1, 'alice')"
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO tenant_marker (singleton, user_id) VALUES (2, 'mallory')"
            )


def test_connect_matching_expected_user_id(tmp_path: Path) -> None:
    db_path = _provisioned_db(tmp_path, "alice")

    with connect(db_path, expected_user_id="alice") as conn:
        row = conn.execute(
            "SELECT user_id FROM tenant_marker WHERE singleton = 1"
        ).fetchone()

    assert row[0] == "alice"


def test_connect_mismatched_expected_user_id_raises(tmp_path: Path) -> None:
    db_path = _provisioned_db(tmp_path, "alice")

    with pytest.raises(TenantMismatchError) as excinfo:
        connect(db_path, expected_user_id="bob")

    assert excinfo.value.reason == "mismatch"
    assert excinfo.value.expected_user_id == "bob"
    assert excinfo.value.actual_user_id == "alice"


def test_connect_missing_marker_row_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with pytest.raises(TenantMismatchError) as excinfo:
        connect(db_path, expected_user_id="alice")

    assert excinfo.value.reason == "missing_row"


def test_connect_missing_marker_table_raises(tmp_path: Path) -> None:
    db_path = _blank_sqlite_db(tmp_path / "finance.db")

    with pytest.raises(TenantMismatchError) as excinfo:
        connect(db_path, expected_user_id="alice")

    assert excinfo.value.reason == "missing_table"


def test_connect_missing_file_raises_and_no_create(tmp_path: Path) -> None:
    db_path = tmp_path / "missing-parent" / "finance.db"

    with pytest.raises(TenantMismatchError) as excinfo:
        connect(db_path, expected_user_id="alice")

    assert excinfo.value.reason == "missing_file"
    assert not db_path.exists()
    assert not db_path.parent.exists()


def test_connect_without_expected_user_id_skips_check(tmp_path: Path) -> None:
    db_path = _blank_sqlite_db(tmp_path / "finance.db")

    with connect(db_path) as conn:
        conn.execute("SELECT 1").fetchone()

    assert db_path.exists()


def test_connect_without_expected_user_id_still_autocreates(tmp_path: Path) -> None:
    db_path = tmp_path / "auto-create" / "finance.db"

    with connect(db_path) as conn:
        conn.execute("SELECT 1").fetchone()

    assert db_path.exists()
    assert db_path.parent.exists()


def test_restore_backup_mismatched_marker_refuses(tmp_path: Path) -> None:
    live_db = _restore_target(tmp_path / "live")
    bundle_path = _build_bundle(tmp_path / "bundle-bob.tar.gz", _provisioned_db(tmp_path / "source", "bob"))
    conn = connect(live_db)

    try:
        with pytest.raises(TenantMismatchError) as excinfo:
            restore_backup(
                bundle_path,
                conn=conn,
                target_db_path=live_db,
                target_data_dir=live_db.parent,
                dry_run=False,
                data_dir=live_db.parent,
                rules_path=live_db.parent / "rules.yaml",
                expected_user_id="alice",
            )
    finally:
        with suppress(Exception):
            conn.close()

    assert excinfo.value.reason == "mismatch"
    assert excinfo.value.actual_user_id == "bob"
    assert _marker_row(live_db)["user_id"] == "alice"


def test_restore_backup_premarker_bundle_rejected_on_guarded_path(tmp_path: Path) -> None:
    live_db = _restore_target(tmp_path / "live")
    premarker_db = _blank_sqlite_db(tmp_path / "source" / "premarker.db")
    bundle_path = _build_bundle(tmp_path / "bundle-premarker.tar.gz", premarker_db)
    conn = connect(live_db)

    try:
        with pytest.raises(TenantMismatchError) as excinfo:
            restore_backup(
                bundle_path,
                conn=conn,
                target_db_path=live_db,
                target_data_dir=live_db.parent,
                dry_run=False,
                data_dir=live_db.parent,
                rules_path=live_db.parent / "rules.yaml",
                expected_user_id="alice",
            )
    finally:
        with suppress(Exception):
            conn.close()

    assert excinfo.value.reason == "missing_table"
    assert _marker_row(live_db)["user_id"] == "alice"


def test_restore_backup_empty_marker_table_rejected(tmp_path: Path) -> None:
    live_db = _restore_target(tmp_path / "live")
    empty_marker_db = tmp_path / "source" / "empty-marker.db"
    initialize_database(empty_marker_db)
    bundle_path = _build_bundle(tmp_path / "bundle-empty-marker.tar.gz", empty_marker_db)
    conn = connect(live_db)

    try:
        with pytest.raises(TenantMismatchError) as excinfo:
            restore_backup(
                bundle_path,
                conn=conn,
                target_db_path=live_db,
                target_data_dir=live_db.parent,
                dry_run=False,
                data_dir=live_db.parent,
                rules_path=live_db.parent / "rules.yaml",
                expected_user_id="alice",
            )
    finally:
        with suppress(Exception):
            conn.close()

    assert excinfo.value.reason == "missing_row"
    assert _marker_row(live_db)["user_id"] == "alice"


def test_restore_backup_premarker_bundle_allowed_unguarded(tmp_path: Path) -> None:
    live_db = _restore_target(tmp_path / "live")
    premarker_db = _blank_sqlite_db(tmp_path / "source" / "premarker.db")
    bundle_path = _build_bundle(tmp_path / "bundle-premarker.tar.gz", premarker_db)
    conn = connect(live_db)

    try:
        result = restore_backup(
            bundle_path,
            conn=conn,
            target_db_path=live_db,
            target_data_dir=live_db.parent,
            dry_run=False,
            data_dir=live_db.parent,
            rules_path=live_db.parent / "rules.yaml",
        )
    finally:
        with suppress(Exception):
            conn.close()

    assert result.restored is True
    with sqlite3.connect(str(live_db)) as restored_conn:
        row = restored_conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tenant_marker'"
        ).fetchone()
    assert row is None


def test_restore_backup_verified_on_staged_copy_not_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live_db = _restore_target(tmp_path / "live")
    bundle_path = _build_bundle(tmp_path / "bundle-bob.tar.gz", _provisioned_db(tmp_path / "source", "bob"))
    before_bytes = live_db.read_bytes()
    monkeypatch.setenv("FINANCE_CLI_DB", str(live_db))

    with pytest.raises(TenantMismatchError):
        restore_backup(
            bundle_path,
            target_db_path=live_db,
            target_data_dir=live_db.parent,
            dry_run=False,
            data_dir=live_db.parent,
            rules_path=live_db.parent / "rules.yaml",
            expected_user_id="alice",
        )

    assert live_db.read_bytes() == before_bytes
