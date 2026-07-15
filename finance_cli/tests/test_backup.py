"""Tests for finance_cli.backup module."""

from __future__ import annotations

import json
import tarfile
import tempfile
import uuid
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import pytest
import boto3
from moto import mock_aws

import finance_cli.backup as backup_module
from finance_cli import backup_crypto, crypto_envelope, db as db_module, secrets_backend
from finance_cli.__main__ import main
from finance_cli.backup import (
    create_backup,
    list_backups,
    prune_backups,
    restore_backup,
    verify_backup,
)
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import TenantMismatchError
from finance_cli.sync.exceptions import SubscriberActiveError
from finance_cli.sync.subscriber_lock import InstallSubscriberLock


def _setup(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    db_path = tmp_path / "finance.db"
    data_dir = tmp_path
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(data_dir))
    monkeypatch.setenv("FINANCE_CLI_USER_ID", "default")
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    (data_dir / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    (data_dir / "agent_memory.md").write_text("# Test memory\n", encoding="utf-8")
    sessions = data_dir / "sessions"
    sessions.mkdir()
    (sessions / "2026-03-10.md").write_text("Session note\n", encoding="utf-8")
    return db_path, data_dir


def test_envelope_data_dir_multitenant_web_root_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", "/data/finance/users")

    assert (
        backup_module._envelope_data_dir(Path("/data/finance/users/1"), "1")
        == Path("/data/finance/users").resolve()
    )


def test_envelope_data_dir_multitenant_web_root_symlink(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    web_root = tmp_path / "users"
    physical_user_dir = tmp_path / "actual-user-1"
    web_root.mkdir()
    physical_user_dir.mkdir()
    (web_root / "1").symlink_to(physical_user_dir, target_is_directory=True)
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(web_root))

    assert backup_module._envelope_data_dir(web_root / "1", "1") == web_root.resolve()


def test_envelope_data_dir_multitenant_users_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FINANCE_WEB_DATA_ROOT", raising=False)

    assert (
        backup_module._envelope_data_dir(Path("/srv/users/1"), "1")
        == Path("/srv/users").resolve()
    )


def test_verify_integrity_encrypted_copy_uses_explicit_connection_options(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "finance.db"
    envelope_root = tmp_path / "users"
    calls: dict[str, object] = {}

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, sql: str):
            calls["sql"] = sql
            return self

        def fetchone(self):
            return ("ok",)

    def fake_open_encrypted_connection(
        path: Path,
        *,
        user_id: str,
        check_same_thread: bool,
        factory=None,
        data_dir: Path | None = None,
    ):
        calls.update(
            {
                "path": path,
                "user_id": user_id,
                "check_same_thread": check_same_thread,
                "factory": factory,
                "data_dir": data_dir,
            }
        )
        return FakeConn()

    monkeypatch.setattr(backup_module, "_is_plaintext_backup_db", lambda _path: False)
    monkeypatch.setattr(
        backup_module,
        "open_encrypted_connection",
        fake_open_encrypted_connection,
    )

    assert (
        backup_module._verify_integrity(
            db_path,
            user_id="42",
            data_dir=envelope_root,
        )
        == "ok"
    )
    assert calls == {
        "path": db_path,
        "user_id": "42",
        "check_same_thread": True,
        "factory": None,
        "data_dir": envelope_root,
        "sql": "PRAGMA integrity_check",
    }


def test_envelope_data_dir_single_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FINANCE_WEB_DATA_ROOT", raising=False)

    assert (
        backup_module._envelope_data_dir(Path("./data"), "default")
        == Path("./data").resolve()
    )


def test_envelope_data_dir_basename_collision_no_users_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FINANCE_WEB_DATA_ROOT", raising=False)

    assert (
        backup_module._envelope_data_dir(Path("/x/foo"), "foo")
        == Path("/x/foo").resolve()
    )


@pytest.fixture(autouse=True)
def _mock_backup_secrets(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    secrets_backend._client = None
    with mock_aws():
        key_arn = boto3.client("kms", region_name="us-east-1").create_key(
            Description="test"
        )["KeyMetadata"]["Arn"]
        monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
        yield
    secrets_backend._client = None


def _seed_txn(conn, description: str) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
        VALUES (?, '2026-03-10', ?, -500, 'manual', 1)
        """,
        (txn_id, description),
    )
    conn.commit()
    return txn_id


def _sha256_bytes(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def _read_manifest(bundle_path: Path) -> dict:
    bundle_bytes = bundle_path.read_bytes()
    if (
        bundle_bytes[: len(backup_crypto.MAGIC)] == backup_crypto.MAGIC
        or bundle_bytes[: len(backup_crypto.MAGIC_V3)] == backup_crypto.MAGIC_V3
    ):
        tar_bytes, _ = backup_crypto.decrypt_bundle_any_version(bundle_bytes, "default")
        with tarfile.open(fileobj=BytesIO(tar_bytes), mode="r:gz") as tar:
            manifest_file = tar.extractfile("manifest.json")
            assert manifest_file is not None
            return json.load(manifest_file)
    with tarfile.open(bundle_path, "r:gz") as tar:
        manifest_file = tar.extractfile("manifest.json")
        assert manifest_file is not None
        return json.load(manifest_file)


def _read_bundle_names(bundle_path: Path, *, user_id: str = "default") -> set[str]:
    bundle_bytes = bundle_path.read_bytes()
    if (
        bundle_bytes[: len(backup_crypto.MAGIC)] == backup_crypto.MAGIC
        or bundle_bytes[: len(backup_crypto.MAGIC_V3)] == backup_crypto.MAGIC_V3
    ):
        tar_bytes, _ = backup_crypto.decrypt_bundle_any_version(bundle_bytes, user_id)
        with tarfile.open(fileobj=BytesIO(tar_bytes), mode="r:gz") as tar:
            return set(tar.getnames())
    with tarfile.open(bundle_path, "r:gz") as tar:
        return set(tar.getnames())


def _sidecar_path(data_dir: Path) -> Path:
    return data_dir / "backups" / "backup_audit.jsonl"


def _read_sidecar_entries(data_dir: Path) -> list[dict]:
    sidecar = _sidecar_path(data_dir)
    if not sidecar.exists():
        return []
    return [
        json.loads(line)
        for line in sidecar.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _append_sidecar_entry(data_dir: Path, entry: dict) -> None:
    sidecar = _sidecar_path(data_dir)
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    with sidecar.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry) + "\n")


def _created_at_for(day, hour: int) -> str:
    return (
        datetime(day.year, day.month, day.day, hour, 0, 0, tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _record_backup_entry(
    conn,
    data_dir: Path,
    bundle_path: Path,
    *,
    created_at: str,
    backup_type: str = "local",
    bundle_format_version: int = 1,
    bundle_id: str | None = None,
    user_id: str | None = None,
    dek_secret_ref: str | None = None,
    signing_key_secret_ref: str | None = None,
) -> None:
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_bytes(bundle_path.name.encode("utf-8"))
    entry = {
        "backup_type": backup_type,
        "status": "completed",
        "bundle_path": str(bundle_path),
        "bundle_sha256": uuid.uuid4().hex,
        "bundle_size": bundle_path.stat().st_size,
        "db_sha256": uuid.uuid4().hex,
        "migration_ver": 39,
        "duration_ms": 1,
        "bundle_format_version": bundle_format_version,
        "dek_secret_ref": dek_secret_ref,
        "signing_key_secret_ref": signing_key_secret_ref,
        "bundle_id": bundle_id,
        "user_id": user_id,
        "created_at": created_at,
    }
    _append_sidecar_entry(data_dir, entry)
    conn.execute(
        """
        INSERT INTO backup_log (
            backup_type, status, bundle_path, bundle_sha256, bundle_size,
            db_sha256, migration_ver, duration_ms, bundle_format_version,
            dek_secret_ref, signing_key_secret_ref, bundle_id, user_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entry["backup_type"],
            entry["status"],
            entry["bundle_path"],
            entry["bundle_sha256"],
            entry["bundle_size"],
            entry["db_sha256"],
            entry["migration_ver"],
            entry["duration_ms"],
            entry["bundle_format_version"],
            entry["dek_secret_ref"],
            entry["signing_key_secret_ref"],
            entry["bundle_id"],
            entry["user_id"],
            entry["created_at"],
        ),
    )
    conn.commit()


def _build_legacy_bundle(bundle_path: Path, db_path: Path, data_dir: Path) -> Path:
    with tempfile.TemporaryDirectory(prefix="legacy_backup_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with connect(db_path) as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        (temp_dir / "finance.db").write_bytes(db_path.read_bytes())
        if (data_dir / "rules.yaml").exists():
            (temp_dir / "rules.yaml").write_bytes(
                (data_dir / "rules.yaml").read_bytes()
            )
        if (data_dir / "agent_memory.md").exists():
            (temp_dir / "agent_memory.md").write_bytes(
                (data_dir / "agent_memory.md").read_bytes()
            )
        if (data_dir / "sessions").is_dir():
            (temp_dir / "sessions").mkdir(parents=True, exist_ok=True)
            for session_path in (data_dir / "sessions").rglob("*"):
                if not session_path.is_file():
                    continue
                target = temp_dir / "sessions" / session_path.name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(session_path.read_bytes())

        files = []
        for file_path in sorted(path for path in temp_dir.rglob("*") if path.is_file()):
            files.append(
                {
                    "path": file_path.relative_to(temp_dir).as_posix(),
                    "sha256": _sha256_bytes(file_path.read_bytes()),
                    "size_bytes": int(file_path.stat().st_size),
                }
            )

        db_file = next(entry for entry in files if entry["path"] == "finance.db")
        manifest = {
            "version": 1,
            "created_at": "2026-04-15T12:34:56Z",
            "finance_cli_version": "test",
            "db_sha256": db_file["sha256"],
            "db_size_bytes": db_file["size_bytes"],
            "files": files,
            "db_integrity_check": "ok",
            "migration_version": db_module.SCHEMA_VERSION,
        }
        (temp_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        with tarfile.open(bundle_path, "w:gz") as tar:
            for file_path in sorted(
                path for path in temp_dir.rglob("*") if path.is_file()
            ):
                tar.add(file_path, arcname=file_path.relative_to(temp_dir).as_posix())
    return bundle_path


def _prepare_restore_fixture(tmp_path: Path, monkeypatch):
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    original_rules = "keyword_rules:\n- keywords: ['COFFEE']\n  category: Coffee\n"
    original_memory = "# Original memory\n"
    original_session = "Original session note\n"
    (data_dir / "rules.yaml").write_text(original_rules, encoding="utf-8")
    (data_dir / "agent_memory.md").write_text(original_memory, encoding="utf-8")
    (data_dir / "sessions" / "2026-03-10.md").write_text(
        original_session, encoding="utf-8"
    )

    with connect(db_path) as conn:
        _seed_txn(conn, "Original transaction")
        backup = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml"
        )

    with connect(db_path) as conn:
        _seed_txn(conn, "Mutated transaction")
    (data_dir / "rules.yaml").write_text(
        "keyword_rules:\n- keywords: ['MUTATED']\n  category: Dining\n",
        encoding="utf-8",
    )
    (data_dir / "agent_memory.md").write_text("# Mutated memory\n", encoding="utf-8")
    (data_dir / "sessions" / "2026-03-10.md").write_text(
        "Mutated session note\n", encoding="utf-8"
    )
    return db_path, data_dir, backup, original_rules, original_memory, original_session


def _setup_restore_target(data_dir: Path) -> Path:
    db_path = data_dir / "finance.db"
    initialize_database(db_path)
    (data_dir / "rules.yaml").write_text(
        "keyword_rules:\n- keywords: ['TARGET']\n  category: Target\n", encoding="utf-8"
    )
    (data_dir / "agent_memory.md").write_text("# Target memory\n", encoding="utf-8")
    sessions = data_dir / "sessions"
    sessions.mkdir(parents=True, exist_ok=True)
    (sessions / "2026-03-10.md").write_text("Target session note\n", encoding="utf-8")
    return db_path


def test_create_backup_produces_bundle(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml"
        )

    assert result.bundle_path.exists()
    assert result.bundle_path.suffix == ".bundle"
    assert (
        result.bundle_path.read_bytes()[: len(backup_crypto.MAGIC)]
        == backup_crypto.MAGIC
    )


def test_create_backup_writes_v2_when_db_dek_missing(
    tmp_path: Path, monkeypatch
) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml", portable=True
        )

    assert (
        result.bundle_path.read_bytes()[: len(backup_crypto.MAGIC)]
        == backup_crypto.MAGIC
    )
    entries = _read_sidecar_entries(data_dir)
    completed = [entry for entry in entries if entry.get("status") == "completed"]
    assert completed[-1]["bundle_format_version"] == 2


def test_create_backup_writes_v3_when_db_dek_present(
    tmp_path: Path, monkeypatch
) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    crypto_envelope.FileStorageBackend(data_dir).put(
        "default", "db-dek", b"encrypted db dek"
    )

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml", compact=True
        )

    bundle_bytes = result.bundle_path.read_bytes()
    assert bundle_bytes[: len(backup_crypto.MAGIC_V3)] == backup_crypto.MAGIC_V3
    header = backup_crypto.parse_bundle_header_v3(bundle_bytes)
    assert header["mode"] == "compact"
    assert header["recovery_db_dek_present"] is False


def test_create_backup_portable_embeds_recovery_from_backend(
    tmp_path: Path, monkeypatch
) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    class FakeBackend:
        name = "fake"

        def __init__(self) -> None:
            self.get_calls: list[tuple[str, str]] = []

        def get(self, user_id: str, kind: str) -> bytes | None:
            self.get_calls.append((user_id, kind))
            return b"backend db dek bytes"

        def put(self, user_id: str, kind: str, blob: bytes) -> None:
            raise AssertionError("put should not be called during backup creation")

        def delete(self, user_id: str, kind: str) -> None:
            raise AssertionError("delete should not be called during backup creation")

    backend = FakeBackend()
    monkeypatch.setattr(
        crypto_envelope, "select_backend", lambda _user_id, _data_dir: backend
    )

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml", portable=True
        )

    assert backend.get_calls == [("default", "db-dek")]
    assert not (data_dir / "default" / "db-dek.enc").exists()
    bundle_bytes = result.bundle_path.read_bytes()
    assert bundle_bytes[: len(backup_crypto.MAGIC_V3)] == backup_crypto.MAGIC_V3
    tar_bytes, header = backup_crypto.decrypt_bundle_v3(bundle_bytes, "default")
    assert header["mode"] == "portable"
    assert header["recovery_db_dek_present"] is True
    with tarfile.open(fileobj=BytesIO(tar_bytes), mode="r:gz") as tar:
        recovery_file = tar.extractfile("recovery/db-dek.enc")
        assert recovery_file is not None
        assert recovery_file.read() == b"backend db dek bytes"


def test_create_backup_compact_does_not_embed_recovery(
    tmp_path: Path, monkeypatch
) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    crypto_envelope.FileStorageBackend(data_dir).put(
        "default", "db-dek", b"encrypted db dek"
    )

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml", compact=True
        )

    names = _read_bundle_names(result.bundle_path)
    header = backup_crypto.parse_bundle_header_v3(result.bundle_path.read_bytes())
    assert "recovery/db-dek.enc" not in names
    assert header["mode"] == "compact"
    assert header["recovery_db_dek_present"] is False


def test_create_backup_v3_populates_mode_columns(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    crypto_envelope.FileStorageBackend(data_dir).put(
        "default", "db-dek", b"encrypted db dek"
    )

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml", portable=True
        )
        row = conn.execute(
            """
            SELECT bundle_format_version, mode, recovery_db_dek_present
              FROM backup_log
             WHERE bundle_path = ? AND status = 'completed'
            """,
            (str(result.bundle_path),),
        ).fetchone()

    entries = _read_sidecar_entries(data_dir)
    completed = [
        entry
        for entry in entries
        if entry.get("bundle_path") == str(result.bundle_path)
    ]

    assert row["bundle_format_version"] == 3
    assert row["mode"] == "portable"
    assert row["recovery_db_dek_present"] == 1
    assert completed[-1]["mode"] == "portable"
    assert completed[-1]["recovery_db_dek_present"] is True


def test_create_backup_manifest_signed_v2(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml"
        )

    manifest = _read_manifest(result.bundle_path)
    assert manifest["schema_version"] == 2
    assert manifest["user_id"] == "default"
    assert manifest["signature"]["alg"] == "HMAC-SHA256"
    assert backup_crypto.verify_manifest(manifest) is True
    assert all("sha256" not in file_entry for file_entry in manifest["files"])


def test_create_backup_includes_all_files(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml"
        )

    names = _read_bundle_names(result.bundle_path)

    assert {
        "manifest.json",
        "finance.db",
        "rules.yaml",
        "agent_memory.md",
        "sessions/2026-03-10.md",
    }.issubset(names)


def test_create_backup_logs_to_sidecar(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml"
        )

    entries = _read_sidecar_entries(data_dir)
    assert any(
        entry.get("backup_type") == "local"
        and entry.get("status") == "completed"
        and entry.get("bundle_path") == str(result.bundle_path)
        and entry.get("bundle_format_version") == 2
        and entry.get("user_id") == "default"
        for entry in entries
    )


def test_create_backup_logs_to_db(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        result = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml"
        )
        row = conn.execute(
            """
            SELECT bundle_path, bundle_sha256, bundle_format_version,
                   dek_secret_ref, signing_key_secret_ref, bundle_id, user_id
              FROM backup_log
             WHERE backup_type = 'local' AND status = 'completed'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert row is not None
    assert row["bundle_path"] == str(result.bundle_path)
    assert row["bundle_sha256"] == result.bundle_sha256
    assert row["bundle_format_version"] == 2
    assert (
        row["dek_secret_ref"]
        == f"finance-cli-test/users/default/backup-keys/{row['bundle_id']}"
    )
    assert (
        row["signing_key_secret_ref"]
        == "finance-cli-test/users/default/backup-signing-key"
    )
    assert row["user_id"] == "default"


def test_list_backups_reads_sidecar(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    _append_sidecar_entry(
        data_dir,
        {
            "backup_type": "local",
            "status": "started",
            "bundle_path": "/tmp/old.tar.gz",
            "created_at": "2026-03-10T00:00:00Z",
        },
    )
    _append_sidecar_entry(
        data_dir,
        {
            "backup_type": "local",
            "status": "completed",
            "bundle_path": "/tmp/old.tar.gz",
            "bundle_size": 1,
            "created_at": "2026-03-10T00:00:01Z",
        },
    )
    _append_sidecar_entry(
        data_dir,
        {
            "backup_type": "local",
            "status": "completed",
            "bundle_path": "/tmp/new.tar.gz",
            "bundle_size": 1,
            "created_at": "2026-03-11T00:00:00Z",
        },
    )

    with connect(db_path) as conn:
        entries = list_backups(conn, data_dir=data_dir)

    assert [entry["bundle_path"] for entry in entries] == [
        "/tmp/new.tar.gz",
        "/tmp/old.tar.gz",
    ]


def test_list_backups_falls_back_to_db(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO backup_log (
                backup_type, status, bundle_path, bundle_sha256, bundle_size,
                db_sha256, migration_ver, duration_ms, created_at
            ) VALUES (
                'local', 'completed', '/tmp/db_only.tar.gz', 'sha', 12,
                'dbsha', 39, 1, '2026-03-12T00:00:00Z'
            )
            """
        )
        conn.commit()
        entries = list_backups(conn, data_dir=data_dir)

    assert len(entries) == 1
    assert entries[0]["bundle_path"] == "/tmp/db_only.tar.gz"


def test_verify_backup_valid(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        backup = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml"
        )
        result = verify_backup(backup.bundle_path, conn=conn, user_id="default")

    assert result.valid is True
    assert result.errors == []
    assert result.warnings == []
    assert result.manifest["schema_version"] == 2


def test_verify_plaintext_v3_backup_under_encryption_required(
    tmp_path: Path, monkeypatch
) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    crypto_envelope.FileStorageBackend(data_dir).put(
        "default", "db-dek", b"encrypted db dek"
    )

    with connect(db_path) as conn:
        backup = create_backup(
            conn,
            data_dir=data_dir,
            rules_path=data_dir / "rules.yaml",
            compact=True,
        )
        previous = db_module.set_db_encryption_mode_override("require")
        try:
            result = verify_backup(backup.bundle_path, conn=conn, user_id="default")
        finally:
            db_module.set_db_encryption_mode_override(previous)

    assert result.valid is True
    assert result.errors == []
    assert result.manifest["schema_version"] == 2
    assert result.manifest["sqlcipher_db"] is False


def test_verify_backup_corrupt(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        backup = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml"
        )

    tampered_bundle = tmp_path / "tampered.bundle"
    tampered_bytes = bytearray(backup.bundle_path.read_bytes())
    tampered_bytes[-17] ^= 0x01
    tampered_bundle.write_bytes(bytes(tampered_bytes))

    with connect(db_path) as conn:
        result = verify_backup(tampered_bundle, conn=conn, user_id="default")

    assert result.valid is False
    assert any("authentication failed" in error for error in result.errors)


def test_restore_dry_run(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir, backup, _, _, _ = _prepare_restore_fixture(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        result = restore_backup(
            backup.bundle_path,
            conn=conn,
            dry_run=True,
            data_dir=data_dir,
            rules_path=data_dir / "rules.yaml",
        )
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()[
            "n"
        ]

    assert result.restored is False
    assert result.dry_run is True
    assert txn_count == 2
    assert "MUTATED" in (data_dir / "rules.yaml").read_text(encoding="utf-8")


def test_restore_replaces_db(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir, backup, _, _, _ = _prepare_restore_fixture(tmp_path, monkeypatch)

    conn = connect(db_path)
    try:
        result = restore_backup(
            backup.bundle_path,
            conn=conn,
            dry_run=False,
            data_dir=data_dir,
            rules_path=data_dir / "rules.yaml",
        )
    finally:
        with suppress(Exception):
            conn.close()

    with connect(db_path) as restored_conn:
        rows = restored_conn.execute(
            "SELECT description FROM transactions ORDER BY description"
        ).fetchall()

    assert result.restored is True
    assert [row["description"] for row in rows] == ["Original transaction"]


def test_restore_blocks_on_active_subscriber_lock(tmp_path: Path, monkeypatch) -> None:
    _, _, backup, _, _, _ = _prepare_restore_fixture(tmp_path / "source", monkeypatch)
    canonical_data_dir = tmp_path / "install" / ".cashnerd" / "data"
    canonical_db_path = _setup_restore_target(canonical_data_dir)
    monkeypatch.setattr(
        backup_module, "canonical_install_db_path", lambda: canonical_db_path
    )

    with connect(canonical_db_path) as seed_conn:
        _seed_txn(seed_conn, "Canonical live transaction")
    before_bytes = canonical_db_path.read_bytes()

    lock = InstallSubscriberLock(backup_module.install_subscriber_lock_path())
    assert lock.try_acquire() is True

    conn = connect(canonical_db_path)
    try:
        with pytest.raises(SubscriberActiveError) as excinfo:
            restore_backup(
                backup.bundle_path,
                conn=conn,
                target_db_path=canonical_db_path,
                target_data_dir=canonical_data_dir,
                dry_run=False,
                data_dir=canonical_data_dir,
                rules_path=canonical_data_dir / "rules.yaml",
            )
    finally:
        lock.release()
        with suppress(Exception):
            conn.close()

    assert excinfo.value.user_message == (
        "Cannot restore: another CashNerd local MCP process is running. "
        "Stop it (e.g., close Claude Code or kill mcp_local) and retry."
    )
    assert canonical_db_path.read_bytes() == before_bytes


def test_restore_succeeds_when_lock_free(tmp_path: Path, monkeypatch) -> None:
    _, _, backup, _, _, _ = _prepare_restore_fixture(tmp_path / "source", monkeypatch)
    canonical_data_dir = tmp_path / "install" / ".cashnerd" / "data"
    canonical_db_path = _setup_restore_target(canonical_data_dir)
    monkeypatch.setattr(
        backup_module, "canonical_install_db_path", lambda: canonical_db_path
    )

    with connect(canonical_db_path) as seed_conn:
        _seed_txn(seed_conn, "Canonical live transaction")

    conn = connect(canonical_db_path)
    try:
        result = restore_backup(
            backup.bundle_path,
            conn=conn,
            target_db_path=canonical_db_path,
            target_data_dir=canonical_data_dir,
            dry_run=False,
            data_dir=canonical_data_dir,
            rules_path=canonical_data_dir / "rules.yaml",
        )
    finally:
        with suppress(Exception):
            conn.close()

    with connect(canonical_db_path) as restored_conn:
        rows = restored_conn.execute(
            "SELECT description FROM transactions ORDER BY description"
        ).fetchall()

    assert result.restored is True
    assert [row["description"] for row in rows] == ["Original transaction"]


def test_restore_against_scratch_db_skips_lock_check(
    tmp_path: Path, monkeypatch
) -> None:
    db_path, data_dir, backup, _, _, _ = _prepare_restore_fixture(tmp_path, monkeypatch)
    canonical_db_path = tmp_path / "install" / ".cashnerd" / "data" / "finance.db"
    monkeypatch.setattr(
        backup_module, "canonical_install_db_path", lambda: canonical_db_path
    )

    lock = InstallSubscriberLock(backup_module.install_subscriber_lock_path())
    assert lock.try_acquire() is True

    conn = connect(db_path)
    try:
        result = restore_backup(
            backup.bundle_path,
            conn=conn,
            dry_run=False,
            data_dir=data_dir,
            rules_path=data_dir / "rules.yaml",
        )
    finally:
        lock.release()
        with suppress(Exception):
            conn.close()

    with connect(db_path) as restored_conn:
        rows = restored_conn.execute(
            "SELECT description FROM transactions ORDER BY description"
        ).fetchall()

    assert result.restored is True
    assert [row["description"] for row in rows] == ["Original transaction"]


def test_restore_creates_pre_restore_backup(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir, backup, _, _, _ = _prepare_restore_fixture(tmp_path, monkeypatch)

    conn = connect(db_path)
    try:
        restore_backup(
            backup.bundle_path,
            conn=conn,
            dry_run=False,
            data_dir=data_dir,
            rules_path=data_dir / "rules.yaml",
        )
    finally:
        with suppress(Exception):
            conn.close()

    entries = _read_sidecar_entries(data_dir)
    pre_restore_entries = [
        entry
        for entry in entries
        if entry.get("backup_type") == "pre_restore"
        and entry.get("status") == "completed"
    ]
    assert pre_restore_entries
    assert Path(pre_restore_entries[-1]["bundle_path"]).exists()


def test_restore_replays_audit_entries(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir, backup, _, _, _ = _prepare_restore_fixture(tmp_path, monkeypatch)

    conn = connect(db_path)
    try:
        restore_backup(
            backup.bundle_path,
            conn=conn,
            dry_run=False,
            data_dir=data_dir,
            rules_path=data_dir / "rules.yaml",
        )
    finally:
        with suppress(Exception):
            conn.close()

    with connect(db_path) as restored_conn:
        rows = restored_conn.execute(
            """
            SELECT backup_type
              FROM backup_log
             WHERE status = 'completed' AND backup_type IN ('pre_restore', 'restore')
             ORDER BY id
            """
        ).fetchall()

    assert {row["backup_type"] for row in rows} == {"pre_restore", "restore"}


def test_restore_copies_config_files(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir, backup, original_rules, original_memory, original_session = (
        _prepare_restore_fixture(
            tmp_path,
            monkeypatch,
        )
    )

    conn = connect(db_path)
    try:
        restore_backup(
            backup.bundle_path,
            conn=conn,
            dry_run=False,
            data_dir=data_dir,
            rules_path=data_dir / "rules.yaml",
        )
    finally:
        with suppress(Exception):
            conn.close()

    assert (data_dir / "rules.yaml").read_text(encoding="utf-8") == original_rules
    assert (data_dir / "agent_memory.md").read_text(encoding="utf-8") == original_memory
    assert (data_dir / "sessions" / "2026-03-10.md").read_text(
        encoding="utf-8"
    ) == original_session


def test_cross_user_restore_rejected(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        _seed_txn(conn, "Original transaction")
        backup = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml", user_id="alice"
        )

    conn = connect(db_path)
    try:
        with pytest.raises(TenantMismatchError) as excinfo:
            restore_backup(
                backup.bundle_path,
                conn=conn,
                dry_run=False,
                data_dir=data_dir,
                rules_path=data_dir / "rules.yaml",
                expected_user_id="bob",
                user_id="bob",
            )
    finally:
        with suppress(Exception):
            conn.close()

    assert excinfo.value.actual_user_id == "alice"


def test_legacy_v1_bundle_restores(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    legacy_bundle = tmp_path / "legacy_backup.tar.gz"

    with connect(db_path) as conn:
        _seed_txn(conn, "Original transaction")

    _build_legacy_bundle(legacy_bundle, db_path, data_dir)

    with connect(db_path) as conn:
        _seed_txn(conn, "Mutated transaction")

    conn = connect(db_path)
    try:
        result = restore_backup(
            legacy_bundle,
            conn=conn,
            dry_run=False,
            data_dir=data_dir,
            rules_path=data_dir / "rules.yaml",
        )
    finally:
        with suppress(Exception):
            conn.close()

    with connect(db_path) as restored_conn:
        rows = restored_conn.execute(
            "SELECT description FROM transactions ORDER BY description"
        ).fetchall()

    assert result.restored is True
    assert [row["description"] for row in rows] == ["Original transaction"]


def test_prune_tiered_retention(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    today = datetime.now(timezone.utc).date()
    days_since_sunday = (today.weekday() + 1) % 7
    weekly_sunday = today - timedelta(days=days_since_sunday + 14)
    monthly_base = (today.replace(day=1) - timedelta(days=40)).replace(day=1)

    with connect(db_path) as conn:
        keep_daily = data_dir / "backups" / "keep_daily.tar.gz"
        drop_daily = data_dir / "backups" / "drop_daily.tar.gz"
        keep_weekly = data_dir / "backups" / "keep_weekly.tar.gz"
        drop_weekly = data_dir / "backups" / "drop_weekly.tar.gz"
        keep_monthly = data_dir / "backups" / "keep_monthly.tar.gz"
        drop_monthly = data_dir / "backups" / "drop_monthly.tar.gz"
        drop_old = data_dir / "backups" / "drop_old.tar.gz"

        _record_backup_entry(
            conn,
            data_dir,
            keep_daily,
            created_at=_created_at_for(today - timedelta(days=1), 12),
        )
        _record_backup_entry(
            conn,
            data_dir,
            drop_daily,
            created_at=_created_at_for(today - timedelta(days=1), 8),
        )
        _record_backup_entry(
            conn,
            data_dir,
            keep_weekly,
            created_at=_created_at_for(weekly_sunday + timedelta(days=2), 12),
        )
        _record_backup_entry(
            conn, data_dir, drop_weekly, created_at=_created_at_for(weekly_sunday, 8)
        )
        _record_backup_entry(
            conn,
            data_dir,
            keep_monthly,
            created_at=_created_at_for(monthly_base + timedelta(days=10), 12),
        )
        _record_backup_entry(
            conn,
            data_dir,
            drop_monthly,
            created_at=_created_at_for(monthly_base + timedelta(days=5), 8),
        )
        _record_backup_entry(
            conn,
            data_dir,
            drop_old,
            created_at=_created_at_for(today - timedelta(days=400), 8),
        )

        result = prune_backups(conn, dry_run=False, data_dir=data_dir)

    assert result.deleted == 4
    assert keep_daily.exists()
    assert keep_weekly.exists()
    assert keep_monthly.exists()
    assert not drop_daily.exists()
    assert not drop_weekly.exists()
    assert not drop_monthly.exists()
    assert not drop_old.exists()

    entries = _read_sidecar_entries(data_dir)
    prune_events = [entry for entry in entries if entry.get("action") == "pruned"]
    assert len(prune_events) == 4


def test_prune_dry_run(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    old_day = datetime.now(timezone.utc).date() - timedelta(days=400)

    with connect(db_path) as conn:
        old_bundle = data_dir / "backups" / "dry_run_old.tar.gz"
        _record_backup_entry(
            conn, data_dir, old_bundle, created_at=_created_at_for(old_day, 8)
        )
        result = prune_backups(conn, dry_run=True, data_dir=data_dir)

    assert result.deleted == 1
    assert old_bundle.exists()
    assert not any(
        entry.get("action") == "pruned" for entry in _read_sidecar_entries(data_dir)
    )


def test_prune_cleans_v2_and_v3_in_one_pass(tmp_path: Path, monkeypatch) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)
    old_day = datetime.now(timezone.utc).date() - timedelta(days=400)
    deleted_keys: list[tuple[str, str]] = []
    monkeypatch.setattr(
        backup_crypto,
        "delete_bundle_key",
        lambda user_id, bundle_id: deleted_keys.append((user_id, bundle_id)),
    )

    with connect(db_path) as conn:
        v2_bundle_id = str(uuid.uuid4())
        v3_bundle_id = str(uuid.uuid4())
        v2_bundle = data_dir / "backups" / "old_v2.bundle"
        v3_bundle = data_dir / "backups" / "old_v3.bundle"
        _record_backup_entry(
            conn,
            data_dir,
            v2_bundle,
            created_at=_created_at_for(old_day, 8),
            bundle_format_version=2,
            bundle_id=v2_bundle_id,
            user_id="default",
        )
        _record_backup_entry(
            conn,
            data_dir,
            v3_bundle,
            created_at=_created_at_for(old_day, 9),
            bundle_format_version=3,
            bundle_id=v3_bundle_id,
            user_id="default",
        )
        result = prune_backups(
            conn, dry_run=False, data_dir=data_dir, user_id="default"
        )

    assert result.deleted == 2
    assert not v2_bundle.exists()
    assert not v3_bundle.exists()
    assert deleted_keys == [("default", v2_bundle_id)]
    assert result.scheduled_key_deletions == 1


def test_restore_cli_dispatch(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path, data_dir = _setup(tmp_path, monkeypatch)

    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO tenant_marker (singleton, user_id) VALUES (1, 'default')"
        )
        conn.commit()
        backup = create_backup(
            conn, data_dir=data_dir, rules_path=data_dir / "rules.yaml"
        )

    code = main(
        [
            "db",
            "restore",
            "--file",
            str(backup.bundle_path),
            "--yes",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "db.restore"
