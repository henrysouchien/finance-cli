from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from finance_cli.db import connect, initialize_database
from finance_cli.scripts import backup_prune_job
from finance_cli.user_provisioning import user_db_path


def _init_user_db(data_root: Path, user_id: str) -> Path:
    db_path = user_db_path(data_root, user_id)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    initialize_database(db_path)
    with connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO tenant_marker (singleton, user_id) VALUES (1, ?)",
            (user_id,),
        )
        conn.commit()
    return db_path


def _created_at(days_ago: int) -> str:
    created = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return created.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _append_sidecar_entry(data_dir: Path, entry: dict) -> None:
    sidecar = data_dir / "backups" / "backup_audit.jsonl"
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    with sidecar.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


def _record_backup_entry(data_root: Path, user_id: str, bundle_name: str, *, days_ago: int = 400) -> Path:
    data_dir = user_db_path(data_root, user_id).parent
    bundle_path = data_dir / "backups" / bundle_name
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_bytes(bundle_name.encode("utf-8"))
    entry = {
        "backup_type": "local",
        "status": "completed",
        "bundle_path": str(bundle_path),
        "bundle_sha256": uuid.uuid4().hex,
        "bundle_size": bundle_path.stat().st_size,
        "db_sha256": uuid.uuid4().hex,
        "migration_ver": 60,
        "duration_ms": 1,
        "bundle_format_version": 3,
        "dek_secret_ref": None,
        "signing_key_secret_ref": None,
        "bundle_id": str(uuid.uuid4()),
        "user_id": user_id,
        "created_at": _created_at(days_ago),
    }
    _append_sidecar_entry(data_dir, entry)
    with connect(user_db_path(data_root, user_id), expected_user_id=user_id) as conn:
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
    return bundle_path


def test_backup_prune_job_uses_per_user_paths_not_cli_default(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    _init_user_db(data_root, "1")
    default_db = tmp_path / "default-cli" / "finance.db"
    calls: list[tuple[Path, str]] = []

    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))
    monkeypatch.setenv("FINANCE_CLI_DB", str(default_db))

    def fake_prune_backups(_conn, *, dry_run: bool, data_dir: Path, user_id: str):
        calls.append((Path(data_dir), user_id))
        return SimpleNamespace(
            dry_run=dry_run,
            kept=1,
            deleted=0,
            freed_bytes=0,
            scheduled_key_deletions=0,
        )

    monkeypatch.setattr(backup_prune_job, "prune_backups", fake_prune_backups)

    summary = backup_prune_job.run_backup_prune(
        settings=backup_prune_job.load_settings(),
        dry_run=True,
    )

    assert summary.processed_users == 1
    assert calls == [(data_root / "1", "1")]
    assert not default_db.exists()


def test_backup_prune_job_prunes_multiple_users(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    for user_id in ("1", "2"):
        _init_user_db(data_root, user_id)
    old_one = _record_backup_entry(data_root, "1", "old-user-1.bundle")
    old_two = _record_backup_entry(data_root, "2", "old-user-2.bundle")

    summary = backup_prune_job.run_backup_prune(
        settings=backup_prune_job.BackupPruneSettings(data_root=data_root),
        dry_run=False,
    )

    assert summary.user_count == 2
    assert summary.processed_users == 2
    assert summary.error_users == 0
    assert summary.deleted == 2
    assert not old_one.exists()
    assert not old_two.exists()


def test_backup_prune_job_missing_single_user_db_is_skipped(tmp_path: Path) -> None:
    data_root = tmp_path / "users"

    summary = backup_prune_job.run_backup_prune(
        settings=backup_prune_job.BackupPruneSettings(data_root=data_root),
        dry_run=True,
        user_id="missing",
    )

    assert summary.user_count == 1
    assert summary.processed_users == 0
    assert summary.skipped_users == 1
    assert summary.error_users == 0
