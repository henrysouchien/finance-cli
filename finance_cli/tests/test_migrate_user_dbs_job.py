from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from finance_cli.db import SCHEMA_VERSION, connect, initialize_database
from finance_cli.scripts import migrate_user_dbs_job
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


def test_user_db_migration_job_applies_pending_migration(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    db_path = _init_user_db(data_root, "1")
    with connect(db_path, expected_user_id="1") as conn:
        conn.execute("DROP TABLE telegram_link_attempts")
        conn.execute("DELETE FROM schema_version WHERE version = 62")
        conn.commit()

    summary = migrate_user_dbs_job.run_user_db_migrations(
        settings=migrate_user_dbs_job.UserDbMigrationSettings(data_root=data_root),
    )

    assert summary.user_count == 1
    assert summary.processed_users == 1
    assert summary.migrated_users == 1
    assert summary.error_users == 0
    with connect(db_path, expected_user_id="1") as conn:
        version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'telegram_link_attempts'"
        ).fetchone()
        marker = conn.execute("SELECT user_id FROM tenant_marker WHERE singleton = 1").fetchone()[0]
    assert version == SCHEMA_VERSION
    assert table is not None
    assert marker == "1"


def test_user_db_migration_job_uses_per_user_paths_not_cli_default(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    _init_user_db(data_root, "1")
    default_db = tmp_path / "default-cli" / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(default_db))
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(data_root))

    summary = migrate_user_dbs_job.run_user_db_migrations(
        settings=migrate_user_dbs_job.load_settings(),
    )

    assert summary.processed_users == 1
    assert summary.error_users == 0
    assert not default_db.exists()


def test_user_db_migration_job_missing_single_user_db_is_skipped(tmp_path: Path) -> None:
    data_root = tmp_path / "users"

    summary = migrate_user_dbs_job.run_user_db_migrations(
        settings=migrate_user_dbs_job.UserDbMigrationSettings(data_root=data_root),
        user_id="missing",
    )

    assert summary.user_count == 1
    assert summary.processed_users == 0
    assert summary.skipped_users == 1
    assert summary.error_users == 0


def test_user_db_migration_job_routes_remote_users_without_local_file(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    settings = migrate_user_dbs_job.UserDbMigrationSettings(
        data_root=data_root,
        database_url="postgres://example/db",
    )
    migrated: list[tuple[Path, str, str | None, object]] = []

    @contextmanager
    def remote_acquire(*_args, **_kwargs):
        yield SimpleNamespace(storage_mode="remote")

    def fake_migrate_user_database(*, data_root, user_id, expected_user_id=None, storage_session_manager):
        migrated.append((data_root, user_id, expected_user_id, storage_session_manager))
        return migrate_user_dbs_job.UserMigrationResult(
            user_id=user_id,
            db_path=migrate_user_dbs_job.user_db_path(data_root, user_id),
            before_version=66,
            after_version=SCHEMA_VERSION,
            migrated=True,
        )

    monkeypatch.setattr(
        migrate_user_dbs_job,
        "_fetch_pg_user_records",
        lambda _settings, _user_id=None: [{"user_id": "42", "storage_mode": "remote"}],
    )
    monkeypatch.setattr(migrate_user_dbs_job, "_default_session_manager", lambda: object())
    monkeypatch.setattr(migrate_user_dbs_job.LeaseScope, "acquire", remote_acquire)
    monkeypatch.setattr(migrate_user_dbs_job, "migrate_user_database", fake_migrate_user_database)

    summary = migrate_user_dbs_job.run_user_db_migrations(settings=settings)

    assert len(migrated) == 1
    assert migrated[0][0] == data_root
    assert migrated[0][1] == "42"
    assert migrated[0][2] == "42"
    assert migrated[0][3] is not None
    assert not (data_root / "42" / "finance.db").exists()
    assert summary.user_count == 1
    assert summary.processed_users == 1
    assert summary.migrated_users == 1
    assert summary.skipped_users == 0
    assert summary.error_users == 0


def test_user_db_migration_job_reports_missing_pg_local_db(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    settings = migrate_user_dbs_job.UserDbMigrationSettings(
        data_root=data_root,
        database_url="postgres://example/db",
    )

    monkeypatch.setattr(
        migrate_user_dbs_job,
        "_fetch_pg_user_records",
        lambda _settings, _user_id=None: [{"user_id": "42", "storage_mode": "local"}],
    )

    summary = migrate_user_dbs_job.run_user_db_migrations(settings=settings)

    assert summary.user_count == 1
    assert summary.processed_users == 0
    assert summary.skipped_users == 1
    assert summary.error_users == 0


def test_user_db_migration_job_skips_queued_storage_mode(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    _init_user_db(data_root, "1")

    @contextmanager
    def queued_acquire(*_args, **_kwargs):
        yield migrate_user_dbs_job.Queued(storage_mode="migrating")

    monkeypatch.setattr(migrate_user_dbs_job.LeaseScope, "acquire", queued_acquire)

    summary = migrate_user_dbs_job.run_user_db_migrations(
        settings=migrate_user_dbs_job.UserDbMigrationSettings(data_root=data_root),
        storage_session_manager=object(),
    )

    assert summary.user_count == 1
    assert summary.processed_users == 0
    assert summary.skipped_users == 1
    assert summary.error_users == 0
