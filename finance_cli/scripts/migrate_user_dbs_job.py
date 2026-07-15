"""Apply pending finance_cli SQLite migrations to all per-user web databases."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from finance_cli.db import SCHEMA_VERSION, connect, initialize_database
from finance_cli.storage_lease import (
    LeaseScope,
    LeaseUnavailableError,
    Queued,
    lease_enforcement_enabled,
)
from finance_cli.user_provisioning import ensure_tenant_marker, user_db_path


@dataclass(frozen=True)
class UserDbMigrationSettings:
    data_root: Path
    database_url: str = ""


@dataclass(frozen=True)
class UserMigrationResult:
    user_id: str
    db_path: Path
    before_version: int | None
    after_version: int | None
    migrated: bool
    skipped: bool = False
    skip_reason: str | None = None


@dataclass
class UserDbMigrationSummary:
    target_schema_version: int = SCHEMA_VERSION
    user_count: int = 0
    processed_users: int = 0
    migrated_users: int = 0
    skipped_users: int = 0
    error_users: int = 0


def _json_log(event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))


def default_data_root() -> Path:
    raw_root = os.getenv("FINANCE_WEB_DATA_ROOT") or os.getenv("FINANCE_GATEWAY_DATA_ROOT")
    if raw_root:
        return Path(raw_root).expanduser().resolve()
    return (Path(__file__).resolve().parents[2] / "finance-web" / "data" / "users").resolve()


def load_settings(*, data_root: Path | None = None) -> UserDbMigrationSettings:
    return UserDbMigrationSettings(
        data_root=(data_root or default_data_root()).expanduser().resolve(),
        database_url=str(os.getenv("DATABASE_URL") or "").strip(),
    )


def iter_user_ids(data_root: Path, only_user_id: str | None = None) -> Iterable[str]:
    if only_user_id is not None:
        yield str(only_user_id)
        return
    if not data_root.exists():
        return
    for child in sorted(data_root.iterdir()):
        if child.name.startswith(".") or not child.is_dir():
            continue
        if not (child / "finance.db").exists():
            continue
        yield child.name


def _postgres_connect(database_url: str):
    if not database_url:
        raise ValueError("DATABASE_URL is required to enumerate finance-web users")
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise ValueError("psycopg2 is required to enumerate finance-web users") from exc
    return psycopg2.connect(database_url, cursor_factory=psycopg2.extras.RealDictCursor)


def _fetch_pg_user_records(settings: UserDbMigrationSettings, only_user_id: str | None = None) -> list[dict[str, Any]]:
    with _postgres_connect(settings.database_url) as pg_conn:
        cursor = pg_conn.cursor()
        if only_user_id is not None:
            cursor.execute(
                """
                SELECT id, storage_mode
                  FROM users
                 WHERE id = %s
                   AND deleted_at IS NULL
                """,
                (only_user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT id, storage_mode
                  FROM users
                 WHERE deleted_at IS NULL
                 ORDER BY id
                """
            )
        rows = cursor.fetchall()
    return [
        {
            "user_id": str(row["id"]),
            "storage_mode": str(row.get("storage_mode") or "local").strip().lower() or "local",
        }
        for row in rows
    ]


def iter_user_records(
    settings: UserDbMigrationSettings,
    only_user_id: str | None = None,
) -> Iterable[dict[str, str]]:
    if settings.database_url:
        yield from _fetch_pg_user_records(settings, only_user_id)
        return

    for user_id in iter_user_ids(settings.data_root, only_user_id):
        yield {"user_id": str(user_id), "storage_mode": "local"}


def _default_session_manager():
    if not str(os.getenv("DATABASE_URL") or "").strip():
        return None
    try:
        from app_platform.db.session import SessionManager
    except Exception:
        return None
    try:
        return SessionManager._get_default_manager()
    except Exception:
        return None


def _schema_versions(
    db_path: Path,
    *,
    expected_user_id: str | None = None,
    storage_session_manager=None,
) -> set[int]:
    with connect(
        db_path=db_path,
        expected_user_id=expected_user_id,
        storage_session_manager=storage_session_manager,
    ) as conn:
        try:
            rows = conn.execute("SELECT version FROM schema_version").fetchall()
        except Exception:
            return set()
    return {int(row["version"] if hasattr(row, "keys") else row[0]) for row in rows}


def _schema_version(
    db_path: Path,
    *,
    expected_user_id: str | None = None,
    storage_session_manager=None,
) -> int:
    versions = _schema_versions(
        db_path,
        expected_user_id=expected_user_id,
        storage_session_manager=storage_session_manager,
    )
    return max(versions) if versions else 0


def migrate_user_database(
    *,
    data_root: Path,
    user_id: str,
    expected_user_id: str | None = None,
    storage_session_manager=None,
) -> UserMigrationResult:
    db_path = user_db_path(data_root, user_id)
    tenant_guard_user_id = str(expected_user_id) if expected_user_id is not None else None
    before_versions = _schema_versions(
        db_path,
        expected_user_id=tenant_guard_user_id,
        storage_session_manager=storage_session_manager,
    )
    before_version = max(before_versions) if before_versions else 0
    initialize_database(
        db_path,
        expected_user_id=tenant_guard_user_id,
        storage_session_manager=storage_session_manager,
    )
    ensure_tenant_marker(
        data_root=data_root,
        user_id=user_id,
        storage_session_manager=storage_session_manager,
    )
    after_versions = _schema_versions(
        db_path,
        expected_user_id=tenant_guard_user_id,
        storage_session_manager=storage_session_manager,
    )
    after_version = max(after_versions) if after_versions else 0
    return UserMigrationResult(
        user_id=user_id,
        db_path=db_path,
        before_version=before_version,
        after_version=after_version,
        migrated=after_versions != before_versions,
    )


def _migrate_user_with_optional_lease(
    *,
    data_root: Path,
    user_id: str,
    storage_mode: str | None,
    storage_session_manager,
) -> UserMigrationResult:
    db_path = user_db_path(data_root, user_id)
    normalized_storage_mode = str(storage_mode or "local").strip().lower() or "local"
    if normalized_storage_mode == "local" and not db_path.exists():
        return UserMigrationResult(
            user_id=user_id,
            db_path=db_path,
            before_version=None,
            after_version=None,
            migrated=False,
            skipped=True,
            skip_reason="missing_finance_db",
        )

    manager = storage_session_manager if storage_session_manager is not None else _default_session_manager()
    if normalized_storage_mode == "remote" and manager is None:
        raise LeaseUnavailableError("session_manager_unavailable")
    if manager is None:
        if lease_enforcement_enabled():
            raise LeaseUnavailableError("session_manager_unavailable")
            return migrate_user_database(
                data_root=data_root,
                user_id=user_id,
                expected_user_id=user_id if normalized_storage_mode == "remote" else None,
                storage_session_manager=None,
            )

    try:
        with LeaseScope.acquire(
            user_id,
            session_manager=manager,
            operation="user_db_migration",
            metadata={"source": "migrate_user_dbs_job"},
        ) as scope:
            if isinstance(scope, Queued):
                return UserMigrationResult(
                    user_id=user_id,
                    db_path=db_path,
                    before_version=None,
                    after_version=None,
                    migrated=False,
                    skipped=True,
                    skip_reason=f"storage_mode_{scope.storage_mode}",
                )
            return migrate_user_database(
                data_root=data_root,
                user_id=user_id,
                expected_user_id=user_id if normalized_storage_mode == "remote" else None,
                storage_session_manager=manager,
            )
    except LeaseUnavailableError:
        if lease_enforcement_enabled():
            raise
        return migrate_user_database(
            data_root=data_root,
            user_id=user_id,
            expected_user_id=user_id if normalized_storage_mode == "remote" else None,
            storage_session_manager=manager,
        )


def run_user_db_migrations(
    *,
    settings: UserDbMigrationSettings,
    user_id: str | None = None,
    storage_session_manager=None,
) -> UserDbMigrationSummary:
    user_records = list(iter_user_records(settings, user_id))
    summary = UserDbMigrationSummary(user_count=len(user_records))
    started = datetime.now(timezone.utc)
    _json_log(
        "user_db_migration_start",
        data_root=str(settings.data_root),
        target_schema_version=summary.target_schema_version,
        user=user_id,
        user_count=summary.user_count,
        started_at=started.isoformat(),
    )

    for record in user_records:
        current_user_id = str(record.get("user_id") or "")
        storage_mode = str(record.get("storage_mode") or "local").strip().lower() or "local"
        try:
            result = _migrate_user_with_optional_lease(
                data_root=settings.data_root,
                user_id=current_user_id,
                storage_mode=storage_mode,
                storage_session_manager=storage_session_manager,
            )
        except Exception as exc:
            summary.error_users += 1
            _json_log(
                "user_db_migration_user_error",
                user_id=current_user_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            continue

        if result.skipped:
            summary.skipped_users += 1
            _json_log(
                "user_db_migration_user_skipped",
                user_id=result.user_id,
                db_path=str(result.db_path),
                reason=result.skip_reason,
            )
            continue

        summary.processed_users += 1
        if result.migrated:
            summary.migrated_users += 1
        _json_log(
            "user_db_migration_user_complete",
            user_id=result.user_id,
            db_path=str(result.db_path),
            before_version=result.before_version,
            after_version=result.after_version,
            migrated=result.migrated,
        )

    finished = datetime.now(timezone.utc)
    _json_log(
        "user_db_migration_finish",
        target_schema_version=summary.target_schema_version,
        user_count=summary.user_count,
        processed_users=summary.processed_users,
        migrated_users=summary.migrated_users,
        skipped_users=summary.skipped_users,
        error_users=summary.error_users,
        finished_at=finished.isoformat(),
        duration_ms=int((finished - started).total_seconds() * 1000),
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user", help="migrate a single finance-web user id")
    parser.add_argument("--data-root", type=Path, default=None, help="per-user SQLite data root")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_user_db_migrations(
        settings=load_settings(data_root=args.data_root),
        user_id=args.user,
    )
    return 1 if summary.error_users else 0


if __name__ == "__main__":
    raise SystemExit(main())
